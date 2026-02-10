with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                else init_commission_after+nvl(ext_plat_certificate,0) end as final_commission_after
            ,CAST(a.init_commission_after AS DOUBLE) + COALESCE(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN COALESCE(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + COALESCE(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,user_info as (  --- 用户维表
  select user_id,
        gender,
        city_name,
        prov_name,
        city_level,
        birth_year_month
        ,CASE 
            WHEN birth_year_month IS NULL THEN '未知'
            ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
        END AS age
  from pub.dim_user_profile_nd
)
,platform_new as (--- 判定平台新
    select distinct dt,
                    user_pk,
                    user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-01-01'  
        
        and dict_type = 'pncl_wl_username'
)
,user_xhs as --小红书 宽口径
(
    select distinct uv.order_date
                   , mdd
                   , uv.user_name
                   , '小红书' as `渠道`
                   , 1  as user_number
    from q_order uv
    left join(
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '2022-12-01'
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(order_date, 7)
       and red.flow_dt <= uv.order_date
       and red.user_name is not null
)
,order_data as (
    select t1.order_date
        ,t1.user_id
        ,case
            when (t1.user_type = '新客' and t2.user_id is not null) then '平台新业务新'
            when t1.user_type = '新客' then '平台老业务新'
            else '老客'
        end as user_type
        ,city_name
        ,gender
        ,age
        ,sum(yj) yj,sum(final_commission_after)final_commission_after
    from q_order t1 
    left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk 
    left join user_info t3 on t1.user_id=t3.user_id
    --- 小红书渠道
    left join user_xhs t5 on t1.order_date=t5.order_date and t1.user_name=t5.user_name
    where t5.user_name is not null 
    group by 1,2,3,4,5,6
)



select order_date
,user_type,city_name,gender,age
        ,uv
        ,yj0
        ,yj1
        ,yj2
        ,yj3
        ,yj4
        ,yj5
        ,yj6
        ,yj0 / uv   `当日单客收益`
        ,yj1 / uv   `1日单客收益`
        ,yj2 / uv    `2日单客收益`
        ,yj3 / uv    `3日单客收益`
        ,yj4 / uv    `4日单客收益`
        ,yj5 / uv    `5日单客收益`
        ,yj6 / uv     `6日单客收益`
        ,yj7 / uv     `7日单客收益`
        ,yj180 / uv   `180日单客收益`
from (
    select t1.order_date,t1.user_type,t1.city_name,t1.gender,t1.age
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0 then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1 then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2 then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3 then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4 then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5 then final_commission_after end)  yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6 then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7 then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) yj180
    from (
        select order_date,user_type,gender,age,user_id,city_name
        from order_data
    ) t1 left join order_data t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2,3,4,5
) 
;




with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                else init_commission_after+nvl(ext_plat_certificate,0) end as final_commission_after
            ,CAST(a.init_commission_after AS DOUBLE) + COALESCE(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN COALESCE(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + COALESCE(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2025-09-01' and order_date <= date_sub(current_date,1)
)
,user_info as (  --- 用户维表
  select user_id,
        gender,
        city_name,
        prov_name,
        city_level,
        birth_year_month
        ,CASE 
            WHEN birth_year_month IS NULL THEN '未知'
            ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
        END AS age
  from pub.dim_user_profile_nd
)
,platform_new as (--- 判定平台新
    select distinct dt,
                    user_pk,
                    user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-01-01'  
        
        and dict_type = 'pncl_wl_username'
)
,order_data as (
    select t1.order_date
        ,t1.user_id
        ,case
            when (t1.user_type = '新客' and t2.user_id is not null) then '平台新业务新'
            when t1.user_type = '新客' then '平台老业务新'
            else '老客'
        end as user_type
        ,city_name
        ,gender
        ,age
        ,sum(yj) yj,sum(final_commission_after)final_commission_after
    from q_order t1 
    left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk 
    left join user_info t3 on t1.user_id=t3.user_id
    group by 1,2,3,4,5,6
)



select order_date
,user_type,city_name,gender,age
        ,uv
        ,yj0
        ,yj1
        ,yj2
        ,yj3
        ,yj4
        ,yj5
        ,yj6
        ,yj0 / uv   `当日单客收益`
        ,yj1 / uv   `1日单客收益`
        ,yj2 / uv    `2日单客收益`
        ,yj3 / uv    `3日单客收益`
        ,yj4 / uv    `4日单客收益`
        ,yj5 / uv    `5日单客收益`
        ,yj6 / uv     `6日单客收益`
        ,yj7 / uv     `7日单客收益`
        ,yj180 / uv   `180日单客收益`
from (
    select t1.order_date,t1.user_type,t1.city_name,t1.gender,t1.age
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0 then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1 then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2 then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3 then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4 then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5 then final_commission_after end)  yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6 then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7 then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) yj180
    from (
        select order_date,user_type,gender,age,user_id,city_name
        from order_data
    ) t1 left join order_data t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2,3,4,5
) 

;