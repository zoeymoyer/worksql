---- 1、分国家新客ARPU值变化
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
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
            ,ROW_NUMBER() OVER (PARTITION BY order_date,a.user_id ORDER BY order_time) as order_rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date,1)
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-01-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)


select order_date
        ,mdd
        ,uv
        ,yj0
        ,yj1
        ,yj2
        ,yj3
        ,yj4
        ,yj5
        ,yj6
        ,yj7
        ,yj30
        ,yj180
        ,yj0 / uv    ARPU0
        ,yj1 / uv    ARPU1
        ,yj2 / uv    ARPU2
        ,yj3 / uv    ARPU3
        ,yj4 / uv    ARPU4
        ,yj5 / uv    ARPU5
        ,yj6 / uv    ARPU6
        ,yj7 / uv    ARPU7
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
from (
    select t1.order_date,t1.mdd
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=1   then sum(case when datediff(t2.order_date, t1.order_date) <= 1   then final_commission_after end) else null end yj1
        ,case when max(datediff(t2.order_date,t1.order_date))>=2   then sum(case when datediff(t2.order_date, t1.order_date) <= 2   then final_commission_after end) else null end yj2
        ,case when max(datediff(t2.order_date,t1.order_date))>=3   then sum(case when datediff(t2.order_date, t1.order_date) <= 3   then final_commission_after end) else null end yj3
        ,case when max(datediff(t2.order_date,t1.order_date))>=4   then sum(case when datediff(t2.order_date, t1.order_date) <= 4   then final_commission_after end) else null end yj4
        ,case when max(datediff(t2.order_date,t1.order_date))>=5   then sum(case when datediff(t2.order_date, t1.order_date) <= 5   then final_commission_after end) else null end yj5
        ,case when max(datediff(t2.order_date,t1.order_date))>=6   then sum(case when datediff(t2.order_date, t1.order_date) <= 6   then final_commission_after end) else null end yj6
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then final_commission_after end) else null end yj7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180
    from (
        select distinct t1.order_date,t1.user_id,t1.user_type,t1.mdd
            ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
        from q_order t1 
        left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk
        where t1.user_type = '新客'  
        -- and order_rn=1
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2
) 
order by order_date 
;


---新客占比
with user_type as
(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
            --            when e.area in ('欧洲','亚太','美洲') then e.area
            --            else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)

,q_uv_info as
(   ---- 流量汇总
    select dt
        ,if(grouping(mdd)=1 ,'ALL' ,mdd) as  mdd
        ,if(grouping(user_type)=1 ,'ALL' ,user_type) as  user_type
        ,count(user_id)   uv
    from uv
    group by dt,cube(user_type, mdd)
) 

,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,ROW_NUMBER() OVER (PARTITION BY order_date,a.user_id ORDER BY order_time) as order_rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order as (----订单明细表包含取消  分目的地、新老维度 
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,ROW_NUMBER() OVER (PARTITION BY order_date,a.user_id ORDER BY order_time) as order_rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info_app as ( --- q app 订单汇总
    select t1.order_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金_app`
         ,sum(init_gmv) as `Q_GMV_app`
         ,sum(coupon_substract_summary) as `Q_券额_app`
         ,count(distinct order_no) as `Q_订单量_app`
         ,count(distinct t1.user_id) as `Q_下单用户_app`
         ,sum(room_night) as `Q_间夜量_app`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量_app`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量_app`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量_app`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量_app`
    from q_order_app t1 
    where order_rn = 1
    group by t1.order_date,cube(t1.mdd,t1.user_type)
)
,order_info as ( --- q 订单汇总
    select t1.order_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金`
         ,sum(init_gmv) as `Q_GMV`
         ,sum(coupon_substract_summary) as `Q_券额`
         ,count(distinct order_no) as `Q_订单量`
         ,count(distinct t1.user_id) as `Q_下单用户`
         ,sum(room_night) as `Q_间夜量`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from q_order t1  
    where order_rn = 1
    group by t1.order_date,cube(t1.mdd,t1.user_type)
)

/******************************** 预定口径Q分区域分新老结果数据 ********************************/ 
    select t1.dt
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.uv, 0)   as uv
            ,coalesce(t2.`Q_间夜量`, 0) as `Q_间夜量`
            ,coalesce(t2.`Q_订单量`, 0)  as `Q_订单量`
            ,coalesce(t2.`Q_下单用户`, 0)   as `Q_下单用户`
            ,coalesce(t2.`Q_GMV`, 0)      as `Q_GMV`
            ,coalesce(t2.`Q_佣金`, 0)      as `Q_佣金`
            ,coalesce(t2.`Q_券额`, 0)      as `Q_券额`
            ,coalesce(t2.`Q_高星间夜量`, 0)      as `Q_高星间夜量`
            ,coalesce(t2.`Q_中星间夜量`, 0)      as `Q_中星间夜量`
            ,coalesce(t2.`Q_低星间夜量`, 0)      as `Q_低星间夜量`
            ,concat(round(coalesce(t1.uv / t3.uv, 0) * 100, 2), '%')   as `Q_流量占比`
            ,concat(round(coalesce(t4.`Q_间夜量_app` / t5.`Q_间夜量_app`, 0) * 100, 2), '%')   as `Q_间夜占比_app`
            ,concat(round(coalesce(t4.`Q_订单量_app` / t5.`Q_订单量_app`, 0) * 100, 2), '%')   as `Q_订单量占比_app`
            ,concat(round(coalesce(t4.`Q_GMV_app` /   t5.`Q_GMV_app`, 0) * 100, 2), '%')   as `Q_GMV占比_app`
            ,concat(round(coalesce(t4.`Q_佣金_app` /   t5.`Q_佣金_app`, 0) * 100, 2), '%')   as `Q_佣金占比_app`
            ,concat(round(coalesce(t4.`Q_券额_app` /   t5.`Q_券额_app`, 0) * 100, 2), '%')   as `Q_券额占比_app`

            ,concat(round(coalesce(t2.`Q_间夜量` / t6.`Q_间夜量`, 0) * 100, 2), '%')   as `Q_间夜占比`
            ,concat(round(coalesce(t2.`Q_订单量` / t6.`Q_订单量`, 0) * 100, 2), '%')   as `Q_订单量占比`
            ,concat(round(coalesce(t2.`Q_GMV` /   t6.`Q_GMV`, 0) * 100, 2), '%')   as `Q_GMV占比`
            ,concat(round(coalesce(t2.`Q_佣金` /   t6.`Q_佣金`, 0) * 100, 2), '%')   as `Q_佣金占比`
            ,concat(round(coalesce(t2.`Q_券额` /   t6.`Q_券额`, 0) * 100, 2), '%')   as `Q_券额占比`

            ,coalesce(t2.`Q_订单量` / t1.uv, 0)  as `Q_CR`
            ,coalesce(t2.`Q_间夜量`, 0) / coalesce(t2.`Q_订单量`, 0)  as `Q_单间夜`
            ,coalesce(t2.`Q_佣金`, 0) / coalesce(t2.`Q_GMV`, 0)  as `Q_收益率`
            ,coalesce(t2.`Q_券额`, 0) / coalesce(t2.`Q_GMV`, 0)  as `Q_券补贴率`
            ,coalesce(t2.`Q_GMV`, 0) / coalesce(t2.`Q_间夜量`, 0)  as `Q_ADR`
            ,coalesce(t2.`Q_券额`, 0) / coalesce(t2.`Q_下单用户`, 0)  as `Q_单用户补贴金额`
            ,concat(round(coalesce(t2.`Q_用券订单量`, 0) / coalesce(t2.`Q_订单量`, 0) * 100, 1), '%') as `Q_用券订单占比`

            ,coalesce(t4.`Q_间夜量_app`, 0)  as `Q_间夜量_app`
            ,coalesce(t4.`Q_订单量_app`, 0)  as `Q_订单量_app`
            ,coalesce(t4.`Q_下单用户_app`, 0) as `Q_下单用户_app`
            ,coalesce(t4.`Q_GMV_app`, 0)      as `Q_GMV_app`
            ,coalesce(t4.`Q_佣金_app`, 0)      as `Q_佣金_app`
            ,coalesce(t4.`Q_券额_app`, 0)      as `Q_券额_app`
            ,coalesce(t4.`Q_高星间夜量_app`, 0)      as `Q_高星间夜量_app`
            ,coalesce(t4.`Q_中星间夜量_app`, 0)      as `Q_中星间夜量_app`
            ,coalesce(t4.`Q_低星间夜量_app`, 0)      as `Q_低星间夜量_app`
            ,coalesce(t4.`Q_订单量_app` / t1.uv, 0)  as `Q_CR_app`
            ,coalesce(t4.`Q_间夜量_app`, 0) / coalesce(t4.`Q_订单量_app`, 0) as `Q_单间夜_app`
            ,coalesce(t4.`Q_佣金_app`, 0)  /  coalesce(t4.`Q_GMV_app`, 0)   as `Q_收益率_app`
            ,coalesce(t4.`Q_券额_app`, 0)  /  coalesce(t4.`Q_GMV_app`, 0)   as `Q_券补贴率_app`
            ,coalesce(t4.`Q_GMV_app`, 0)  /  coalesce(t4.`Q_间夜量_app`, 0) as `Q_ADR_app`
            ,coalesce(t4.`Q_券额_app`, 0) /   coalesce(t4.`Q_下单用户_app`, 0)  as `Q_单用户补贴金额_app`
            ,concat(round(coalesce(t4.`Q_用券订单量_app`, 0) / coalesce(t4.`Q_订单量_app`, 0) * 100, 1), '%') as `Q_用券订单占比_app`
    from q_uv_info t1 
    left join order_info t2 on t1.dt=t2.order_date and t1.mdd=t2.mdd 
    and t1.user_type=t2.user_type 
    left join order_info_app t4 on t1.dt=t4.order_date and t1.mdd=t4.mdd 
    and t1.user_type=t4.user_type 
    left join (  --- 计算流量占比
        select dt,uv,user_type
        from q_uv_info 
        where  mdd='ALL'
    ) t3 on t1.dt=t3.dt  and t1.user_type=t3.user_type
    left join (  --- 计算订单占比 APP
        select order_date,`Q_佣金_app`,`Q_GMV_app`,`Q_订单量_app`,`Q_券额_app`,`Q_间夜量_app`,user_type
        from order_info_app 
        where  mdd='ALL'
    ) t5 on t1.dt=t5.order_date  and t1.user_type=t5.user_type
    left join (  --- 计算订单占比 全端
        select order_date,`Q_佣金`,`Q_GMV`,`Q_订单量`,`Q_券额`,`Q_间夜量`,user_type
        from order_info
        where  mdd='ALL'
    ) t6 on t1.dt=t6.order_date  and t1.user_type=t6.user_type
;


--- 复购率
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----订单明细表包含取消  分目的地、新老维度 
    select order_date dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

select t1.dt
        ,t1.mdd
        ,count(distinct user_id) uv
        ,count(distinct case when is_re = 'Y' then user_id end) re180_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'Y' then user_id end) re180_same_mdd_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'Y' and mdds=1 then user_id end) re180_only_same_mdd_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'Y' and mdds>1 then user_id end) re180_same_other_mdd_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'N' and mdds=1 then user_id end) re180_only_other_mdd_uv
from (
select t1.dt
      ,t1.user_id
      ,t1.mdd
      ,case when t13.user_id is not null then 'Y' else 'N' end is_re
      ,case when t1.mdd=t13.mdd then 'Y' else 'N' end is_same_mdd
      ,count(distinct t13.mdd) mdds
      
from  (select dt,mdd,user_id from uv where user_type='新客' group by 1,2,3)t1
left join uv t13 on t1.user_id=t13.user_id and datediff(t13.dt,t1.dt) between 1 and 180
group by 1,2,3,4,5
) t1 
group by t1.dt,t1.mdd
order by 1
;


select t1.dt,t1.mdd
      ,count(distinct t1.user_id) uv
      ,count(distinct t13.user_id) re180
      ,count(distinct case when t1.mdd=t13.mdd then t13.user_id end) re180_same_mdd
      
from  (select dt,mdd,user_id from uv where user_type='新客' group by 1,2,3)t1
left join uv t13 on t1.user_id=t13.user_id and datediff(t13.dt,t1.dt) between 1 and 180
group by t1.dt,t1.mdd

order by 1
;


---- 复购数据
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----订单明细表包含取消  分目的地、新老维度 
    select order_date dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,ROW_NUMBER() OVER (PARTITION BY a.user_id ORDER BY order_time) as order_rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

select t1.dt
        ,t1.mdd
        ,count(distinct user_id) uv
        ,count(distinct case when is_re = 'Y' then user_id end) re180_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'Y' then user_id end) re180_same_mdd_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'Y' and mdd_size=1 then user_id end) re180_only_same_mdd_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'Y' and mdd_size>1 then user_id end) re180_same_other_mdd_uv
        ,count(distinct case when is_re = 'Y' and is_same_mdd = 'N' and mdd_size=1 then user_id end) re180_only_other_mdd_uv
        ,count(distinct case when is_re = 'Y' and fgmdd_new = '港澳' then user_id end) re180_hm_uv

        ,sum(case when is_re = 'Y' then final_commission_after end) re180_final_commission_after
        ,sum(case when is_re = 'Y' and is_same_mdd = 'Y' then final_commission_after end) re180_same_mdd_final_commission_after
        ,sum(case when is_re = 'Y' and is_same_mdd = 'Y' and mdd_size=1 then final_commission_after end) re180_only_same_mdd_final_commission_after
        ,sum(case when is_re = 'Y' and is_same_mdd = 'Y' and mdd_size>1 then final_commission_after end) re180_same_other_mdd_final_commission_after
        ,sum(case when is_re = 'Y' and is_same_mdd = 'N' and mdd_size=1 then final_commission_after end) re180_only_other_mdd_final_commission_after
        ,sum(case when is_re = 'Y' and fgmdd_new = '港澳' then final_commission_after end) re180_hm_final_commission_after
from (
    select t1.dt
        ,t1.user_id
        ,t1.mdd
        ,case when t2.user_id is not null then 'Y' else 'N' end is_re
        ,t2.mdd fgmdd
        ,t2.init_gmv
        ,t2.room_night
        ,t2.final_commission_after
        ,t2.coupon_substract_summary
        ,case when t2.mdd in ('香港', '澳门') then '港澳' else t2.mdd end fgmdd_new
        ,case when t1.mdd = t2.mdd then 'Y' else 'N' end is_same_mdd
        ,COLLECT_SET(t2.mdd) over(partition by t1.user_id) mdd_set
        ,size(COLLECT_SET(t2.mdd) over(partition by t1.user_id)) mdd_size
        ,COLLECT_SET(case when t2.mdd in ('香港', '澳门') then '港澳' else t2.mdd end) over(partition by t1.user_id) mdd_gh_set
        ,size(COLLECT_SET(case when t2.mdd in ('香港', '澳门') then '港澳' else t2.mdd end) over(partition by t1.user_id)) mdd_gh_size
    from (
        select dt,mdd,user_id 
        from uv 
        where user_type='新客' 
        group by 1,2,3
    )t1
    left join uv t2 on t1.user_id=t2.user_id and datediff(t2.dt,t1.dt) between 1 and 180
) t1 
group by t1.dt,t1.mdd
order by 1
;


--- ARPU拆解，公式：单用户订单*单订单间夜*ADR*佣金率
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
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,room_night,order_no,init_gmv
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date,1)
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-01-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)


select order_date
        ,mdd
        ,uv
        ,yj0
        ,yj7
        ,yj30
        ,yj180
        ,init_gmv0
        ,init_gmv7
        ,init_gmv30
        ,init_gmv180
        ,room_night0
        ,room_night7
        ,room_night30
        ,room_night180
        ,order_no0
        ,order_no7
        ,order_no30
        ,order_no180

        ,yj0 / uv    ARPU0
        ,yj7 / uv    ARPU7
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,order_no0   / uv single_order0
        ,order_no7   / uv single_order7
        ,order_no30  / uv single_order30
        ,order_no180 / uv single_order180
        ,room_night0 / order_no0   single_roomnight0
        ,room_night7 / order_no7  single_roomnight7
        ,room_night30 / order_no30  single_roomnight30
        ,room_night180 / order_no180  single_roomnight180
        ,init_gmv0 / room_night0  adr0
        ,init_gmv7 / room_night7  adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv180 / room_night180 adr180
        ,yj0 / init_gmv0    yj_rate0
        ,yj7 / init_gmv7    yj_rate7
        ,yj30 / init_gmv30  yj_rate30
        ,yj180 / init_gmv180 yj_rate180
from (
    select t1.order_date,t1.mdd
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then final_commission_after end) else null end yj7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then init_gmv end) init_gmv0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) else null end init_gmv7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) else null end init_gmv30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) else null end init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then room_night end) room_night0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) else null end room_night7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) else null end room_night180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0  then order_no end) order_no0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) else null end order_no7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) else null end order_no30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) else null end order_no180
    from (
        select distinct t1.order_date,t1.user_id,t1.user_type,t1.mdd
            ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
        from q_order t1 
        left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk
        where t1.user_type = '新客'  
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2
) 
order by order_date 
;