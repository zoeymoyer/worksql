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
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
            ,coupon_substract_summary
            ,follow_price_amount,extendinfomap,cashbackmap
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
)
,q_order_crowd as ( --- 预定订单在24.6-25.3人群
    select order_date,mdd,user_type,user_id,user_name,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
            ,coupon_substract_summary
            ,follow_price_amount,extendinfomap,cashbackmap
    from q_order 
    where order_date >= '2024-06-01' and  order_date <= '2025-03-01'
)
,q_yj180 as (   ---- T+180日佣金（收益）
  select t1.order_date
      ,t1.user_id
      ,sum(`当天佣金`) as `半年佣金`
  from (select distinct order_date,user_id from q_order_crowd) t1 
  left join (
      select order_date
            ,user_id
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                       then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                   else init_commission_after+nvl(ext_plat_certificate,0) end) as `当天佣金`
      from q_order 
      group by 1,2
    ) t2 on  t1.user_id=t2.user_id  
      and datediff(t2.order_date, t1.order_date) <= 180 
      and datediff(t2.order_date, t1.order_date) >= 0 
  group by 1,2
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

,platform_new as (--- 判定平台新
    select distinct dt,
                    user_pk,
                    user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-06-01'  
        
        and dict_type = 'pncl_wl_username'
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



select t1.order_date
      ,t1.user_id
      ,t1.order_no
      ,case
           when (t1.user_type = '新客' and t2.user_id is not null) then '平台新业务新'
           when t1.user_type = '新客' then '平台老业务新'
           else '老客'
      end as user_type
      ,max(city_name)  `城市`
      ,max(gender)  `性别`
      ,max(age)  `年龄`
      ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                       then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                   else init_commission_after+nvl(ext_plat_certificate,0) end)  as `当单收益`
      ,max(`半年佣金`) `半年收益`   --- 180天
from q_order_crowd t1 
left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk 
left join user_info t3 on t1.user_id=t3.user_id
left join q_yj180 t4  on t1.user_id=t4.user_id and t1.order_date = t4.order_date
--- 筛选小红书渠道 去掉下面两行注释
-- left join user_xhs t5 on t1.order_date=t5.order_date and t1.user_name=t5.user_name
-- where t5.user_name is not null   
group by 1,2,3,4

;

