--- 用户生命周期
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
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
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2024-12-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-12-01'  
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,init_uv as (---大盘日活分平台新日活用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from uv t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk    
)

select dt,life_type
        ,count(distinct user_id)uv
        ,sum(count(distinct user_id)) over(partition by dt) suv
        ,sum(yj180) yj180
        ,sum(yj0) yj0
        ,sum(yj_all) yj_all
        ,'小红书' channel
from (
    select dt,user_id,user_type1,history_orders,yj180,yj0,yj_all
        ,case when user_type1='平台新业务新' then '新客-平台新'
                when user_type1='平台老业务新' then '新客-平台老-业务新'
                when (user_type1='老客' and history_orders<=3 and yj180 is not null) then '老客-活跃-成长'
                when (user_type1='老客' and history_orders>3 and yj180 is not null) then '老客-活跃-成熟'
                when (user_type1='老客' and history_orders<=3 and yj180 is null) then '老客-休眠-成长'
                when (user_type1='老客' and history_orders>3 and yj180 is null) then '老客-休眠-成熟'
                end life_type
    from (
        select t1.dt
            ,t1.user_id
            ,t1.user_type1
            ,history_orders,yj_all
            ,sum(case when datediff(t2.order_date, t1.dt) <= 180 then final_commission_after end) yj180
            ,sum(case when datediff(t2.order_date, t1.dt) = 0 then final_commission_after end) yj0
        from user_xhs t1 
        left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.dt
        left join user_type t3 on t1.user_id=t3.user_id
        group by 1,2,3,4,5
    )
) group by 1,2 

union all

select dt,life_type
        ,count(distinct user_id)uv
        ,sum(count(distinct user_id)) over(partition by dt) suv
        ,sum(yj180) yj180
        ,sum(yj0) yj0
        ,sum(yj_all) yj_all
        ,'大盘' channel
from (
    select dt,user_id,user_type1,history_orders,yj180,yj0,yj_all
        ,case when user_type1='平台新业务新' then '新客-平台新'
                when user_type1='平台老业务新' then '新客-平台老-业务新'
                when (user_type1='老客' and history_orders<=3 and yj180 is not null) then '老客-活跃-成长'
                when (user_type1='老客' and history_orders>3 and yj180 is not null) then '老客-活跃-成熟'
                when (user_type1='老客' and history_orders<=3 and yj180 is null) then '老客-休眠-成长'
                when (user_type1='老客' and history_orders>3 and yj180 is null) then '老客-休眠-成熟'
                end life_type
    from (
        select t1.dt
            ,t1.user_id
            ,t1.user_type1
            ,history_orders,yj_all
            ,sum(case when datediff(t2.order_date, t1.dt) <= 180 then final_commission_after end) yj180
            ,sum(case when datediff(t2.order_date, t1.dt) = 0 then final_commission_after end) yj0
        from init_uv t1 
        left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.dt
        left join user_type t3 on t1.user_id=t3.user_id
        group by 1,2,3,4,5
    )
) group by 1,2 
order by dt desc,uv desc
;

----- 用户下单间隔分布
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
,q_order as (
select order_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,a.user_id,init_gmv,order_no,room_night,a.user_name
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
        ,lead(order_date,1) over(partition by a.user_id order by order_time  desc)  near_order_date
        ,row_number() over(partition by a.user_id order by order_date desc) rn
from mdw_order_v3_international a 
left join user_type b on a.user_id = b.user_id 
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
where dt = '%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
    and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
    -- and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    and is_valid='1'
    and order_status not in ('CANCELLED', 'REJECTED')
    and order_no <> '103576132435'
)

select case when user_type='新客' then '0新客'
            when diff >= 0 and diff<= 7 then '1[1,7]'
            when diff >= 8 and diff<= 15 then '2[8,15]'
            when diff >= 16 and diff<= 30 then '3[16,30]'
            when diff >= 31 and diff<= 60 then '4[31,60]'
            when diff >= 61 and diff<= 90 then '5[61,90]'
            when diff >= 91 and diff<= 180 then '6[91,180]'
            when diff >= 181 and diff<= 360 then '7[181,360]'
            when diff >= 361 then '8[361+'
            end fb
        ,user_type,user_id,init_gmv,room_night,final_commission_after,diff,near_order_date,rn

from (
    select user_type,order_date,mdd,user_id,init_gmv,room_night,final_commission_after,near_order_date,rn
        ,case when near_order_date is null then 0 else datediff(order_date,near_order_date) end diff
    from q_order t1
    where rn = 1
    and order_date = date_sub(current_date,1)
)
;

---- 日活留存
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
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

select t1.dt
      ,count(distinct t1.user_id) uv
      ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  uv1
      ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  uv7
      ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  uv14
      ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  uv30
      ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  uv60
      ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
      ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  / count(distinct t1.user_id) re1
      ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  / count(distinct t1.user_id) re7
      ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  / count(distinct t1.user_id) re14
      ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  / count(distinct t1.user_id) re30
      ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  / count(distinct t1.user_id) re60
      ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180
from  uv t1
left join uv t2 on t1.user_id=t2.user_id and datediff(t2.dt,t1.dt) = 1
left join uv t3 on t1.user_id=t3.user_id and datediff(t3.dt,t1.dt) = 7
left join uv t4 on t1.user_id=t4.user_id and datediff(t4.dt,t1.dt) = 14
left join uv t5 on t1.user_id=t5.user_id and datediff(t5.dt,t1.dt) = 30
left join uv t6 on t1.user_id=t6.user_id and datediff(t6.dt,t1.dt) = 60
left join uv t7 on t1.user_id=t7.user_id and datediff(t7.dt,t1.dt) = 180
group by 1
order by t1.dt 
;


---- 小红书渠道用户留存
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
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
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2024-12-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select distinct t1.dt
        ,t1.user_id
        ,t1.user_name
        ,t1.user_type
    from uv  t1
    left join red t2 on t1.user_name = t2.user_name
    where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
    and t2.user_name is not null
)

select t1.dt
      ,count(distinct t1.user_id) uv
      ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  uv1
      ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  uv7
      ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  uv14
      ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  uv30
      ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  uv60
      ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
      ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  / count(distinct t1.user_id) re1
      ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  / count(distinct t1.user_id) re7
      ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  / count(distinct t1.user_id) re14
      ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  / count(distinct t1.user_id) re30
      ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  / count(distinct t1.user_id) re60
      ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180
from  user_xhs t1
left join user_xhs t2 on t1.user_id=t2.user_id and datediff(t2.dt,t1.dt) = 1
left join user_xhs t3 on t1.user_id=t3.user_id and datediff(t3.dt,t1.dt) = 7
left join user_xhs t4 on t1.user_id=t4.user_id and datediff(t4.dt,t1.dt) = 14
left join user_xhs t5 on t1.user_id=t5.user_id and datediff(t5.dt,t1.dt) = 30
left join user_xhs t6 on t1.user_id=t6.user_id and datediff(t6.dt,t1.dt) = 60
left join user_xhs t7 on t1.user_id=t7.user_id and datediff(t7.dt,t1.dt) = 180
group by 1
order by t1.dt 
;


--- 小红书渠道用户之前活跃分布
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
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
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2024-12-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-12-01'  
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)


select *,datediff(t1.dt,near_act_dt) diff
from (
    select t1.dt,t1.user_id,t2.dt,user_type1
        ,lead(t2.dt,1) over(partition by t1.user_id order by t2.dt  desc)  near_act_dt
        ,row_number() over(partition by t1.user_id order by t2.dt  desc) rn
    from (select * from user_xhs where dt=date_sub(current_date,1)) t1 
    left join uv t2 on t1.user_id=t2.user_id
    and t2.dt <= t1.dt
  )
where rn =1
;


---- 小红书增量用户
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
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-04-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5

)

,xhs_user_info as (   ---小红书生命周期分层  过往180天是否有国酒订单
    select dt,user_id,user_type1,user_type,history_orders,yj180,yj0,yj_all,is_bulking
        ,case when user_type1='平台新业务新' then '新客-平台新'
                when user_type1='平台老业务新' then '新客-平台老-业务新'
                when (user_type1='老客' and history_orders<=3 and yj180 is not null) then '老客-活跃-成长'
                when (user_type1='老客' and history_orders>3 and yj180 is not null) then '老客-活跃-成熟'
                when (user_type1='老客' and history_orders<=3 and yj180 is null) then '老客-休眠-成长'
                when (user_type1='老客' and history_orders>3 and yj180 is null) then '老客-休眠-成熟'
                end life_type
    from (
        select t1.dt
            ,t1.user_id
            ,t1.user_type1
            ,t1.user_type
            ,history_orders,yj_all,is_bulking
            ,sum(case when datediff(t1.dt, t2.order_date) between 1 and 180 then final_commission_after end) yj180
            ,sum(case when datediff(t1.dt, t2.order_date) = 0 then final_commission_after end) yj0
        from xhs t1 
        left join q_order t2 on t1.user_id=t2.user_id and t2.order_date <= t1.dt
        left join user_type t3 on t1.user_id=t3.user_id
        group by 1,2,3,4,5,6,7
    )
)

select dt,life_type
        ,count(distinct user_id) uv
        ,sum(count(distinct user_id)) over(partition by dt) suv
        ,count(distinct case when is_bulking = 'Y' then user_id end) buv
        ,sum(count(distinct case when is_bulking = 'Y' then user_id end)) over(partition by dt) bsuv
        ,count(distinct case when is_bulking = 'Y' then user_id end) / count(distinct user_id) ratio
from xhs_user_info group by 1,2 
order by dt
;


---小红书报表SQL1--小红书渠道引流转化量&大盘贡献t-7-宽口径  增量口径
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
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-04-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5

)
-- ,red_guiyi as (
--     select statistic_date,
--            count(distinct user_name) as order_user,
--            sum(nums) as room_night
--        from (
--             select distinct statistic_date,order_user_name,is_first_log_new,order_no,business_type
--             from pp_pub.ads_redbook_funnel_analysis_detail_di
--             where dt>=date_sub(current_date,14)
--             and funnel_type in ('order')  -- flow 是流量，不归一
--             and platform = 'redbook'
--             --and business_type='国际酒店' --宽口径不需要这个字段
--             and is_first_log_new='1'
--             and group_name='投流'
--         )a
--         left join(
--             select distinct statistic_dt,bu_type,user_name,is_yw_new,order_no,nums
--             from pp_pub.ads_redbook_bu_board_info_di
--             where dt>=date_sub(current_date,14)
--             -- and is_yw_new='1'--判断是否为业务新客
--             and bu_type in ('inter_hotel')
--         )b
--        on a.statistic_date=b.statistic_dt and a.order_user_name=b.user_name and a.order_no=b.order_no
--    --where is_yw_new='1'
--     group by 1
--     order by statistic_date
-- )
,init_uv_all as
(
  select dt
        ,count(distinct user_id) all_uv
  from uv
  group by 1
)
,order_all as
(
  select order_date
         ,count(distinct order_no) order_all
         ,sum(room_night) room_night_all
  from q_order
  group by 1
)

select a.dt
       ,date_format(a.dt,'u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
--   d.order_user as `归一生单用户量`,
--   d.room_night as `归一间夜量`,
--   concat(round(d.room_night / room_night_all * 100, 1), '%') as `归一间夜占比`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from xhs
        where is_bulking = 'Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1
  ) a
  left join init_uv_all b on a.dt = b.dt
  left join order_all c on a.dt = c.order_date
  --left join red_guiyi d on a.`日期`=d.statistic_date
order by a.dt desc
;

----小红书报表SQL2-小红书业务新客引流转化量-宽口径  增量口径
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
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-04-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5

)
-- ,red_guiyi as (
--     select statistic_date,
--            count(distinct user_name) as order_user,
--            sum(nums) as room_night
--        from (
--             select distinct statistic_date,order_user_name,is_first_log_new,order_no,business_type
--             from pp_pub.ads_redbook_funnel_analysis_detail_di
--             where dt>=date_sub(current_date,14)
--             and funnel_type in ('order')  -- flow 是流量，不归一
--             and platform = 'redbook'
--             --and business_type='国际酒店' --宽口径不需要这个字段
--             and is_first_log_new='1'
--             and group_name='投流'
--         )a
--         left join(
--             select distinct statistic_dt,bu_type,user_name,is_yw_new,order_no,nums
--             from pp_pub.ads_redbook_bu_board_info_di
--             where dt>=date_sub(current_date,14)
--             -- and is_yw_new='1'--判断是否为业务新客
--             and bu_type in ('inter_hotel')
--         )b
--        on a.statistic_date=b.statistic_dt and a.order_user_name=b.user_name and a.order_no=b.order_no
--    --where is_yw_new='1'
--     group by 1
--     order by statistic_date
-- )
,init_uv_all as
(
  select dt
        ,count(distinct user_id) all_uv
  from uv where user_type = '新客'
  group by 1
)
,order_all as
(
  select order_date
         ,count(distinct order_no) order_all
         ,count(distinct user_id) user_cnt
         ,sum(room_night) room_night_all
  from q_order where user_type = '新客'
  group by 1
)

select a.dt
       ,date_format(a.dt,'u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,concat(round(order_uv / user_cnt * 100, 1), '%')  order_uv_rate -- `新客占比`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
--   d.order_user as `归一生单新客量`,
--   concat(round(d.order_user / user_cnt * 100, 1), '%') as `归一新客量占比`,
--   d.room_night as `归一新客间夜量`,
--   concat(round(d.room_night / room_night_all * 100, 1), '%') as `归一新客间夜占比`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from xhs
        where is_bulking = 'Y'
        and   user_type = '新客'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1
  ) a
  left join init_uv_all b on a.dt = b.dt
  left join order_all c on a.dt = c.order_date
  --left join red_guiyi d on a.`日期`=d.statistic_date
order by a.dt desc
;


---小红书报表SQL3--小红书渠道引流转化量&大盘贡献t-7-窄口径  增量口径
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
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-04-01' and date_sub(current_date,1)
  and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5

)
-- ,red_guiyi as (
--     select statistic_date,
--            count(distinct user_name) as order_user,
--            sum(nums) as room_night
--        from (
--             select distinct statistic_date,order_user_name,is_first_log_new,order_no,business_type
--             from pp_pub.ads_redbook_funnel_analysis_detail_di
--             where dt>=date_sub(current_date,14)
--             and funnel_type in ('order')  -- flow 是流量，不归一
--             and platform = 'redbook'
--             --and business_type='国际酒店' --宽口径不需要这个字段
--             and is_first_log_new='1'
--             and group_name='投流'
--         )a
--         left join(
--             select distinct statistic_dt,bu_type,user_name,is_yw_new,order_no,nums
--             from pp_pub.ads_redbook_bu_board_info_di
--             where dt>=date_sub(current_date,14)
--             -- and is_yw_new='1'--判断是否为业务新客
--             and bu_type in ('inter_hotel')
--         )b
--        on a.statistic_date=b.statistic_dt and a.order_user_name=b.user_name and a.order_no=b.order_no
--    --where is_yw_new='1'
--     group by 1
--     order by statistic_date
-- )
,init_uv_all as
(
  select dt
        ,count(distinct user_id) all_uv
  from uv
  group by 1
)
,order_all as
(
  select order_date
         ,count(distinct order_no) order_all
         ,sum(room_night) room_night_all
  from q_order
  group by 1
)

select a.dt
       ,date_format(a.dt,'u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
--   d.order_user as `归一生单用户量`,
--   d.room_night as `归一间夜量`,
--   concat(round(d.room_night / room_night_all * 100, 1), '%') as `归一间夜占比`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from xhs
        where is_bulking = 'Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1
  ) a
  left join init_uv_all b on a.dt = b.dt
  left join order_all c on a.dt = c.order_date
  --left join red_guiyi d on a.`日期`=d.statistic_date
order by a.dt desc
;


-----小红书报表SQL4-小红书业务新客引流转化量-窄口径 增量口径
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
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-04-01' and date_sub(current_date,1)
  and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5

)
-- ,red_guiyi as (
--     select statistic_date,
--            count(distinct user_name) as order_user,
--            sum(nums) as room_night
--        from (
--             select distinct statistic_date,order_user_name,is_first_log_new,order_no,business_type
--             from pp_pub.ads_redbook_funnel_analysis_detail_di
--             where dt>=date_sub(current_date,14)
--             and funnel_type in ('order')  -- flow 是流量，不归一
--             and platform = 'redbook'
--             --and business_type='国际酒店' --宽口径不需要这个字段
--             and is_first_log_new='1'
--             and group_name='投流'
--         )a
--         left join(
--             select distinct statistic_dt,bu_type,user_name,is_yw_new,order_no,nums
--             from pp_pub.ads_redbook_bu_board_info_di
--             where dt>=date_sub(current_date,14)
--             -- and is_yw_new='1'--判断是否为业务新客
--             and bu_type in ('inter_hotel')
--         )b
--        on a.statistic_date=b.statistic_dt and a.order_user_name=b.user_name and a.order_no=b.order_no
--    --where is_yw_new='1'
--     group by 1
--     order by statistic_date
-- )
,init_uv_all as
(
  select dt
        ,count(distinct user_id) all_uv
  from uv where user_type = '新客'
  group by 1
)
,order_all as
(
  select order_date
         ,count(distinct order_no) order_all
         ,count(distinct user_id) user_cnt
         ,sum(room_night) room_night_all
  from q_order where user_type = '新客'
  group by 1
)

select a.dt
       ,date_format(a.dt,'u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,concat(round(order_uv / user_cnt * 100, 1), '%')  order_uv_rate -- `新客占比`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
--   d.order_user as `归一生单新客量`,
--   concat(round(d.order_user / user_cnt * 100, 1), '%') as `归一新客量占比`,
--   d.room_night as `归一新客间夜量`,
--   concat(round(d.room_night / room_night_all * 100, 1), '%') as `归一新客间夜占比`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from xhs
        where is_bulking = 'Y'
        and   user_type = '新客'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1
  ) a
  left join init_uv_all b on a.dt = b.dt
  left join order_all c on a.dt = c.order_date
  --left join red_guiyi d on a.`日期`=d.statistic_date
order by a.dt desc
;


---- 十一冲高影响 流量间夜   QC 对比
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
,uv1 as ----分日去重活跃用户 D页
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and checkout_date between '2025-10-01' and '2025-10-08'
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,mdd
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-06-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        ---and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_status not in ('CANCELLED', 'REJECTED')
        and checkout_date between '2025-10-01' and '2025-10-08'
        and order_no <> '103576132435'
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-05-01' and date_sub(current_date,1)
  -- and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-05-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5
)

,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,order_c as -- c_25分区_25数据_分日
(   select substr(order_date,1,10) as dt
        , count(order_no) order_no
        , sum(extend_info['room_night']) as room_night
        , sum(case when terminal_channel_type = 'app' then extend_info['room_night'] else 0 end ) as room_night_app_c
        , sum(case when terminal_channel_type = 'app' then coalesce(comission,0) else 0 end ) as yj_app
        ,sum(case when terminal_channel_type = 'app' and  min_order_date=substr(a.order_date,1,10) then extend_info['room_night'] else 0 end) nu_room_night_app_c
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da a
    left join c_user_type b on a.user_id=b.ubt_user_id
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
        and checkout_date between '2025-10-01' and '2025-10-08' --十一假期：2025.10.01 - 2025.10.08（共8天）
        and substr(order_date,1,10) >= '2025-06-01'
        and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1
)
,order_q as -- q_25分区_25数据_分日
(   select order_date
        , count(distinct order_no) asorder_no
        , sum(room_night) as room_night
        , sum(case when terminal_channel_type = 'app' then room_night else 0 end ) as room_night_app_q
        , sum(case when terminal_channel_type = 'app' and ((batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%'))
                then (coalesce(final_commission_after,0)+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                when terminal_channel_type = 'app' then coalesce(final_commission_after,0)+coalesce(ext_plat_certificate,0)
                end) as yj_app
    from mdw_order_v3_international
    where dt = '%(DATE)s'
        and is_valid='1'
        and checkout_date between '2025-10-01' and '2025-10-08' --十一假期：2025.10.01 - 2025.10.08（共8天）
        and order_date <> current_date
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2025-06-01'
        and order_date <= date_sub(current_date, 1)
    group by 1
)


select t1.dt,q_uv,q_red_uv,q_red_add_uv,q_new_uv,q_red_new_uv,q_red_add_new_uv
       ,t2.room_night,red_room_night,red_add_room_night,new_room_night,red_new_room_night,red_new_add_room_night
       ,room_night_app_q,room_night_app_c,room_night_app_q/room_night_app_c room_night_qc
       ,nu_room_night_app_c
from (
    select t1.dt
          ,count(t1.user_id) q_uv
          ,count(t2.user_id) q_red_uv
          ,count(case when is_bulking = 'Y' then t2.user_id end) q_red_add_uv
          ,count(case when t1.user_type = '新客' then t1.user_id end) q_new_uv
          ,count(case when t1.user_type = '新客' then t2.user_id end) q_red_new_uv
          ,count(case when t1.user_type = '新客' and is_bulking = 'Y' then t2.user_id end) q_red_add_new_uv
    from uv t1
    left join xhs t2 on t1.user_id = t2.user_id and t1.dt=t2.dt
    group by 1
)t1 left join (
    select t1.order_date
          ,sum(room_night) room_night
          ,sum(case when t2.user_id is not null then room_night end) red_room_night
          ,sum(case when t2.user_id is not null and is_bulking = 'Y' then room_night end) red_add_room_night
          ,sum(case when t1.user_type = '新客'  then room_night end) new_room_night
          ,sum(case when t1.user_type = '新客' and t2.user_id is not null then room_night end) red_new_room_night
          ,sum(case when t1.user_type = '新客' and t2.user_id is not null and is_bulking = 'Y' then room_night end) red_new_add_room_night
    from q_order t1 
    left join xhs t2 on t1.user_id = t2.user_id and t1.order_date=t2.dt
    group by 1
)t2 on t1.dt=t2.order_date
left join order_q t3 on t1.dt = t3.order_date
left join order_c t4 on t1.dt = t4.dt
order by dt desc
;


--- 小红书预算收缩周期内，对大盘流量QC、间夜QC、新客离店QC的实际影响值是多少
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
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type,mdd
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,c_uv as
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>=  '2025-05-01' and dt<= date_sub(current_date,1)
    group by 1,2,3
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >=  '2025-05-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-04-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join red t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,xhs as (--- 增量用户，小红书渠道用户在过往7天未访问国酒页面

        select t1.dt,t1.user_id,t1.user_type,t1.user_type1
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
        from user_xhs t1 
        left join uv1 t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
        group by 1,2,3,4,5

)
select t1.dt
       ,q_DAU
       ,C_DAU
       ,q_DAU / C_DAU dau_qc
       ,q_room_nights
       ,c_room_nights_app
       ,q_room_nights / c_room_nights_app room_night_qc
       ,red_uv,q_red_room_nights,add_red_uv,add_q_red_room_nights

       ,q_DNU,C_DNU,q_DNU / C_DNU dnu_qc,q_room_nights_nu,c_room_nights_nu,q_room_nights_nu / c_room_nights_nu room_night_qc_nu
       ,red_uv_nu,q_red_room_nights_nu,add_red_uv_nu,add_q_red_room_nights_nu
from (-- Q流量
    select dt
        ,count(user_id) q_DAU
        ,count(case when user_type = '新客' then user_id end) q_DNU
    from uv1
    group by 1
)t1
left join (-- Q订单
    select order_date
            ,sum(room_night) as q_room_nights
            ,sum(case when user_type = '新客' then room_night end) as q_room_nights_nu
    from q_order
    group by 1
)t2 on t1.dt=t2.order_date
left join (-- C流量
    select dt
          ,sum(c_uv) as C_DAU
          ,sum(case when user_type = '新客' then c_uv end) C_DNU
    from c_uv
    group by 1
)t4 on t1.dt=t4.dt
left join (-- C订单 APP端
    select dt
            ,sum(room_night) as c_room_nights_app
            ,sum(case when user_type = '新客' then room_night end) as c_room_nights_nu
    from c_order
    group by 1
)t5 on t1.dt=t5.dt
left join (  --- 小红书流量
    select t1.dt
            ,count(distinct t1.user_id) as red_uv
            ,count(distinct case when is_bulking = 'Y' then t1.user_id end) as add_red_uv
            ,sum(room_night) as q_red_room_nights
            ,sum(case when is_bulking = 'Y' then room_night end) as add_q_red_room_nights

            ,count(distinct case when t1.user_type = '新客' then t1.user_id end) as red_uv_nu
            ,count(distinct case when is_bulking = 'Y' and t1.user_type = '新客' then t1.user_id end) as add_red_uv_nu
            ,sum(case when t1.user_type = '新客' then room_night end) as q_red_room_nights_nu
            ,sum(case when is_bulking = 'Y' and t1.user_type = '新客' then room_night end) as add_q_red_room_nights_nu
    from xhs t1 
    left join q_order t2 on t1.dt=t2.order_date and t1.user_id=t2.user_id
    group by 1
)t6 on t1.dt=t6.dt
order by t1.dt desc
;