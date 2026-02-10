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
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
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
,init_uv as(
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
    where dt >= '2024-01-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,red as(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2023-12-01' and date_sub(current_date,1)
 -- and business_type = 'hotel-inter'  --宽口径不用该字段
  and query_platform = 'redbook'
)
,red_res as (--- 小红书生单人群
    select uv.dt,uv.user_id,uv.user_type
           ,case
                when (uv.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when uv.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from(
        select distinct uv.dt,uv.user_id,user_type,uv.user_name
        from init_uv uv
        left join red r on uv.user_name = r.user_name
        where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    ) uv
    left join platform_new t2 on uv.dt=t2.dt and uv.user_name=t2.user_pk
    left join q_order ord on uv.user_id = ord.user_id
    and uv.dt = ord.order_date
    where ord.user_id is not null
)



select order_date
        ,user_type1
        ,uv
        ,yj0
        ,yj30
        ,yj180
        ,gmv30
        ,qe30
        ,room_night30
        ,order_no30
        ,yj0 / uv    ARPU0
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,'大盘' person
from (
    select t1.order_date,t1.user_type1
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) else null end gmv30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) else null end qe30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) else null end order_no30

    from (
        select distinct t1.order_date,t1.user_id,t1.user_type
            ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
        from q_order t1 
        left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk 
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2
) 

union all
select order_date
        ,user_type1
        ,uv
        ,yj0
        ,yj30
        ,yj180
        ,gmv30
        ,qe30
        ,room_night30
        ,order_no30
        ,yj0 / uv    ARPU0
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,'小红书' person
from (
    select t1.order_date,t1.user_type1
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) else null end gmv30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) else null end qe30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) else null end order_no30

    from (
        select distinct t1.dt as order_date
            ,t1.user_id
            ,t1.user_type
            ,t1.user_type1
        from red_res t1 
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2
) 
order by order_date 
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
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2025-08-01' and order_date <= date_sub(current_date,1)
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-06-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)


select order_date
        ,'V9' user_type1
        ,uv
        ,yj0
        ,yj30
        ,yj180
        ,gmv30
        ,qe30
        ,room_night30
        ,order_no30
        ,yj0 / uv    ARPU0
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,'V9' person
from (
    select t1.order_date
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) else null end gmv30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) else null end qe30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) else null end order_no30

    from (
        select distinct t1.order_date,t1.user_id,t1.user_type
            ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
        from q_order t1 
        left join temp.inter_hotel_user_order_251117 t3 on t1.user_name=t3.username and substr(t1.order_date,1,7) = t3.order_mth --- V9
        left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk 
        where t3.username is not null
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1
) 


;








--每日红包领取金额
--  1. 转化漏斗用前端埋点来跑：
   -- 1. 进入取消原因页：key ='ihotel/OrderDetail/cancelReason/show/cancelReason'，value中的ext.orderNo代表订单号
   -- 2. 弹出挽留弹窗：key ='ihotel/OrderDetail/cancelReason/show/cancelBlock'，如果想限制领取红包的弹窗，条件为value中的scene = 'unclaimed' and trendType in ('cash','all')，并可以用value中的traceId关联进入取消页的埋点来关联订单号
   -- 3. 挽留成功：key ='ihotel/OrderDetail/cancelReason/click/cancelBlocked'，点击收下不取消，如果想限制领取红包的弹窗，条件为value中的scene = 'unclaimed' and trendType in ('cash','all')，并可以用value中的traceId关联进入取消页的埋点来关联订单号
            --key= 'ihotel/OrderDetail/cancelReason/click/cancelConfirmed'点击坚持取消
   --场景：trendType = cash (红包) / point（积分）/ all（红包+积分），是否已领取：scene = claimed（已领取）/unclaimed（未领取）
   -- 4. 取消挽留订单相关的字段存在订单表的cancel_red_packet_data_track_map中，其中的字段含义参考【FD-362883】取消环节红包挽留中的埋点字段，核心就是actual_cash_back_amount，即返现金额。
with q_order as
(
select
order_no,user_name,
case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
   then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
     else init_commission_after+nvl(ext_plat_certificate,0) 
  end as `新佣金`,
get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as `返现`
FROM default.mdw_order_v3_international
WHERE dt = '20251119' 
and order_status not in ('CANCELLED','REJECTED')
and is_valid = 1
--and checkout_date >= '2025-09-19' and  checkout_date <= '2025-11-18'
and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null 
)
,cancel_page as
(
select 
distinct 
dt,
CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dates,
user_name,
--get_json_object(value, '$.ext.button.menu') as menu,
--get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
get_json_object(value, '$.common.traceId') as trace_id,
get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as order_no
from default.dw_qav_ihotel_track_info_di
where 
dt between '20251106' and '20251119'
--and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
and get_json_object(value, '$.ext.button.menu') = '取消订单'
)
,wanliu_order as 
(
select 
dt,
user_name,
--get_json_object(value, '$.ext.orderNo') as order_no,
get_json_object(value, '$.common.traceId') as trace_id
from default.dw_qav_ihotel_track_info_di
where 
dt between '20251106' and '20251119'
and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
group by 1,2

)

select a.dt,
    count (distinct a.user_name) as `领取红包用户数`,
    count (distinct b.order_no) as `领取红包订单量`,
    sum (c.`新佣金`) as `领取红包佣金`,
    sum (c.`返现`) as `领取红包金额`
from (
    select dt,user_name,order_no
    from wanliu_order a
    left join cancel_page b on a.trace_id = b.trace_id and a.user_name = b.user_name and a.dt = b.dt
    group by 1,2,3
) t1 
left join q_order t2 on t1.order_no = t2.order_no 
group by 1
order by a.dt desc
;
