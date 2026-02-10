--- 1、随机触发策略
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select  user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  CONCAT(SUBSTR(a.dt, 1, 4), '-', SUBSTR(a.dt, 5, 2), '-', SUBSTR(a.dt, 7, 2)) AS dt,
            version,
            clientcode AS user_id,
            b.user_name
    FROM default.ods_abtest_sdk_log_endtime_hotel a --user_id
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    WHERE a.dt between '20251209' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        --get_json_object(value, '$.ext.button.menu') as menu,
        --get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2
)
,cancelOrder AS (--- 取消订单
    SELECT  order_no,
            DATE(first_cancelled_time) AS cancelDate,
            user_id,
            user_name,
            hotel_seq,
            room_night,
            case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
                end as `取消订单佣金`
            ,checkout_date
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
      AND (province_name IN ('台湾', '澳门', '香港') OR country_name != '中国')
      AND terminal_channel_type = 'app'
      AND first_cancelled_time IS NOT NULL
      AND order_status = 'CANCELLED'
      AND is_valid = '1'
      AND order_no <> '103576132435'
      AND DATE(first_cancelled_time) >= '2025-12-09'
      AND DATE(first_cancelled_time) <= date_sub(current_date, 1)
)
,q_cashback as (----领取返现红包且未取消订单
    select user_name,case when order_nos >= 5 then '5+' else order_nos end order_nos,yj,cb
    from (
    SELECT user_name
        ,count(distinct order_no) as order_nos
        ,sum (case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)  end) as yj
        ,sum(get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount'))as cb
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
    group by 1
    ) t
)
,order_all as (
    select order_no,user_name,room_night,
        case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
            end as `佣金`,
        get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as `返现`
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
--and order_status not in ('CANCELLED','REJECTED')
        and is_valid = 1
--and checkout_date >= '2025-09-19' and  checkout_date <= '2025-11-18'
--and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
)


select
    a.dt,
    b.version,
    case when f.user_name is null then '0'
         else f.order_nos end  `历史领取返现红包订单次数`,
    count(distinct a.user_name) as `进入取消页面uv`,
    count(distinct e.user_name) as `触发挽留弹窗UV`,
    count(distinct d.user_name) as `挽留成功UV`,
    count(distinct c.user_name) as `取消订单uv`,

    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct case when e.user_name is not null then a.orderNo end) `触发挽留弹窗订单量`,
    count(distinct case when d.user_name is not null then a.orderNo end) `挽留成功订单量`,
    count (distinct c.order_no) as `取消订单量`,

    sum(o.`佣金`) `进入取消页面佣金`,
    sum (o.room_night) as `进入取消页面间夜量`,
    sum (c.room_night) as `取消间夜量`,
    sum (c.`取消订单佣金`) as `取消订单佣金`,
    sum(case when d.user_name is not null then o.room_night end) `挽留成功间夜量`,
    sum(case when d.user_name is not null then o.`佣金` end) `挽留成功佣金`,
    sum(case when d.user_name is not null then o.`返现` end) `挽留成功红包金额`,

    count (distinct c.user_name) /  count (distinct a.user_name) as `取消率`,
    count (distinct d.user_name) /  count (distinct a.user_name) as `挽留成功率`

from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join cancelOrder c on a.user_name = c.user_name and a.dt = c.cancelDate and c.order_no = a.orderNo
left join wanliu_order d on a.dt=d.dt and a.user_name=d.user_name and a.trace_id=d.trace_id
left join wanliu_show e on a.dt=e.dt and a.user_name=e.user_name
left join order_all o on a.orderNo = o.order_no
left join q_cashback f on a.user_name=f.user_name
where b.version is not null
group by 1,2,3
order by a.dt desc;


--- 2、限频策略
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select  user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  CONCAT(SUBSTR(a.dt, 1, 4), '-', SUBSTR(a.dt, 5, 2), '-', SUBSTR(a.dt, 7, 2)) AS dt,
            version,
            clientcode AS user_id,
            b.user_name
    FROM default.ods_abtest_sdk_log_endtime_hotel a --user_id
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    WHERE a.dt between '20251209' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        --get_json_object(value, '$.ext.button.menu') as menu,
        --get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
)
,q_cashback as (----领取返现红包且未取消订单
    SELECT user_name
        ,order_no
        ,case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)  end as yj
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') as cb
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
)


select
    a.dt,
    b.version,
    count(distinct a.user_name) as `进入取消页面uv`,
    count(distinct d.user_name) as `挽留成功UV`,
    count(distinct f.user_name) as `实际返现UV`,

    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct case when d.user_name is not null then a.orderNo end) `挽留成功订单量`,
    count(distinct f.order_no) as `实际返现订单量`,
    sum(cb) as `实际返现金额`

from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join wanliu_order d on a.dt=d.dt and a.user_name=d.user_name and a.trace_id=d.trace_id
left join q_cashback f on d.user_name=f.user_name and a.orderNo=f.order_no
where b.version is not null
group by 1,2
order by a.dt desc;


---- 3、离店48h返现
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select  user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  CONCAT(SUBSTR(a.dt, 1, 4), '-', SUBSTR(a.dt, 5, 2), '-', SUBSTR(a.dt, 7, 2)) AS dt,
            version,
            clientcode AS user_id,
            b.user_name
    FROM default.ods_abtest_sdk_log_endtime_hotel a --user_id
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    WHERE a.dt between '20251209' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页且排查近期大单用户影响
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        --get_json_object(value, '$.ext.button.menu') as menu,
        --get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251209' and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
)

,q_cashback as (----实际领取返现红包且发放
    SELECT user_name
        ,order_no
        ,sum (case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)  end) as yj
        ,sum(get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount'))as cb
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED', 'REJECTED')
    and is_valid = 1
    and (get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null   --- 返现红包大于0
            or get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') != 0 
        )
    and substr(cast(ext_flag_map['cashback_finish_time_wanliu'] as string), 1, 8) is not null  --- 发送时间不为空
    group by 1,2
)


select
    a.dt,
    b.version,
    count(distinct a.user_name) as `进入取消页面uv`,
    count(distinct d.user_name) as `挽留成功UV`,

    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct case when d.user_name is not null then a.orderNo end) `挽留成功订单量`,
    count(distinct f.order_no) as `实际返现订单量`,
    sum(cb) as `实际返现金额`

from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join wanliu_order d on a.dt=d.dt and a.user_name=d.user_name and a.trace_id=d.trace_id
left join q_cashback f on d.user_name=f.user_name 
where b.version is not null
group by 1,2
order by a.dt desc;


--- 4、负佣转0佣
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select  user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  CONCAT(SUBSTR(a.dt, 1, 4), '-', SUBSTR(a.dt, 5, 2), '-', SUBSTR(a.dt, 7, 2)) AS dt,
            version,
            clientcode AS user_id,
            b.user_name
    FROM default.ods_abtest_sdk_log_endtime_hotel a --user_id
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    WHERE a.dt between '20251211' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页且排查近期大单用户影响
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        --get_json_object(value, '$.ext.button.menu') as menu,
        --get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251211' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,order_all as (
    select order_no,user_name,room_night,init_gmv,
        case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
            end as `佣金`,
        get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as `订单返现`
        ,case when init_payamount_price * 0.04 >= 150 then  150 else init_payamount_price * 0.04 end as `订单原返现4`
        ,case when init_payamount_price * 0.05 >= 150 then  150 else init_payamount_price * 0.05 end as `订单原返现5`
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    --and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
    --and checkout_date >= '2025-09-19' and  checkout_date <= '2025-11-18'
    --and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
)
,q_cashback as (----领取返现红包且未取消订单
    SELECT user_name
        ,order_no
        ,case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)  end as yj
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') as cb
        ,case when init_payamount_price * 0.04 >= 150 then  150 else init_payamount_price * 0.04 end as `原返现4`
        ,case when init_payamount_price * 0.05 >= 150 then  150 else init_payamount_price * 0.05 end as `原返现5`
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
)


select
    a.dt,
    b.version,
    count(distinct a.user_name) as `进入取消页面uv`
    ,count(distinct a.orderNo)  as `进入取消页面订单量`
    ,count(distinct case when `佣金` < 0 then a.orderNo end)  as `取消页面负佣订单量`
    ,sum(case when `佣金` < 0 then `订单原返现4` end)  as `取消页面负佣订单原返现4`
    ,sum(case when `佣金` < 0 then `订单原返现5` end)  as `取消页面负佣订单原返现5`

    ,count(distinct case when (`佣金` < 0 or (`佣金` > 0 and `佣金` - `订单原返现4` < 0)) then  a.orderNo end )  as `取消页面负佣或0佣订单量4`
    ,count(distinct case when (`佣金` < 0 or (`佣金` > 0 and `佣金` - `订单原返现5` < 0)) then  a.orderNo end )  as `取消页面负佣或0佣订单量5`
    ,sum(case when (`佣金` < 0 or (`佣金` > 0 and `佣金` - `订单原返现4` < 0)) then  `订单原返现4` end )  as `取消页面负佣或0佣订单原返现4`
    ,sum(case when (`佣金` < 0 or (`佣金` > 0 and `佣金` - `订单原返现4` < 0)) then  `订单原返现5` end )  as `取消页面负佣或0佣订单原返现5`

    ,count(distinct e.order_no )  as `负佣返现订单量`
    ,count(distinct case when yj < 0 then e.order_no end)  as `负佣返现订单量`
    ,sum(case when yj < 0 then `原返现4` end)  as `负佣返现订单返现4`
    ,sum(case when yj < 0 then `原返现5` end)  as `负佣返现订单返现5`

    ,sum(case when (yj > 0 and yj -  cb < 0 ) then `原返现4` end)  as `0佣返现订单返现4`
    ,sum(case when (yj > 0 and yj -  cb < 0 ) then `原返现5` end)  as `0佣返现订单返现5`
    ,sum(cb) `返现`
from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join order_all f on a.orderNo = f.order_no
left join q_cashback e on a.orderNo = e.order_no
where b.version is not null
group by 1,2
order by a.dt desc
;


--- 5、捕鱼网增量收益
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select  user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  CONCAT(SUBSTR(a.dt, 1, 4), '-', SUBSTR(a.dt, 5, 2), '-', SUBSTR(a.dt, 7, 2)) AS dt,
            version,
            clientcode AS user_id,
            b.user_name
    FROM default.ods_abtest_sdk_log_endtime_hotel a --user_id
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    WHERE a.dt between '20251222' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页且排查近期大单用户影响
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        --get_json_object(value, '$.ext.button.menu') as menu,
        --get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251222' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251222' and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251222' and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2
)
,goFishDetail AS(-- 捕鱼网收益
    select order_date1
        ,order_no
        ,commission_map['settle_base_price_diff']  `转单收益`
    from 
        (select *
            , row_number() over(partition by dt, order_no order by hour desc) rn
            , if(commission_map['order_date'] is null,order_date,commission_map['order_date']) as order_date1
        from ihotel_default.dw_qunar_three_order_detail_intl_hi 
        where dt between date_sub(current_date,30) and date_sub(current_date,1)
            and if(commission_map['order_date'] is null,order_date,commission_map['order_date']) = dt
        ) a
    where rn = 1
        and (province in ('台湾','澳门','香港') or country !='中国') 
        and terminal_channel in ('app')
        and order_status not in('已删除','已经取消','已经拒单')
        --and (a.cancel_time is null or date(a.cancel_time) > a.order_date) and a.order_status not in ('REJECTED')
        and is_valid='1'
        and order_date1= dt
        and commission_map['settle_base_price_diff'] > 0 
        and order_no <> '103576132435'
    group by 1,2,3
)
   
select
    a.dt,
    b.version,

    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct f.orderNo)  as `进入取消页面捕鱼网订单量`,
    count(distinct case when d.user_name is not null then f.orderNo end)   as `触发挽留弹窗捕鱼网订单量`,
    sum(case when d.user_name is not null then f.`转单收益` end) `触发挽留弹窗捕鱼网转单收益`

from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join wanliu_show d on a.dt=d.dt and a.user_name=d.user_name
left join goFishDetail f on a.dt=f.order_date1 and a.orderNo=f.order_no 
where b.version is not null
group by 1,2
order by a.dt desc;



--- 6、底价优势变现订单
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select  user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  CONCAT(SUBSTR(a.dt, 1, 4), '-', SUBSTR(a.dt, 5, 2), '-', SUBSTR(a.dt, 7, 2)) AS dt,
            version,
            clientcode AS user_id,
            b.user_name
    FROM default.ods_abtest_sdk_log_endtime_hotel a --user_id
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    WHERE a.dt between '20251222' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页且排查近期大单用户影响
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        --get_json_object(value, '$.ext.button.menu') as menu,
        --get_json_object(value, '$.ext.exposeLogData') as exposeLogData,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251222' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251222' and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20251222' and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2
)
,q_order as ( --Q间夜量，返后佣金，ADR
    select order_date
        ,order_no
        ,count(distinct hotel_seq) as hotel_num
        ,sum(Q_room_night) as Q_room_night
        ,sum(Q_commission) as Q_commission
        ,sum(case when Q_commission<0 then Q_room_night end) as Q_losing_room_night
        ,sum(case when Q_commission<0 then Q_commission end) as Q_losing_commission
        ,sum(bp_realized*Q_room_night) as `变现提`
        ,sum(beat_amount*Q_room_night) as `定价补`
        ,sum(nvl(frame_amount,0)*Q_room_night)+sum(nvl(framework_amount,0)) as `协议补`
        ,sum(nvl(platform_amount,0)*Q_room_night) as `平台补`
        ,sum(case when supplier_group!='小代理' then nvl(follow_amount,0) end) as `追价补`
        ,sum(coupon_amount) as `券补`
        ,sum(exchange_amount) as `积分补`
      
        ,sum(Q_GMV) as Q_GMV
        ,sum(Q_commission)/sum(Q_GMV) as Q_commission_rate
        ,sum(Q_GMV)/sum(Q_room_night) as Q_ADR
        ,sum(ctrip_commission_amount) as compare_c_commission_amount
        ,sum(qunar_commission_amount) as compare_q_commission_amount
        ,sum(ctrip_before_coupons_cashback_price) as c_sp_sum
        ,sum(qunar_before_coupons_cashback_price) as q_sp_sum
         --,concat(round((sum(bp_advantage_amount)/sum(q_sp_sum))*100,2),"%") as `底价优势率`
        ,sum(bp_advantage_amount) as bp_advantage_amount
        ,sum(case when qta_supplier_id!='1615667' then bp_advantage_amount end) as non_c_bp_advantage_amount
        ,sum(case when qta_supplier_id!='1615667' then bp_advantage_amount_limit20 end) as non_c_bp_advantage_amount_limit20
        ,sum(sp_advantage_amount) as sp_advantage_amount
        ,sum(case when pricing_ccr is not null then Q_GMV*pricing_ccr end) as pricing_c_commission_amount
        ,sum(case when pricing_ccr is not null then Q_GMV end) as pricing_c_gmv
       

    from (
        select 
            order_date 
            ,hotel_grade
            ,hotel_seq
            ,case when supplier_code in ('hca9008oc4l') then 'Ctrip'
                when supplier_code in ('hca908oh60s','hca908oh60t') then 'Agoda'
                when supplier_code in ('hca9008pb7m', 'hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r') then 'Booking'
                when supplier_code in ('hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'EAN'
                when supplier_code in ('hca1f71a00i','hca1f71a00j') then 'HB'
                else '小代理' end as supplier_group
            ,a.order_no
            ,physical_room_name
            ,case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') then (init_commission_after+nvl(coupon_substract_summary ,0)) when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0)) else init_commission_after+nvl(ext_plat_certificate,0) end as Q_commission
            ,room_night as Q_room_night
            ,init_gmv as Q_GMV
            ,nvl(follow_price_amount,0) as follow_amount
            ,case when (coupon_substract_summary is null or batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0 else nvl(coupon_substract_summary,0) end as coupon_amount
            ,get_json_object(extendinfomap,'$.v2_c_incr') as pricing_ccr--定价参考C佣金率
            ,nvl(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) as bp_realized --实际变现底价优势金额（间夜均）
            ,nvl(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) as beat_amount --实际beat金额（间夜均）
            ,get_json_object(extendinfomap,'$.frame_amount') as frame_amount --基础定价协议beat金额（间夜均）
            ,cashbackmap['framework_amount'] as framework_amount   --券补协议后返金额（订单）
            ,get_json_object(extendinfomap,'$.platform_amount') as platform_amount --平台beat金额（间夜均）
            ,NVL(get_json_object(promotion_score_info, '$.deductionPointsInfo.exchangeAmount'), 0) as exchange_amount --积分抵扣金额（订单总）
            
            ,qta_supplier_id
        from 
            default.mdw_order_v3_international a
            --left join q_user_type b on a.user_id = b.user_id 
            --left join q_sugar_order c on a.order_no = c.order_no
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where 
            a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid='1'
            and order_date >= '%(FORMAT_DATE_30)s' and order_date <= '%(FORMAT_DATE)s'
            --and hotel_grade in (1,2,3,4,5)
    ) order_intl
    left join
    (
        select 
            distinct uniq_id,qunar_price_info['orderNum'] as orderNum,--qunar_price_info,qunar_price_info['traceId'] as trace_id,
            qunar_physical_room_name,ctrip_before_coupons_cashback_price,qunar_before_coupons_cashback_price,
            -- 佣金率信息
            qunar_before_coupons_cashback_price-qunar_chased_discount_price as qunar_commission_amount,
            (qunar_before_coupons_cashback_price-qunar_chased_discount_price)/qunar_before_coupons_cashback_price as qunar_commission_rate,
            ctrip_before_coupons_cashback_price-ctrip_discount_base_price as ctrip_commission_amount,
            (ctrip_before_coupons_cashback_price-ctrip_discount_base_price)/ctrip_before_coupons_cashback_price as ctrip_commission_rate,
            -- 底价优势信息
            -chased_discount_price_diff as bp_advantage_amount,
            -chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate,
            case when -chased_discount_price_diff/qunar_before_coupons_cashback_price>0.2 then qunar_before_coupons_cashback_price*0.2 else -chased_discount_price_diff end as bp_advantage_amount_limit20,
            -- 卖价优势信息
            -pay_price_diff as sp_advantage_amount
        from 
            default.dwd_hotel_cq_compare_price_result_intl_hi
        where 
            dt between '20251222' and '%(DATE)s'
            and business_type = 'intl_crawl_cq_api_order'
            and compare_type="PHYSICAL_ROOM_TYPE_LOWEST" --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST--- 物理房型最低价
            and room_type_cover="Qmeet" 
            and ctrip_room_status="true"
            and ctrip_pay_type="预付"
            and qunar_pay_type="预付"
            and qunar_room_status="true"
            --and qunar_price_info['order_product_similar_lowest']="1"
            and substr(uniq_id,1,11) = "h_datacube_"
    ) cq_compare on cq_compare.orderNum = order_intl.order_no and cq_compare.qunar_physical_room_name = order_intl.physical_room_name
    
    group by 1,2
    
)


select
    a.dt,
    b.version,
    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct f.order_no)  as `变现提订单量`,
    sum(`变现提`) `变现提`,
    sum(bp_advantage_amount) bp_advantage_amount, 
    sum(q_sp_sum) q_sp_sum,
    sum(bp_advantage_amount)  /  sum(q_sp_sum) `底价优势率`

from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join wanliu_show d on a.dt=d.dt and a.user_name=d.user_name
left join q_order f on a.dt=f.order_date and a.orderNo=f.order_no 
where b.version is not null
group by 1,2
order by a.dt desc;





,case when province_name in ('澳门','香港') then '港澳'  
      when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
      when a.country_name in ('日本','韩国','泰国') then a.country_name 
    else '其他' end as new_mdd

select dt 
    ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
    ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
    ,case when provincename in ('澳门','香港') then '港澳'  
        when a.countryname in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
        when a.countryname in ('日本','韩国','泰国') then a.countryname 
        else '其他' end as new_mdd
    ,uid
    ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
    ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
    ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
    ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
left join c_user_type b on a.uid=b.ubt_user_id
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
where device_chl='app'
and  dt>= '2024-01-01'  and dt<= date_sub(current_date, 1)
group by 1,2,3,4
;



---- 不同佣金的挽留成功率
with cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
         user_name,
        get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
        ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20260101' AND '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
    group by 1,2,3,4
)
-- ,wanliu_order as ( --- 挽留成功
--     select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
--             user_name,
--             --get_json_object(value, '$.ext.orderNo') as order_no,
--             get_json_object(value, '$.common.traceId') as trace_id
--     from default.dw_qav_ihotel_track_info_di
--     where  dt between '20260101' and '%(DATE)s'
--         and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
--         and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
--     group by 1,2,3
-- )
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20260101' and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2,3
)

,order_all as (
    select order_no,user_name,room_night,init_gmv,order_status,`佣金`,`返现`
          ,case when `佣金` / init_gmv < 0 then '0.负佣' 
               when `佣金` / init_gmv >= 0 and `佣金` / init_gmv <= 0.01 then '1.(0,1%]'
               when `佣金` / init_gmv > 0.01 and `佣金` / init_gmv <= 0.03 then '2.(1%,3%]'
               when `佣金` / init_gmv > 0.03 and `佣金` / init_gmv <= 0.05 then '3.(3%,5%]'
               when `佣金` / init_gmv > 0.05 and `佣金` / init_gmv <= 0.1 then '4.(5%,10%]'
               else '5.(10%+]' end `佣金率分布`

    from (
        select order_no,user_name,room_night,init_gmv,order_status,
            case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                        then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                    else init_commission_after+coalesce(ext_plat_certificate,0)
                end as `佣金`,
            get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as `返现`
        FROM default.mdw_order_v3_international
        WHERE dt = '%(DATE)s'
            and is_valid = 1
            
    )t1
)

select
    a.dt,`佣金率分布`,
    -- count(distinct a.user_name) as `进入取消页面uv`,
    count(distinct d.user_name) as `挽留弹窗曝光UV`,

    -- count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct case when d.user_name is not null then a.orderNo end) `挽留弹窗曝光订单量`,
    count(distinct case when `返现` > 0 and order_status not in ('CANCELLED', 'REJECTED') then f.order_no end) `挽留成功订单量`

from cancel_page a
left join wanliu_show d on a.dt=d.dt and a.user_name=d.user_name and a.trace_id=d.trace_id
left join order_all f on a.orderNo=f.order_no 
group by 1,2
order by a.dt desc;



with q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,a.user_id,init_gmv,order_no,room_night
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
            ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') cb
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid='1'
        and order_date >= '2025-11-09' 
)

SELECT order_date
    ,count(distinct order_no)order_no
    ,count(distinct user_id)uv
    ,sum (final_commission_after) as yj
    ,sum(cb)as cb
    ,count(distinct case when final_commission_after < 0 then order_no end ) b_order_no
    ,sum( case when final_commission_after < 0 then final_commission_after end ) b_yj
    ,sum( case when final_commission_after < 0 then cb end ) b_cb
FROM q_order
WHERE (cb is not null   --- 返现红包大于0
        or cb != 0 
    )
group by 1
order by 1
;