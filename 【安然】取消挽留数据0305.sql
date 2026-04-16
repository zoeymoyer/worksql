--- 1、离店口径挽留订单数据
select t1.checkout_date, "实际返现订单量", "实返金额","返现间夜量","返现佣金","返现GMV","离店订单量","离店间夜量","离店佣金","离店GMV"
from (
    SELECT checkout_date
        ,count(distinct order_no) "实际返现订单量"
        ,sum(get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount'))as "实返金额"
        ,sum(room_night)  "返现间夜量"
        ,sum(case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0)  end) as "返现佣金"
        ,sum(init_gmv)  "返现GMV"
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')   --- CHECKED_OUT
    and is_valid = 1
    and (get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null   --- 返现红包大于0
            or get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') != 0 
        )
    and checkout_date >= '2026-02-01' and checkout_date <= date_sub(current_date, 1)
    group by 1
) t1 
left join (
    SELECT checkout_date
        ,count(distinct order_no) "离店订单量"
        ,sum(room_night)  "离店间夜量"
        ,sum(case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0)  end) as "离店佣金"
        ,sum(init_gmv)  "离店GMV"
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')   --- CHECKED_OUT
    and is_valid = 1
    and checkout_date >= '2026-02-01' and checkout_date <= date_sub(current_date, 1)
    group by 1
) t2 on t1.checkout_date=t2.checkout_date
order by 1 
;

--- 2、取消挽留实验数据
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
    select user_name
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
    WHERE a.dt between '20260203' AND '%(DATE)s'
         AND expid = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
         ,user_name
         ,get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
         ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20260203' and '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,get_json_object(value, '$.common.traceId') as trace_id,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20260203' and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2,3
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between '20260203' and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
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
                end as cancel_yj
            ,checkout_date
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
      AND (province_name IN ('台湾', '澳门', '香港') OR country_name != '中国')
      AND terminal_channel_type = 'app'
      AND first_cancelled_time IS NOT NULL
      AND order_status = 'CANCELLED'
      AND is_valid = '1'
      AND order_no <> '103576132435'
      AND DATE(first_cancelled_time) >= '2026-02-03'
      AND DATE(first_cancelled_time) <= date_sub(current_date, 1)
)
,q_cashback as (--- 挽留成功订单-离店
    SELECT substr(cast(ext_flag_map['cancel_red_packet_join_activity_time'] as string), 1, 8) draw_date  ---格式20260203
        ,user_name
        ,order_no
        ,case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0)  end as yj
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as cb
        ,room_night,init_gmv
        
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
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
        and is_valid = 1
)


select
    a.dt,
    b.version,
    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct case when d.user_name is not null then a.orderNo end)  as `进入取消页面展示红包订单量`,
    count(distinct case when e.user_name is not null then a.orderNo end)  as `进入取消页面挽留成功订单量`,
    count (distinct c.order_no) as `取消订单量`,
    count (distinct f.order_no) as `挽留成功订单量(离店)`,
    sum(f.room_night) `挽留成功间夜量(离店)`,
    sum(f.yj) `挽留成功佣金(离店)`,
    sum(f.init_gmv) `挽留成功GMV(离店)`,
    sum(f.cb) `挽留成功返现成本(离店)`
from cancel_page a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join cancelOrder c on a.user_name = c.user_name and a.dt = c.cancelDate and c.order_no = a.orderNo
left join wanliu_show d on a.user_name = d.user_name and a.dt = d.dt
left join wanliu_order e on a.user_name=e.user_name and a.dt=e.dt
left join q_cashback f on a.orderNo=f.order_no and replace(a.dt,'-','')=f.draw_date
where b.version is not null
group by 1,2
order by a.dt;


--- 3、取消挽留实验数据最新-分区域
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
    select user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  dt,
            ab_version version,
            ab_exp_value AS user_id,
            b.user_name
    FROM ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    left join pub.dim_user_profile_nd b on a.ab_exp_value = b.user_id
    WHERE a.dt between date_sub(current_date, 30)  AND date_sub(current_date, 1)
         and type = 'flow'
         and user_id_type = 'user_id' 
        and ab_exp_id = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
         ,user_name
         ,get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
         ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,get_json_object(value, '$.common.traceId') as trace_id,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2,3
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
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
                end as cancel_yj
            ,checkout_date
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
      AND (province_name IN ('台湾', '澳门', '香港') OR country_name != '中国')
      AND terminal_channel_type = 'app'
      AND first_cancelled_time IS NOT NULL
      AND order_status = 'CANCELLED'
      AND is_valid = '1'
      AND order_no <> '103576132435'
      AND DATE(first_cancelled_time) >= '2026-02-03'
      AND DATE(first_cancelled_time) <= date_sub(current_date, 1)
)
,q_cashback as (--- 挽留成功订单-离店
    SELECT substr(cast(ext_flag_map['cancel_red_packet_join_activity_time'] as string), 1, 8) draw_date  ---格式20260203
        ,user_name
        ,order_no
        ,case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0)  end as yj
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as cb
        ,room_night,init_gmv
        
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
)
,order_all as (
    select order_no,user_name,room_night,
        case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
            end as `佣金`,
        get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as `返现`
        ,case when country_name = '日本' then '日本' else '非日本' end is_jp
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
)


select
    a.dt,
    b.version,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp,
    count(distinct a.orderNo)  as `进入取消页面订单量`,
    count(distinct case when d.user_name is not null then a.orderNo end)  as `进入取消页面展示红包订单量`,
    count(distinct case when e.user_name is not null then a.orderNo end)  as `进入取消页面挽留成功订单量`,
    count (distinct c.order_no) as `取消订单量`,
    count (distinct f.order_no) as `挽留成功订单量(离店)`,
    sum(f.room_night) `挽留成功间夜量(离店)`,
    sum(f.yj) `挽留成功佣金(离店)`,
    sum(f.init_gmv) `挽留成功GMV(离店)`,
    sum(f.cb) `挽留成功返现成本(离店)`
from (select t1.*,is_jp from  cancel_page t1 left join order_all t2 on t1.orderNo=t2.order_no) a
left join abtest b on a.user_name = b.user_name and a.dt = b.dt
left join cancelOrder c on a.user_name = c.user_name and a.dt = c.cancelDate and c.order_no = a.orderNo
left join wanliu_show d on a.user_name = d.user_name and a.dt = d.dt and a.trace_id=d.trace_id
left join wanliu_order e on a.user_name=e.user_name and a.dt=e.dt
left join q_cashback f on a.orderNo=f.order_no and replace(a.dt,'-','')=f.draw_date
where b.version is not null
group by 1,2,cube(is_jp)
order by a.dt;



--- 4、最新取消挽留实验数据-20260313
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
    select user_name
    from order_90
    where order_nos_90 >= 10
)
,abtest AS (--- 实验明细
    SELECT  dt,
            ab_version version,
            ab_exp_value AS user_id,
            b.user_name
    FROM ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    left join pub.dim_user_profile_nd b on a.ab_exp_value = b.user_id
    WHERE a.dt between date_sub(current_date, 30)  AND date_sub(current_date, 1)
         and type = 'flow'
         and user_id_type = 'user_id' 
        and ab_exp_id = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
         ,user_name
         ,get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
         ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
      and user_name not in (select user_name from no_user)
    group by 1,2,3,4
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,get_json_object(value, '$.common.traceId') as trace_id,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2,3
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
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
                end as cancel_yj
            ,checkout_date
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
      AND (province_name IN ('台湾', '澳门', '香港') OR country_name != '中国')
      AND terminal_channel_type = 'app'
      AND first_cancelled_time IS NOT NULL
      AND order_status = 'CANCELLED'
      AND is_valid = '1'
      AND order_no <> '103576132435'
      AND DATE(first_cancelled_time) >= '2026-02-03'
      AND DATE(first_cancelled_time) <= date_sub(current_date, 1)
)
,q_cashback as (--- 挽留成功订单-离店
    SELECT substr(cast(ext_flag_map['cancel_red_packet_join_activity_time'] as string), 1, 8) draw_date  ---格式20260203
        ,user_name
        ,order_no
        ,case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0)  end as yj
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as cb
        ,room_night,init_gmv
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid = 1
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
)
,order_all as (---所有订单
    select order_no,user_name,room_night,init_gmv,
        case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
            end as `佣金`,
        get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as `返现`
        ,case when country_name = '日本' then '日本' else '非日本' end is_jp
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
)
,q_order as (---- 大盘预定订单量
    select order_date
            ,count(distinct order_no)  `大盘预定订单量`
            ,sum(room_night) `大盘预定间夜量`
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1
)

select t1.* ,t2.`大盘预定订单量`
from (
    select
        a.dt,
        b.version,
        count(distinct a.orderNo)  as `进入取消页面订单量`,
        count(distinct case when d.user_name is not null then a.orderNo end)  as `进入取消页面展示弹窗订单量`,
        count(distinct case when e.user_name is not null then a.orderNo end)  as `挽留成功订单量(预定)`,
        sum(case when e.user_name is not null then a.room_night end)  as `挽留成功间夜量(预定)`,
        sum(case when e.user_name is not null then a.`佣金` end)  as `挽留成功佣金(预定)`,
        sum(case when e.user_name is not null then a.init_gmv end)  as `挽留成功GMV(预定)`,
        count (distinct c.order_no) as `取消订单量`,
        count (distinct f.order_no) as `挽留成功订单量(离店)`,
        sum(f.room_night) `挽留成功间夜量(离店)`,
        sum(f.yj) `挽留成功佣金(离店)`,
        sum(f.init_gmv) `挽留成功GMV(离店)`,
        sum(f.cb) `挽留成功返现成本(离店)`
    from (select t1.*,is_jp,order_no,room_night,init_gmv, `佣金` from  cancel_page t1 left join order_all t2 on t1.orderNo=t2.order_no) a
    left join abtest b on a.user_name = b.user_name and a.dt = b.dt
    left join cancelOrder c on a.user_name = c.user_name and a.dt = c.cancelDate and c.order_no = a.orderNo
    left join wanliu_show d on a.user_name = d.user_name and a.dt = d.dt and a.trace_id=d.trace_id
    left join wanliu_order e on a.user_name=e.user_name and a.dt=e.dt
    left join q_cashback f on a.orderNo=f.order_no and replace(a.dt,'-','')=f.draw_date
    where b.version is not null
    group by 1,2
)t1 left join q_order t2 on t1.dt=t2.order_date

order by 1;




--- 4、最新取消挽留实验数据-20260414
with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  orig_device_id
    from(
            select
                order_date,
                user_info['orig_device_id'] as orig_device_id,
                count(order_no) as order_nos_90,
                sum(room_night) as room_nights_90
            from mdw_order_v3_international
            where dt = '%(DATE)s'
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2
        )a where room_nights_90>=15
    group by 1
)
,abtest AS (--- 实验明细
    SELECT  dt,
            ab_version version,
            ab_exp_value AS user_id,
            b.user_name
    FROM ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    left join pub.dim_user_profile_nd b on a.ab_exp_value = b.user_id
    WHERE a.dt between date_sub(current_date, 30)  AND date_sub(current_date, 1)
         and type = 'flow'
         and user_id_type = 'user_id' 
        and ab_exp_id = '251210_ho_gj_qxwl'
    group by 1,2,3,4
)
,cancel_page AS ( --- O页取消页
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
         ,user_name
         ,get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
         ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
    -- and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason')
      and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
      and get_json_object(value, '$.ext.button.menu') = '取消订单'
    --   and user_name not in (select user_name from no_user)
      and lower(device_id) not in (select orig_device_id from biguser) --- 新逻辑大单用户过滤
    group by 1,2,3,4
)
,wanliu_show as (--- 挽留弹窗曝光
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,get_json_object(value, '$.common.traceId') as trace_id,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
    and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
    group by 1,2,3
)
,wanliu_order as ( --- 挽留成功
    select  CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt,
            user_name,
            --get_json_object(value, '$.ext.orderNo') as order_no,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace(date_sub(current_date, 30),'-','') and '%(DATE)s'
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
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
                end as cancel_yj
            ,checkout_date
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
      AND (province_name IN ('台湾', '澳门', '香港') OR country_name != '中国')
      AND terminal_channel_type = 'app'
      AND first_cancelled_time IS NOT NULL
      AND order_status = 'CANCELLED'
      AND is_valid = '1'
      AND order_no <> '103576132435'
      AND DATE(first_cancelled_time) >= '2026-02-03'
      AND DATE(first_cancelled_time) <= date_sub(current_date, 1)
)
,q_cashback as (--- 挽留成功订单-离店
    select *
          ,case when yj / init_gmv  < 0 then '0负佣'  
            when yj / init_gmv  >= 0 and yj / init_gmv < 0.03 then '1低佣[0-3%]' 
            when yj / init_gmv  >= 0.03 and yj / init_gmv < 0.1 then '2中佣(3-10%]'
            else '3高佣(10%+]' end as yj_type
          ,case when bp_realized is not null then 'Y' else 'N' end is_bxt
    from (
        SELECT substr(cast(ext_flag_map['cancel_red_packet_join_activity_time'] as string), 1, 8) draw_date  ---格式20260203
            ,user_name
            ,order_no
            ,case  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                    else final_commission_after+coalesce(ext_plat_certificate,0)  end as yj
            ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as cb
            ,room_night,init_gmv
            ,coalesce(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) as bp_realized --实际变现底价优势金额（间夜均）变现提
        FROM default.mdw_order_v3_international
        WHERE dt = '%(DATE)s'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = 1
        and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
    )
)
,order_all as (---所有订单
    select *,case when yj / init_gmv  < 0 then '0负佣'  
            when yj / init_gmv  >= 0 and yj / init_gmv < 0.03 then '1低佣[0-3%]' 
            when yj / init_gmv  >= 0.03 and yj / init_gmv < 0.1 then '2中佣(3-10%]'
            else '3高佣(10%+]' end as yj_type
            ,case when bp_realized is not null then 'Y' else 'N' end is_bxt
    from (
        select order_no,user_name,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
                end as yj,
            get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as fx
            ,case when country_name = '日本' then '日本' else '非日本' end is_jp
            ,coalesce(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) as bp_realized --实际变现底价优势金额（间夜均） 变现提
        FROM default.mdw_order_v3_international
        WHERE dt = '%(DATE)s' and order_date >= '2024-01-01'
    )
)
,q_order as (---- 大盘预定订单量
    select order_date
            ,count(distinct order_no)  `大盘预定订单量`
            ,sum(room_night) `大盘预定间夜量`
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1
)
,goFishDetail AS(-- 捕鱼网收益
    select  order_no
        ,commission_map['settle_base_price_diff']  zdsy  --`转单收益`
        -- ,order_date1
    from (
        select *
            ,row_number() over(partition by dt, order_no order by hour desc) rn
            ,if(commission_map['order_date'] is null,order_date,commission_map['order_date']) as order_date1
        from ihotel_default.dw_qunar_three_order_detail_intl_hi 
        where dt between date_sub(current_date, 30) and date_sub(current_date,1)
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
    group by 1,2
)


select t1.* ,t2.`大盘预定订单量`
from (
    select
        a.dt,
        b.version,a.yj_type,
        count(distinct a.orderNo)  as `进入取消页面订单量`,
        count(distinct case when d.user_name is not null then a.orderNo end)  as `进入取消页面展示弹窗订单量`,

        count(distinct case when e.user_name is not null then a.orderNo end)  as `挽留成功订单量(预定)`,
        sum(case when e.user_name is not null then a.room_night end)  as `挽留成功间夜量(预定)`,
        sum(case when e.user_name is not null then a.yj end)  as `挽留成功佣金(预定)`,
        sum(case when e.user_name is not null then a.init_gmv end)  as `挽留成功GMV(预定)`,
        sum(case when e.user_name is not null then a.fx end)  as `挽留成功返现(预定)`,

        count(distinct case when e.user_name is not null and is_bxt='Y' then a.orderNo end)  as `挽留成功变现提订单量(预定)`,
        sum(case when e.user_name is not null and is_bxt='Y' then a.room_night end)  as `挽留成功变现提间夜量(预定)`,
        sum(case when e.user_name is not null and is_bxt='Y' then a.yj end)  as `挽留成功变现提佣金(预定)`,
        sum(case when e.user_name is not null and is_bxt='Y' then a.init_gmv end)  as `挽留成功变现提GMV(预定)`,
        sum(case when e.user_name is not null and is_bxt='Y' then a.fx end)  as `挽留成功变现提返现(预定)`,

        count(distinct case when g.order_no is not null  then a.orderNo end)  as `挽留成功捕鱼网订单量(预定)`,
        sum(case when g.order_no is not null  then a.room_night end)  as `挽留成功捕鱼网间夜量(预定)`,
        sum(case when g.order_no is not null  then a.yj end)  as `挽留成功捕鱼网佣金(预定)`,
        sum(case when g.order_no is not null  then a.init_gmv end)  as `挽留成功捕鱼网GMV(预定)`,
        sum(case when g.order_no is not null  then a.fx end)  as `挽留成功捕鱼网返现(预定)`,


        count (distinct c.order_no) as `取消订单量`,
        count (distinct f.order_no) as `挽留成功订单量(离店)`,
        sum(f.room_night) `挽留成功间夜量(离店)`,
        sum(f.yj) `挽留成功佣金(离店)`,
        sum(f.init_gmv) `挽留成功GMV(离店)`,
        sum(f.cb) `挽留成功返现成本(离店)`
    from (
        select t1.*,is_jp,order_no,room_night,init_gmv, yj,fx,yj_type,bp_realized
        from  cancel_page t1 
        left join order_all t2 on t1.orderNo=t2.order_no
    ) a
    left join abtest b on a.user_name = b.user_name and a.dt = b.dt
    -- 取消订单
    left join cancelOrder c on a.user_name = c.user_name and a.dt = c.cancelDate and c.order_no = a.orderNo
    -- 挽留弹窗曝光
    left join wanliu_show d on a.user_name = d.user_name and a.dt = d.dt and a.trace_id=d.trace_id
    -- 挽留成功订单（点击领取）
    left join wanliu_order e on a.user_name=e.user_name and a.dt=e.dt
    -- 挽留成功订单（离店）
    left join q_cashback f on a.orderNo=f.order_no and replace(a.dt,'-','')=f.draw_date
    -- 捕鱼网订单
    left join goFishDetail g on a.orderNo=g.order_no
    where b.version is not null
    group by 1,2,3
)t1 
left join q_order t2 on t1.dt=t2.order_date
order by 1;