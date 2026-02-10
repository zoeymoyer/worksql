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
group by 1,2,3

)

select t1.dt,
    count (distinct t1.user_name) as `领取红包用户数`,
    count (distinct t1.order_no) as `领取红包订单量`,
    sum (t2.`新佣金`) as `领取红包佣金`,
    sum (t2.`返现`) as `领取红包金额`
from (
    select a.dt,a.user_name,b.order_no
    from wanliu_order a
    left join cancel_page b on a.trace_id = b.trace_id and a.user_name = b.user_name and a.dt = b.dt
    group by 1,2,3
) t1 
left join q_order t2 on t1.order_no = t2.order_no 
group by 1
order by t1.dt desc
;


--  1. 转化漏斗用前端埋点来跑：
   -- 1. 进入取消原因页：key ='ihotel/OrderDetail/cancelReason/show/cancelReason'，value中的ext.orderNo代表订单号
   -- 2. 弹出挽留弹窗：key ='ihotel/OrderDetail/cancelReason/show/cancelBlock'，如果想限制领取红包的弹窗，条件为value中的scene = 'unclaimed' and trendType in ('cash','all')，并可以用value中的traceId关联进入取消页的埋点来关联订单号
   -- 3. 挽留成功：key ='ihotel/OrderDetail/cancelReason/click/cancelBlocked'，点击收下不取消，如果想限制领取红包的弹窗，条件为value中的scene = 'unclaimed' and trendType in ('cash','all')，并可以用value中的traceId关联进入取消页的埋点来关联订单号
            --key= 'ihotel/OrderDetail/cancelReason/click/cancelConfirmed'点击坚持取消
   --场景：trendType = cash (红包) / point（积分）/ all（红包+积分），是否已领取：scene = claimed（已领取）/unclaimed（未领取）
   --4、订单详情页点击按钮，ihotel/OrderDetail/OrderInfo/click/actionBtn，其中get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') 点击订单号；get_json_object(value, '$.ext.button.menu') = '取消订单'对应取消的button
   --5、进入取消原因页：ihotel/CancelReason/Page/show/pageShow， 无订单号
   --6、 取消挽留订单相关的字段存在订单表的cancel_red_packet_data_track_map中，其中的字段含义参考【FD-362883】取消环节红包挽留中的埋点字段，核心就是actual_cash_back_amount，即返现金额。

select 
dt,
case when key = 'ihotel/CancelReason/Page/show/pageShow' then '进入取消原因页'
   when key = 'ihotel/OrderDetail/cancelReason/show/cancelReason' then '取消成功页面'
    when key = 'ihotel/OrderDetail/cancelReason/show/cancelBlock' then '弹出挽留弹窗'
    when key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked' then '挽留成功'
    when key = 'ihotel/OrderDetail/cancelReason/click/cancelConfirmed' then '坚持取消'
    end as key,
count (distinct user_name) as uv
--get_json_object(value, '$.ext.orderNo') 
from default.dw_qav_ihotel_track_info_di
where 
dt between '20251106' and '20251120'
and key in ('ihotel/OrderDetail/cancelReason/show/cancelReason','ihotel/OrderDetail/cancelReason/show/cancelBlock','ihotel/OrderDetail/cancelReason/click/cancelBlocked','ihotel/OrderDetail/cancelReason/click/cancelBlocked','ihotel/OrderDetail/cancelReason/click/cancelConfirmed','ihotel/CancelReason/Page/show/pageShow')
group by 1,2
order by dt desc