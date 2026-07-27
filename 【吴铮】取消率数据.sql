with q_data as ( -- Q取消率数据
    select order_date
        ,case when province_name in ('澳门','香港') then province_name when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when a.country_name = '日本' then '日本' else '非日本' end is_jp
        ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
        ,order_status,init_gmv,room_night
        ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date)
               and (first_rejected_time is null or date(first_rejected_time) > order_date)
               and (refund_time is null or date(refund_time) > order_date)
              then 'Y' else 'N' end is_not_cancel_d0
        ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date)
               or (order_status = 'REJECTED' and date(first_rejected_time) > order_date)
               or (refund_time is not null and date(refund_time) > order_date)
              then 'Y' else 'N' end is_cancel_d0
        ,case when order_status = 'CANCELLED' and date(first_cancelled_time) > order_date then date(first_cancelled_time)
              when order_status = 'REJECTED' and date(first_rejected_time) > order_date then date(first_rejected_time)
              when refund_time is not null and date(refund_time) > order_date then date(refund_time)
         end as cancel_date
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
        on a.country_name = e.country_name
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2026-06-01' and date_sub(current_date, 1)
)
,c_data as ( --- C取消率数据
    select substr(o.checkout_date, 1, 10) as checkout_date
        ,substr(order_date,1,10) as order_date
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
              when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
              when c.area in ('欧洲','亚太','美洲') then c.area
              else '其他' end as mdd
        ,case when extend_info['COUNTRY'] = '日本' then '日本' else '非日本' end is_jp
        ,o.user_id,order_no,room_fee,order_status
        ,extend_info['room_night'] room_night
        ,case when o.extend_info['CANCEL_TIME'] is null or o.extend_info['CANCEL_TIME'] = 'NULL' or substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0
        ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
        ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
        ,case when (order_status = 'C' and substr(o.extend_info['CANCEL_TIME'],1,10) > substr(order_date,1,10))
              then 'Y' else 'N' end is_cancel_d0
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c
        on extend_info['COUNTRY'] = c.country_name
    where o.dt = '%(FORMAT_DATE)s'
        and o.extend_info['IS_IBU'] = '0'
        and o.extend_info['book_channel'] = 'Ctrip'
        and o.extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and o.terminal_channel_type = 'app'
        and substr(o.checkout_date, 1, 10) between '2026-06-01' and date_sub(current_date, 1)
)
,week_base as (
    select order_date
        ,date_sub(order_date, pmod(datediff(order_date, '2018-06-29'), 7)) as week_start_date
        ,date_add(date_sub(order_date, pmod(datediff(order_date, '2018-06-29'), 7)), 6) as week_end_date
    from (
        select order_date from q_data
        union
        select order_date from c_data
    ) t
)
,q_base as (
    select w.week_start_date
        ,w.week_end_date
        ,case when q.is_cancel_d0 = 'Y' and datediff(q.cancel_date, q.order_date) between 1 and 7 then '1周内'
              when q.is_cancel_d0 = 'Y' and datediff(q.cancel_date, q.order_date) between 8 and 14 then '1-2周'
              when q.is_cancel_d0 = 'Y' and datediff(q.cancel_date, q.order_date) between 15 and 21 then '2-3周'
              when q.is_cancel_d0 = 'Y' and datediff(q.cancel_date, q.order_date) between 22 and 28 then '3-4周'
              when q.is_cancel_d0 = 'Y' and datediff(q.cancel_date, q.order_date) > 28 then '4周以上'
              else '未取消' end as cancel_period
        ,count(distinct q.order_no) as `Q订单`
        ,count(distinct case when q.is_not_cancel_d0 = 'Y' then q.order_no end) as `Q未取消订单_当日`
        ,count(distinct case when q.is_cancel_d0 = 'Y' then q.order_no end) as `Q取消订单_当日`
        ,count(distinct case when q.is_cancel_d0 = 'Y' then q.order_no end) / count(distinct case when q.is_not_cancel_d0 = 'Y' then q.order_no end) as `Q取消率`
    from q_data q
    left join week_base w
        on q.order_date = w.order_date
    group by 1,2,3
)
,c_base as (
    select w.week_start_date
        ,w.week_end_date
        ,case when c.is_cancel_d0 = 'Y' and datediff(c.cancel_date, c.order_date) between 1 and 7 then '1周内'
              when c.is_cancel_d0 = 'Y' and datediff(c.cancel_date, c.order_date) between 8 and 14 then '1-2周'
              when c.is_cancel_d0 = 'Y' and datediff(c.cancel_date, c.order_date) between 15 and 21 then '2-3周'
              when c.is_cancel_d0 = 'Y' and datediff(c.cancel_date, c.order_date) between 22 and 28 then '3-4周'
              when c.is_cancel_d0 = 'Y' and datediff(c.cancel_date, c.order_date) > 28 then '4周以上'
              else '未取消' end as cancel_period
        ,count(distinct c.order_no) as `C订单`
        ,count(distinct case when c.is_not_cancel_d0 = 'Y' then c.order_no end) as `C未取消订单_当日`
        ,count(distinct case when c.is_cancel_d0 = 'Y' then c.order_no end) as `C取消订单_当日`
        ,count(distinct case when c.is_cancel_d0 = 'Y' then c.order_no end) / count(distinct case when c.is_not_cancel_d0 = 'Y' then c.order_no end) as `C取消率`
    from c_data c
    left join week_base w
        on c.order_date = w.order_date
    group by 1,2,3
)
select coalesce(t1.week_start_date, t2.week_start_date) as `周起始日期`
    ,coalesce(t1.week_end_date, t2.week_end_date) as `周终止日期`
    ,concat(
        date_format(coalesce(t1.week_start_date, t2.week_start_date), 'MMdd')
        ,'~'
        ,date_format(coalesce(t1.week_end_date, t2.week_end_date), 'MMdd')
    ) as `周标签`
    ,coalesce(t1.cancel_period, t2.cancel_period) as `取消时间-预定时间`
    ,`Q订单`
    ,`C订单`
    ,`Q取消率`
    ,`C取消率`
    ,round(`Q取消率` / `C取消率` * 100, 2) as `取消率QC`
    ,`Q未取消订单_当日`
    ,`Q取消订单_当日`
    ,`C未取消订单_当日`
    ,`C取消订单_当日`
from q_base t1
full outer join c_base t2
    on t1.week_start_date = t2.week_start_date
    and t1.week_end_date = t2.week_end_date
    and t1.cancel_period = t2.cancel_period
order by `周起始日期` desc
    ,`取消时间-预定时间`
;