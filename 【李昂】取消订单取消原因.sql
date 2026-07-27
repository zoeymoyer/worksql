with q_cancel_order as ( --- Q侧取消订单
    select
        checkout_date
        ,case when a.province_name in ('澳门', '香港') then province_name 
              when a.country_name in ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯') then a.country_name 
              when c.area = '亚太' then '亚太'
              when c.area in ('欧洲', '美洲') then '欧美'
              else '其他'
         end as mdd
        ,order_no
        ,order_date
        ,date(first_cancelled_time) as first_cancelled_time
        ,order_cancel_reason
        ,order_status
        ,user_name
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c
        on a.country_name = c.country_name 
    where dt = '20260609'
        and is_valid = 1
        and order_status = 'CANCELLED'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and checkout_date between date_sub(current_date, 60) and date_sub(current_date, 1)
        and order_cancel_reason is not null
)
select
    checkout_date
    ,order_cancel_reason
    ,count(distinct order_no) as order_cnt
    ,sum(count(distinct order_no)) over(partition by checkout_date) as total_order_cnt
    ,concat(round(count(distinct order_no) / sum(count(distinct order_no)) over(partition by checkout_date) * 100, 2), '%') as cancel_rate
from q_cancel_order
group by 1,2
order by checkout_date desc ,order_cnt desc
;