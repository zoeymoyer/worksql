with date_range as (   ---- 生成要看的时间序列
     select date_sub(current_date, n) startdate
     from (
          SELECT explode(sequence(1, 7)) as n
     )
)

select t1.startdate
       ,count(distinct o_qunarusername)
       ,count(distinct flight_order_no)
from date_range t1
left join (
     select to_date(create_time) as create_date 
          ,o_qunarusername 
          ,biz_order_no as flight_order_no
     from f_fuwu.dw_fact_inter_order_wide 
     where dt>='2025-01-01'                   -- 随UV主表时间调整 
     and substr(create_time,1,10)>='2025-01-01' -- 随UV主表时间调整
     and ticket_time is not null 
     and refund_complete_time is null -- 已出票未退款
     and platform <> 'fenxiao' -- 去分销
     and (s_arrcountryname !='中国' or s_depcountryname !='中国')
)t2 ON t2.create_date >= date_sub(t1.startdate, 14)  --- 取最近15天数据
AND t2.create_date <= t1.startdate 
group by 1
order by 1
;