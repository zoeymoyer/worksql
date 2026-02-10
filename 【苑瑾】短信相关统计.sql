-- 短信相关统计
select a.dt
,a.task_id
,a.son_abvalue    -- 分组
,count(distinct a.user_name) as `计划发送成功用户数`
,count(distinct b.user_name) as `计划到端活跃用户数`
,count(distinct c.user_name) as `计划下单用户数`
,count(distinct c.order_no) as `计划Q订单量`
,count(distinct case when a.status='0' then a.user_name else null end) as `发送成功用户数`
,count(distinct case when a.status='0' then b.user_name else null end) as `到端活跃用户数`
,count(distinct case when a.status='1' then a.user_name else null end) as `未发送成功用户数`
,count(distinct case when a.status='1' then b.user_name else null end) as `未收到短信且到端活跃用户数`
,count(distinct case when a.status='0' then c.user_name else null end) as `下单用户数`
,count(distinct case when a.status='0' then c.order_no else null end) as `Q订单量`
from
(select distinct dt,son_abvalue,user_name,status,task_id
from pp_pub.ods_push_f_apollo_x_union_contact_log_orc_di
where dt >= '2025-10-18'
and template_type_code = 'qunarSms'
and task_id in ('286811','285851','286064','286049','286034','286022','286016','285848')
and son_abcode in ('241108_ho_gj_GJpushtest')
-- and status = '0'
)a
left join
(select distinct user_id,user_name,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dates
from mdw_user_app_sdbo_di_v3
where dt >='20251018'
and business_type = 'hotel'
and (province_name in ('台湾','澳门','香港') or country_name !='中国')
and (search_pv + detail_pv + booking_pv + order_pv)>0
and trim(user_name) not in ('','NULL','null')
and user_name not in ('','NULL','null') and user_name is not null           
)b
on a.user_name = b.user_name and a.dt=b.dates
left join
(select distinct user_id,user_name,order_no,order_date
 from mdw_order_v3_international
 where dt ='%(DATE)s'
   and terminal_channel_type in ('www','app','touch')
   and (province_name in ('台湾','澳门','香港') or country_name !='中国')
 --and order_status not in ('CANCELLED','REJECTED')
   and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
   and (first_rejected_time is null or date(first_rejected_time) > order_date) 
  and (refund_time is null or date(refund_time) > order_date)
   and is_valid='1'
   and order_date >='2025-10-18'
)c
on a.user_name = c.user_name and a.dt=c.order_date
group by 1,2,3
;



select a.dt
,a.task_id
,a.son_abvalue    -- 分组
,b.user_name
,count(distinct status)
from
(select distinct dt,son_abvalue,user_name,status,task_id
from pp_pub.ods_push_f_apollo_x_union_contact_log_orc_di
where dt >= '2025-10-18'
and template_type_code = 'qunarSms'
and task_id in ('286811','285851','286064','286049','286034','286022','286016','285848')
and son_abcode in ('241108_ho_gj_GJpushtest')
)a
left join
(select distinct user_id,user_name,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dates
from mdw_user_app_sdbo_di_v3
where dt >='20251018'
and business_type = 'hotel'
and (province_name in ('台湾','澳门','香港') or country_name !='中国')
and (search_pv + detail_pv + booking_pv + order_pv)>0
and trim(user_name) not in ('','NULL','null')
and user_name not in ('','NULL','null') and user_name is not null           
)b
on a.user_name = b.user_name and a.dt=b.dates
group by 1,2,3,4