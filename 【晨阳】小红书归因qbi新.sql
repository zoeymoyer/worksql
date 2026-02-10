---小红书报表SQL1--小红书渠道引流转化量&大盘贡献t-7-宽口径
with init_uv as
(
  select dt as dates ,user_name,user_id
  from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
  where dt>= '2025-09-01' and dt<= date_sub(current_date,1)
  and business_type = 'hotel'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and (search_pv+detail_pv+booking_pv+order_pv)>0
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '2025-08-01' and date_sub(current_date,1)
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,user_type -----新老客
as (
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt =  '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,order_a as
(
  select order_date ,a.user_id,order_no ,room_night ,init_gmv
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
  from default.mdw_order_v3_international a
  left join user_type b on a.user_id = b.user_id 
  where dt= '%(DATE)s'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and terminal_channel_type in ('www','app','touch') and is_valid='1'
  and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
  and (first_rejected_time is null or date(first_rejected_time) > order_date)
  and (refund_time is null or date(refund_time) > order_date)
  and order_date>='2025-09-01' and order_date<=date_sub(current_date,1)
)
,user_xhs as (
select distinct uv.dates,uv.user_id,uv.user_name
from init_uv uv
left join red r on uv.user_name = r.user_name
where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates and r.user_name is not null

)

,read_uv as (  --- 每日小红书引流用户-首次归因业务
  select flow_dt
          ,user_name
          ,business_name
  from (
      select  flow_dt
              ,user_name
              ,business_name
              ,log_time
              ,row_number() over(partition by flow_dt,user_name order by log_time) rn
      from pp_pub.dwd_redbook_global_flow_detail_di
      where dt >= date_sub('2025-08-01', 8)
        --   and business_type = 'hotel-inter'
          and query_platform = 'redbook'
  ) t where rn = 1
)
,xhs_channel as ( --- 每日小红书引流用户-近7天引流中首次归因业务
  select flow_dt,user_name
          ,case when business_name = '酒店' then '酒店'
                when business_name = '机票' then '机票'
                when business_name = '国际酒店' then '国际酒店'
                when business_name = '国际机票' then '国际机票'
                when business_name = '门票' then '门票'
                else '其他' end business_name
          ,row_number() over(partition by user_name order by flow_dt desc) rn
  from (
      select 
          t1.flow_dt
          ,t1.user_name
          ,t1.business_name
          ,t2.flow_dt as dt
          ,row_number() over(partition by t1.flow_dt, t1.user_name order by t2.flow_dt) rn 
      from read_uv t1 
      left join read_uv t2 on t1.user_name = t2.user_name
      and  t2.flow_dt >= date_sub(t1.flow_dt, 7)
      and  t2.flow_dt <= t1.flow_dt
  ) t where rn = 1
)

select t1.`日期`
      ,t1.business_name
      ,uv `引流uv`
      ,concat(round(uv / sum(uv) over(partition by t1.`日期`) * 100,2), '%') `引流uv占比`
      ,`新客订单uv`
      ,concat(round(`新客订单uv` / sum(`新客订单uv`) over(partition by t1.`日期`) * 100,2),'%')  `新客订单uv占比`
      ,`新客订单量`
      ,concat(round(`新客订单量` / sum(`新客订单量`) over(partition by t1.`日期`) * 100,2),'%')  `新客订单量占比`
      ,`订单uv`
      ,concat(round(`订单uv` / sum(`订单uv`) over(partition by t1.`日期`) * 100,2),'%')  `订单uv占比`
      ,`订单量`
      ,concat(round(`订单量` / sum(`订单量`) over(partition by t1.`日期`) * 100,2),'%')  `订单量占比`
from (
    select uv.dates as `日期`,business_name,
        count(distinct uv.user_id) as uv,
        count(distinct ord.user_id) as `订单uv`,
        count(distinct ord.order_no) as `订单量`,
        sum(ord.room_night) as `间夜量`
        ,count(distinct case when ord.user_type = '新客' then ord.user_id end) as `新客订单uv`
        ,count(distinct case when ord.user_type = '新客' then ord.order_no end) as `新客订单量`
    from (
    select uv.dates,uv.user_id,max(business_name) business_name
    from user_xhs uv 
    left join (select user_name,business_name from xhs_channel where rn =1 ) t2 on uv.user_name = t2.user_name
    group by 1,2
    ) uv
        left join order_a ord on uv.user_id = ord.user_id and uv.dates = ord.order_date
    group by 1,2
) t1
order by t1.`日期` desc
;