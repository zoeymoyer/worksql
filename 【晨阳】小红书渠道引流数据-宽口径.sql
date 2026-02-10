---小红书报表SQL1--小红书渠道引流转化量&大盘贡献t-7-宽口径
with init_uv as
(
  select dt as dates ,user_name,user_id
  from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
  where dt>= date_sub(current_date,14) and dt<= date_sub(current_date,1)
  and business_type = 'hotel'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and (search_pv+detail_pv+booking_pv+order_pv)>0
)
,red as
(
  select distinct flow_dt as dt,user_name
  from pp_pub.dwd_redbook_global_flow_detail_di
  where dt between '%(FORMAT_DATE_SUB_1_M)s' and '%(FORMAT_DATE)s'
  --and business_type = 'hotel-inter'  --宽口径不需要这个
  and query_platform = 'redbook'
)
,red_guiyi as (
    select statistic_date,
           count(distinct user_name) as order_user,
           sum(nums) as room_night
       from
          (select distinct statistic_date,order_user_name,is_first_log_new,order_no,business_type
          from pp_pub.ads_redbook_funnel_analysis_detail_di
          where dt>=date_sub(current_date,14)
          and funnel_type in ('order')  -- flow 是流量，不归一
          and platform = 'redbook'
          --and business_type='国际酒店' --宽口径不需要这个字段
          and is_first_log_new='1'
          and group_name='投流'
          )a
          left join
          (select distinct statistic_dt,bu_type,user_name,is_yw_new,order_no,nums
          from pp_pub.ads_redbook_bu_board_info_di
          where dt>=date_sub(current_date,14)
          -- and is_yw_new='1'--判断是否为业务新客
          and bu_type in ('inter_hotel')
          )b
       on a.statistic_date=b.statistic_dt and a.order_user_name=b.user_name and a.order_no=b.order_no
   --where is_yw_new='1'
    group by 1
    order by statistic_date
)
,order_a as
(
  select order_date ,user_id,order_no ,room_night ,init_gmv ,
  case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
            then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            else init_commission_after+nvl(ext_plat_certificate,0) end as final_commission_after
  from default.mdw_order_v3_international
  where dt='%(DATE)s'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and terminal_channel_type in ('www','app','touch') and is_valid='1'
  and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
  and (first_rejected_time is null or date(first_rejected_time) > order_date)
  and (refund_time is null or date(refund_time) > order_date)
  and order_date>=date_sub(current_date,14) and order_date<=date_sub(current_date,1)
),
init_uv_all as
(
  select dt as order_date ,count(distinct user_id) all_uv
  from ihotel_default.mdw_user_app_log_sdbo_di_v1 
  where dt>= date_sub(current_date,15) and dt<= date_sub(current_date,1)
  and business_type = 'hotel'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and (search_pv+detail_pv+booking_pv+order_pv)>0
  group by 1
)
,order_all as
(
  select order_date
  ,count(distinct order_no) order_all
  ,sum(room_night) room_night_all
  from default.mdw_order_v3_international
  where dt='%(DATE)s'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and terminal_channel_type in ('www','app','touch') and is_valid='1'
  and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
  and (first_rejected_time is null or date(first_rejected_time) > order_date)
  and (refund_time is null or date(refund_time) > order_date)
  and order_date>=date_sub(current_date,14) and order_date<=date_sub(current_date,1)
  group by 1
)
select
  a.`日期`,
  date_format(a.`日期`,'u')`星期`,
  `引流UV`,
  concat(round(`引流UV` / all_uv * 100, 1), '%') as `UV占比`,
  `生单用户量`,
  `订单量`,
  concat(round(`订单量` / order_all * 100, 1), '%') as `订单占比`,
  `间夜量`,
  concat(round(`间夜量` / room_night_all * 100, 1), '%') as `间夜占比`,
  concat(round(`订单量` / `引流UV` * 100, 1), '%') as `CR`,
  round(`GMV` / `间夜量`, 0) as `ADR`,
  d.order_user as `归一生单用户量`,
  d.room_night as `归一间夜量`,
  concat(round(d.room_night / room_night_all * 100, 1), '%') as `归一间夜占比`
from
  (
    select
      uv.dates as `日期`,
      count(distinct uv.user_id) as `引流UV`,
      count(distinct ord.user_id) as `生单用户量`,
      count(distinct ord.order_no) as `订单量`,
      sum(ord.room_night) as `间夜量`,
      sum(ord.init_gmv) as `GMV`
    from
      (
        select distinct uv.dates,uv.user_id
        from init_uv uv
        left join red r on uv.user_name = r.user_name
        where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates and r.user_name is not null
      ) uv
      left join order_a ord on uv.user_id = ord.user_id
      and uv.dates = ord.order_date
    group by 1
  ) a
  left join init_uv_all b on a.`日期` = b.order_date
  left join order_all c on a.`日期` = c.order_date
  left join red_guiyi d on a.`日期`=d.statistic_date
order by `日期` desc
