-------整体
with user_type as 
(
    select user_id 
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,init_uv as ----分日去重活跃用户
(
    select dt as dates 
        ,user_name
        ,a.user_id
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join user_type b on a.user_id = b.user_id
    where dt>= '2025-06-01' 
        and dt<= '2025-08-31'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and (search_pv+detail_pv+booking_pv+order_pv)>0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,video as   -- 换成短视频数据
(
    select distinct 
           t1.dt,split(t1.query,'_')[0] as query
           ,user_name,potential_new_flag,page
    from 
    (   --- 小红书引流表
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1 
        WHERE dt between '2025-05-01' and date_sub(current_date, 1)
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1   --- 短视频引流码维表
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd  --- 短视频-query货品cid映射表
            where platform in ('douyin','vedio')
            ) t2 
        on t1.query = t2.query
        where dt >= '2025-05-01'
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)
,order_a as
(
    select order_date 
        ,a.user_id
        ,order_no 
        ,room_night 
        ,init_gmv 
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
            then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0))
            else final_commission_after end as final_commission_after
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,n.user_pk
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id
    left join ( --- 平台新客
        select distinct dt as dates
              ,user_pk
        from pub.dwd_flow_accapp_potential_user_di  
        where dt>= '2025-06-01' and dt<='2025-08-31'
        and dict_type = 'pncl_wl_username' 
    )n  on  a.order_date=n.dates and a.user_name=n.user_pk
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and order_date>='2025-06-01' and order_date<='2025-08-31'
)
,order_b as
(
    select order_date 
        ,a.user_id
        ,order_no 
        ,room_night 
        ,init_gmv 
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
            then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0))
            else final_commission_after end as final_commission_after
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,n.user_pk
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id
    left join ( --- 平台新客
        select distinct dt as dates
              ,user_pk
        from pub.dwd_flow_accapp_potential_user_di  
        where dt>= '2025-06-01' and dt<='2025-08-31'
        and dict_type = 'pncl_wl_username' 
    )n  on  a.order_date=n.dates and a.user_name=n.user_pk
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and order_date>='2025-06-01' and order_date<='2025-08-31'
        and order_status not in ('CANCELLED')
)
,order_ld as
(   ---- 离店佣金
    select a.user_id
        ,order_no 
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
            then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0))
            else final_commission_after end as final_commission_after
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and  checkout_date <= '2025-09-09'
        and  order_status = 'CHECKED_OUT'
)
,init_uv_all as
(  --- 汇总uv
  select dates as order_date 
        ,count(distinct user_id) all_uv
  from init_uv
  group by 1
)
,order_all as
( ---  汇总订单
  select order_date
        ,count(distinct order_no) order_all
        ,count(distinct a.user_id) user_cnt 
        ,sum(room_night) room_night_all
  from order_a a
  group by 1
)


select * from (
select
  a.`日期`,
  date_format(a.`日期`,'u')`星期`,
  a.query as `搜索码`,
  `引流UV`,
  `生单客量`,
  `订单量`,
  `间夜量`,
  concat(round(`订单量` / `引流UV` * 100, 1), '%') as `CR`,
  round(`GMV` / `间夜量`, 0) as `ADR`,
  `生单平台新客量`
  ,`订单佣金`
  ,`离店佣金`

  ,`生单客量_不含取消单`
  ,`订单量_不含取消单`
  ,`间夜量_不含取消单`
  ,concat(round(`订单量_不含取消单` / `引流UV` * 100, 1), '%') as `CR_不含取消单`
  ,round(`GMV_不含取消单` / `间夜量_不含取消单`,0)  as `ADR_不含取消单`
  ,`生单平台新客量_不含取消单`
  ,`订单佣金_不含取消单`
  ,`离店佣金_不含取消单`
from
  (
    select
      uv.dates as `日期`,
      uv.query,
      count(distinct uv.user_id) as `引流UV`,
      count(distinct ord.user_id) as `生单客量`,
      count(distinct ord.order_no) as `订单量`,
      count(distinct case when ord.user_pk is not null then ord.user_id else null end) as `生单平台新客量`,
      sum(ord.room_night) as `间夜量`,
      sum(ord.init_gmv) as `GMV`
      ,sum(ld.final_commission_after) as `离店佣金`
      ,sum(ord.final_commission_after) as `订单佣金`
    from
      (
        select distinct uv.dates,uv.user_id,r.query
        from init_uv uv
        left join video r on uv.user_name = r.user_name
        where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates 
              and r.user_name is not null
              --and uv.user_type = '新客'  --- 用于筛选新客
      ) uv
    left join (select * from  order_a 
               --where user_type = '新客'  --- 用于筛选新客
              ) ord 
        on uv.user_id = ord.user_id and uv.dates = ord.order_date
    left join (select * from  order_ld ) ld 
        on uv.user_id = ld.user_id and ord.order_no = ld.order_no
    group by 1,2
  ) a
  left join (
    select
      uv.dates as `日期`,
      uv.query
      ,count(distinct ord_new.user_id) as `生单客量_不含取消单`
      ,count(distinct ord_new.order_no) as `订单量_不含取消单`
      ,count(distinct case when ord_new.user_pk is not null then ord_new.user_id else null end) as `生单平台新客量_不含取消单`
      ,sum(ord_new.room_night) as `间夜量_不含取消单`
      ,sum(ord_new.init_gmv) as `GMV_不含取消单`
      ,sum(case when ord_new.user_id is not null then  ld.final_commission_after end) as `离店佣金_不含取消单`
      ,sum(ord_new.final_commission_after) as `订单佣金_不含取消单`
    from
      (
        select distinct uv.dates,uv.user_id,r.query
        from init_uv uv
        left join video r on uv.user_name = r.user_name
        where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates 
              and r.user_name is not null
              --and uv.user_type = '新客'  --- 用于筛选新客
      ) uv
    left join (select * from  order_b
               --where user_type = '新客'  --- 用于筛选新客
              ) ord_new
        on uv.user_id = ord_new.user_id and uv.dates = ord_new.order_date
    left join (select * from  order_ld ) ld 
        on uv.user_id = ld.user_id and ord_new.order_no = ld.order_no
    group by 1,2
  ) b on a.`日期`=b.`日期` and a.query=b.query

)t 
where  t.`搜索码` in (177436,874729,875068,423858,944577,182387,561517,339458,726753,375406,970721,916100,407707,266299,581800,728482,799929,402266,607966,226252,395067,308842,233567,288515,144519,154762,212177,319468,320063,323702,431441,431949,570329,537252,507570,665141,665584,748687,748824,871793,892660,991457,989541,991065,376672,801024,872272,237611,730224,620488,805296,160986,287713,505345,487313,163293,163286,485409,266952,448931,867075,426577,460704,323936,179442,275224,542024,347153,539128,868984,171884,367296,172902,795475,533401,287044,896371,727721,177107,177087,177324,448580,733586,303416,846835,694404,548602,436322,340712,232923,722140,537338,745746,237852,278622,717753,213974,242696,369356,905370,247931,556187,861677,352021,757411,893711,225498,846563,812273,593411,730621,616865,419354,402124,481029,253786,986817,407338,556931,251799,349123,428750,963845,684914,259367,259965,436262,348867,821806,923838,667821,324580,611627,910999,849448,436011,287234,369203,450508,181355,622836,252962,481528,677491,172870,212572,344297,175140,781385,176164,397098,305850,17743)  --- 这里改为具体的搜索码

order by `日期` desc
;