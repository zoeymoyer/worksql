-- 窄口径分码表6
with init_uv as 
(
  select dt as dates ,user_name,a.user_id
  from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
  where dt>= date_sub(current_date,15) and dt<= date_sub(current_date,1)
  and business_type = 'hotel'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and (search_pv+detail_pv+booking_pv+order_pv)>0
  and nvl(user_name ,'')<>'' and user_name is not null and lower(user_name)<>'null'
)
,video as   -- 换成短视频数据
(
select dt,user_name,query,log_time
from (
    select dt
            ,user_name
            ,query
            ,log_time
            ,row_number() over(partition by dt,user_name,query order by log_time desc) rn
    from (
        select  t1.dt,user_name,log_time
            --   ,page
                ,split(t1.query,'_')[0] query
        from
        (
            SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
                ,log_time
            FROM pp_pub.dwd_redbook_global_flow_detail_di t1
            WHERE dt between '%(FORMAT_DATE_SUB_1_M)s' and '%(FORMAT_DATE)s' and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
        ) t1
        inner join
        (
            select
            t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
            FROM pp_pub.dim_video_query_mapping_da t1
            left join (
                select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
                where platform in ('douyin','vedio')
                ) t2
            on t1.query_ori = t2.query
            where dt >=date_sub(current_date,30)
            and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹')
        ) t2
        on t1.query = t2.query
        and t1.dt = t2.dt
        left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
        where t3.query is null
        group by 1,2,3,4
    ) t 
) t where rn = 1
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
  and order_date >= date_sub(current_date,15)
),
init_uv_all as
(
  select dt as order_date ,count(distinct user_id) all_uv
  from ihotel_default.mdw_user_app_log_sdbo_di_v1
  where dt>= date_sub(current_date,15)
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
  and order_date >=date_sub(current_date,15)
  group by 1
)
select
  a.`日期`,
  date_format(a.`日期`,'u')`星期`,
  query,
  `引流UV`,
  concat(round(`引流UV` / all_uv * 100, 1), '%') as `UV占比`,
  `生单用户量`,
  `订单量`,
  concat(round(`订单量` / order_all * 100, 1), '%') as `订单占比`,
  `间夜量`,
  concat(round(`间夜量` / room_night_all * 100, 1), '%') as `间夜占比`,
  concat(round(`订单量` / `引流UV` * 100, 1), '%') as `CR`,
  round(`GMV` / `间夜量`, 0) as `ADR`
from
  (
    select
      uv.dates as `日期`,
      query,
      count(distinct uv.user_id) as `引流UV`,
      count(distinct ord.user_id) as `生单用户量`,
      count(distinct ord.order_no) as `订单量`,
      sum(ord.room_night) as `间夜量`,
      sum(ord.init_gmv) as `GMV`
    from(--- 码末次归因
        select dates,user_id,query,log_time
        from (
            select dates,user_id,query,log_time,row_number() over(partition by dates,user_id order by log_time desc) rn
            from (
                select  uv.dates,uv.user_id,r.query,r.log_time
                from init_uv uv
                left join video r on uv.user_name = r.user_name
                where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates and r.user_name is not null
                group by 1,2,3,4
            ) t 
        ) t where rn = 1
    ) uv
    left join order_a ord on uv.user_id = ord.user_id
    and uv.dates = ord.order_date
    group by 1,2
  ) a
  left join init_uv_all b on a.`日期` = b.order_date
  left join order_all c on a.`日期` = c.order_date
order by `日期` desc
;



---- 表7
with user_type as
(
  select user_id ,min(order_date) as min_order_date
      from default.mdw_order_v3_international
      where dt='%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name !='中国')
      and terminal_channel_type in ('www','app','touch') and is_valid='1'
      and order_status not in ('CANCELLED','REJECTED')
      group by 1
)
,init_uv as
(
  select dt as dates ,user_name,a.user_id
  from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
  left join user_type b on a.user_id = b.user_id
  where dt>= date_sub(current_date,15) and dt<= date_sub(current_date,1)
  and business_type = 'hotel'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and (search_pv+detail_pv+booking_pv+order_pv)>0
  and (dt <= b.min_order_date or b.min_order_date is null) and nvl(user_name ,'')<>'' and user_name is not null and lower(user_name)<>'null'
)
,
video as   -- 换成短视频数据
(
 select dt,user_name,query,log_time
from (
    select dt
            ,user_name
            ,query
            ,log_time
            ,row_number() over(partition by dt,user_name,query order by log_time desc) rn
    from (
        select  t1.dt,user_name,log_time
            --   ,page
                ,split(t1.query,'_')[0] query
        from
        (
            SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
                ,log_time
            FROM pp_pub.dwd_redbook_global_flow_detail_di t1
            WHERE dt between '%(FORMAT_DATE_SUB_1_M)s' and '%(FORMAT_DATE)s' and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
        ) t1
        inner join
        (
            select
            t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
            FROM pp_pub.dim_video_query_mapping_da t1
            left join (
                select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
                where platform in ('douyin','vedio')
                ) t2
            on t1.query_ori = t2.query
            where dt >=date_sub(current_date,30)
            and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹xy（李语乔）')
        ) t2
        on t1.query = t2.query
        and t1.dt = t2.dt
        left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
        where t3.query is null
        group by 1,2,3,4
    ) t 
) t where rn = 1
)

,order_a as
(
  select order_date ,user_id,order_no ,room_night ,init_gmv ,
  case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
            then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            else init_commission_after+nvl(ext_plat_certificate,0) end as final_commission_after,n.user_pk
  from default.mdw_order_v3_international a 
  left join ( select distinct dt as dates,
      user_pk
      from pub.dwd_flow_accapp_potential_user_di
      where dt>= date_sub(current_date,15) and dt<=date_sub(current_date,1)
      and dict_type = 'pncl_wl_username' 
    )n
    on  a.order_date=n.dates and a.user_name=n.user_pk
  where dt='%(DATE)s'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and terminal_channel_type in ('www','app','touch') and is_valid='1'
  and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
  and (first_rejected_time is null or date(first_rejected_time) > order_date)
  and (refund_time is null or date(refund_time) > order_date)
  and order_date>=date_sub(current_date,15) and order_date<=date_sub(current_date,1)
),
init_uv_all as
(
  select dt as order_date ,count(distinct a.user_id) all_uv
  from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
  left join user_type b on a.user_id = b.user_id
  where dt>= date_sub(current_date,15) and dt<= date_sub(current_date,1)
  and business_type = 'hotel'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and (search_pv+detail_pv+booking_pv+order_pv)>0
  and (dt <= b.min_order_date or b.min_order_date is null)
  group by 1
)
,order_all as
(
  select order_date
  ,count(distinct order_no) order_all
  ,count(distinct a.user_id) user_cnt  --大盘新客交易用户
  ,sum(room_night) room_night_all
  from default.mdw_order_v3_international a
  left join user_type b on a.user_id = b.user_id
  where dt='%(DATE)s'
  and (province_name in ('台湾','澳门','香港') or country_name !='中国')
  and terminal_channel_type in ('www','app','touch') and is_valid='1'
  and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
  and (first_rejected_time is null or date(first_rejected_time) > order_date)
  and (refund_time is null or date(refund_time) > order_date)
  and order_date>=date_sub(current_date,15) and order_date<=date_sub(current_date,1)
  and a.order_date=b.min_order_date
  group by 1
)
select
  a.`日期`,
  date_format(a.`日期`,'u')`星期`,
  a.query as `搜索码`,
  `引流新客UV`,
  concat(round(`引流新客UV` / all_uv * 100, 1), '%') as `UV占比`,
  `生单新客量`,
  concat(round(`生单新客量` / user_cnt * 100, 1), '%') as `新客占比`,
  `新客订单量`,
  concat(round(`新客订单量` / order_all * 100, 1), '%') as `订单占比`,
  `新客间夜量`,
  concat(round(`新客间夜量` / room_night_all * 100, 1), '%') as `间夜占比`,
  concat(round(`新客订单量` / `引流新客UV` * 100, 1), '%') as `新客CR`,
  round(`新客GMV` / `新客间夜量`, 0) as `新客ADR`,
  `生单平台新客量`
from
  (
    select
      uv.dates as `日期`,
      uv.query,
      count(distinct uv.user_id) as `引流新客UV`,
      count(distinct ord.user_id) as `生单新客量`,
      count(distinct ord.order_no) as `新客订单量`,
      count(distinct case when ord.user_pk is not null then ord.user_id else null end) as `生单平台新客量`,
      sum(ord.room_night) as `新客间夜量`,
      sum(ord.init_gmv) as `新客GMV`
    from
      (
        select dates,user_id,query,log_time
        from (
            select dates,user_id,query,log_time,row_number() over(partition by dates,user_id order by log_time desc) rn
            from (
                select  uv.dates,uv.user_id,r.query,r.log_time
                from init_uv uv
                left join video r on uv.user_name = r.user_name
                where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates and r.user_name is not null
                group by 1,2,3,4
            ) t 
        ) t where rn = 1
      ) uv
      left join order_a ord on uv.user_id = ord.user_id
      and uv.dates = ord.order_date
    group by 1,2
  ) a
  left join init_uv_all b on a.`日期` = b.order_date
  left join order_all c on a.`日期` = c.order_date
order by `日期` desc