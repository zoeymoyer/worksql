---- sql1 宽口径段视频引流数据 增量口径
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
           ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-04-01'
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt >= '2025-05-01' and dt <= date_sub(current_date,1) 
             and COALESCE(t1.user_name ,'')<>'' 
             and t1.user_name is not null 
             and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
        ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-05-01'
    --   and member_name = '吴卓奇'
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_video as (--- 宽口径短视频分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join video t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,video_data as (--- 增量用户，短视频用户在过往7天未访问国酒页面

    select t1.dt,t1.user_id,t1.user_type,t1.user_type1
        ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
    from user_video t1 
    left join uv1 t2 on t1.user_id=t2.user_id
    and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
    group by 1,2,3,4,5

)
,init_uv_all as
(
    select dt
            ,count(distinct user_id) all_uv
    from uv
    group by 1
)
,order_all as
(
    select order_date
            ,count(distinct order_no) order_all
            ,sum(room_night) room_night_all
    from q_order
    group by 1
)

select a.dt
       ,date_format(a.dt,'%u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from video_data
        where is_bulking = 'Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1
  ) a
  left join init_uv_all b on a.dt = b.dt
  left join order_all c on a.dt = c.order_date
order by a.dt desc
;


---- sql2 宽口径段视频新客引流数据 增量口径
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-04-01'
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
    from uv1
    where dt >= '2025-05-01'
    and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt >= '2025-05-01' and dt <= date_sub(current_date,1) 
             and COALESCE(t1.user_name ,'')<>'' 
             and t1.user_name is not null 
             and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
        ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-05-01'
    --   and member_name = '吴卓奇'
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_video as (--- 宽口径短视频分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join video t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,video_data as (--- 增量用户，短视频用户在过往7天未访问国酒页面

    select t1.dt,t1.user_id,t1.user_type,t1.user_type1
        ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
    from user_video t1 
    left join uv1 t2 on t1.user_id=t2.user_id
    and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
    group by 1,2,3,4,5

)
,init_uv_all as
(
    select dt
        ,count(distinct user_id) all_uv
    from uv where user_type = '新客'
    group by 1
)
,order_all as
(
    select order_date
            ,count(distinct order_no) order_all
            ,count(distinct user_id) user_cnt
            ,sum(room_night) room_night_all
    from q_order where user_type = '新客'
    group by 1
)

select a.dt
       --,date_format(a.dt,'%u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,concat(round(order_uv / user_cnt * 100, 1), '%')  order_uv_rate -- `新客占比`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
       ,new_orders -- `生单平台新客量`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
           , count(distinct case when t3.user_pk is not null then t2.user_id else null end) new_orders
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from video_data
        where user_type = '新客' and is_bulking='Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    left join platform_new t3 on t2.order_date=t3.dt and t2.user_name=t3.user_pk
    group by 1
  ) a
  left join init_uv_all b on a.dt = b.dt
  left join order_all c on a.dt = c.order_date
order by a.dt desc
;


---- sql3 窄口径段视频引流数据 增量口径
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-04-01'
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
    from uv1
    where dt >= '2025-05-01'
    and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt >= '2025-05-01' and dt <= date_sub(current_date,1) 
             and COALESCE(t1.user_name ,'')<>'' 
             and t1.user_name is not null 
             and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
            
        ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-05-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_video as (--- 宽口径短视频分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join video t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,video_data as (--- 增量用户，短视频用户在过往7天未访问国酒页面

    select t1.dt,t1.user_id,t1.user_type,t1.user_type1
        ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
    from user_video t1 
    left join uv1 t2 on t1.user_id=t2.user_id
    and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
    group by 1,2,3,4,5

)
,init_uv_all as
(
    select dt
        ,count(distinct user_id) all_uv
    from uv
    group by 1
)
,order_all as
(
    select order_date
            ,count(distinct order_no) order_all
            ,sum(room_night) room_night_all
    from q_order
    group by 1
)

select a.dt
       ,date_format(a.dt,'%u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from video_data
        where is_bulking = 'Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1
  ) a
left join init_uv_all b on a.dt = b.dt
left join order_all c on a.dt = c.order_date
order by a.dt desc
;


---- sql4 窄口径段视频新客引流数据 增量口径
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-04-01'
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt >= '2025-05-01' and dt <= date_sub(current_date,1) 
             and COALESCE(t1.user_name ,'')<>'' 
             and t1.user_name is not null 
             and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
        ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-05-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_video as (--- 宽口径短视频分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join video t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,video_data as (--- 增量用户，短视频用户在过往7天未访问国酒页面

    select t1.dt,t1.user_id,t1.user_type,t1.user_type1
        ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
    from user_video t1 
    left join uv1 t2 on t1.user_id=t2.user_id
    and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
    group by 1,2,3,4,5

)
,init_uv_all as
(
    select dt
            ,count(distinct user_id) all_uv
    from uv where user_type = '新客'
    group by 1
)
,order_all as
(
    select order_date
            ,count(distinct order_no) order_all
            ,count(distinct user_id) user_cnt
            ,sum(room_night) room_night_all
    from q_order where user_type = '新客'
    group by 1
)

select a.dt
       --,date_format(a.dt,'%u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,concat(round(order_uv / user_cnt * 100, 1), '%')  order_uv_rate -- `新客占比`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
       ,new_orders -- `生单平台新客量`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
           , count(distinct case when t3.user_pk is not null then t2.user_id else null end) new_orders
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from video_data
        where user_type = '新客'
        and is_bulking = 'Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    left join platform_new t3 on t2.order_date=t3.dt and t2.user_name=t3.user_pk
    group by 1
  ) a
left join init_uv_all b on a.dt = b.dt
left join order_all c on a.dt = c.order_date
order by a.dt desc
;



--- 表4   3月至今数据
with user_type -----用户首单日
as (
    select user_id
            ,min(order_date) as min_order_date
            ,count(distinct order_no) history_orders
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv1 as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-02-01'
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,user_type
            ,user_id
            ,user_name
     from uv1
     where dt >= '2025-03-01'
       and dt <= date_sub(current_date, 1)
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt >= '2025-02-01' and dt <= date_sub(current_date,1) 
             and COALESCE(t1.user_name ,'')<>'' 
             and t1.user_name is not null 
             and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
        ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-02-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-03-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,user_video as (--- 宽口径短视频分平台新老用户
    select t1.dt,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from (
        select distinct t1.dt
            ,t1.user_id
            ,t1.user_name
            ,t1.user_type
        from uv  t1
        left join video t2 on t1.user_name = t2.user_name
        where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
        and t2.user_name is not null
    ) t1 left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,video_data as (--- 增量用户，短视频用户在过往7天未访问国酒页面

    select t1.dt,t1.user_id,t1.user_type,t1.user_type1
        ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking   --- 是否增量用户
    from user_video t1 
    left join uv1 t2 on t1.user_id=t2.user_id
    and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
    group by 1,2,3,4,5

)
,init_uv_all as
(
    select dt
            ,count(distinct user_id) all_uv
    from uv where user_type = '新客'
    group by 1
)
,order_all as
(
    select order_date
            ,count(distinct order_no) order_all
            ,count(distinct user_id) user_cnt
            ,sum(room_night) room_night_all
    from q_order where user_type = '新客'
    group by 1
)

select a.dt
       --,date_format(a.dt,'%u') weekday --`星期`
       ,uv  -- `引流UV`
       ,concat(round(uv / all_uv * 100, 1), '%')  uv_rate -- `UV占比`
       ,order_uv -- `生单用户量`
       ,concat(round(order_uv / user_cnt * 100, 1), '%')  order_uv_rate -- `新客占比`
       ,orders -- `订单量`
       ,concat(round(orders / order_all * 100, 1), '%')  order_rate -- `订单占比`
       ,room_night  -- `间夜量`
       ,concat(round(room_night / room_night_all * 100, 1), '%')  roomnight_rate -- `间夜占比`
       ,concat(round(orders / uv * 100, 1), '%')  CR
       ,round(init_gmv / room_night, 0)  ADR 
       ,new_orders -- `生单平台新客量`
from
  (
    select t1.dt
           ,count(distinct t1.user_id) uv
           ,count(distinct t2.user_id) order_uv
           ,count(distinct t2.order_no) orders
           ,sum(t2.room_night) room_night
           ,sum(t2.init_gmv) init_gmv
           , count(distinct case when t3.user_pk is not null then t2.user_id else null end) new_orders
    from ( --- 小红书渠道增量uv口径用户
        select * 
        from video_data
        where user_type = '新客'
        -- and is_bulking = 'Y'
    ) t1
    left join q_order t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date
    left join platform_new t3 on t2.order_date=t3.dt and t2.user_name=t3.user_pk
    group by 1
  ) a
left join init_uv_all b on a.dt = b.dt
left join order_all c on a.dt = c.order_date
order by a.dt desc
;