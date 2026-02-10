with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt ='20260103'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_uv as (
        -- select distinct dt
        --         ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        --         ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
        --                     when e.area in ('欧洲','亚太','美洲') then e.area
        --                     else '其他' end as mdd
        --         ,a.user_id
        -- from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        -- left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        -- left join user_type b on a.user_id = b.user_id 
        -- where dt >='2025-01-01'
        --     and business_type = 'hotel'
        --     and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        --     and (search_pv + detail_pv + booking_pv + order_pv) > 0
        --     and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        --     and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        --     and fromforlog in ('4104','4106') 
select distinct dt 
      ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
      ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
      ,a.user_id,user_name,max(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) action_time
from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
left join user_type b on a.user_id = b.user_id 
where dt >='2025-01-01'
and business_type = 'hotel'
and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
and action_entrance_map['fromforlog'] in ('4104','4106') 
        )
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,order_time
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'
        and order_no <> '103576132435'
)


select t1.dt
      ,uv
      ,concat(round(order_no/uv *100, 2), '%') cr
      ,concat(round(order_no_conpon / order_no *100, 2), '%') order_no_conpon  -- 用券订单占比
      ,order_no    --- 订单量
      ,room_night  --- 间夜量
      ,nuv   --- 新客
      ,concat(round(nuv/uv *100, 2), '%') nrate --- 新客占比
      ,concat(round(order_no_nu/nuv *100, 2), '%') ncr --- 新客cr
      ,concat(round(order_no_conpon_nu/order_no_nu *100, 2), '%')  order_no_conpon_nu --- 新客用券订单占比
      ,order_no_nu     --- 新客订单量
      ,room_night_nu   --- 新客间夜量
      ,order_no_t0
      ,room_night_t0
      ,order_no_conpon_t0
      ,order_no_nu_t0
      ,room_night_nu_t0
      ,order_no_conpon_nu_t0
from (
    select dt
          ,count(user_id) uv
          ,count(case when user_type = '新客' then user_id end) nuv
    from q_uv
    group by 1
)t1 
left join (--- T-7
    select order_date
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon
          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu
    from q_app_order t1 
    left join (
        select dt,user_id
        from q_uv 
        group by 1,2
    )t2 on t1.user_id=t2.user_id
      where t2.user_id is not null and t2.dt >= date_sub(t1.order_date, 7) and t2.dt <= t1.order_date
    group by 1
)t2 on t1.dt=t2.order_date
left join (--- T0
    select order_date
          ,count(distinct t1.order_no) order_no_t0
          ,sum(t1.room_night) room_night_t0
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon_t0
          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu_t0
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu_t0
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu_t0
    from q_app_order t1 
    left join (
        select dt,user_id,action_time
        from q_uv 
        group by 1,2,3
    )t2 on t1.user_id=t2.user_id and t1.order_date=t2.dt and t1.order_time > t2.action_time
    where t2.user_id is not null
    group by 1
)t3 on t1.dt=t3.order_date

order by 1
;

--- 话题数据
SELECT T2_1.travel_name, --话题名称
       T2_1.travel_id, --话题id
       COUNT(DISTINCT T2_3.global_key) AS detail_show_gk_num, --话题下帖子曝光数
       COUNT(DISTINCT T2_3.uid) AS detail_show_uv, --话题下帖子曝光uv
       SUM(COALESCE(T2_4.show_pv, 0)) AS product_show_pv, --商卡曝光pv
       COUNT(DISTINCT IF(COALESCE(T2_4.show_pv, 0) > 0, T2_4.uid, NULL)) AS product_show_uv, --商卡曝光uv
       COUNT(DISTINCT IF(COALESCE(T2_4.show_pv, 0) > 0, T2_4.global_key, NULL)) AS product_gk_show_gk_num, --有国酒商卡曝光帖子数
       SUM(COALESCE(T2_4.click_pv, 0)) AS product_click_pv, --商卡点击pv
       COUNT(DISTINCT IF(COALESCE(T2_4.click_pv, 0) > 0, T2_4.uid, NULL)) AS product_click_uv, --商卡点击pv
       COUNT(DISTINCT IF(COALESCE(T2_4.click_pv, 0) > 0, T2_4.global_key, NULL)) AS product_gk_click_gk_num --有国酒商卡点击帖子数
FROM (
    SELECT DISTINCT
            T.travel_name,
            T.travel_id,
            T.feed_id
        FROM c_desert_feed.ods_content_data_topic_info T
        WHERE T.dt = '2025-12-21'
        AND DATEDIFF('2025-12-21',SUBSTR(T.feed_effective_end_time, 1, 10)) < 16
        AND T.feed_effective_start_time >= '2025-09-01' --日期不需要改 限制20250901日期
        AND COALESCE(T.travel_name, '') <> ''
) T2_1
JOIN (
    SELECT DISTINCT
            T.global_key,
            T.topic_id
    FROM c_desert_feed.ods_feedstream_qulang_footprint_detail_tag_info T
    WHERE T.dt = '20251222'
) T2_2 ON T2_1.feed_id = T2_2.topic_id
JOIN (
    SELECT T2_3_1.global_key,
            T2_3_1.uid
    FROM (SELECT T.dt,
                T.global_key,
                T.uid
            FROM c_desert_feed.dwd_content_flow_user_gk_day_active_di T
            WHERE T.dt >= '2025-09-01'
            AND T.is_gk_detail_show = 1) T2_3_1
    LEFT JOIN (SELECT MIN(SUBSTR(T.feed_effective_start_time, 1, 10)) AS min_effective_date
                FROM c_desert_feed.ods_content_data_topic_info T
                WHERE T.dt = '2025-12-21'
                AND DATEDIFF('2025-12-21',SUBSTR(T.feed_effective_end_time, 1, 10)) < 16
                AND T.feed_effective_start_time > '2025-01-01'
                AND COALESCE(T.travel_name, '') <> '') T2_3_2
    ON T2_3_1.dt >= T2_3_2.min_effective_date
    GROUP BY T2_3_1.global_key,
                T2_3_1.uid
) T2_3 ON T2_2.global_key = T2_3.global_key
LEFT JOIN (
    SELECT T2_4_1.*
    FROM (
        SELECT T.dt,
                T.user_name,
                T.uid,
                T.biz_type,
                T.global_key,
                T.seq,
                T.show_pv,
                T.click_pv
        FROM c_desert_feed.dwd_content_flow_detailpage_gk_user_product_index_di T
        WHERE T.dt >= '2025-09-01'
        AND T.biz_type = 'ihotel'
    )T2_4_1
    LEFT JOIN (
        SELECT MIN(SUBSTR(T.feed_effective_start_time, 1, 10)) AS min_effective_date
        FROM c_desert_feed.ods_content_data_topic_info T
        WHERE T.dt = '2025-12-21'
            AND DATEDIFF('2025-12-21',SUBSTR(T.feed_effective_end_time, 1, 10)) < 16
            AND T.feed_effective_start_time > '2025-01-01'
            AND COALESCE(T.travel_name, '') <> ''
    ) T2_4_2
        ON T2_4_1.dt >= T2_4_2.min_effective_date
) T2_4
ON T2_3.global_key = T2_4.global_key
AND T2_3.uid = T2_4.uid
GROUP BY T2_1.travel_name,
          T2_1.travel_id


----- 新的帖子数据
with topic_info as (---
    select travel_name,travel_id,feed_id,global_key
    FROM (
        SELECT T.travel_name,  --- 话题名称
                T.travel_id,   --- 话题ID
                T.feed_id
            FROM c_desert_feed.ods_content_data_topic_info T
            WHERE T.dt = '2025-12-21'
            AND DATEDIFF('2025-12-21',SUBSTR(T.feed_effective_end_time, 1, 10)) < 16
            AND T.feed_effective_start_time >= '2025-09-01' --日期不需要改 限制20250901日期
            AND COALESCE(T.travel_name, '') <> ''
            GROUP BY 1,2,3
    ) T2_1
    JOIN (
        SELECT T.global_key,
                T.topic_id
        FROM c_desert_feed.ods_feedstream_qulang_footprint_detail_tag_info T
        WHERE T.dt = '20251222'
        GROUP BY 1,2
    ) T2_2 ON T2_1.feed_id = T2_2.topic_id
)
,exp_info as (--- 帖子曝光数据
    SELECT T2_3_1.global_key,
            T2_3_1.uid,dt
    FROM (SELECT T.dt,
                T.global_key,
                T.uid
            FROM c_desert_feed.dwd_content_flow_user_gk_day_active_di T
            WHERE T.dt >= '2025-09-01'
            AND T.is_gk_detail_show = 1) T2_3_1
    LEFT JOIN (SELECT MIN(SUBSTR(T.feed_effective_start_time, 1, 10)) AS min_effective_date  --- 取值2025-10-14
                FROM c_desert_feed.ods_content_data_topic_info T
                WHERE T.dt = '2025-12-21'
                AND DATEDIFF('2025-12-21',SUBSTR(T.feed_effective_end_time, 1, 10)) < 16
                AND T.feed_effective_start_time > '2025-01-01'
                AND COALESCE(T.travel_name, '') <> '') T2_3_2
    ON T2_3_1.dt >= T2_3_2.min_effective_date
    GROUP BY 1,2,3
)
,click_info as (--- 帖子点击数据
    SELECT T2_4_1.dt,user_name,uid,global_key
    FROM (
        SELECT T.dt,
                T.user_name,
                T.uid,
                T.global_key
        FROM c_desert_feed.dwd_content_flow_detailpage_gk_user_product_index_di T
        WHERE T.dt >= '2025-09-01'
        AND T.biz_type = 'ihotel'
        and click_pv > 0 
    )T2_4_1
    LEFT JOIN (
        SELECT MIN(SUBSTR(T.feed_effective_start_time, 1, 10)) AS min_effective_date  --- 取值2025-10-14
        FROM c_desert_feed.ods_content_data_topic_info T
        WHERE T.dt = '2025-12-21'
            AND DATEDIFF('2025-12-21',SUBSTR(T.feed_effective_end_time, 1, 10)) < 16
            AND T.feed_effective_start_time > '2025-01-01'
            AND COALESCE(T.travel_name, '') <> ''
    ) T2_4_2
    ON T2_4_1.dt >= T2_4_2.min_effective_date
    group by 1,2,3,4
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,user_name
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,order_time
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'
        and order_no <> '103576132435'
)
,click_uv as (
    select t1.dt,travel_name,travel_id,t2.user_name,t1.uid
    from (
        SELECT t1.dt,travel_name,travel_id,uid
        FROM exp_info t1 
        join topic_info t2 on t1.global_key=t2.global_key
    ) t1
    join click_info t2 on t1.dt=t2.dt and t1.uid=t2.uid
    group by 1,2,3,4,5
)
,click_order_t7 as (
    select order_date,travel_name,travel_id
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
    from q_app_order t1 
    left join click_uv t2 on t1.user_name=t2.user_name
    where t2.user_name is not null and t2.dt >= date_sub(t1.order_date, 7) and t2.dt <= t1.order_date
    group by 1,2,3
)
,click_order_t0 as (
    select order_date,travel_name,travel_id
          ,count(distinct t1.order_no) order_no_t0
          ,sum(t1.room_night) room_night_t0
    from q_app_order t1 
    left join click_uv t2 on t1.user_name=t2.user_name and t2.dt = t1.order_date
    group by 1,2,3
)


select t1.dt,t1.travel_name,t1.travel_id,exp_uv,clk_uv,order_no,room_night,order_no_t0,room_night_t0
from (
    SELECT t1.dt,travel_name,travel_id,count(distinct uid) exp_uv
    FROM exp_info t1 
    join topic_info t2 on t1.global_key=t2.global_key
    group by 1,2,3
) t1
left join (
    SELECT t1.dt,travel_name,travel_id,count(distinct uid) clk_uv
    FROM click_uv t1 
    group by 1,2,3
)t2 on t1.dt=t2.dt and t1.travel_name=t2.travel_name and t1.travel_id =t2.travel_id
left join click_order_t7 t3 on t1.dt=t3.order_date and t1.travel_name=t3.travel_name and t1.travel_id =t3.travel_id
left join click_order_t0 t4 on t1.dt=t4.order_date and t1.travel_name=t4.travel_name and t1.travel_id =t4.travel_id

;











--CQ取消率数据
with q_order as(
select substr(checkout_date,1,10) as checkout_date
    , count(distinct order_no) as `Q订单`
    , count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end)) as `Q未取消订单-当日`
    , count (distinct (case when order_status = 'CHECKED_OUT' then order_no end)) as `Q已离店订单-总共`
    , (1 - count (distinct (case when order_status = 'CHECKED_OUT' then order_no end))/count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end))) as `Q取消率(不含当日取消）`
from mdw_order_v3_international a
where dt = '%(DATE)s'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    and terminal_channel_type = 'app'
    and is_valid = '1'
    and checkout_date between date_sub(current_date, 90) and date_sub(current_date, 1)
    and a.order_no <> '103576132435'
group by 1
)

,c_order as(
SELECT 
    substr(o.checkout_date, 1, 10) AS checkout_date
   ,COUNT(DISTINCT o.order_no) AS `C订单`
   ,COUNT(DISTINCT CASE 
        WHEN o.extend_info['CANCEL_TIME'] IS NULL 
            OR o.extend_info['CANCEL_TIME'] = 'NULL' 
            OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) 
        THEN o.order_no 
    END) AS `C未取消订单-当日`
    ,COUNT(DISTINCT CASE WHEN o.order_status <> 'C' THEN o.order_no END) AS `C已离店订单-总共`
    ,(1 - COUNT(DISTINCT CASE WHEN o.order_status <> 'C' THEN o.order_no END)/COUNT(DISTINCT CASE WHEN o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) THEN o.order_no END)) as `C取消率（不含当日取消）`
FROM ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
WHERE 
    o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
    AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
    AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
    AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
    AND o.terminal_channel_type = 'app'
    AND substr(o.checkout_date, 1, 10) between date_sub(current_date, 90) and date_sub(current_date, 1) -- 退房日期范围
GROUP BY 1
)
,q_cashback as
(
SELECT checkout_date,
 count(distinct order_no) as `领取返现订单量`,
  sum (case 
                  when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
        then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
     else init_commission_after+nvl(ext_plat_certificate,0) 
  end) as `领取返现订单佣金`,sum(get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount'))as `返现`
FROM default.mdw_order_v3_international
WHERE dt = '%(DATE)s'
and order_status = 'CHECKED_OUT'
and is_valid = 1
and checkout_date between date_sub(current_date, 90) and date_sub(current_date, 1)
and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
group by 1
order by checkout_date desc
)
select 
  a.checkout_date
  ,a.`Q订单`
  ,a.`Q未取消订单-当日`
  ,a.`Q已离店订单-总共`
  ,CONCAT(ROUND(a.`Q取消率(不含当日取消）` * 100, 2), '%') AS `Q取消率(不含当日取消）`
  ,b.`C订单`
  ,b.`C未取消订单-当日`
  ,b.`C已离店订单-总共`
  ,CONCAT(ROUND(b.`C取消率（不含当日取消）` * 100, 2), '%') AS `C取消率(不含当日取消）`
  ,CONCAT( ROUND((a.`Q取消率(不含当日取消）`/b.`C取消率（不含当日取消）`) * 100,2),'%') AS `取消率QC`
  ,c.`领取返现订单量`
  ,ROUND(c.`领取返现订单佣金`) as `领取返现订单佣金`
  ,ROUND(c.`返现`) as `返现`
  ,CONCAT( ROUND((c.`领取返现订单量`/a.`Q已离店订单-总共`) * 100,2),'%') AS `领取返现订单占比`
  from q_order a
  left join c_order b on a.checkout_date = b.checkout_date 
  left join q_cashback c on a.checkout_date = c.checkout_date 
  order by a.checkout_date desc