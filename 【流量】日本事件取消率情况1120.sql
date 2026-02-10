WITH base AS (--- 日本订单
    SELECT
        order_date,
        order_no,
        order_status,
        first_cancelled_time,
        CASE
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                AND substr(first_cancelled_time, 1, 10) >= '2025-11-14'
                THEN '11.14之后'
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                AND substr(first_cancelled_time, 1, 10) < '2025-11-14'
                THEN '11.14之前'
            ELSE '未取消'
            END AS date_group
        ,user_id
    FROM mdw_order_v3_international a   -- 海外订单表
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name != '中国')
      AND terminal_channel_type IN ('www','app','touch')
      AND is_valid = '1'
      AND order_date >= date_sub(current_date, 60)     -- 近一个月预订
      AND order_date <= date_sub(current_date, 1)
      AND order_no <> '103576132435'
      AND a.country_name = '日本'
)
,q_order as (
    SELECT
        order_date,
        order_no,
        order_status,
        first_cancelled_time,
        CASE
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                THEN '取消'
            ELSE '未取消'
            END AS date_group
        ,user_id
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
    FROM mdw_order_v3_international a   -- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name != '中国')
      AND terminal_channel_type IN ('www','app','touch')
      AND is_valid = '1'
      AND order_date >= date_sub(current_date, 60)     -- 近一个月预订
      AND order_date <= date_sub(current_date, 1)
      AND order_no <> '103576132435'
)
,init_uv as
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= date_sub(current_date, 60)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)


select t1.order_date
      ,order_no_all
      ,order_no
      ,cancel_order_no / order_no_all cancel_order_no_rate
      ,cancel_order_uv
      ,order_no_all_agin
      ,order_no_all_jp_agin
      ,order_no_agin
      ,order_no_jp_agin
      ,act_uv_agin
      ,act_uv_jp_agin
from (
select order_date
      ,count(distinct order_no) order_no_all
      ,count(distinct case when order_status in ('CANCELLED', 'REJECTED') then order_no end) cancel_order_no
      ,count(distinct case when order_status not in ('CANCELLED', 'REJECTED') then order_no end) order_no
      ,count(distinct case when order_status  in ('CANCELLED', 'REJECTED') then user_id end) cancel_order_uv
      ,count(distinct case when order_status not in ('CANCELLED', 'REJECTED') then user_id end) order_uv
from base
group by 1
) t1 
left join (--- 取消单用户3天内再次下单活跃
select t1.order_date
      ,count(distinct t2.order_no) order_no_all_agin
      ,count(distinct case when t2.order_status not in ('CANCELLED', 'REJECTED') then t2.order_no end) order_no_agin
      ,count(distinct case when t2.mdd= '日本' then t2.order_no end)  order_no_all_jp_agin
      ,count(distinct case when t2.mdd= '日本' and t2.order_status not in ('CANCELLED', 'REJECTED') then t2.order_no end)  order_no_jp_agin
      ,count(distinct t3.user_id) act_uv_agin
      ,count(distinct case when t3.mdd= '日本' then t3.user_id end)  act_uv_jp_agin
from (
    select order_date
        ,user_id
        ,substr(first_cancelled_time, 1, 10) first_cancelled_time
    from base
    where order_status in ('CANCELLED', 'REJECTED')
    group by 1,2,3
) t1 
left join q_order t2 on t1.user_id = t2.user_id and datediff(t2.order_date, t1.order_date) >= 1 and datediff(t2.order_date,t1.order_date) <= 3 and t2.order_date > t1.order_date
left join init_uv t3 on t1.user_id = t3.user_id and datediff(t3.dt,t1.order_date) >= 1 and  datediff(t3.dt,t1.order_date) <= 3 and t3.dt > t1.order_date
group by 1
)t2 on t1.order_date=t2.order_date
order by 1
;




WITH base AS (--- 日本订单
    SELECT
        order_date,
        order_no,
        order_status,
        first_cancelled_time,
        CASE
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                AND substr(first_cancelled_time, 1, 10) >= '2025-11-14'
                THEN '11.14之后'
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                AND substr(first_cancelled_time, 1, 10) < '2025-11-14'
                THEN '11.14之前'
            ELSE '未取消'
            END AS date_group
        ,user_id
    FROM mdw_order_v3_international a   -- 海外订单表
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name != '中国')
      AND terminal_channel_type IN ('www','app','touch')
      AND is_valid = '1'
      AND order_date >= date_sub(current_date, 60)     -- 近一个月预订
      AND order_date <= date_sub(current_date, 1)
      AND order_no <> '103576132435'
      AND a.country_name = '日本'
)
,q_order as (
    SELECT
        order_date,
        order_no,
        order_status,
        first_cancelled_time,
        CASE
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                THEN '取消'
            ELSE '未取消'
            END AS date_group
        ,user_id
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
    FROM mdw_order_v3_international a   -- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name != '中国')
      AND terminal_channel_type IN ('www','app','touch')
      AND is_valid = '1'
      AND order_date >= date_sub(current_date, 60)     -- 近一个月预订
      AND order_date <= date_sub(current_date, 1)
      AND order_no <> '103576132435'
)

select t1.order_date,date_group,mdd
      ,count(distinct t2.order_no) order_no_all_agin
      ,count(distinct case when t2.order_status not in ('CANCELLED', 'REJECTED') then t2.order_no end) order_no_agin
from (  ---- 日本取消单用户
    select order_date
        ,user_id,date_group
    from base
    where order_status in ('CANCELLED', 'REJECTED')
    group by 1,2,3
) t1 
left join q_order t2 on t1.user_id = t2.user_id and datediff(t2.order_date, t1.order_date) >= 1 and t2.order_date > t1.order_date
group by 1,2,3
order by 1,2,3
;