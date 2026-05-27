
--- sql1实验组逻辑
with biguser as ( -- 新逻辑大单用户，15间夜以上
    select orig_device_id,user_name
    from (
        select order_date
              ,user_info['orig_device_id'] as orig_device_id
              ,user_name
              ,count(order_no) as order_nos_90
              ,sum(room_night) as room_nights_90
        from default.mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type = 'app'
            and is_valid = '1'
            and order_status not in ('CANCELLED', 'REJECTED')
            and order_date >= date_sub(current_date, 90)
            and order_date <= date_sub(current_date, 1)
        group by 1,2,3
    ) a
    where room_nights_90 >= 15
    group by 1,2
)
,uv as (-- 活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            -- ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    -- left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-04-08'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,q_order_app as (-- 订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-04-08' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,ab_info as ( -- 步骤1: 只保留用户第一次进入实验的数据（剔除大单用户）
    select ab_version,p_uniqueid user_name
          ,concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
    from (
        select p_group as ab_version
              ,dt,p_uniqueid
              ,row_number() over (partition by p_uniqueid order by dt) as rn
        from flight.dwd_abt_split_log_di
        where dt >= '20260408'   -- 分区限制
            and p_testcode = '260325_fl_gj_gj_home_popup_wy'
        group by 1,2,3
    ) t1 left join biguser t2 on t1.p_uniqueid = t2.user_name
    where t2.user_name is null  -- 去除大单用户
        and rn = 1
        and ab_version = 'B'  -- 只保留实验组用户
)
,behavior_users as ( -- 步骤2: 关联埋点表，统计show/click行为
    select f.dt
          ,f.ab_version
          ,count(distinct case when h.oper_type = 'show' then f.user_name end) as show_users   -- 曝光用户数
          ,count(distinct case when h.oper_type = 'click' then f.user_name end) as click_users  -- 点击用户数
    from ab_info f
    left join tmp.homePopUp260415 h on f.user_name = h.user_name and f.dt = h.dt 
            and h.biz_name = 'guojijipiao_26wuyi' 
            and h.oper_type in ('show', 'click') 
            and h.dt >= '2026-04-08'   -- 分区限制
    group by 1,2
)
,show_users as ( -- 埋点表，统计show行为
    select f.dt,f.ab_version,f.user_name
    from ab_info f
    left join tmp.homePopUp260415 h on f.user_name = h.user_name and f.dt = h.dt 
            and h.biz_name = 'guojijipiao_26wuyi' and h.oper_type = 'show' 
            and h.dt >= '2026-04-08'   -- 分区限制
    where h.user_name is not null
    group by 1,2,3
)
,active_users as ( -- 步骤3: 关联活跃用户表，统计活跃用户数
    select t1.dt,t1.ab_version,count(distinct t1.user_name) as active_uv
    from show_users t1
    left join uv t2 on t1.user_name = t2.user_name and t1.dt = t2.dt
    where t2.user_name is not null  -- 只保留活跃用户
    group by 1,2
)
,order_users as ( -- 步骤4: 关联订单表，统计订单用户数
    select t1.dt,t1.ab_version,count(distinct t2.order_no) as order_cnt,count(distinct t3.order_no) as order_cnt_7d,max(t3.order_date) as last_order_date
    from show_users t1
    left join q_order_app t2 on t1.user_name = t2.user_name and t1.dt = t2.order_date
    left join q_order_app t3 on t1.user_name = t3.user_name and datediff(t3.order_date, t1.dt) <= 7 and t3.order_date >= t1.dt
    where t2.user_name is not null  -- 只保留有订单的用户
    group by 1,2
)


select   t1.dt
        ,t1.ab_version
        ,t1.split_users
        ,t2.show_users
        ,t2.click_users
        ,t3.active_uv
        ,t4.order_cnt
        ,t4.order_cnt_7d
        ,last_order_date
from (-- 1、统计每个版本每天的分流用户数（即进入实验的用户数）
    select dt,ab_version,count(distinct user_name) as split_users
    from ab_info
    group by 1,2
) t1
-- 2、统计每个版本每天的活跃用户数
left join behavior_users t2 on t1.dt = t2.dt and t1.ab_version = t2.ab_version
left join active_users t3 on t1.dt = t3.dt and t1.ab_version = t3.ab_version
left join order_users t4 on t1.dt = t4.dt and t1.ab_version = t4.ab_version
order by 1,2
;

--- sql2对照组逻辑
with biguser as ( -- 新逻辑大单用户，15间夜以上
    select orig_device_id,user_name
    from (
        select order_date
              ,user_info['orig_device_id'] as orig_device_id
              ,user_name
              ,count(order_no) as order_nos_90
              ,sum(room_night) as room_nights_90
        from default.mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type = 'app'
            and is_valid = '1'
            and order_status not in ('CANCELLED', 'REJECTED')
            and order_date >= date_sub(current_date, 90)
            and order_date <= date_sub(current_date, 1)
        group by 1,2,3
    ) a
    where room_nights_90 >= 15
    group by 1,2
)
,uv as (-- 活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            -- ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    -- left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-04-08'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,q_order_app as (-- 订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-04-08' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,ab_info as ( -- 步骤1: 只保留用户第一次进入实验的数据（剔除大单用户）
    select ab_version,p_uniqueid user_name
          ,concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
    from (
        select p_group as ab_version
              ,dt,p_uniqueid
              ,row_number() over (partition by p_uniqueid order by dt) as rn
        from flight.dwd_abt_split_log_di
        where dt >= '20260408'   -- 分区限制
            and p_testcode = '260325_fl_gj_gj_home_popup_wy'
        group by 1,2,3
    ) t1 left join biguser t2 on t1.p_uniqueid = t2.user_name
    where t2.user_name is null  -- 去除大单用户
        and rn = 1
        and ab_version = 'A'  -- 只保留对照组用户
)
,behavior_users as ( -- 步骤2: 关联埋点表，统计show/click行为
    select f.dt
          ,f.ab_version
          ,count(distinct case when h.oper_type = 'show' then f.user_name end) as show_users   -- 曝光用户数
          ,count(distinct case when h.oper_type = 'click' then f.user_name end) as click_users  -- 点击用户数
    from ab_info f
    left join tmp.homePopUp260415 h on f.user_name = h.user_name and f.dt = h.dt 
            and h.biz_name = 'guojijipiao_26wuyi' 
            and h.oper_type in ('show', 'click') 
            and h.dt >= '2026-04-08'   -- 分区限制
    group by 1,2
)
,active_users as ( -- 步骤3: 关联活跃用户表，统计活跃用户数
    select t1.dt,t1.ab_version,count(distinct t1.user_name) as active_uv
    from ab_info t1
    left join uv t2 on t1.user_name = t2.user_name and t1.dt = t2.dt
    where t2.user_name is not null  -- 只保留活跃用户
    group by 1,2
)
,order_users as ( -- 步骤4: 关联订单表，统计订单用户数
    select t1.dt,t1.ab_version,count(distinct t2.order_no) as order_cnt,count(distinct t3.order_no) as order_cnt_7d,max(t3.order_date) as last_order_date
    from ab_info t1
    left join q_order_app t2 on t1.user_name = t2.user_name and t1.dt = t2.order_date
    left join q_order_app t3 on t1.user_name = t3.user_name and datediff(t3.order_date, t1.dt) <= 7 and t3.order_date >= t1.dt
    where t2.user_name is not null  -- 只保留有订单的用户
    group by 1,2
)


select   t1.dt
        ,t1.ab_version
        ,t1.split_users
        ,t2.show_users
        ,t2.click_users
        ,t3.active_uv
        ,t4.order_cnt
        ,t4.order_cnt_7d
        ,last_order_date
from (-- 1、统计每个版本每天的分流用户数（即进入实验的用户数）
    select dt,ab_version,count(distinct user_name) as split_users
    from ab_info
    group by 1,2
) t1
-- 2、统计每个版本每天的活跃用户数
left join behavior_users t2 on t1.dt = t2.dt and t1.ab_version = t2.ab_version
left join active_users t3 on t1.dt = t3.dt and t1.ab_version = t3.ab_version
left join order_users t4 on t1.dt = t4.dt and t1.ab_version = t4.ab_version
order by 1,2
;




------QC
with compare as 
(
    select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as order_date
    , a.uniq_id
    , user_id
    , a.id
    , case when province_name in ('澳门','香港') then '港澳'    
           when a.country_name in ('泰国','日本','韩国') then a.country_name 
           else '其他' end as country_are
    , hotel_seq
    ,qunar_physical_room_id --物理房型
    ,check_in --入住日期
    , dt
    , qunar_pay_price - ctrip_pay_price as pay_price_diff
    , ctrip_pay_price
    , pay_price_compare_result
    , a.ctrip_discount_base_price
    , a.qunar_discount_base_price
    , qunar_chased_discount_price as qunar_after_chase_discount_base_price
    , qunar_chased_discount_price - a.ctrip_discount_base_price as discount_base_price_diff
    , a.qunar_before_coupons_cashback_price-a.ctrip_before_coupons_cashback_price as before_coupons_price_diff
    , chased_discount_price_compare_result as after_chase_discount_base_price_compare_result
    , chased_discount_price_diff
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    WHERE a.dt >= date_format(add_months(current_date(), -1), 'yyyyMM01')  -- 上个月1号
                    AND dt <= date_format(date_sub(current_date(), 1), 'yyyyMMdd')
        and compare_type ='PHYSICAL_ROOM_TYPE_LOWEST'
        and business_type='intl_crawl_cq_api_userview_acc'
        and ctrip_room_status = 'true'
        and qunar_room_status = 'true'
        and room_type_cover='Qmeet'
)
, uv_loses as (
    select 
        order_date 
        ,country_are
        ,count(case when `lose条数`>0 then 1 end) as `user+seq+roomlose次数`
        ,count(*) as `user+seq+room比价次数`
        ,concat(round((count(case when `lose条数`>0 then 1 end) /count(*))*100, 2),'%') AS `uv_lose率`
    from(
        select 
            order_date
            ,user_id
            ,hotel_seq
            ,qunar_physical_room_id --物理房型
            ,country_are
            ,sum(case when pay_price_diff >0 then 1 else 0 end) as `lose条数`
        from compare
        group by 1,2,3,4,5
    ) t
    group by 1,2
)
, kpi_loses as (
    select
        order_date 
        ,country_are
        ,count(DISTINCT case when pay_price_diff >0 then id end)`pv_lose次数`
        ,count(DISTINCT id)`pv_比价次数`
        ,concat(round((count(DISTINCT case when pay_price_diff >0 then id end) / count(DISTINCT id))*100, 2),'%') as `pv_lose率`
    from compare
    group by 1,2
)
SELECT
    month(order_date) as `月`,
    concat(round((sum(`pv_lose次数`)/sum(`pv_比价次数`))*100, 2),'%') as `整体pv_lose率`,
    concat(round((sum(case when country_are = '港澳' then `pv_lose次数` end)/sum(case when country_are = '港澳' then `pv_比价次数` end))*100, 2),'%') as `港澳pv_lose率`,
    concat(round((sum(case when country_are = '日本' then `pv_lose次数` end)/sum(case when country_are = '日本' then `pv_比价次数` end))*100, 2),'%') as `日本pv_lose率`,
    concat(round((sum(case when country_are = '韩国' then `pv_lose次数` end)/sum(case when country_are = '韩国' then `pv_比价次数` end))*100, 2),'%') as `韩国pv_lose率`,
    concat(round((sum(case when country_are = '泰国' then `pv_lose次数` end)/sum(case when country_are = '泰国' then `pv_比价次数` end))*100, 2),'%') as `泰国pv_lose率`,
    concat(round((sum(case when country_are = '其他' then `pv_lose次数` end)/sum(case when country_are = '其他' then `pv_比价次数` end))*100, 2),'%') as `其他pv_lose率`,
    concat(round((sum(case when country_are in ('港澳','日本','韩国','泰国') then `pv_lose次数` end)/sum(case when country_are in ('港澳','日本','韩国','泰国') then `pv_比价次数` end))*100, 2),'%') as `港澳日韩泰pv_lose率`,
    -- UV维度lose率
    concat(round((sum(`user+seq+roomlose次数`)/sum(`user+seq+room比价次数`))*100, 2),'%') as `整体uv_lose率`,
    concat(round((sum(case when country_are = '港澳' then `user+seq+roomlose次数` end)/sum(case when country_are = '港澳' then `user+seq+room比价次数` end))*100, 2),'%') as `港澳uv_lose率`,
    concat(round((sum(case when country_are = '日本' then `user+seq+roomlose次数` end)/sum(case when country_are = '日本' then `user+seq+room比价次数` end))*100, 2),'%') as `日本uv_lose率`,
    concat(round((sum(case when country_are = '韩国' then `user+seq+roomlose次数` end)/sum(case when country_are = '韩国' then `user+seq+room比价次数` end))*100, 2),'%') as `韩国uv_lose率`,
    concat(round((sum(case when country_are = '泰国' then `user+seq+roomlose次数` end)/sum(case when country_are = '泰国' then `user+seq+room比价次数` end))*100, 2),'%') as `泰国uv_lose率`,
    concat(round((sum(case when country_are = '其他' then `user+seq+roomlose次数` end)/sum(case when country_are = '其他' then `user+seq+room比价次数` end))*100, 2),'%') as `其他uv_lose率`,
    concat(round((sum(case when country_are in ('港澳','日本','韩国','泰国') then `user+seq+roomlose次数` end)/sum(case when country_are in ('港澳','日本','韩国','泰国') then `user+seq+room比价次数` end))*100, 2),'%') as `港澳日韩泰uv_lose率`
FROM  (
    SELECT
        t1.order_date,
        t1.country_are,
        `pv_lose次数`,
        `pv_比价次数`,
        t1.`pv_lose率`,
        `user+seq+roomlose次数`,
        `user+seq+room比价次数`,
        t2.`uv_lose率`
    FROM kpi_loses t1
    LEFT JOIN uv_loses t2 ON t1.order_date = t2.order_date AND t1.country_are = t2.country_are
) a
GROUP BY 1
ORDER BY `月` DESC