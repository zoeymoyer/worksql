--- 1、小红书渠道下单前首访时间分布
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')

)
,q_order as (----订单明细表
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
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
)
,red as
(
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2024-12-01' and date_sub(current_date,1)
    --and business_type = 'hotel-inter'  --宽口径不需要这个
    and query_platform = 'redbook'
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select distinct t1.dt
          ,t1.user_id
          ,t1.user_name
          ,t1.user_type
    from uv  t1
    left join red t2 on t1.user_name = t2.user_name
    where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
    and t2.user_name is not null
)

,user_xhs_order as (
    select t1.order_date,t1.user_type,t2.user_id,min(t2.dt)min_dt, datediff(t1.order_date,min(t2.dt)) diff_days
    from (--- 小红书渠道下单用户
        select t1.order_date 
            ,t1.user_type
            ,t1.user_id
        from q_order t1 
        left join user_xhs t2 on t1.user_id=t2.user_id and t1.order_date = t2.dt
        where t2.user_id is not null
        group by 1,2,3
    ) t1 
    left join uv t2 on t1.user_id=t2.user_id and datediff(t1.order_date,t2.dt) < 60 and t1.order_date >=t2.dt
  group by 1,2,3
)

--- 最终结果输出：首访时间分布
select order_date,diff_days
      ,count(distinct user_id) as total_order_uv
      ,count(distinct user_id) / sum(count(distinct user_id)) over(partition by order_date)  uv_rate
from user_xhs_order
group by 1,2
order by order_date desc,diff_days asc;




--- 2、下单前首访时间分布
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (----订单明细表
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
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
)


,user_order as (
    select t1.order_date,t1.user_type,t2.user_id,min(t2.dt)min_dt, datediff(t1.order_date,min(t2.dt)) diff_days
    from (
        select t1.order_date 
            ,t1.user_type
            ,t1.user_id
        from q_order t1 
        group by 1,2,3
    ) t1 
    left join uv t2 on t1.user_id=t2.user_id and datediff(t1.order_date,t2.dt) < 60 and t1.order_date >= t2.dt
  group by 1,2,3
)

--- 最终结果输出：首访时间分布
select order_date,diff_days
      ,count(distinct user_id) as total_order_uv
      ,count(distinct user_id) / sum(count(distinct user_id) ) over(partition by order_date)  uv_rate
from user_order
group by 1,2
order by order_date desc,diff_days asc;


--- 2.2、下单前首访时间分布新逻辑。30天内下单用户中有69%用户在
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (----订单明细表
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
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
)
--- 最终结果输出：首访时间分布
select t1.dt,diff_days,count(distinct t1.user_id) order_uv
        ,sum(count(distinct t1.user_id)) over(partition by t1.dt) act_uv_all
        ,sum(case when diff_days is not null then count(distinct t1.user_id) end) over(partition by t1.dt) order_uv_all
from (--- 沉默30天活跃用户在未来30天下单分布
    select t1.dt,t1.user_id,min(t2.order_date) min_order_date,datediff(min(t2.order_date),t1.dt) diff_days
    from ( --- 近30天没有活跃用户
        select t1.dt,t1.user_id
        from (select * from uv where dt = '2025-10-01') t1
        left join uv t2 on t1.user_id=t2.user_id and datediff(t1.dt,t2.dt) < 30 and t1.dt > t2.dt
        where t2.user_id is null
        group by 1,2
    )t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.dt and datediff(t2.order_date,t1.dt) < 30
    group by 1,2
) t1
group by 1,2
order by dt desc,diff_days asc
;







-- 3、小红书拉活明细
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')

)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
            ,terminal_channel_type
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
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
)
,xhs_lh as (
    select dt,order_no,uid,username,income, case when business_name = '国际酒店' then '窄口径' end is_ihotel
    from pub.dwd_ord_order_media_lahuo_attribution_di
    where is_media_lahuo_kpi = 1 and order_type_class = 'hotel-inter'
    and dt between '2026-02-12' and '2026-02-24'
)
q_order_info as (
    select order_date,sum(room_night) room_night,count(distinct order_no) order_no,sum(final_commission_after) yj
    from q_order where terminal_channel_type = 'app'
    group by 1
)

select t1.dt,t1.order_no,t1.uid,t1.username,t1.income,t1.is_ihotel,t1.room_night,t1.final_commission_after,case when t2.user_name is null then 'Y' else 'N' end `是否增量`,t3.room_night `当日大盘间夜`,t3.order_no `当日大盘订单量`,t2.yj `当日大盘收益`
from (
    select t1.dt,t1.order_no,t1.uid,t1.username,t1.income,t1.is_ihotel,t2.room_night,t2.final_commission_after
    from xhs_lh t1 
    left join q_order t2 on t1.dt=t2.order_date and t1.order_no=t2.order_no 
) t1
left join (
    select t1.dt,t1.username,t2.user_name
    from xhs_lh t1 
    left join uv t2 on t1.username=t2.user_name and t1.dt > t2.dt and datediff(t1.dt,t2.dt) <= 3 
    group by 1,2,3
) t2 on t1.username=t2.user_name and t1.dt=t2.dt
left join q_order_info t3 on t1.dt=t3.order_date
order by 1
;
-- 3、小红书拉活（q口径）
select dt
        ,count(distinct order_no) "拉活订单量"
        ,count(distinct uid) "拉活生单uv"
        ,sum(income) "拉活收益"
        ,count(distinct case when business_name = '国际酒店' then order_no end) "拉活订单量_窄口径"
        ,count(distinct case when business_name = '国际酒店' then uid end)  "拉活生单uv_窄口径"
        ,sum( case when business_name = '国际酒店' then income end) "拉活收益_窄口径"
from pub.dwd_ord_order_media_lahuo_attribution_di
where is_media_lahuo_kpi = 1 and order_type_class = 'hotel-inter'
  and dt between '2026-02-12' and '2026-02-24'
group by 1
order by 1 desc
;

---------------------------------
--- 4、信息流拉活（q口径）
select dt,order_no,uid,username,income,case when account_id in ('73904399','73904400','75506778','75506762') then '窄口径' end is_ihotel
from pub.dwd_mkt_xxl_touch_start_order_single_label_di
where  abt = 'valid' and order_type_class = 'hotel-inter'
  and is_mkt_lahuo_kpi = 1
  and dt between '2026-02-12' and '2026-02-24'
;


--- 4、信息流拉活（q口径）
select dt
        ,count(distinct order_no) "拉活订单量"
        ,count(distinct uid) "拉活生单uv"
        ,sum(income) "拉活收益"
        ,count(distinct case when account_id in ('73904399','73904400','75506778','75506762') then order_no end) "拉活订单量_窄口径"
        ,count(distinct case when account_id in ('73904399','73904400','75506778','75506762') then uid end)  "拉活生单uv_窄口径"
        ,sum( case when account_id in ('73904399','73904400','75506778','75506762')  then income end) "拉活收益_窄口径"
from pub.dwd_mkt_xxl_touch_start_order_single_label_di
where is_mkt_lahuo_kpi = 1 and abt = 'valid' and order_type_class = 'hotel-inter'
  and dt between '2026-02-12' and '2026-02-24'
group by 1
order by 1 desc
;


--- 5、小红书拉活用户明细
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')

)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after,terminal_channel_type
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
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
)
,red as
(
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2024-12-01' and date_sub(current_date,1)
    --and business_type = 'hotel-inter'  --宽口径不需要这个
    and query_platform = 'redbook'
)
,user_xhs as (--- 宽口径小红书渠道分平台新老用户
    select  t1.dt
          ,t1.user_id
          ,t1.user_name
          ,t1.user_type
    from uv  t1
    left join red t2 on t1.user_name = t2.user_name
    where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
    and t2.user_name is not null
    group by 1,2,3,4
)
,xhs_lh as (
    select dt,order_no,uid,username,income, case when business_name = '国际酒店' then '窄口径' end is_ihotel
    from pub.dwd_ord_order_media_lahuo_attribution_di
    where is_media_lahuo_kpi = 1 and order_type_class = 'hotel-inter'
    and dt between '2026-02-12' and '2026-02-24'
)
,q_order_info as (
    select order_date,sum(room_night) room_night,count(distinct order_no) order_no,sum(final_commission_after) yj
    from q_order where terminal_channel_type = 'app'
    group by 1
)

select t1.*,t2.room_night "大盘间夜量",t2.order_no "大盘订单量",t2.yj "大盘收益"
from (
    select t1.dt
            ,count(distinct t1.order_no) "拉活订单量"
            ,count(distinct case when is_increment = 'Y' then t1.order_no end) "拉活增量订单量"
            ,sum(room_night)  "拉活间夜量"
            ,sum(case when is_increment = 'Y' then room_night end)  "拉活增量间夜量"
            ,count(distinct uid) "拉活生单uv"
            ,count(distinct case when is_increment = 'Y' then uid end) "拉活增量生单uv"
            ,sum(income) "拉活收益"
            ,sum(case when is_increment = 'Y' then income end) "拉活增量收益"


            ,count(distinct case when is_ihotel = '窄口径' then t1.order_no end) "拉活订单量_窄口径"
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t1.order_no end) "拉活增量订单量_窄口径"
            ,sum(case when is_ihotel = '窄口径' then room_night end) "拉活间夜量_窄口径"
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then room_night end) "拉活增量间夜量_窄口径"
            ,count(distinct case when is_ihotel = '窄口径' then uid end)  "拉活生单uv_窄口径"
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then uid end)  "拉活增量生单uv_窄口径"
            ,sum(case when is_ihotel = '窄口径' then income end) "拉活收益_窄口径"
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then income end) "拉活增量收益_窄口径"
    from xhs_lh t1
    left join q_order t2 on t1.order_no=t2.order_no
    left join (
        select t1.dt,t1.username,min_act_date_14d,case when t2.user_name is null then 'Y' else 'N' end is_increment
        from (--- 小红书拉活订单在过往14天小红书最早访问日期
            select t1.dt,t1.username,min(t2.dt) min_act_date_14d
            from xhs_lh t1
            left join uv t2 on t1.username=t2.user_name and datediff(t1.dt,t2.dt) <= 3 and t1.dt >= t2.dt
            group by 1,2
        )t1 left join uv t2 on t1.username=t2.user_name and datediff(t1.min_act_date_14d,t2.dt) < 7 and t1.min_act_date_14d > t2.dt
        group by 1,2,3,4
    )t3 on t1.dt=t3.dt and t1.username=t3.username
    group by 1
)t1 left join q_order_info t2 on t1.dt=t2.order_date
order by 1
;


--- 6、信息流拉活用户明细
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
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')

)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after,terminal_channel_type
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
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
)

,xxl_lh as (
    select dt,order_no,uid,username,income,case when account_id in ('73904399','73904400','75506778','75506762') then '窄口径' end is_ihotel
    from pub.dwd_mkt_xxl_touch_start_order_single_label_di
    where  abt = 'valid' and order_type_class = 'hotel-inter'
    and is_mkt_lahuo_kpi = 1
    and dt between '2026-02-12' and '2026-02-24'
)
,q_order_info as (
    select order_date,sum(room_night) room_night,count(distinct order_no) order_no,sum(final_commission_after) yj
    from q_order where terminal_channel_type = 'app'
    group by 1
)

select t1.*,t2.room_night "大盘间夜量",t2.order_no "大盘订单量",t2.yj "大盘收益"
from (
    select t1.dt
            ,count(distinct t1.order_no) "拉活订单量"
            ,count(distinct case when is_increment = 'Y' then t1.order_no end) "拉活增量订单量"
            ,sum(room_night)  "拉活间夜量"
            ,sum(case when is_increment = 'Y' then room_night end)  "拉活增量间夜量"
            ,count(distinct uid) "拉活生单uv"
            ,count(distinct case when is_increment = 'Y' then uid end) "拉活增量生单uv"
            ,sum(income) "拉活收益"
            ,sum(case when is_increment = 'Y' then income end) "拉活增量收益"


            ,count(distinct case when is_ihotel = '窄口径' then t1.order_no end) "拉活订单量_窄口径"
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t1.order_no end) "拉活增量订单量_窄口径"
            ,sum(case when is_ihotel = '窄口径' then room_night end) "拉活间夜量_窄口径"
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then room_night end) "拉活增量间夜量_窄口径"
            ,count(distinct case when is_ihotel = '窄口径' then uid end)  "拉活生单uv_窄口径"
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then uid end)  "拉活增量生单uv_窄口径"
            ,sum(case when is_ihotel = '窄口径' then income end) "拉活收益_窄口径"
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then income end) "拉活增量收益_窄口径"
    from xxl_lh t1
    left join q_order t2 on t1.order_no=t2.order_no
    left join (
        select t1.dt,t1.username,min_act_date_14d,case when t2.user_name is null then 'Y' else 'N' end is_increment
        from (--- 小红书拉活订单在过往14天小红书最早访问日期
            select t1.dt,t1.username,min(t2.dt) min_act_date_14d
            from xxl_lh t1
            left join uv t2 on t1.username=t2.user_name and datediff(t1.dt,t2.dt) <= 3 and t1.dt >= t2.dt
            group by 1,2
        )t1 left join uv t2 on t1.username=t2.user_name and datediff(t1.min_act_date_14d,t2.dt) < 7 and t1.min_act_date_14d > t2.dt
        group by 1,2,3,4
    )t3 on t1.dt=t3.dt and t1.username=t3.username
    group by 1
)t1 left join q_order_info t2 on t1.dt=t2.order_date
order by 1
;






with q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            -- ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= date_sub(current_date, 30)
      and substr(order_date,1,10) <= date_sub(current_date, 1)

)

select * 
from (
select order_date,sum(room_night)room_night,count(distinct order_no)order_no
from q_order
group by 1
)t1 left join 
(
select dt,sum(room_night)room_night,count(distinct order_no)order_no
from c_order
group by 1
)t2 on t1.dt=t2.dt
order by 1




    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,final_payamount_price
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        
        and is_valid='1'
        and order_status = 'CANCELLED'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'