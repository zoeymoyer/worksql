-----1、小红书交叉-宽口径
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
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,red as
(
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub(current_date, 40) and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
)

,order_a as
(
    select order_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        terminal_channel_type,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
        ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,red_uv as (
    select distinct uv.dt
           ,uv.user_id
           ,uv.user_type
           ,uv.mdd
    from init_uv uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
)
,init_uv_all as (
    select dt,count(distinct user_id) all_uv
    from init_uv
    group by 1
)
,order_all as (
    select order_date
        ,count(distinct order_no) order_all
        ,sum(room_night) room_night_all
        ,sum(final_commission_after) yj_all
        ,sum(init_gmv) gmv_all
    from order_a
    group by 1
)


select  a.dt  `日期`,
        date_format(a.dt,'u')`星期`,
        `引流UV`,
        concat(round(`引流UV` / all_uv * 100, 2), '%') as `UV占比`,
        `生单用户量`,
        `订单量`,
        `用券订单量`,
        concat(round(`订单量` / `用券订单量` * 100, 2), '%') as `用券订单占比`,
        concat(round(`券额` / `GMV` * 100, 2), '%') as `券补率`,
        concat(round(`订单量` / order_all * 100, 2), '%') as `订单占比`,
        `间夜量`,
        concat(round(`间夜量` / room_night_all * 100, 2), '%') as `间夜占比`,
        concat(round(`订单量` / `引流UV` * 100, 2), '%') as `CR`,
        round(`GMV` / `间夜量`, 0) as `ADR`
        ,`佣金`
        ,`GMV`
        ,`券额`
        ,concat(round(`佣金` / yj_all * 100, 2), '%') as `佣金占比`
        ,concat(round(`GMV` / gmv_all * 100, 2), '%') as `GMV占比`
from (
    select
        uv.dt,
        count(distinct uv.user_id) as `引流UV`,
        count(distinct ord.user_id) as `生单用户量`,
        count(distinct ord.order_no) as `订单量`,
        count(distinct case when is_user_conpon = 'Y' then ord.order_no end) as `用券订单量`,
        sum(ord.room_night) as `间夜量`,
        sum(ord.init_gmv) as `GMV`,
        sum(final_commission_after)  as `佣金`,
        sum(coupon_substract_summary)  as `券额`
    from (select distinct dt, user_id from red_uv) uv
    left join order_a ord on uv.user_id = ord.user_id
    and uv.dt = ord.order_date
    group by 1
)a 
left join init_uv_all b on a.dt = b.dt
left join order_all c on a.dt = c.order_date
order by 1 desc
;



-----1、小红书交叉-宽口径
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
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,red as
(
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub(current_date, 40) and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
)

,order_a as
(
    select order_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        terminal_channel_type,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
        ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,red_uv as (
    select distinct uv.dt
           ,uv.user_id
           ,uv.user_type
           ,uv.mdd
    from init_uv uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
)
,init_uv_all as (
    select dt,user_type,count(distinct user_id) all_uv
    from init_uv
    group by 1,2
)
,order_all as (
    select order_date,user_type
        ,count(distinct order_no) order_all
        ,sum(room_night) room_night_all
        ,sum(final_commission_after) yj_all
        ,sum(init_gmv) gmv_all
    from order_a
    group by 1,2
)


select  a.dt  `日期`,a.user_type,
        date_format(a.dt,'u')`星期`,
        `引流UV`,
        concat(round(`引流UV` / all_uv * 100, 2), '%') as `UV占比`,
        `生单用户量`,
        `订单量`,
        `用券订单量`,
        concat(round(`订单量` / `用券订单量` * 100, 2), '%') as `用券订单占比`,
        concat(round(`券额` / `GMV` * 100, 2), '%') as `券补率`,
        concat(round(`订单量` / order_all * 100, 2), '%') as `订单占比`,
        `间夜量`,
        concat(round(`间夜量` / room_night_all * 100, 2), '%') as `间夜占比`,
        concat(round(`订单量` / `引流UV` * 100, 2), '%') as `CR`,
        round(`GMV` / `间夜量`, 0) as `ADR`
        ,`佣金`
        ,`GMV`
        ,`券额`
        ,concat(round(`佣金` / yj_all * 100, 2), '%') as `佣金占比`
        ,concat(round(`GMV` / gmv_all * 100, 2), '%') as `GMV占比`
from (
    select
        uv.dt,uv.user_type
        count(distinct uv.user_id) as `引流UV`,
        count(distinct ord.user_id) as `生单用户量`,
        count(distinct ord.order_no) as `订单量`,
        count(distinct case when is_user_conpon = 'Y' then ord.order_no end) as `用券订单量`,
        sum(ord.room_night) as `间夜量`,
        sum(ord.init_gmv) as `GMV`,
        sum(final_commission_after)  as `佣金`,
        sum(coupon_substract_summary)  as `券额`
    from (select distinct dt, user_id,user_type from red_uv) uv
    left join order_a ord on uv.user_id = ord.user_id
    and uv.dt = ord.order_date
    group by 1,2
)a 
left join init_uv_all b on a.dt = b.dt and a.user_type=b.user_type
left join order_all c on a.dt = c.order_date and a.user_type=c.user_type
order by 1 desc
;