with user_type as ( --- 用户历史首单
    select  user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ( --- 分日去重活跃用户，补充鸿蒙识别
    select distinct
            dt
            -- ,case when os_version is not null
            --         and os_version != ''
            --         and os_version like '%OpenHarmony%'
            --         and cast(regexp_extract(os_version, '([0-9]+)', 1) as int) < 9
            --         then 'Harmony'
            --     when platform = 'adr' then 'AndroidPhone'
            --     when platform = 'ios' then 'iPhone'
            --     else platform
            -- end as mobile_platform
            ,case when  (
                    (cast(app_version as bigint) >= 60001582 and cast(app_version as bigint) <= 60001589) 
                    or (cast(app_version as bigint) >= 50000101 and cast(app_version as bigint) <= 50009999)
                ) then 'Harmony' 
                when platform = 'adr' then 'AndroidPhone'
                when platform = 'ios' then 'iPhone'
                else platform end as mobile_platform
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e  on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null
        and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null
        and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order_app as ( --- app订单明细
    select  order_date as dt
            ,case
                when mobile_platform = 'Harmony' then 'Harmony'
                when mobile_platform = 'AndroidPhone' then 'AndroidPhone'
                when mobile_platform = 'iPhone' then 'iPhone'
                else mobile_platform
            end as mobile_platform
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type
            ,a.user_id
            ,a.order_no
    from default.mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and order_date >= date_sub(current_date, 30)
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,dau as (
    select  dt
            ,mobile_platform
            ,user_type
            ,count(distinct user_name) as dau
    from uv
    group by 1,2,3
)
,orders as (
    select  dt
            ,mobile_platform
            ,user_type
            ,count(distinct order_no) as order_no
    from q_order_app
    group by 1,2,3
)

select  a.dt
        ,a.mobile_platform
        ,a.user_type
        ,a.dau
        ,coalesce(b.order_no, 0) as order_no
        ,coalesce(b.order_no, 0) / a.dau as cr
from dau a
left join orders b on a.dt = b.dt and a.mobile_platform = b.mobile_platform and a.user_type = b.user_type
order by 1,2,3
;


