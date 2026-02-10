
select count(distinct user_id) from (
select '国际酒店' as busi_type 
    , user_name 
    , 'S' as page_type 
    , max(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as max_action_time
    , min(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as min_action_time
    , 'app' as active_channel
    , user_id
from ihotel_default.dw_user_app_log_search_di_v1
where dt = '2025-10-23'
and trim(user_name) <> ''
and action_time is not null 
and device_id is not null
and device_id <> ''
and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
and business_type = 'hotel'
group by user_name, user_id

union all 

select '国际酒店' as busi_type 
    , user_name 
    , 'D' as page_type 
    , max(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as max_action_time
    , min(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as min_action_time
    , 'app' as active_channel
    , user_id
from ihotel_default.dw_user_app_log_detail_visit_di_v1
where dt = '2025-10-23'
and trim(user_name) <> ''
and action_time is not null 
and device_id is not null
and device_id <> ''
and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
and business_type = 'hotel'
group by user_name, user_id

union all 

select '国际酒店' as busi_type 
    , user_name 
    , 'B' as page_type 
    , max(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as max_action_time
    , min(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as min_action_time
    , 'app' as active_channel
    , user_id
from ihotel_default.dw_user_app_log_booking_di_v1
where dt = '2025-10-23'
and trim(user_name) <> ''
and action_time is not null 
and device_id is not null
and device_id <> ''
and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
and business_type = 'hotel'
group by user_name, user_id

union all 

select '国际酒店' as busi_type 
    , user_name 
    , 'O' as page_type 
    , max(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as max_action_time
    , min(substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19)) as min_action_time
    , 'app' as active_channel
    , user_id 
from ihotel_default.dw_user_app_log_order_submit_di_v1
where dt = '2025-10-23'
and nvl(trim(user_id),'') <> ''
and action_time is not null 
and device_id is not null
and device_id <> ''
and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
and business_type = 'hotel'
group by user_name, user_id

)


select dt,sum(uv) uv ,sum(buv) uv 
from (
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他' end as mdd
            ,count(distinct a.user_id) uv
            ,count(distinct a.user_name) buv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= date_sub(current_date, 14)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1
) t group by 1 order by 1 ;



with ldbo_uv as (  --- sdbo 小时级
    select dt,user_id,action_time
    from (
        select distinct dt 
                ,a.user_id
                ,action_time
                ,'S' as page_type 
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        union all
        select distinct dt 
                ,a.user_id
                ,action_time
                ,'D' as page_type
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        union all
        select distinct dt 
                ,a.user_id
                ,action_time
                ,'B' as page_type
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        union all
        select distinct dt 
                ,a.user_id
                ,action_time
                ,'O' as page_type
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    ) group by 1,2,3
)


select dt
       ,count(distinct user_id) uv
from ldbo_uv
group by 1
order by 1
--- 分时

-- select substr(action_time,1,11) hh
--        ,count(distinct user_id) uv
-- from ldbo_uv
-- group by 1
-- order by hh
;






