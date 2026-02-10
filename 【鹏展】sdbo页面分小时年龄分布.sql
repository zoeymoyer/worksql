with uv as (  --- sdbo 小时级

    select dt,user_id,action_time
    from (
        select distinct dt 
                ,a.user_id
                ,action_time
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= '2025-09-10'
        and dt <= '2025-09-16'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_id
                ,action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= '2025-09-10'
        and dt <= '2025-09-16'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_id
                ,action_time
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= '2025-09-10'
        and dt <= '2025-09-16'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_id
                ,action_time
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= '2025-09-10'
        and dt <= '2025-09-16'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    ) group by 1,2,3
)

,user_info as (
    select  distinct user_id
            ,birth_year_month
            ,CASE  WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
                END AS age
     from pub.dim_user_profile_nd
)


select substr(action_time,1,11) dt_hour,
       CASE
           when u.age = '未知' then null
           WHEN u.age BETWEEN 0 AND 18   THEN '18岁以下'
           WHEN u.age BETWEEN 18 AND 24  THEN '18-24岁'
           WHEN u.age BETWEEN 25 AND 40  THEN '25-40岁'
           WHEN u.age BETWEEN 41 AND 55  THEN '41-55岁'
           WHEN u.age >= 56 then '56岁及以上'
           END AS age_bucket,
       count(distinct u.user_id) uv
from uv o
inner join user_info u on u.user_id = o.user_id
group by 1,2
;