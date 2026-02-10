-- 近7天日期参数
with uv1 as ----分日去重活跃用户 D页
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id
            ,a.user_name
     from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     where dt >= date_sub(current_date, 7)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and checkout_date between '2025-10-01' and '2025-10-08'
)


SELECT 
    count(CASE WHEN p.dt = date_sub(current_date,1) THEN p.user_id END) AS `%(DATE)s`,
    count(CASE WHEN p.dt = date_sub(current_date,2) THEN p.user_id END) AS `%(DATE_1)s`,
    count(CASE WHEN p.dt = date_sub(current_date,3) THEN p.user_id END) AS `%(DATE_2)s`,
    count(CASE WHEN p.dt = date_sub(current_date,4) THEN p.user_id END) AS `%(DATE_3)s`,
    count(CASE WHEN p.dt = date_sub(current_date,5) THEN p.user_id END) AS `%(DATE_4)s`,
    count(CASE WHEN p.dt = date_sub(current_date,6) THEN p.user_id END) AS `%(DATE_5)s`,
    count(CASE WHEN p.dt = date_sub(current_date,7) THEN p.user_id END) AS `%(DATE_6)s`
FROM uv1 p
;