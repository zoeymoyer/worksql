with q_gj_uv as (--- 国酒WAU
select  concat(year(max(dt)), '~', lpad(weekofyear(dt), 2, '0')) as year_week
        ,concat(min(dt), '~', max(dt)) week
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                    when e.area in ('欧洲','亚太','美洲') then e.area
                    else '其他' end as mdd
        ,count(distinct a.user_id) uv
from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
where dt >= '2024-12-30' 
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
group by year(date_trunc('week', dt)), weekofyear(dt),mdd
)
,q_uv as (--- 去哪儿APP WAU
    select concat(year(max(dt)), '~', lpad(weekofyear(dt), 2, '0')) as year_week
           ,concat(min(dt), '~', max(dt)) week
           ,count(distinct username) uv
    from pub.dws_flow_app_wechat_active_user_di 
    where dt>= '2024-12-30' 
        and dt <= date_sub(current_date,1)
        and channel='APP' and trim(username)!=''
    group by  year(date_trunc('week', dt)), weekofyear(dt)
)

select t1.year_week,t1.week,t1.gj_uv,uv,t1.gj_uv / uv  re
from (
    select  year_week
        ,week
        ,sum(uv) gj_uv
    from q_gj_uv
    group by 1,2
) t1 
left join (
    select year_week,week,uv
    from q_uv
) t2 on t1.year_week=t2.year_week and t1.week=t2.week
order by 1
;




with q_gj_uv as (--- 国酒MAU
    select  substr(dt,1, 7) mth
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他' end as mdd
            ,count(distinct a.user_id) uv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2023-01-01' 
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2
)
,q_uv as (--- 去哪儿APP MAU
    select substr(dt,1, 7) mth
           ,count(distinct username) uv
    from pub.dws_flow_app_wechat_active_user_di 
    where dt>= '2023-01-01'  
        and dt <= date_sub(current_date,1)
        and channel='APP' and trim(username)!=''
    group by  1
)

select t1.mth,gj_uv,uv,t1.gj_uv / uv  re
from (
    select  mth
        ,sum(uv) gj_uv
    from q_gj_uv
    group by 1
) t1 
left join (
    select mth,uv
    from q_uv
) t2 on t1.mth=t2.mth
order by 1
;




---- 新客渗透占比
with user_type as
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,q_gj_uv as (--- 国酒WAU
    select  concat(year(max(dt)), '~', lpad(weekofyear(dt), 2, '0')) as year_week
            ,concat(min(dt), '~', max(dt)) week
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他' end as mdd
            ,count(distinct a.user_id) uv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2024-12-30' 
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and( a.dt <= b.min_order_date or b.user_name  is null)
    group by year(date_trunc('week', dt)), weekofyear(dt),mdd
)
,q_uv as (--- 去哪儿APP WAU
    select concat(year(max(dt)), '~', lpad(weekofyear(dt), 2, '0')) as year_week
           ,concat(min(dt), '~', max(dt)) week
           ,count(distinct username) uv
    from pub.dws_flow_app_wechat_active_user_di  a
    left join user_type b on a.username = b.user_name 
    where dt>= '2024-12-30' 
        and dt <= date_sub(current_date,1)
        and channel='APP' and trim(username)!=''
        and( a.dt <= b.min_order_date or b.user_name  is null)
    group by  year(date_trunc('week', dt)), weekofyear(dt)
)

select t1.year_week,t1.week,t1.gj_uv,uv,t1.gj_uv / uv  re
from (
    select  year_week
        ,week
        ,sum(uv) gj_uv
    from q_gj_uv
    group by 1,2
) t1 
left join (
    select year_week,week,uv
    from q_uv
) t2 on t1.year_week=t2.year_week and t1.week=t2.week
order by 1
;



with q_gj_uv as (--- 国酒WAU
select  concat(year(max(dt)), '~', lpad(weekofyear(dt), 2, '0')) as year_week
        ,concat(min(dt), '~', max(dt)) week
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                    when e.area in ('欧洲','亚太','美洲') then e.area
                    else '其他' end as mdd
        ,count(distinct a.user_id) uv
from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
where dt >= '2024-12-30' 
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
group by year(date_trunc('week', dt)), weekofyear(dt),mdd
)
,q_uv as (--- 去哪儿APP WAU
    select concat(year(max(dt)), '~', lpad(weekofyear(dt), 2, '0')) as year_week
           ,concat(min(dt), '~', max(dt)) week
           ,count(distinct username) uv
    from pub.dws_flow_app_wechat_active_user_di 
    where dt>= '2024-12-30' 
        and dt <= date_sub(current_date,1)
        and channel='APP' and trim(username)!=''
    group by  year(date_trunc('week', dt)), weekofyear(dt)
)

select t1.year_week,t1.week,t1.gj_uv,uv,t1.gj_uv / uv  re
from (
    select  year_week
        ,week
        ,sum(uv) gj_uv
    from q_gj_uv
    group by 1,2
) t1 
left join (
    select year_week,week,uv
    from q_uv
) t2 on t1.year_week=t2.year_week and t1.week=t2.week
order by 1
;