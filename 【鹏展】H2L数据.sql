with h_uv as ( --- H页流量
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,a.user_name
    from default.dw_qav_ihotel_track_info_di a
    where dt >= '20251201' and dt <= '%(DATE)s'
        and key in ('ihotel/home/preload/monitor/homePreFetch'      --- H页曝光
        )
    group by 1,2
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     where dt >= '2025-12-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)

select t1.dt,h_uv,ldbo_uv,h_uv_z,ldbo_uv_z,ldbo_uv / h_uv h2l,ldbo_uv_z / h_uv_z h2l_z
from (
    select dt,count(distinct user_name) h_uv
    from h_uv
    group by 1
) t1 left join (
    select dt,count(distinct user_name) ldbo_uv
    from uv
    group by 1 
)t2 on t1.dt=t2.dt
left join (
    select t1.dt,count(distinct t1.user_name) h_uv_z
            ,count(distinct t1.user_name) ldbo_uv_z
    from h_uv t1 
    left join uv t2 on t1.dt=t2.dt and t1.user_name=t2.user_name
    group by 1

)t3 on t1.dt=t3.dt
order by 1
;