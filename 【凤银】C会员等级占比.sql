select user_level
    ,count(distinct user_id) as ouv_cnt
    ,count(distinct order_no) as order_cnt
    ,sum(room_night) as room_night_cnt
    ,concat(round((count(distinct user_id) / sum(count(distinct user_id)) over()) * 100, 2), '%') as ouv_rate
    ,concat(round((count(distinct order_no) / sum(count(distinct order_no)) over()) * 100, 2), '%') as order_rate
    ,concat(round((sum(room_night) / sum(sum(room_night)) over()) * 100, 2), '%') as room_night_rate
from (
    select substr(o.order_date,1,10) as dt          
          ,case when extend_info['user_grade'] = 'Normal' then '大众'
                when extend_info['user_grade'] = 'Silver' then '白银'
                when extend_info['user_grade'] = 'Gold' then '黄金'
                when extend_info['user_grade'] = 'Platnium' then '铂金'
                when extend_info['user_grade'] = 'Diamond' then '钻石'
                when extend_info['user_grade'] = 'Gold Diamond' then '金钻'
                when extend_info['user_grade'] = 'Black Diamond' then '黑钻'
                else '其他' end as user_level
          ,order_no,extend_info['room_night'] room_night,o.user_id
          
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= date_sub(current_date, 1)
) a
group by user_level
;


select user_level
    ,count(distinct user_id) as ouv_cnt
    ,count(distinct order_no) as order_cnt
    ,sum(room_night) as room_night_cnt
    ,concat(round((count(distinct user_id) / sum(count(distinct user_id)) over()) * 100, 2), '%') as ouv_rate
    ,concat(round((count(distinct order_no) / sum(count(distinct order_no)) over()) * 100, 2), '%') as order_rate
    ,concat(round((sum(room_night) / sum(sum(room_night)) over()) * 100, 2), '%') as room_night_rate
from (
    select order_date
            ,a.user_id,init_gmv,order_no,room_night,t.level_desc as user_level
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join pub.dim_user_profile_nd t on a.user_id = t.user_id 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
) a
group by user_level