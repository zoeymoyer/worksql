select t1.mth,hotel_name,hotel_seq,t1.country_name,t1.city_name,room_night,room_night_citys
      ,concat(round(room_night/room_night_citys*100,2),'%') ratio
from (
    select substr(order_date,1,7) mth
            ,hotel_name,hotel_seq
            ,max(a.country_name)country_name 
            ,max(city_name)city_name
            ,sum(room_night) room_night
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2024-01-01' and order_date <= '2024-12-31'
        and order_no <> '103576132435'
        and hotel_seq in ('i-aklan_606','i-timur_laut_pulau_pinang_1041','i-bahagian_pantai_barat_623','i-cebu_1388','i-singapore_149','i-singapore_576','i-bahagian_pantai_barat_989','i-bangkok_4471','i-hong_kong_596')
    group by 1,2,3
) t1 left join (
    select substr(order_date,1,7) mth
            ,a.country_name 
            ,city_name
            ,sum(room_night) room_night_citys
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2024-01-01' and order_date <= '2024-12-31'
        and order_no <> '103576132435'
        and city_name in ('西海岸省','东北县','曼谷','新加坡','宿务','阿卡兰','香港')
        and a.country_name in ('马来西亚','中国','泰国','新加坡','菲律宾')
    group by 1,2,3
) t2 on t1.mth=t2.mth and t1.country_name=t2.country_name and t1.city_name=t2.city_name
order by t1.mth asc, t1.room_night desc
;
