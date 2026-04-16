with display_table as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
            --- 不符房型
            ,case when ext_pricing_map['not_match_adult'] = 'true' then 'Y' else 'N' end as not_match_adult  
            ,user_name,adults_num
    from ihotel_default.dw_hotel_price_display
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= date_sub(current_date, 30)
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7
)
,q_order as (
    select  order_date
        ,get_json_object(extendinfomap,'$.traceId') as traceId
        ,hotel_seq
        ,room_night
        ,a.order_no
        ,qta_product_id
        ,physical_room_id
        ,max_c
    from default.mdw_order_v3_international a
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= date_sub(current_date, 30)
        and get_json_object(extendinfomap,'$.traceId') is not null
)


select t1.dt
    ,count(distinct t1.traceId) `D页曝光pv`
    ,count(distinct case when t1.not_match_adult = 'Y' then t1.traceId end) as `D页不符房型曝光pv`
    ,count(distinct t1.user_name) as `D页曝光uv`
    ,count(distinct case when t1.not_match_adult = 'Y' then t1.user_name end) as `D页不符房型曝光uv`
    ,count(distinct t2.order_no) as `D页订单量`
    ,count(distinct case when t1.not_match_adult = 'Y' then t2.order_no end) as `D页不符房型订单量`
    ,count(distinct case when t1.not_match_adult = 'Y'  and t1.adults_num > t2.max_c then t2.order_no end) as `D页不符房型订单量-成人数超标`
    ,count(distinct case when t1.not_match_adult = 'N'  then t1.room_id end) as `D页符合房型曝光数`
    ,count(distinct case when t1.not_match_adult = 'Y'  then t1.room_id end) as `D页不符房型曝光数`

    ,count(distinct case when t1.not_match_adult = 'Y' then t2.order_no end) / count(distinct case when t1.not_match_adult = 'Y' then t1.user_name end) as `D页不符房型CR`
    ,count(distinct case when t1.not_match_adult = 'Y' then t1.traceId end) / count(distinct t1.traceId) as `D页不符房型曝光率`
    ,count(distinct case when t1.not_match_adult = 'Y' then t2.order_no end) / count(distinct t2.order_no) as `D页不符房型订单占比`
    ,count(distinct case when t1.not_match_adult = 'Y'  and t1.adults_num > t2.max_c then t2.order_no end) / count(distinct t2.order_no) as `D页不符房型订单占比-成人数超标` 
from display_table t1
left join q_order t2 on t1.traceId = t2.traceId and t1.room_id = t2.qta_product_id and t1.physical_room_id = t2.physical_room_id 
    -- and t1.dt=t2.order_date
group by 1
order by 1 desc ;
