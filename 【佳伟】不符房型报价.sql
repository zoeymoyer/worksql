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


--- 房型报价平铺场景
with display_table as ( -- 每日D页曝光及平铺曝光情况
    select    dt
            ,user_name
            ,case when tiled_price = 'true' then 'Y' else 'N' end as is_tiled
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
    from ihotel_default.dw_hotel_price_display
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= date_sub(current_date, 30)
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6
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
    ,count(distinct case when t1.is_tiled = 'Y' then t1.traceId end) as `房型报价平铺曝光pv`
    ,count(distinct t1.user_name) as `D页曝光uv`
    ,count(distinct case when t1.is_tiled = 'Y' then t1.user_name end) as `房型报价平铺曝光uv`
    ,count(distinct t2.order_no) as `D页订单量`
    ,count(distinct case when t1.is_tiled = 'Y' then t2.order_no end) as `D页不符房型订单量`

    ,count(distinct t2.order_no) / count(distinct t1.user_name) as `D页整体CR`
    ,count(distinct case when t1.is_tiled = 'Y' then t2.order_no end) / count(distinct case when t1.is_tiled = 'Y' then t1.user_name end) as `房型报价平铺CR`
    ,count(distinct case when t1.is_tiled = 'Y' then t2.order_no end) / count(distinct t2.order_no) as `房型报价平铺订单占比`
from display_table t1
left join q_order t2 on t1.traceId = t2.traceId and t1.room_id = t2.qta_product_id 
        and t1.physical_room_id = t2.physical_room_id 
    -- and t1.dt=t2.order_date
group by 1
order by 1 desc ;


with display_table as ( -- 每日D页曝光及平铺曝光情况
    select    dt
            ,user_name
            ,tiled_price
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
    from ihotel_default.dw_hotel_price_display
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= date_sub(current_date, 30)
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6
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

select t1.dt,tiled_price
    ,count(distinct t1.traceId) `D页曝光pv`
    ,count(distinct t1.user_name) as `D页曝光uv`
    ,count(distinct t2.order_no) as `D页订单量`

    ,count(distinct t2.order_no) / count(distinct t1.user_name) as `CR`
from display_table t1
left join q_order t2 on t1.traceId = t2.traceId and t1.room_id = t2.qta_product_id 
        and t1.physical_room_id = t2.physical_room_id 
    -- and t1.dt=t2.order_date
group by 1,2
order by 1 desc ;

--- 
with display_table as ( -- 每日D页曝光及平铺曝光情况
    select    dt
            ,user_name
            ,tiled_price
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
    from ihotel_default.dw_hotel_price_display
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= date_sub(current_date, 30)
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6
)
,book_table as (
    select replace(dt, '-', '') as dt,user_name
            ,qtrace_id
            ,split(twell_product_id,'_')[0] as room_id
    from ihotel_default.dw_user_app_log_booking_di_v1
    where  dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
)

select t1.dt,tiled_price
    ,count(distinct t1.traceId) `D页曝光pv`
    ,count(distinct t1.user_name) as `D页曝光uv`

    ,count(distinct t2.qtrace_id) as `B页曝光pv`
    ,count(distinct t2.user_name) as `B页曝光uv`
from display_table t1
left join book_table t2 on t1.room_id = t2.room_id and t1.user_name = t2.user_name 
        and t1.dt=t2.dt
group by 1,2
order by 1 desc ;



--- 桐桐：C无报价&Q有报价数据
with display_table as ( 
    select dt
        ,count(distinct case when flag_a2 is not null then hotel_seq end) as `C无报价&Q有报价酒店数(酒店维度)`
        ,count(distinct case when flag_b2 is not null then hotel_seq end) as `C无报价&Q有报价酒店数(物理房型维度)`
    from (
        select    concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
                ,hotel_seq
                ,flag_a2,flag_b2
        from ihotel_default.dw_hotel_price_display
        where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= '2026-04-20'
            and (flag_a2 is not null or flag_b2 is not null)
        group by 1,2,3,4
    ) t group by 1
)
,order_table as (
    select order_date
        ,count(distinct case when flag_a2 is not null then hotel_seq end) as `C无报价&Q有报价酒店数(酒店维度)`
        ,count(distinct case when flag_b2 is not null then hotel_seq end) as `C无报价&Q有报价酒店数(物理房型维度)`
    from (
        select order_date
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
                ,a.user_id,init_gmv,order_no,room_night
                ,hotel_seq
                ,ext_flag_map['flag_a1'] as flag_a1
                ,ext_flag_map['flag_a2'] as flag_a2
                ,ext_flag_map['flag_b2'] as flag_b2
            
        from default.mdw_order_v3_international a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            -- and terminal_channel_type in ('www','app','touch')
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid='1'
            and order_date >= '2026-04-20' and order_date <= date_sub(current_date, 1)
            and order_no <> '103576132435'
            and (ext_flag_map['flag_a2'] is not null or ext_flag_map['flag_b2'] is not null)
    ) t group by 1
)

select coalesce(t1.dt, t2.order_date) as dt
    ,t1.`C无报价&Q有报价酒店数(酒店维度)` as `D页C无报价&Q有报价酒店数(酒店维度)`
    ,t1.`C无报价&Q有报价酒店数(物理房型维度)` as `D页C无报价&Q有报价酒店数(物理房型维度)`
    ,t2.`C无报价&Q有报价酒店数(酒店维度)` as `订单C无报价&Q有报价酒店数(酒店维度)`
    ,t2.`C无报价&Q有报价酒店数(物理房型维度)` as `订单C无报价&Q有报价酒店数(物理房型维度)`
from display_table t1
full join order_table t2 on t1.dt = t2.order_date
order by 1 desc ;



with display_table as ( 
    select    concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
            ,hotel_seq
            ,flag_a2,flag_b2
    from ihotel_default.dw_hotel_price_display
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= '2026-04-20'
        and (flag_a2 is not null or flag_b2 is not null)
    group by 1,2,3,4
)
,order_table as (
    select order_date
        ,hotel_seq
        ,flag_a2,flag_b2
    from (
        select order_date
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
                ,a.user_id,init_gmv,order_no,room_night
                ,hotel_seq
                ,ext_flag_map['flag_a1'] as flag_a1
                ,ext_flag_map['flag_a2'] as flag_a2
                ,ext_flag_map['flag_b2'] as flag_b2
            
        from default.mdw_order_v3_international a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            -- and terminal_channel_type in ('www','app','touch')
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid='1'
            and order_date >= '2026-04-20' and order_date <= date_sub(current_date, 1)
            and order_no <> '103576132435'
            and (ext_flag_map['flag_a2'] is not null or ext_flag_map['flag_b2'] is not null)
    ) t 
)
,xray_table as (
    select dt ,hotel_seq
    from ihotel_default.dw_data_log_hour_xray_intl_galaxy_filter 
    where dt >= '20260420' and msg_3 = '605' and msg_2='FF'
    group by 1,2
)

select coalesce(t1.dt, t2.order_date) as dt
    ,t1.hotel_seq `D页酒店id`,t1.flag_a2 `D页flag_a2`,t1.flag_b2 `D页flag_b2`
    ,t2.hotel_seq `订单酒店id`,t2.flag_a2 `订单flag_a2`,t2.flag_b2 `订单flag_b2`
    ,t3.hotel_seq `Xray酒店id`
from display_table t1
full join order_table t2 on t1.dt = t2.order_date
left join xray_table t3 on t1.hotel_seq = t3.hotel_seq and t1.dt = concat(substr(t3.dt,1,4),'-',substr(t3.dt,5,2),'-',substr(t3.dt,7,2))
order by 1 desc ;
