with display_table as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,substring_index(room_id, '_', 1) as room_id
            ,physical_room_id
            ,user_id
            ,user_name
            ,children_num
            ,hotel_seq
            ,cast(min_amount as int) as min_amount
            ,cast(product_room_index as int) as product_room_index
    from ihotel_default.dw_hotel_price_display
    where dt >= '20260616' and dt <= '%(DATE)s'
        and get_json_object(extendinfomap,'$.traceId') is not null
        and substring_index(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7,8,9,10
)
,q_order as (
    select  order_date
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,user_info['orig_device_id'] as orig_device_id
            ,hotel_seq
            ,room_night
            ,a.order_no
            ,qta_product_id
            ,physical_room_id
            ,max_c
    from default.mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= '2026-06-16'
        and get_json_object(extendinfomap,'$.traceId') is not null
)
,base as (
    select  t1.dt
            ,t1.traceId
            ,t1.room_id
            ,t1.physical_room_id
            ,t1.user_id
            ,t1.user_name
            ,t1.hotel_seq
            ,t1.min_amount
            ,t1.product_room_index
            ,t2.order_no
            ,t2.room_night
    from display_table t1
    left join q_order t2
        on t1.hotel_seq=t2.hotel_seq
        and t1.dt=t2.order_date
        and t1.traceId = t2.traceId
        and t1.room_id = t2.qta_product_id
        and t1.physical_room_id = t2.physical_room_id
)
,agg as (
    select  dt
            ,count(distinct user_id) as d_room_display_uv
            ,count(distinct case when min_amount >= 2 then user_id end) as multi_room_display_uv
            ,count(distinct order_no) as order_cnt
            ,count(distinct case when min_amount >= 2 then order_no end) as multi_room_order_cnt
            ,count(distinct case when min_amount >= 2 then concat(traceId, '_', room_id, '_', physical_room_id) end) as multi_room_quote_cnt
            ,count(distinct case when min_amount = 1 then order_no end) as non_multi_room_order_cnt
            ,count(distinct case when min_amount = 1 then concat(traceId, '_', room_id, '_', physical_room_id) end) as non_multi_room_quote_cnt
            ,count(distinct concat(traceId, '_', room_id, '_', physical_room_id)) as total_room_quote_cnt
            ,avg(case when min_amount >= 2 then product_room_index end) as multi_room_avg_quote_rank
    from base
    group by 1
)

select  dt as `日期`
        ,d_room_display_uv as `D页房型曝光量UV`
        ,multi_room_display_uv as `多间起订房型曝光量UV`
        ,order_cnt as `成单量`
        ,multi_room_order_cnt as `多间起订房型成单量`
        ,multi_room_quote_cnt as `多间起订产品房型报价数量`
        ,non_multi_room_order_cnt as `非多间起订房型成单量`
        ,non_multi_room_quote_cnt as `非多间起订产品房型报价量`
        ,total_room_quote_cnt as `整体产品房型报价数量`
        ,multi_room_avg_quote_rank as `多间起订房型报价位序`
        ,concat(round(multi_room_display_uv / nullif(d_room_display_uv, 0) * 100, 2), '%') as `多间起订房型曝光量占比`
        ,concat(round(multi_room_order_cnt / nullif(order_cnt, 0) * 100, 2), '%') as `多间起订房型成单占比`
        ,concat(round(multi_room_order_cnt / nullif(multi_room_display_uv, 0) * 100, 2), '%') as `多间起订房型CR`
        ,concat(round(non_multi_room_order_cnt / nullif(d_room_display_uv - multi_room_display_uv, 0) * 100, 2), '%') as `非多间起订房型CR`
        ,concat(round(multi_room_quote_cnt / nullif(total_room_quote_cnt, 0) * 100, 2), '%') as `多间起订房型报价数量占比`
        ,round(multi_room_avg_quote_rank, 2) as `多间起订房型报价平均位序`
from agg
order by `日期` desc
;



with display_table as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,substring_index(room_id, '_', 1) as room_id
            ,physical_room_id
            ,user_id
            ,user_name
            ,hotel_seq
    from ihotel_default.dw_hotel_price_display
    where dt >= '%(DATE_7)s' and dt <= '%(DATE)s'
        and get_json_object(extendinfomap,'$.traceId') is not null
        and substring_index(room_id, '_', 1) is not null
        and physical_room_id is not null
        and qs_extend_map['vendor_type'] = 2  --- 猜喜
    group by 1,2,3,4,5,6,7
)
,q_order as (
    select  order_date
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,user_info['orig_device_id'] as orig_device_id
            ,hotel_seq
            ,room_night
            ,a.order_no
            ,qta_product_id
            ,physical_room_id
            ,max_c
    from default.mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= '2026-06-16'
        and get_json_object(extendinfomap,'$.traceId') is not null
)
,base as (
    select  t1.dt
            ,t1.traceId
            ,t1.room_id
            ,t1.physical_room_id
            ,t1.user_id
            ,t1.user_name
            ,t1.hotel_seq
            ,t2.order_no
            ,t2.room_night
    from display_table t1
    left join q_order t2
        on t1.hotel_seq=t2.hotel_seq
        and t1.dt=t2.order_date
        and t1.traceId = t2.traceId
        and t1.room_id = t2.qta_product_id
        and t1.physical_room_id = t2.physical_room_id
)

select  dt
        ,count(distinct user_id) as d_room_display_uv
        ,count(distinct order_no) as order_cnt
        ,count(distinct order_no) /  count(distinct user_id) as cr
from base
group by 1
order by 1 desc
;



