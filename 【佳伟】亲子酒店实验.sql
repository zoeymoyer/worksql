
with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date
                ,user_info['orig_device_id'] as orig_device_id
                ,user_id
                ,user_name
                ,count(order_no) as order_nos_90
                ,sum(room_night) as room_nights_90
            from mdw_order_v3_international
            where dt = '%(DATE)s'
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2,3,4
        )a where room_nights_90>=15
    group by 1
)
,abtest AS (--- 实验明细
    select  a.dt,
            ab_version version,
            ab_exp_value AS uid
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    where a.dt >= '2026-06-16'  and a.dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '260609_ho_gj_ParentChildScenario'
        and ab_exp_value not in (select user_id from biguser)  --- 去除大单用户
    group by 1,2,3
)
,family_hotel as (-- 亲子酒店  
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-06-18' 
        and dt <= date_sub(current_date, 1)
        and tree_node_name='亲子酒店'
    group by 1
)
,family_rank_popup as ( --- 亲子口碑榜  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
        ,key
        ,get_json_object(value, '$.ext.hotelSeq') hotelSeq
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in( 'ihotel/Rank/Access/show/hotelCard', 'ihotel/touchDetail/default/click/rankHotelClick'
        )
        and get_json_object(value, '$.ext.source') = 'app'
        and get_json_object(value, '$.ext.rankingId') = '100200156519'
    group by 1,2,3,4
)
,room_layer_popup as ( --- 房型浮层页曝光  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
        -- ,key
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in('ihotel/detail/priceList/show/roomInfoShow'
        )
        and get_json_object(value, '$.ext.hotelType') = '1'
    group by 1,2
)
,rapid_popup as ( --- 亲子快筛栏曝光  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
    from default.dw_qav_hotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in('ihotel/List/Filter/show/parentChildRapidScreenExposure'
        )
    group by 1,2
)
,rapid_popup_clk as ( --- 亲子快筛栏点击  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
    from default.dw_qav_hotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in('ihotel/Rank/Access/show/hotelCard')
    group by 1,2
)
,family_rapid_popup as ( --- 家庭亲子快筛栏按键点击  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
        -- ,key
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in( 'ihotel/List/Filter/click/parentChildRapidScreen'
        )
    group by 1,2
)
,display_table as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
            ,user_id,user_name,children_num,hotel_seq
    from ihotel_default.dw_hotel_price_display
    where dt >= '20260616' and dt <= '%(DATE)s'
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7,8
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
    where  dt = '%(DATE)s'
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

select t1.dt,version
        ,count(distinct t1.user_name) `D页曝光uv`
        ,count(distinct case when t1.children_info = '亲子场景' then t1.user_name end) `亲子场景曝光uv`
        ,count(distinct case when t1.family_hotel_info = '亲子酒店' then t1.user_name end) `亲子酒店曝光uv`
        ,count(distinct case when t1.family_rank_info = '亲子口碑榜' then t1.user_name end) `亲子口碑榜曝光uv`
        ,count(distinct case when t1.room_layer_info = '亲子酒店房型浮层' then t1.user_name end) `亲子酒店房型浮层曝光uv`

        ,count(distinct case when t1.children_info = '非亲子场景' then t1.user_name end) `非亲子场景曝光uv`
        ,count(distinct case when t1.family_hotel_info = '非亲子酒店' then t1.user_name end) `非亲子酒店曝光uv`
        ,count(distinct case when t1.family_rank_info = '非亲子口碑榜' then t1.user_name end) `非亲子口碑榜曝光uv`
        ,count(distinct case when t1.room_layer_info = '非亲子酒店房型浮层' then t1.user_name end) `非亲子酒店房型浮层曝光uv`   

        ,count(distinct t2.order_no) `D页订单量`
        ,count(distinct case when t1.children_info = '亲子场景' then t2.order_no end) `亲子场景曝光订单量`
        ,count(distinct case when t1.family_hotel_info = '亲子酒店' then t2.order_no end) `亲子酒店曝光订单量`
        ,count(distinct case when t1.family_rank_info = '亲子口碑榜' then t2.order_no end) `亲子口碑榜曝光订单量`
        ,count(distinct case when t1.room_layer_info = '亲子酒店房型浮层' then t2.order_no end) `亲子酒店房型浮层曝光订单量`

        ,count(distinct case when t1.children_info = '非亲子场景' then t2.order_no end) `非亲子场景曝光订单量`
        ,count(distinct case when t1.family_hotel_info = '非亲子酒店' then t2.order_no end) `非亲子酒店曝光订单量`
        ,count(distinct case when t1.family_rank_info = '非亲子口碑榜' then t2.order_no end) `非亲子口碑榜曝光订单量`
        ,count(distinct case when t1.room_layer_info = '非亲子酒店房型浮层' then t2.order_no end) `非亲子酒店房型浮层曝光订单量` 

        ,count(distinct t3.user_name)  `快筛栏曝光uv`
        ,count(distinct t4.user_name)  `快筛栏点击uv` 
        ,count(distinct t5.user_name)  `家庭亲子快筛栏按键点击uv` 
from (
    select t1.dt,t2.version,t1.traceId,t1.user_name,t1.room_id,t1.physical_room_id
        ,case when children_num > 0 then '亲子场景' else '非亲子场景' end as children_info
        ,case when t3.hotel_seq is not null then '亲子酒店' else '非亲子酒店' end as family_hotel_info
        ,case when t4.hotelSeq is not null then '亲子口碑榜' else '非亲子口碑榜' end as family_rank_info
        ,case when t5.user_name is not null then '亲子酒店房型浮层' else '非亲子酒店房型浮层' end as room_layer_info
    from display_table t1 
    left join abtest t2 on t1.user_id = t2.uid and t1.dt = t2.dt
    left join family_hotel t3 on t1.hotel_seq = t3.hotel_seq
    left join family_rank_popup t4 on t1.user_name=t4.user_name and t1.dt=t4.dt and t1.hotel_seq=t4.hotelSeq
    left join room_layer_popup t5 on t1.user_name = t5.user_name and t1.dt = t5.dt
    where t2.uid is not null
)t1 
left join q_order t2 on t1.traceId = t2.traceId and t1.room_id = t2.qta_product_id and t1.physical_room_id = t2.physical_room_id 
left join rapid_popup t3 on t1.user_name=t3.user_name and t1.dt=t3.dt
left join rapid_popup_clk t4 on t1.user_name=t4.user_name and t1.dt=t4.dt
left join family_rapid_popup t5 on t1.user_name=t5.user_name and t1.dt=t5.dt
group by 1,2
order by 1,2 desc ;



--- 不分实验
with family_hotel as (-- 亲子酒店  
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-06-18' 
        and dt <= date_sub(current_date, 1)
        and tree_node_name='亲子酒店'
    group by 1
)
,family_rank_popup as ( --- 亲子口碑榜  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
        ,key
        ,get_json_object(value, '$.ext.hotelSeq') hotelSeq
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in( 'ihotel/Rank/Access/show/hotelCard', 'ihotel/touchDetail/default/click/rankHotelClick'
        )
        and get_json_object(value, '$.ext.source') = 'app'
        and get_json_object(value, '$.ext.rankingId') = '100200156519'
    group by 1,2,3,4
)
,room_layer_popup as ( --- 房型浮层页曝光  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in('ihotel/detail/priceList/show/roomInfoShow'
        )
        and get_json_object(value, '$.ext.hotelType') = '1'
    group by 1,2
)
,rapid_popup as ( --- 亲子快筛栏曝光  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
    from default.dw_qav_hotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in('ihotel/List/Filter/show/parentChildRapidScreenExposure'
        )
    group by 1,2
)
,rapid_popup_clk as ( --- 亲子快筛栏点击  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
    from default.dw_qav_hotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in('ihotel/Rank/Access/show/hotelCard')
    group by 1,2
)
,family_rapid_popup as ( --- 家庭亲子快筛栏按键点击  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dt
        ,user_name
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20260616' AND '%(DATE)s'
        and key in( 'ihotel/List/Filter/click/parentChildRapidScreen'
        )
    group by 1,2
)
,display_table as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
            ,uid,user_name,children_num,hotel_seq
    from ihotel_default.dw_hotel_price_display
    where dt >= '20260616' and dt <= '%(DATE)s'
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7,8
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
    where  dt = '%(DATE)s'
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

select t1.dt
        ,count(distinct t1.user_name) `D页曝光uv`
        ,count(distinct case when t1.children_info = '亲子场景' then t1.user_name end) `亲子场景曝光uv`
        ,count(distinct case when t1.family_hotel_info = '亲子酒店' then t1.user_name end) `亲子酒店曝光uv`
        ,count(distinct case when t1.family_rank_info = '亲子口碑榜' then t1.user_name end) `亲子口碑榜曝光uv`
        ,count(distinct case when t1.room_layer_info = '亲子酒店房型浮层' then t1.user_name end) `亲子酒店房型浮层曝光uv`

        ,count(distinct case when t1.children_info = '非亲子场景' then t1.user_name end) `非亲子场景曝光uv`
        ,count(distinct case when t1.family_hotel_info = '非亲子酒店' then t1.user_name end) `非亲子酒店曝光uv`
        ,count(distinct case when t1.family_rank_info = '非亲子口碑榜' then t1.user_name end) `非亲子口碑榜曝光uv`
        ,count(distinct case when t1.room_layer_info = '非亲子酒店房型浮层' then t1.user_name end) `非亲子酒店房型浮层曝光uv`   

        ,count(distinct t2.order_no) `D页订单量`
        ,count(distinct case when t1.children_info = '亲子场景' then t2.order_no end) `亲子场景曝光订单量`
        ,count(distinct case when t1.family_hotel_info = '亲子酒店' then t2.order_no end) `亲子酒店曝光订单量`
        ,count(distinct case when t1.family_rank_info = '亲子口碑榜' then t2.order_no end) `亲子口碑榜曝光订单量`
        ,count(distinct case when t1.room_layer_info = '亲子酒店房型浮层' then t2.order_no end) `亲子酒店房型浮层曝光订单量`

        ,count(distinct case when t1.children_info = '非亲子场景' then t2.order_no end) `非亲子场景曝光订单量`
        ,count(distinct case when t1.family_hotel_info = '非亲子酒店' then t2.order_no end) `非亲子酒店曝光订单量`
        ,count(distinct case when t1.family_rank_info = '非亲子口碑榜' then t2.order_no end) `非亲子口碑榜曝光订单量`
        ,count(distinct case when t1.room_layer_info = '非亲子酒店房型浮层' then t2.order_no end) `非亲子酒店房型浮层曝光订单量` 

        ,count(distinct t3.user_name)  `快筛栏曝光uv`
        ,count(distinct t4.user_name)  `快筛栏点击uv` 
        ,count(distinct t5.user_name)  `家庭亲子快筛栏按键点击uv` 
from (
    select t1.dt,t1.traceId,t1.user_name,t1.room_id,t1.physical_room_id
        ,case when children_num > 0 then '亲子场景' else '非亲子场景' end as children_info
        ,case when t3.hotel_seq is not null then '亲子酒店' else '非亲子酒店' end as family_hotel_info
        ,case when t4.hotelSeq is not null then '亲子口碑榜' else '非亲子口碑榜' end as family_rank_info
        ,case when t5.user_name is not null then '亲子酒店房型浮层' else '非亲子酒店房型浮层' end as room_layer_info
    from display_table t1 
    left join family_hotel t3 on t1.hotel_seq = t3.hotel_seq
    left join family_rank_popup t4 on t1.user_name=t4.user_name and t1.dt=t4.dt and t1.hotel_seq=t4.hotelSeq
    left join room_layer_popup t5 on t1.user_name = t5.user_name and t1.dt = t5.dt
)t1 
left join q_order t2 on t1.traceId = t2.traceId and t1.room_id = t2.qta_product_id and t1.physical_room_id = t2.physical_room_id 
left join rapid_popup t3 on t1.user_name=t3.user_name and t1.dt=t3.dt
left join rapid_popup_clk t4 on t1.user_name=t4.user_name and t1.dt=t4.dt
left join family_rapid_popup t5 on t1.user_name=t5.user_name and t1.dt=t5.dt
group by 1
order by 1 desc ;

