with hotel_data as (
    select   case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,city_name
            ,city_code
            ,hotel_grade
            ,hotel_seq
            ,hotel_name
            ,count(distinct order_no) order_no
            ,sum(init_gmv) init_gmv
            ,sum(room_night) room_night
            ,sum(init_gmv) / sum(room_night) adr
            ,'2025-01-28' dt
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and order_date >= '2025-01-28' and order_date <= '2025-02-04'
        and order_no <> '103576132435'
    group by 1,2,3,4,5,6
)
-- ,q_order as (
--     select  order_date
--             ,hotel_seq
--             ,count(distinct order_no) order_no
--             ,sum(init_gmv) init_gmv
--             ,sum(room_night) room_night
--             ,sum(init_gmv) / sum(room_night) adr
--     from default.mdw_order_v3_international a 
--     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
--     where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
--         and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
--         and terminal_channel_type in ('www','app','touch')
--         and is_valid='1'
--         and order_date >= '2024-02-28' 
--         and order_no <> '103576132435'
--     group by 1,2
-- )
,q_order as (
    select  concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))  order_date 
            ,hotel_seq
            ,min(price) adr
    from ihotel_default.dw_hotel_price_display a 
    where dt  >= '20250101'  and dt <= '20250128'
    group by 1,2
)

select t1.hotel_name,t1.mdd,city_code,city_name,hotel_grade,t1.hotel_seq,t1.room_night,t7.adr adr_t28,t6.adr adr_t21,t5.adr adr_t14,t4.adr adr_t7,t3.adr adr_t3,t2.adr adr_t0
from hotel_data t1
left join q_order t2 on t1.hotel_seq=t2.hotel_seq and datediff(t1.dt,t2.order_date) = 0
left join q_order t3 on t1.hotel_seq=t3.hotel_seq and datediff(t1.dt,t3.order_date) = 3
left join q_order t4 on t1.hotel_seq=t4.hotel_seq and datediff(t1.dt,t4.order_date) = 7
left join q_order t5 on t1.hotel_seq=t5.hotel_seq and datediff(t1.dt,t5.order_date) = 14
left join q_order t6 on t1.hotel_seq=t6.hotel_seq and datediff(t1.dt,t6.order_date) = 21
left join q_order t7 on t1.hotel_seq=t7.hotel_seq and datediff(t1.dt,t7.order_date) = 28
where t1.order_no >= 10
;


---- 取消后订单数据
with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,conpon_data as (
    select  uid,coupon_id 
          -- ,create_time 
          ,to_date(start_time) as start_date ,to_date(end_time) as end_date,total_fund
    from ihotel_default.ods_hotel_qta_coupon_di
    where dt >= date_sub(current_date, 180)
    and activity_id_ref='20160726113759054704'
    and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83','23base_ZK_728810','23extra_ZK_ce6f99')
    group by 1,2,3,4,5
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度 
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,order_no
            ,order_time
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
                  and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                  and (refund_time is null or date(refund_time) > order_date)
                  then 'Y' else 'N' end is_cancel_t0
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-12-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)


select t1.order_date
      ,count(distinct t1.order_no) order_no   --- 当日生单量
      ,count(distinct case when t1.is_cancel_t0 = 'N' then t1.order_no end) cancel_order_no_t0 --- 当日取消订单量
      ,count(distinct case when t1.is_cancel_t0 = 'N' then t1.user_id end) cancel_order_uv_t0  --- 当日取消订单uv
      ,count(distinct case when t1.is_cancel_t0 = 'N' then t3.order_no end) cancel_order_re    --- 当日取消订单用户24h再次下单量
      ,count(distinct case when t1.is_cancel_t0 = 'N' then t3.user_id end) cancel_order_uv_re  --- 当日取消订单用户24h再次下单UV
      ,count(distinct case when t2.uid is not null and t1.order_date>=start_date and t1.order_date<=end_date and  t1.is_cancel_t0 = 'N' then t1.user_id end) cancel_order_conpon_uv_t0  --- 当日取消订单用户有券UV
from q_order t1
left join conpon_data t2 on t1.user_id=t2.uid
left join q_order t3 
on t1.user_id=t3.user_id and unix_timestamp(t3.order_time) - unix_timestamp(t1.order_time) between 1 and 86400 and t3.order_date >= t1.order_date
group by 1
;


---- 取消订单后行为
with q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,unix_timestamp(order_time) order_time
            ,hotel_seq,physical_room_id
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,room_info as (
    select order_date
            ,a.user_id,order_no,physical_room_id,hotel_seq
    from mdw_order_v3_international a 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and order_no <> '103576132435'
)
,cancel_data as (
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
            ,user_name
            ,unix_timestamp(concat(log_date, ' ' ,log_time)) log_time
            ,get_json_object(get_json_object(value, '$.ext.res'), '$.outTradeNo') order_no
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20251220' and dt <= '%(DATE)s'
        and key = 'ihotel/Booking/Pay/resp/XCPay'
        and get_json_object(value, '$.ext.code') IN ('-3', '-6')
    group by 1,2,3,4
)

select t1.dt
        ,count(distinct t1.user_name)    `取消支付UV`
        ,count(distinct t3.user_name)    `取消支付后成单UV（1h）`
        ,count(distinct case when t2.hotel_seq=t3.hotel_seq then t3.user_name end) `取消支付后同酒店成单UV（1h）`
        ,count(distinct case when t2.physical_room_id=t3.physical_room_id then t3.user_name end) `取消支付后同房型成单UV（1h）`
from cancel_data t1 
left join room_info t2 on t1.order_no=t2.order_no
left join q_order t3 on t1.user_name=t3.user_name 
and t1.order_no!=t3.order_no and t3.order_time - t1.log_time between 1 and 3600
group by 1
order by 1
;



pp_pub.dwd_smm_v9_union_funnel_analysis_detail_di
pp_pub.dwd_smm_ord_user_firstord_mi
pp_pub.dwd_redbook_spotlight_creative_cost_info_da
pp_pub.dwd_redbook_notes_detail_nd_copy
pp_pub.dwd_redbook_spotlight_creative_cost_info_inter_hotel_da

smm.dim_redbook_query_info_stat_apply_org_da
temp.temp_jiawen_huang_redbook_futou_plan_list_20251208_forever	
			