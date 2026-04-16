with q_order as (
    select hotel_seq,hotel_name,count(distinct order_no) as order_cnt,sum(room_night) as room_night_cnt
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type in ('www','app','touch')
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid='1'
            and order_date >= date_sub(current_date, 90) and order_date <= date_sub(current_date,1)
            and order_no <> '103576132435'
    group by 1,2
)
,review_info as (--- cretime限制3年内
    select obj_seq hotel_seq,count(ext5) as totalCount,sum(ext5) as totalSum
    from default.ods_qunar_review_comments
        where dt = '%(DATE)s' 
        and obj_seq like 'i-%' 
        and cretime > '2023-03-27' 
        and pid = 0 
        and rid = 0 
        and status = 2
    group by 1
)
,score_info as (
    select obj_seq hotel_seq,score
    from default.ods_qunar_review_obj_score
        where dt = '%(DATE)s' 
        and obj_seq like 'i-%' 
        and tag = 1
    group by 1,2
)
,search_list as(
    select a.hotel_seq
           ,count(distinct a.search_request_uid) show_pv
           ,count(distinct a.orig_device_id) show_uv
    from default.dwd_ihotel_flow_app_searchlist_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join ihotel_default.dim_baseinfo_hotel_level_sabc_mi c on c.hotel_seq = a.hotel_seq and c.dt = date_sub(current_date,1)
    where a.dt >= replace(date_sub(current_date,30),'-','') and a.dt <= replace(date_sub(current_date,1),'-','')
        and (a.province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and orig_device_id is not null
        and orig_device_id != ''
        and search_type in (0, 16, 17)
        and a.is_display = '1'
    group by a.hotel_seq
)
,q_c_hotel_mapping as (
    select 
        hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
    group by 1,2
)

select a.hotel_seq,a.hotel_name,a.order_cnt,a.room_night_cnt,b.totalCount,c.score,d.show_pv,d.show_uv,e.partner_hotel_id
from q_order a
left join review_info b on a.hotel_seq = b.hotel_seq
left join score_info c on a.hotel_seq = c.hotel_seq
left join search_list d on a.hotel_seq = d.hotel_seq
left join q_c_hotel_mapping e on a.hotel_seq = e.hotel_seq
order by a.room_night_cnt desc
;




