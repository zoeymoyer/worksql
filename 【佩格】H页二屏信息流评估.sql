
--- 二屏信息流曝光点击数据
with h_infomation_flow_exp as (
    select dt,get_json_object(value,'$.ext.hotelSeq') hotelSeq,user_name
    from ihotel_default.dw_qav_hotel_track_info_di_v1 t1
    where dt >= '2026-06-27' and dt <= date_sub(current_date, 1)
        and key in ('ihotel/Home/Hcontent/show/contentShow')
    group by 1,2,3
)
,h_infomation_flow_clk as (
    select dt,get_json_object(value,'$.ext.hotelSeq') hotelSeq,user_name
    from ihotel_default.dw_qav_hotel_track_info_di_v1 t1
    where dt >= '2026-06-27' and dt <= date_sub(current_date, 1)
        and key in ('ihotel/Home/Hcontent/click/contentClick' )
    group by 1,2,3
)
,983hotel_list as (
    select hotel_seq
    from temp.temp_xiaohan_song_hexinjiudianchangshi_983
)
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id,hotel_seq
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-06-27' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

select t1.dt
        ,h_page_exp_uv
        ,h_page_exp_uv_983hotel
        ,h_page_click_uv
        ,h_page_click_uv_983hotel
        ,orders
        ,orders_983hotel
        ,concat(round(h_page_click_uv / h_page_exp_uv * 100, 2), '%') as click_rate
        ,concat(round(orders / h_page_exp_uv * 100, 2), '%') as order_rate
        ,concat(round(h_page_click_uv_983hotel / h_page_exp_uv_983hotel * 100, 2), '%') as click_rate_983hotel
        ,concat(round(orders_983hotel / h_page_exp_uv_983hotel * 100, 2), '%') as order_rate_983hotel
from (
    select t1.dt,count(distinct t1.user_name) as h_page_exp_uv
        ,count(distinct case when t2.hotel_seq is not null then t1.user_name end) as h_page_exp_uv_983hotel
    from h_infomation_flow_exp t1
    left join 983hotel_list t2 on t1.hotelSeq = t2.hotel_seq
    group by 1
) t1 
left join (
    select t1.dt
            ,count(distinct t1.user_name) as h_page_click_uv
            ,count(distinct case when t2.hotel_seq is not null then t1.user_name end) as h_page_click_uv_983hotel
            ,count(distinct t3.order_no) as orders
            ,count(distinct case when t2.hotel_seq is not null and t2.hotel_seq = t3.hotel_seq then t3.order_no end) as orders_983hotel
    from h_infomation_flow_clk t1
    left join 983hotel_list t2 on t1.hotelSeq = t2.hotel_seq
    left join q_order_app t3 on t1.user_name = t3.user_name and t1.dt = t3.order_date
    group by 1
)t2 on t1.dt = t2.dt
order by 1 desc
;





--- 二屏信息流实验数据
with abtest as (
    select a.dt
        ,ab_version as version
        ,ab_exp_value as device_id
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    where a.dt >= '2026-06-27'
        and a.dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'uid'
        and ab_exp_id = '260618_ho_gj_Hcontent'
    group by 1,2,3
)
,h_uv as (
    select dt, a.user_name,orig_device_id
    from ihotel_default.dw_qav_hotel_track_info_di_v1 a
    where dt >= '2026-06-27' and dt <= date_sub(current_date, 1)
        and key = 'hotel/home/searchCard/show/keyword'
        and get_json_object(value, '$.ext.tab') = 'Overseas'
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3
)
,l_uv as (
    select dt, user_name
    from ihotel_default.dw_user_app_log_search_di_v1
    where dt >= '2026-06-27' and dt <= date_sub(current_date, 1)
        and user_name is not null and user_name not in ('null', 'NULL', '', ' ')
    group by 1,2
) 
,uv as (----分日去重活跃用户
    select dt 
          ,case when province_name in ('澳门','香港') then province_name  
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                when e.area in ('欧洲','亚太','美洲') then e.area
                else '其他' end as mdd
          ,a.user_id
          ,a.user_name
          ,sum(search_pv) search_pv
          ,sum(detail_pv) detail_pv
          ,sum(booking_pv) booking_pv
          ,sum(order_pv) order_pv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-06-27' and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,q_order_app as (----订单明细表 app
    select order_date
          ,case when province_name in ('澳门','香港') then province_name  
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  
                when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
          ,a.user_id, init_gmv, order_no, room_night
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-06-27' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,d_uv as ( ---- 核心酒店D页UV
    select  dt
        ,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join temp.temp_xiaohan_song_hexinjiudianchangshi_983 b on a.hotel_seq = b.hotel_seq
    where dt >= '2026-06-27' and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and b.hotel_seq is not null
    group by 1,2
)

select dt,version
      ,sum(uv) / nullif(sum(h_uv), 0) as h2u
      ,sum(o_ds_order) / nullif(sum(h_uv), 0) as h2o
      ,sum(l_uv) / nullif(sum(h_uv), 0) as h2l
      ,sum(d_uv) / nullif(sum(h_uv), 0) as h2d_983hotel
      ,sum(o_d_order) / nullif(sum(d_uv), 0) as d2o_983hotel

      ,sum(h_uv) as h_uv
      ,sum(uv) as uv
      ,sum(l_uv) as l_uv
      ,sum(d_uv) as d_uv_983hotel
      
      ,sum(d_s_UV) / nullif(sum(s_all_UV), 0) as s2d
      ,sum(b_ds_UV) / nullif(sum(d_s_UV), 0) as d2b
      ,sum(o_ds_order) / nullif(sum(b_ds_UV), 0) as b2o
      
      ,sum(s_all_UV) as s_all_UV
      ,sum(d_s_UV) as d_s_UV
      ,sum(b_ds_UV) as b_ds_UV
      ,sum(o_ds_order) as o_ds_order
      ,sum(o_d_order) as o_d_order_983hotel
      
from (
    select t1.dt
          ,t4.version
          ,count(distinct t1.user_name) as h_uv
          ,count(distinct t2.user_id) as uv
          ,count(distinct t3.user_name) as l_uv
          ,count(distinct t6.user_name) as d_uv
          ,count(distinct case when t2.search_pv >0 then t2.user_id else null end) as s_all_UV
          ,count(distinct case when t2.detail_pv >0 and t2.search_pv >0 then t2.user_id else null end) as d_s_UV
          ,count(distinct case when t2.booking_pv >0 and t2.detail_pv >0 and t2.search_pv >0 then t2.user_id else null end) as b_ds_UV
          ,count(distinct case when t5.user_id is not null and t2.detail_pv >0 and t2.search_pv >0 then t5.order_no else null end) as o_ds_order
          ,count(distinct case when t5.user_id is not null and t6.user_name is not null then t5.order_no else null end) as o_d_order
    from h_uv t1 
    left join uv t2 on t1.dt = t2.dt and t1.user_name = t2.user_name 
    left join l_uv t3 on t1.dt = t3.dt and t1.user_name = t3.user_name 
    left join q_order_app t5 on t2.user_id = t5.user_id and t2.dt = t5.order_date and t2.mdd = t5.mdd 
    left join abtest t4 on t1.dt = t4.dt and t1.orig_device_id = t4.device_id
    left join d_uv t6 on t1.dt = t6.dt and t1.user_name = t6.user_name
    where t4.version is not null
    group by 1,2
) base
group by 1,2
order by 1 desc,2
;