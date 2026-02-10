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
,abtest(  --- AB实验表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2)) AS dates
            ,version
            ,clientcode AS user_id
            ,b.user_name
    from default.ods_abtest_sdk_log_endtime_hotel a --user_id   f_abt.abtest_sdk_log_daycombine_new
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    where a.dt between '20251102' AND '%(DATE)s'
    and expid = '251013_ho_gj_BpageRetention'  ---实验ID
    group by 1,2,3,4
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-11-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,user_name
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-11-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,wl_popup as ( --- B页挽留弹窗曝光用户  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dates
        ,user_name
        ,key
        ,get_json_object(value, '$.ext.modalType') modalType   --- modalType='可领优惠' 可领优惠弹窗曝光
        ,get_json_object(value, '$.ext.buttonClicked') buttonClicked  --- 1、继续预定点击 2、狠心离开点击 3、关闭按钮点击
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20251102' AND '%(DATE)s'
        and key in( 'ihotel/Booking/Footer/click/retentionModal' ,'ihotel/Booking/Footer/show/retentionModal')
)


select t1.dt,t1.version
      ,b_ds_UV
      ,o_ds_order
      ,o_ds_order / b_ds_UV b2o
      ,`挽留弹窗曝光UV`
      ,`可领优惠挽留弹窗曝光UV`
      ,`挽留弹窗点击UV`
      ,`挽留弹窗点击继续预定UV`
      ,`挽留弹窗点击狠心离开UV`
      ,`挽留弹窗点击关闭按钮UV`
      ,order_no
      ,order_no / `挽留弹窗曝光UV` `挽留人群B2O`
      ,`挽留弹窗点击继续预定UV` / `挽留弹窗曝光UV` `继续预定点击率`
from (--- B20
    select 
        a.dt
        ,version
        -- ,count(distinct case when search_pv >0 then  a.user_id else null end ) s_all_UV
        -- ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order
    from  uv a -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id   -- 订单表
    left join abtest c on a.dt=c.dates and a.user_name=c.user_name
    group by 1,2
) t1 left join ( --- 挽留弹窗曝光人群订单转化
    select t1.dates
        ,version
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' then t1.user_name end) `挽留弹窗曝光UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' and modalType='可领优惠' then t1.user_name end) `可领优惠挽留弹窗曝光UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' then t1.user_name end) `挽留弹窗点击UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '1' then t1.user_name end) `挽留弹窗点击继续预定UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '2' then t1.user_name end) `挽留弹窗点击狠心离开UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '3' then t1.user_name end) `挽留弹窗点击关闭按钮UV`
        ,count(distinct case when t3.user_name is not null then t3.order_no end)  order_no
    from wl_popup t1 
    left join abtest t2 on t1.dates=t2.dates and t1.user_name=t2.user_name
    left join q_app_order t3 on t1.dates=t3.order_date and t1.user_name=t3.user_name
    group by 1,2
    
) t2 on t1.dt=t2.dates and t1.version=t2.version
order by 1,2
;



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
,abtest(  --- AB实验表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2)) AS dates
            ,version
            ,clientcode AS user_id
            ,b.user_name
    from default.ods_abtest_sdk_log_endtime_hotel a --user_id   f_abt.abtest_sdk_log_daycombine_new
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    where a.dt between '20251102' AND '%(DATE)s'
    and expid = '251013_ho_gj_BpageRetention'  ---实验ID
    group by 1,2,3,4
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-11-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,user_name
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-11-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,wl_popup as ( --- B页挽留弹窗曝光用户  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dates
        ,user_name
        ,key
        ,get_json_object(value, '$.ext.modalType') modalType   --- modalType='可领优惠' 可领优惠弹窗曝光
        ,get_json_object(value, '$.ext.buttonClicked') buttonClicked  --- 1、继续预定点击 2、狠心离开点击 3、关闭按钮点击
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20251102' AND '%(DATE)s'
        and key in( 'ihotel/Booking/Footer/click/retentionModal' ,'ihotel/Booking/Footer/show/retentionModal')
)


select t1.dt,t1.version,t1.user_type
      ,b_ds_UV
      ,o_ds_order
      ,o_ds_order / b_ds_UV b2o
      ,`挽留弹窗曝光UV`
      ,`可领优惠挽留弹窗曝光UV`
      ,`挽留弹窗点击UV`
      ,`挽留弹窗点击继续预定UV`
      ,`挽留弹窗点击狠心离开UV`
      ,`挽留弹窗点击关闭按钮UV`
      ,order_no
      ,order_no / `挽留弹窗曝光UV` `挽留人群B2O`
      ,`挽留弹窗点击继续预定UV` / `挽留弹窗曝光UV` `继续预定点击率`
from (--- B20
    select 
        a.dt
        ,version,a.user_type
        -- ,count(distinct case when search_pv >0 then  a.user_id else null end ) s_all_UV
        -- ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order
    from  uv a -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id   -- 订单表
    left join abtest c on a.dt=c.dates and a.user_name=c.user_name
    group by 1,2,3
) t1 left join ( --- 挽留弹窗曝光人群订单转化
    select t1.dates
        ,version,t4.user_type
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' then t1.user_name end) `挽留弹窗曝光UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' and modalType='可领优惠' then t1.user_name end) `可领优惠挽留弹窗曝光UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' then t1.user_name end) `挽留弹窗点击UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '1' then t1.user_name end) `挽留弹窗点击继续预定UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '2' then t1.user_name end) `挽留弹窗点击狠心离开UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '3' then t1.user_name end) `挽留弹窗点击关闭按钮UV`
        ,count(distinct case when t3.user_name is not null then t3.order_no end)  order_no
    from wl_popup t1 
    left join abtest t2 on t1.dates=t2.dates and t1.user_name=t2.user_name
    left join q_app_order t3 on t1.dates=t3.order_date and t1.user_name=t3.user_name
    left join uv t4 on t1.dates=t4.dt and t1.user_name=t4.user_name
    group by 1,2,3
    
) t2 on t1.dt=t2.dates and t1.version=t2.version and t1.user_type=t2.user_type
order by 1,2
;





---- 不同点击按钮的B2O转化
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
,abtest(  --- AB实验表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2)) AS dates
            ,version
            ,clientcode AS user_id
            ,b.user_name
    from default.ods_abtest_sdk_log_endtime_hotel a --user_id   f_abt.abtest_sdk_log_daycombine_new
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    where a.dt between '20251102' AND '%(DATE)s'
    and expid = '251013_ho_gj_BpageRetention'  ---实验ID
    group by 1,2,3,4
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-11-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,user_name
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-11-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,wl_popup as ( --- B页挽留弹窗曝光用户  埋点表
    select concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2))AS dates
        ,user_name
        ,key
        ,get_json_object(value, '$.ext.modalType') modalType   --- modalType='可领优惠' 可领优惠弹窗曝光
        ,get_json_object(value, '$.ext.buttonClicked') buttonClicked  --- 1、继续预定点击 2、狠心离开点击 3、关闭按钮点击
    from default.dw_qav_ihotel_track_info_di a
    where dt between '20251102' AND '%(DATE)s'
        and key in( 'ihotel/Booking/Footer/click/retentionModal' ,'ihotel/Booking/Footer/show/retentionModal')
)


select t1.dt,t1.version
      ,b_ds_UV
      ,o_ds_order
      ,o_ds_order / b_ds_UV b2o
      ,`挽留弹窗曝光UV`
      ,`可领优惠挽留弹窗曝光UV`
      ,`挽留弹窗点击UV`
      ,`挽留弹窗点击继续预定UV`
      ,`挽留弹窗点击狠心离开UV`
      ,`挽留弹窗点击关闭按钮UV`
      ,order_no
      ,order_no / `挽留弹窗曝光UV` `挽留人群B2O`
      ,`挽留弹窗点击继续预定UV` / `挽留弹窗曝光UV` `继续预定点击率`

      ,`挽留弹窗点击订单`
      ,`挽留弹窗点击继续预定订单`
      ,`挽留弹窗点击狠心离开订单`
      ,`挽留弹窗点击关闭按钮订单`

      ,`挽留弹窗点击订单`  / `挽留弹窗点击UV` as  `点击B2O`
      ,`挽留弹窗点击继续预定订单`  / `挽留弹窗点击继续预定UV` as  `点击继续预定B2O`
      ,`挽留弹窗点击狠心离开订单` / `挽留弹窗点击狠心离开UV` as  `点击狠心离开B2O`
      ,`挽留弹窗点击关闭按钮订单` / `挽留弹窗点击关闭按钮UV` as  `点击关闭按钮B2O`
from (--- B20
    select 
        a.dt
        ,version
        -- ,count(distinct case when search_pv >0 then  a.user_id else null end ) s_all_UV
        -- ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order
    from  uv a -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id   -- 订单表
    left join abtest c on a.dt=c.dates and a.user_name=c.user_name
    group by 1,2
) t1 left join ( --- 挽留弹窗曝光人群订单转化
    select t1.dates
        ,version
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' then t1.user_name end) `挽留弹窗曝光UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' and modalType='可领优惠' then t1.user_name end) `可领优惠挽留弹窗曝光UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' then t1.user_name end) `挽留弹窗点击UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '1' then t1.user_name end) `挽留弹窗点击继续预定UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '2' then t1.user_name end) `挽留弹窗点击狠心离开UV`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '3' then t1.user_name end) `挽留弹窗点击关闭按钮UV`
        ,count(distinct case when t3.user_name is not null then t3.order_no end)  order_no

        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' then  t3.order_no  end) `挽留弹窗点击订单`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '1' then  t3.order_no end) `挽留弹窗点击继续预定订单`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '2' then  t3.order_no end) `挽留弹窗点击狠心离开订单`
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' and buttonClicked = '3' then  t3.order_no end) `挽留弹窗点击关闭按钮订单`
    from wl_popup t1 
    left join abtest t2 on t1.dates=t2.dates and t1.user_name=t2.user_name
    left join q_app_order t3 on t1.dates=t3.order_date and t1.user_name=t3.user_name
    group by 1,2
    
) t2 on t1.dt=t2.dates and t1.version=t2.version
order by 1,2
;