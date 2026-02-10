--- 国庆离店订单Q25
with  user_type_25 as
(
    select user_id
        ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '20251008'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,user_type_24 as
( 
    select user_id
        ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '20241007'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,check_order25 as ( --- 国庆离店订单Q25
    select  case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,case when province_name in ('澳门','香港') then province_name
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚') then a.country_name  
                when a.country_name in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
            ,count(distinct order_no) as `订单量`
            ,sum(room_night) as `Q间夜量`
    from mdw_order_v3_international a 
    left join user_type_25 b on a.user_id = b.user_id 
    where dt = '20251008'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= '2025-10-08'
        and order_no <> '103576132435'
        and checkout_date between '2025-10-01' and '2025-10-08'   --- 离店日期在国庆
    group by 1,2
)
,check_order24 as ( --- 国庆离店订单Q24
    select  case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,case when province_name in ('澳门','香港') then province_name
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚') then a.country_name  
                when a.country_name in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
            ,count (distinct order_no) as `订单量`
            ,sum(room_night) as `Q间夜量`
    from mdw_order_v3_international a 
    left join user_type_24 b on a.user_id = b.user_id 
    where dt = '20241007'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and order_date >= '2024-01-01' and order_date <= '2024-10-07'
        and order_no <> '103576132435'
        and checkout_date between '2024-10-01' and '2024-10-07'   --- 离店日期在国庆
    group by 1,2
)
,check_uv_25 as(--- 国庆离店流量Q25
    select dt as `日期`
        ,case when province_name in ('澳门','香港') then province_name
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚') then a.country_name  
                when a.country_name in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,count (distinct a.user_id) as uv
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type_25 b on a.user_id = b.user_id 
    where dt between '2025-01-01' and '2025-10-08'
        and checkout_date between '2025-10-01' and '2025-10-08'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3
)
,check_uv_25_all as (
    select user_type,mdd,sum(uv) as uv
    from check_uv_25
    group by 1,2
)

,check_uv_24 as(--- 国庆离店流量Q24
    select dt as `日期`
        ,case when province_name in ('澳门','香港') then province_name
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚') then a.country_name  
                when a.country_name in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
        ,case when concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type
        ,count (distinct a.user_id) as uv
    from default.dw_user_app_detail_visit_di_v3 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type_24 b on a.user_id = b.user_id 
    where dt between '20240101' and '20241007'
        and checkout_date between '2024-10-01' and '2024-10-07'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3
)
,check_uv_24_all as (
    select user_type,mdd,sum(uv) as uv
    from check_uv_24
    group by 1,2
)


select t1.mdd,t1.user_type
     ,t1.`订单量`,t1.`Q间夜量`
     ,t3.uv,'Y25' key
from check_order25 t1
left join check_uv_25_all t3 on t1.mdd=t3.mdd and t1.user_type=t3.user_type
union all 
select t1.mdd,t1.user_type
     ,t1.`订单量`,t1.`Q间夜量`
     ,t3.uv,'Y24' key
from check_order24 t1
left join check_uv_25_all t3 on t1.mdd=t3.mdd and t1.user_type=t3.user_type
;


------- C数据

with c_user_type_25 as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '2025-10-08'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_user_type_24 as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '2024-10-07'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )

,c_check_order25 as (
    select  case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚') then extend_info['COUNTRY'] 
                    --'美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国'
                    when extend_info['COUNTRY'] in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                    else 'other' end as mdd
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        , count(order_no) as `C_订单量`
        , sum(extend_info['room_night']) as `C_间夜量`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type_25 u on o.user_id=u.user_id
    where dt = '2025-10-08'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
        and terminal_channel_type = 'app'
        and substr(order_date,1,10) between '2025-01-01' and '2025-10-08'
        and checkout_date between '2025-10-01' and '2025-10-08' --十一假期：2025.10.01 - 2025.10.08（共8天）
    group by 1,2
)

,c_check_order24 as (
    select  case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚') then extend_info['COUNTRY'] 
                    --'美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国'
                    when extend_info['COUNTRY'] in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                    else 'other' end as mdd
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        , count(order_no) as `C_订单量`
        , sum(extend_info['room_night']) as `C_间夜量`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type_24 u on o.user_id=u.user_id
    where dt = '2024-10-07'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
        and terminal_channel_type = 'app'
        and substr(order_date,1,10) between '2024-01-01' and '2024-10-07'
        and checkout_date between '2024-10-01' and '2024-10-07' --十一假期：2024.10.01 - 2024.10.07（共7天）
    group by 1,2
)

,c_uv_25 as (
    select dt as `日期`
         ,case when provincename in ('澳门','香港') then provincename
                when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚') then a.countryname  
                --'美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国'
                when a.countryname in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type 
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    left join c_user_type_25 b on a.uid=b.ubt_user_id
    where device_chl='app'
        and page_short_domain = 'dbo'
        and check_out between '2025-10-01' and '2025-10-08'
        and dt between '2025-01-01' and '2025-10-08'
    group by 1,2,3
)
,c_check_uv_25_all as (
    select user_type,mdd,sum (c_uv) as uv
    from c_uv_25
    group by 1,2
)
,c_uv_24 as (
    select dt as `日期`
         ,case when provincename in ('澳门','香港') then provincename
                when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚') then a.countryname  
                --'美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国'
                when a.countryname in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type 
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    left join c_user_type_25 b on a.uid=b.ubt_user_id
    where device_chl='app'
        and page_short_domain = 'dbo'
        and check_out between '2024-10-01' and '2024-10-07'
        and dt between '2024-01-01' and '2024-10-07'
    group by 1,2,3
)
,c_check_uv_24_all as (
    select user_type,mdd,sum (c_uv) as uv
    from c_uv_24
    group by 1,2
)

select t1.mdd,t1.user_type
     ,t1.`C_订单量`,t1.`C_订单量`
     ,t6.`C_订单量` `C_订单量_24` ,t6.`C_间夜量` `C_间夜量_24` 
     ,t7.uv c_uv,t8.uv c_uv_24
from c_check_order25 t1
left join c_check_order24 t6 on t1.mdd=t6.mdd and t1.user_type=t6.user_type
left join c_check_uv_25_all t7 on t1.mdd=t7.mdd and t1.user_type=t7.user_type
left join c_check_uv_24_all t8 on t1.mdd=t8.mdd and t1.user_type=t8.user_type
;


--- Q近期国际机票和国际酒店情况
select dt,
       count(distinct a.user_id) as user_cnt,
       count(distinct order_no)  as order_cnt,
       sum(flight_size)      as flight_size_cnt
from flight.dwd_ord_wide_order_di_simple a
where pay_ok = 1
  and (dom_inter = 1 or arr_city in ('香港', '澳门') )
  and is_fenxiao = 0
  and pay_time != ''
  and source_type2 IN ('adr', 'ios')
  and trim(qunar_username) != ''
  and substr(dt, 1, 7) >= '2024-01'
  and arr_country = '新加坡'
group by 1
order by 1 desc
;

select order_date
       ,sum(room_night)room_night
       ,count(distinct order_no)order_no
       ,count(distinct a.user_id)order_uv
       ,sum(init_gmv) gmv
       ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end) yj
       ,sum(case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end) qb
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
    and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
    and order_no <> '103576132435'
    and a.country_name = '新加坡'
group by 1
order by 1 desc
;