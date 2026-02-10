with q_user_type as (
    select user_id 
          ,min(order_date) as min_order_date
    from mdw_order_v3_international
    where dt = '%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and terminal_channel_type in ('www','app','touch') 
    and order_status not in ('CANCELLED','REJECTED')
    and is_valid='1'
    group by 1
)
,c_user_type as (
    select 
        user_id,
        ubt_user_id,
        substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,lidian_q_order as (
    select checkout_date as `离店日期`
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when order_date > b.min_order_date then '老客' else '新客' end as user_type 
            ,sum(case when terminal_channel_type = 'app' then room_night end) as `Q_间夜量`
            ,sum(room_night) as `Q_间夜量all`
    from mdw_order_v3_international a 
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        -- and pay_type='PROXY'
        and order_status not in ('CANCELLED','REJECTED')
        and checkout_date >= date_sub(current_date,15) and checkout_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
    group by 1,2,3
)
,lidian_q_order_yoy as (
    select date_add(checkout_date,365) as `离店日期`
            ,checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when order_date > b.min_order_date then '老客' else '新客' end as user_type 
            ,sum(room_night) as `Q_间夜量_yoy`
    from mdw_order_v3_international a 
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt =  '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
            and terminal_channel_type = 'app'
            and is_valid='1'
            -- and pay_type='PROXY'
            and order_status not in ('CANCELLED','REJECTED')
            and checkout_date >= '2024-01-01' and checkout_date <= date_sub(DATE_SUB(CURRENT_DATE, 365),1)
            and order_no <> '103576132435'
    group by 1,2,3,4
)
,lidian_c_order as (
    select  substr(checkout_date,1,10) `离店日期`
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
                when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                when c.area in ('欧洲','亚太','美洲') then c.area
                else '其他' end as `目的地`
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,count(order_no) as `C_订单量`
            ,sum(extend_info['room_night']) as `C_间夜量`
            ,sum(room_fee)as `C_GMV`
            ,sum(comission) as `C_佣金`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
    where dt =  '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
            and terminal_channel_type = 'app'
            and order_status <> 'C'
            -- and pay_type='预付'
            and substr(checkout_date,1,10)>=date_sub(current_date,15) and substr(checkout_date,1,10)<=date_sub(current_date,1)
    group by 1,2,3
)
,lidian_c_order_yoy as (
    select   substr(date_add(checkout_date,365),1,10) `离店日期`
            ,substr(checkout_date,1,10)
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
                when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                when c.area in ('欧洲','亚太','美洲') then c.area
                else '其他' end as `目的地`
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,count(order_no) as `C_订单量`
            ,sum(extend_info['room_night']) as `C_间夜量_yoy`
            ,sum(room_fee)as `C_GMV`
            ,sum(comission) as `C_佣金`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
    where dt =  '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
            and terminal_channel_type = 'app'
            and order_status <> 'C'
            -- and pay_type='预付'
            and substr(checkout_date,1,10)>='2024-01-01' and substr(DATE_SUB(CURRENT_DATE, 365),1,10)<=date_sub(current_date,1)
    group by 1,2,3
)
,uv_q_t60_info as (
    select checkout_date as checkout_date 
            ,case when province_name in ('澳门','香港') then province_name 
                  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                  when c.area in ('欧洲','亚太','美洲') then c.area
             else '其他' end as `目的地`
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type 
            ,a.user_id,dt
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    where dt >=date_sub(date_sub(current_date,15),61)
            and business_type = 'hotel'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
            and device_id is not null
            and device_id <> ''
            and checkout_date>=date_sub(current_date,15) and checkout_date <= date_sub(current_date,1)
    group by 1,2,3,4,5
)
,uv_q_t60 as (
    select t1.checkout_date,`目的地`,user_type,count(distinct t2.user_id) q_60uv,count(distinct dt) dts
    from (select checkout_date from uv_q_t60_info group by 1) t1
    left join uv_q_t60_info t2 on  t1.checkout_date=t2.checkout_date and datediff(t1.checkout_date,t2.dt) between 0 and 59
    group by 1,2,3
)
,cr_q_t60_info as (
    select checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when order_date > b.min_order_date then '老客' else '新客' end as user_type 
            ,room_night,order_no,init_gmv,order_date
            ,case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0 else nvl(coupon_substract_summary,0) end coupon_substract_summary
            ,case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 'N' else 'Y' end is_use_conpon
    from mdw_order_v3_international a 
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt =  '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
    --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
            and terminal_channel_type = 'app'
            and is_valid='1'
            -- and pay_type='PROXY'
    --    and order_status not in ('CANCELLED','REJECTED')
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and checkout_date >= date_sub(current_date,15) and checkout_date <= date_sub(current_date,1)
            and order_date>=date_sub(date_sub(current_date,15),61) and order_date<=date_sub(current_date,1)
            and order_no <> '103576132435'
)
,cr_q_t60 as (
    select t1.checkout_date,`目的地`,user_type
            ,sum(room_night) as `Q_间夜量_t60`
            ,count(distinct order_no) as `Q_订单量_t60`
            ,sum(coupon_substract_summary) as `Q_券额_60`
            ,count(distinct case when is_use_conpon = 'Y' then  order_no end ) as `Q_券订单量_60`
            ,sum(init_gmv) as `Q_gmv_60`
    from (select checkout_date from cr_q_t60_info group by 1) t1
    left join cr_q_t60_info t2 on t1.checkout_date=t2.checkout_date and datediff(t1.checkout_date,t2.order_date) between 0 and 59
    group by 1,2,3
)
,lidian_q_orderall as (
    select checkout_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
        ,case when order_date > b.min_order_date then '老客' else '新客' end as user_type 
        ,sum(room_night) as `Q_间夜量_all`
        ,count(distinct order_no) as `Q_订单量`
        ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                 then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                 else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
        ,sum(init_gmv) as `Q_GMV`
    from mdw_order_v3_international a 
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt =  '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            -- and pay_type='PROXY'
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and checkout_date >= date_sub(current_date,15) and checkout_date <= date_sub(current_date,1)
            and order_no <> '103576132435'
    group by 1,2,3
)
,lidian_q_orderall_1 as (   -- 全部订单
    select checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when order_date > b.min_order_date then '老客' else '新客' end as user_type 
            ,sum(room_night) as `Q_间夜量_all_1`
            ,count(distinct order_no) as `Q_订单量`
            ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                    else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
            ,sum(init_gmv) as `Q_GMV`
    from mdw_order_v3_international a 
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt =  '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            -- and pay_type='PROXY'
            and checkout_date >= date_sub(current_date,15) and checkout_date <= date_sub(current_date,1)
            and order_no <> '103576132435'
    group by 1,2,3
)
,uv_c_t60_info as (
    select check_out `离店日期`
            ,case when a.provincename in ('澳门','香港') then a.provincename 
                    when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname 
                    when d.area in ('欧洲','亚太','美洲') then d.area
                    else '其他' end as `目的地`
            ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
            ,a.uid
            ,dt
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a 
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever d on a.countryname = d.country_name 
    where dt >= date_sub(date_sub(current_date,15),61) 
        and device_chl = 'app'
        and page_short_domain = 'dbo'
        and check_out>=date_sub(current_date,15) and check_out <= date_sub(current_date,1)
    group by 1,2,3,4,5
)
,uv_c_t60 as (
    select t1.`离店日期`,`目的地`,user_type,count(distinct t2.uid) c_60uv,count(distinct dt) dts
    from (select `离店日期` from uv_c_t60_info group by 1) t1
    left join uv_c_t60_info t2 on  t1.`离店日期`=t2.`离店日期` and datediff(t1.`离店日期`,t2.dt) between 0 and 59
    group by 1,2,3
)
,cr_c_t60_info as (
    select  
            substr(checkout_date,1,10) `离店日期`,substr(order_date,1,10)order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as `目的地`
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,order_no,extend_info['room_night'] as room_night
            ,room_fee,comission
            ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') qe
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
    where dt =  '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            and terminal_channel_type = 'app'
            -- and pay_type='预付'
            and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and substr(checkout_date,1,10)>=date_sub(current_date,15) and substr(checkout_date,1,10)<=date_sub(current_date,1)
            and substr(order_date,1,10)>=date_sub(date_sub(current_date,15),61) and substr(order_date,1,10)<=date_sub(current_date,1)
)
,cr_c_t60 as (
    select t1.`离店日期`
            ,`目的地`
            ,user_type 
            ,count(distinct order_no) as `C_订单量_t60`
            ,count(distinct case when qe > 0 then  order_no end ) as `C_券订单量_t60`
            ,sum(room_night) as `C_间夜量_t60`
            ,sum(room_fee) as `C_GMV_60`
            ,sum(comission) as `C_佣金`
            ,sum(qe) as `C_券额_60`
    from (select `离店日期` from cr_c_t60_info group by 1) t1
    left join cr_c_t60_info t2 on t1.`离店日期`=t2.`离店日期` and datediff(t1.`离店日期`,t2.order_date) between 0 and 59
    group by 1,2,3
)
,lidian_c_orderall as (
    select  
        substr(checkout_date,1,10) `离店日期`
         ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
              when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
              when c.area in ('欧洲','亚太','美洲') then c.area
              else '其他' end as `目的地`
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        ,count(order_no) as `C_订单量`
        ,sum(extend_info['room_night']) as `C_间夜量_all`
        ,sum(room_fee)as `C_GMV`
        ,sum(comission) as `C_佣金`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
    where dt =  '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            and terminal_channel_type = 'app'
            -- and pay_type='预付'
            and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and substr(checkout_date,1,10)>=date_sub(current_date,15) and substr(checkout_date,1,10)<=date_sub(current_date,1)
    group by 1,2,3
)
,lidian_c_orderall_1 as (   -- 全部订单
   select  
        substr(checkout_date,1,10) `离店日期`
         ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
              when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
              when c.area in ('欧洲','亚太','美洲') then c.area
              else '其他' end as `目的地`
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        ,count(order_no) as `C_订单量`
        ,sum(extend_info['room_night']) as `C_间夜量_all_1`
        ,sum(room_fee)as `C_GMV`
        ,sum(comission) as `C_佣金`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
    where dt =  '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            and terminal_channel_type = 'app'
            -- and pay_type='预付'
            and substr(checkout_date,1,10)>=date_sub(current_date,15) and substr(checkout_date,1,10)<=date_sub(current_date,1)
    group by 1,2,3
)
,pay_lose_part1 as (
    select substr(check_out,1,10) as check_out   
        ,user_type
        ,`目的地`
        --支付价
        ,count(distinct case when  pay_price_compare_result='Qlose'  then id end) as lose_num
        ,count(distinct id) as num
        ,count(distinct case when  pay_price_compare_result='Qlose'  then id end)/count(distinct id) as pay_price_lose_rate_sq2   -- 支付价lose率
    from (
        select check_out
            ,a.uniq_id
            ,a.crawl_time  -- 抓取时间 
            ,a.id
            ,a.pay_price_compare_result
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type 
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join  q_user_type b on a.user_id = b.user_id 
        where dt >= regexp_replace(date_sub(current_date(),30) ,'-','') and dt <= '%(DATE)s'
            and check_out>=date_sub(current_date(),15) and check_out<=date_sub(current_date(),1)
            and business_type = 'intl_crawl_cq_api_userview'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            -- and ctrip_pay_type <> '现付'
    )a
    group by 1,2,3
)
,pay_lose_part2 as (
    select substr(check_out,1,10) as check_out   
        ,user_type
        ,`目的地`
        --支付价
        ,count(distinct case when  pay_price_compare_result='Qlose'  then id end) as lose_num
        ,count(distinct id) as num
        ,count(distinct case when  pay_price_compare_result='Qlose'  then id end)/count(distinct id) as pay_price_lose_rate_sq2   -- 支付价lose率
    from (
        select check_out
            ,a.uniq_id
            ,a.crawl_time  -- 抓取时间 
            ,a.id
            ,a.pay_price_compare_result
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type 
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join  q_user_type b on a.user_id = b.user_id 
        where dt >= regexp_replace(date_sub(current_date(),61) ,'-','') and dt <= regexp_replace(date_sub(current_date(),31) ,'-','')
            and check_out>=date_sub(current_date(),15) and check_out<=date_sub(current_date(),1)
            and business_type = 'intl_crawl_cq_api_userview'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            -- and ctrip_pay_type <> '现付'
    )a
    group by 1,2,3
)
,pay_lose as (
    select check_out,user_type,`目的地`,sum(lose_num) as lose_num,sum(num) as num 
    from 
    (
    select * from pay_lose_part1 a 
    union all 
    select * from pay_lose_part2 b 
    ) t group by 1,2,3
)
,data_result as (
    select t.`离店日期`
        ,t.user_type
        ,t.`目的地`
        ,t.`Q_间夜量`
        ,t1.`Q_间夜量_yoy`
        ,t2.`C_间夜量`
        ,t3.q_60uv
        ,t4.`Q_订单量_t60`
        ,t4.`Q_间夜量_t60`
        ,t4.`Q_券订单量_60`
        ,t4.`Q_券额_60`
        ,t4.`Q_gmv_60`
        ,t5.`Q_间夜量_all`
        ,t5.`Q_GMV`
        ,t5.`Q_佣金`
        ,concat(round(t.`Q_间夜量`/t5.`Q_间夜量_all`*100,2),'%') as `Q取消率`
        ,t6.`C_间夜量_all`
        ,t7.c_60uv
        ,t8.`C_订单量_t60`
        ,t8.`C_间夜量_t60`
        ,t8.`C_券订单量_t60`
        ,t8.`C_券额_60`
        ,t8.`C_GMV_60`
        ,t9.lose_num
        ,t9.num
        ,concat(round(t2.`C_间夜量`/t6.`C_间夜量_all`*100,2),'%') as `C取消率`
        ,t6.`C_GMV`
        ,t6.`C_佣金`
        ,t10.`C_间夜量_all_1`
        ,t11.`Q_间夜量_all_1`
    from lidian_q_order t 
    left join lidian_q_order_yoy t1 on t.`离店日期`=t1.`离店日期` and t.user_type=t1.user_type and t.`目的地`=t1.`目的地`
    left join lidian_c_order t2 on t.`离店日期`=t2.`离店日期` and t.user_type=t2.user_type and t.`目的地`=t2.`目的地`
    left join uv_q_t60 t3 on t.`离店日期`=t3.checkout_date and t.user_type=t3.user_type and t.`目的地`=t3.`目的地`
    left join cr_q_t60 t4 on t.`离店日期`=t4.checkout_date and t.user_type=t4.user_type and t.`目的地`=t4.`目的地`
    left join lidian_q_orderall t5 on t.`离店日期`=t5.checkout_date and t.user_type=t5.user_type and t.`目的地`=t5.`目的地`
    left join lidian_c_orderall t6 on t.`离店日期`=t6.`离店日期` and t.user_type=t6.user_type and t.`目的地`=t6.`目的地`
    left join uv_c_t60 t7 on t.`离店日期`=t7.`离店日期` and t.user_type=t7.user_type and t.`目的地`=t7.`目的地`
    left join cr_c_t60 t8 on t.`离店日期`=t8.`离店日期` and t.user_type=t8.user_type and t.`目的地`=t8.`目的地`
    left join pay_lose t9 on t.`离店日期`=t9.check_out and t.user_type=t9.user_type and t.`目的地`=t9.`目的地`
    left join lidian_c_orderall_1 t10 on t.`离店日期`=t10.`离店日期` and t.user_type=t10.user_type and t.`目的地`=t10.`目的地`
    left join lidian_q_orderall_1 t11 on t.`离店日期`=t11.checkout_date and t.user_type=t11.user_type and t.`目的地`=t11.`目的地`
)

select '大盘' as data_type
     ,t.`离店日期`
     ,sum(t.`Q_间夜量`) as `离店间夜`
     ,concat(round((sum(t.`Q_间夜量`)/sum(t.`Q_间夜量_yoy`)-1)*100,2),'%') as YOY
      ,concat(round(sum(t.`Q_间夜量`)/sum(t.`C_间夜量`)*100,2),'%') as `离店间夜QC`
      ,concat(round(sum(t.q_60uv)/sum(t.c_60uv)*100,2),'%') as `流量QC_t60`
      ,concat(round((sum(t.`Q_订单量_t60`)/sum(t.q_60uv))/(sum(t.`C_订单量_t60`)/sum(t.C_60uv))*100,2),'%') as `转化QC_t60`
      ,concat(round(sum(t.`Q_间夜量_t60`)/sum(t.`C_间夜量_t60`)*100,2),'%') as `间夜QC_t60`
      ,concat(round((sum(t.`Q_间夜量_t60`)/sum(t.`Q_订单量_t60`))/(sum(t.`C_间夜量_t60`)/sum(t.`C_订单量_t60`))*100,2),'%') as `单间夜QC_t60`
      ,concat(round((sum(t.`Q_佣金`)/sum(t.`Q_GMV`))-(sum(t.`C_佣金`)/sum(t.`C_GMV`))*100,2),'pp') as `收益率QC差`
      ,concat(round(((sum(t.`Q_券订单量_60`)/sum(t.`Q_订单量_t60`))-(sum(t.`C_券订单量_t60`)/sum(t.`C_订单量_t60`)))*100,2),'pp') as `用券订单占比差`
      ,concat(round(((sum(t.`Q_券额_60`)/sum(t.`Q_GMV_60`))-(sum(t.`C_券额_60`)/sum(t.`C_GMV_60`)))*100,2),'pp') as `用券补贴率差`
      ,concat(round(((1-(sum(t.`Q_间夜量`)/sum(t.`Q_间夜量_all_1`)))/(1-(sum(t.`c_间夜量`)/sum(t.`c_间夜量_all_1`))))*100,2),'%') as `取消率QC`  
      ,sum(t.q_60uv) as `Q流量_t60`
      ,concat(round(sum(t.`Q_订单量_t60`)/sum(t.q_60uv)*100,2),'%') as `Q转化_t60`
      ,sum(t.`Q_间夜量_t60`) as `Q_间夜量_t60`
      ,round(sum(t.`Q_间夜量_t60`)/sum(t.`Q_订单量_t60`),2) as `Q单间夜`
      ,concat(round(sum(t.lose_num)/sum(t.num)*100,2),'%')  as `Q支付价lose率`
      ,concat(round(sum(t.`Q_券订单量_60`)/sum(t.`Q_订单量_t60`)*100,2),'%')  as `Q用券订单占比`
      ,concat(round(sum(t.`Q_券额_60`)/sum(t.`Q_GMV_60`)*100,2),'%')  as `Q券补贴率`
      ,concat(round((sum(t.`Q_间夜量_all_1`)-sum(t.`Q_间夜量_all`))/sum(t.`Q_间夜量_all_1`)*100,2),'%') as `Q当天取消率`
      ,concat(round(((sum(t.`Q_间夜量_all`)-sum(t.`Q_间夜量`))/sum(t.`Q_间夜量_all_1`))*100,2),'%') as `Q非当天取消率`
from data_result t where t.user_type='新客'
group by 1,2
order by `离店日期` desc, `离店间夜` desc
;