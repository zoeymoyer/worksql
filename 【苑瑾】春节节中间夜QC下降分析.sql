--- 1、锁定春节离店QC
with c_user_type as(
    select user_id
        , ubt_user_id
        , substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
) 
,q_user_type as (
    select user_id 
        , min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
)
,q_uv as (
    select dt  
        , case when province_name in ('澳门','香港') then province_name 
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
            when c.area in ('欧洲','亚太','美洲') then c.area
            else '其他' end as mdd
        ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
        , case when dt > b.min_order_date then '老客' else '新客' end as user_type
        , a.user_id 
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    left join q_user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-01' and dt<= '2026-02-22'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and device_id is not null
        and device_id <> ''
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' 
    group by 1,2,3,4,5
)
,q_order as (
    select order_date 
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,checkout_date,order_no,room_night
           ,final_gmv
           ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
           ,a.user_id
           ,hotel_grade
           ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
           ,case  when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
    from default.mdw_order_v3_international a
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2026-01-01' and order_date <= '2026-02-22'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)
,c_uv as (
    select dt
            ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when provincename in ('澳门','香港') then '港澳'  when a.countryname in ('泰国','日本','韩国') then a.countryname  else '其他' end as new_mdd
            , case when dt> b.min_order_date then '老客' else '新客' end as user_type 
            ,a.uid
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where dt >= '2026-01-01' and dt <= '2026-02-22'
        and device_chl = 'app'
        and page_short_domain = 'dbo'
        and check_out >= '2026-02-15' and check_out <= '2026-02-23' 
    group by 1,2,3,4,5
) 
,c_order as(
    select substr(order_date,1,10) as order_date 
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'
               when extend_info['COUNTRY'] in ('泰国','日本','韩国') then extend_info['COUNTRY']
               else '其他' end as new_mdd
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        ,order_no
        ,extend_info['room_night'] room_night
        ,extend_info['STAR'] star
        ,comission
        ,room_fee
        ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
        ,o.user_id
        ,case when room_fee / extend_info['room_night'] < 400  then '1[0,400)'
                  when room_fee / extend_info['room_night'] >= 400 and  room_fee / extend_info['room_night'] < 800  then '2[400,800)'
                  when room_fee / extend_info['room_night'] >= 800 and  room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                  when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C' 
        and terminal_channel_type = 'app' 
        and substr(order_date,1,10) >= '2026-01-01' and  substr(order_date,1,10) <= '2026-02-22'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)
,qc_price as (
    select order_date
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id ) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            ,case   when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('泰国','日本','韩国') then a.country_name  
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260101' and dt <= '20260222'
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            and check_out >= '2026-02-15' and check_out <= '2026-02-23'
    )t
    group by order_date,cube(mdd,user_type)
)


select t1.dt `预定日期`
        ,t1.mdd
        ,t1.user_type
        ,date_format(t1.dt,'u') as `星期`
        ,`Q_间夜量` / `C_间夜量` as  `间夜QC`
        ,q_uv / c_uv as  `流量QC`
        ,(`Q_订单量` / q_uv) / (`C_订单量` / c_uv) as  `转化QC`
        ,(`Q_间夜量` / `Q_订单量`) / (`C_间夜量` / `C_订单量`) as  `单间夜QC`
        ,(`Q_收益额` / `Q_GMV`)  as  `Q佣金率`
        ,(`Q_收益额` / `Q_GMV`) - (`C_收益额` / `C_GMV`) as  `QC佣金差`
        ,`支付价lose率`
        ,`支付价lose深度`

        ,q_uv
        ,c_uv
        ,`Q_间夜量`
        ,`C_间夜量`
        ,`Q_订单量`
        ,`C_订单量`
        ,`Q_券额` / `Q_GMV` `Q_券补率`
        ,`C_券额` / `C_GMV` `C_券补率`
        ,`Q_GMV`
        ,`C_GMV`
        ,`Q_收益额`
        ,`C_收益额`
        ,`Q_券额`
        ,`C_券额`
        ,`Q_GMV` / `Q_间夜量` Q_ADR
        ,`C_GMV` / `C_间夜量` C_ADR
        ,(`Q_GMV` / `Q_间夜量`) / (`C_GMV` / `C_间夜量`) ADR_QC
        ,`Q_用券订单量`
        ,`C_用券订单量`
        ,`Q_用券订单量` / `Q_订单量` `Q_用券订单占比`
        ,`C_用券订单量` / `C_订单量` `C_用券订单占比`
        
from  (
    select dt
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(user_id) q_uv
    from  q_uv 
    group by dt,cube(mdd,user_type)
) t1
left join (
    select order_date
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_typeadr_type
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,count(distinct user_id) `Q_生单uv`
        ,count(distinct order_no) `Q_订单量`
        ,sum(room_night) `Q_间夜量`
        ,sum(final_gmv) `Q_GMV`
        ,sum(ld_yj) `Q_收益额`
        ,sum(coupon_substract_summary) `Q_券额`
        ,count(distinct case when coupon_substract_summary > 0 then order_no end) `Q_用券订单量`
    from  q_order 
    group by order_date,cube(mdd,user_type,adr_type)
) t2 on t1.dt = t2.order_date and t1.mdd=t2.mdd and t1.user_type=t2.user_type
left join (
    select dt
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as user_type
        ,count(uid) c_uv
    from  c_uv 
    group by dt,cube(mdd,user_type)
) t3 on t1.dt = t3.dt and t1.mdd=t3.mdd and t1.user_type=t3.user_type
left join (
    select order_date
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(distinct user_id) `C_生单uv`
        ,count(distinct order_no) `C_订单量`
        ,sum(room_night) `C_间夜量`
        ,sum(room_fee) `C_GMV`
        ,sum(comission) `C_收益额`
        ,sum(cqe) `C_券额`
        ,count(distinct case when cqe > 0 then order_no end) `C_用券订单量`
    from  c_order 
    group by order_date,cube(new_mdd,user_type)
) t4 on t1.dt = t4.order_date and t1.mdd=t4.mdd and t1.user_type=t4.user_type
left join qc_price t5 on t1.dt=t5.order_date and t1.mdd=t5.mdd and t1.user_type=t5.user_type
order by 1,2 
;

--- 2、锁定春节离店QC分价格带
with c_user_type as(
    select user_id
        , ubt_user_id
        , substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
) 
,q_user_type as (
    select user_id 
        , min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
)
,q_order as (
    select order_date 
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,checkout_date,order_no,room_night
           ,final_gmv
           ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
           ,a.user_id
           ,hotel_grade
           ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
           ,case  when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
           ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
            ,case 
                when datediff(checkin_date,order_date) <= 0 then '凌晨&当天订'
                when datediff(checkin_date,order_date) between 1 and 7 then '1-7天'
                when datediff(checkin_date,order_date) between 8 and 14 then '8-14天'
                when datediff(checkin_date,order_date) between 15 and 21 then '15-21天'
                when datediff(checkin_date,order_date) between 22 and 28 then '22-28天'
                when datediff(checkin_date,order_date) >= 29 then '29+天'
                else '其他' end as early_day
    from default.mdw_order_v3_international a
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2026-01-01' and order_date <= '2026-02-22'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)
,c_order as(
    select substr(order_date,1,10) as order_date 
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'
               when extend_info['COUNTRY'] in ('泰国','日本','韩国') then extend_info['COUNTRY']
               else '其他' end as new_mdd
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        ,order_no
        ,extend_info['room_night'] room_night
        ,extend_info['STAR'] star
        ,comission
        ,room_fee
        ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
        ,o.user_id
        ,case when room_fee / extend_info['room_night'] < 400  then '1[0,400)'
                  when room_fee / extend_info['room_night'] >= 400 and  room_fee / extend_info['room_night'] < 800  then '2[400,800)'
                  when room_fee / extend_info['room_night'] >= 800 and  room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                  when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
        ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 0 and 3    then '提前订1-3天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                  else '提前订31+'  
            end  per_type
        ,case 
            when datediff(checkin_date,order_date) <= 0 then '凌晨&当天订'
            when datediff(checkin_date,order_date) between 1 and 7 then '1-7天'
            when datediff(checkin_date,order_date) between 8 and 14 then '8-14天'
            when datediff(checkin_date,order_date) between 15 and 21 then '15-21天'
            when datediff(checkin_date,order_date) between 22 and 28 then '22-28天'
            when datediff(checkin_date,order_date) >= 29 then '29+天'
            else '其他' end as early_day
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C' 
        and terminal_channel_type = 'app' 
        and substr(order_date,1,10) >= '2026-01-01' and  substr(order_date,1,10) <= '2026-02-22'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)
,q_data as (
    select order_date
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,count(distinct user_id) `Q_生单uv`
        ,count(distinct order_no) `Q_订单量`
        ,sum(room_night) `Q_间夜量`
        ,sum(final_gmv) `Q_GMV`
        ,sum(ld_yj) `Q_收益额`
        ,sum(coupon_substract_summary) `Q_券额`
        ,count(distinct case when coupon_substract_summary > 0 then order_no end) `Q_用券订单量`
    from  q_order 
    group by order_date,cube(mdd,user_type,adr_type)
)
,c_data as (
    select order_date
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,count(distinct user_id) `C_生单uv`
        ,count(distinct order_no) `C_订单量`
        ,sum(room_night) `C_间夜量`
        ,sum(room_fee) `C_GMV`
        ,sum(comission) `C_收益额`
        ,sum(cqe) `C_券额`
        ,count(distinct case when cqe > 0 then order_no end) `C_用券订单量`
    from  c_order 
    group by order_date,cube(mdd,user_type,adr_type)
)

select t1.order_date
        ,t1.mdd
        ,t1.user_type
        ,t1.adr_type
        ,`Q_间夜量` / `C_间夜量` as  `间夜QC`
        ,(`Q_间夜量` / `Q_订单量`) / (`C_间夜量` / `C_订单量`) as  `单间夜QC`
        ,(`Q_收益额` / `Q_GMV`)  as  `Q佣金率`
        ,(`Q_收益额` / `Q_GMV`) - (`C_收益额` / `C_GMV`) as  `QC佣金差`

        ,`Q_间夜量`
        ,`C_间夜量`
        ,`Q_订单量`
        ,`C_订单量`
        ,`Q_券额` / `Q_GMV` `Q_券补率`
        ,`C_券额` / `C_GMV` `C_券补率`
        ,`Q_GMV`
        ,`C_GMV`
        ,`Q_收益额`
        ,`C_收益额`
        ,`Q_券额`
        ,`C_券额`
        ,`Q_GMV` / `Q_间夜量` Q_ADR
        ,`C_GMV` / `C_间夜量` C_ADR
        ,(`Q_GMV` / `Q_间夜量`) / (`C_GMV` / `C_间夜量`) ADR_QC
        ,`Q_用券订单量`
        ,`C_用券订单量`
        ,`Q_用券订单量` / `Q_订单量` `Q_用券订单占比`
        ,`C_用券订单量` / `C_订单量` `C_用券订单占比`
        ,`Q_订单量_ALL` , `Q_间夜量_ALL` 
        ,`C_订单量_ALL` , `C_间夜量_ALL` 
        
from  q_data t1
left join c_data t4 on t1.order_date = t4.order_date and t1.mdd=t4.mdd and t1.user_type=t4.user_type and t1.adr_type=t4.adr_type
left join (
    select order_date,user_type,`Q_订单量` as `Q_订单量_ALL` ,`Q_间夜量`as `Q_间夜量_ALL` 
    from q_data
    where mdd='ALL' and adr_type='ALL'
) t5 on t1.order_date=t5.order_date  t1.user_type=t5.user_type
left join (
    select order_date,user_type,`C_订单量` as `C_订单量_ALL` ,`C_间夜量`as `C_间夜量_ALL` 
    from c_data
    where mdd='ALL' and adr_type='ALL'
) t6 on t1.order_date=t6.order_date  t1.user_type=t6.user_type
order by 1,2 
;






---2、Q分渠道数据
with user_type -----新老客
as (
    select user_id
            , min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ---D页离店日期在国庆期间
(
    select distinct dt as `日期`
        , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        , case when dt > b.min_order_date then '老客' else '新客' end as user_type
        , a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-01'
        and checkout_date >= '2026-02-15' and  checkout_date <= '2026-02-23'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
)
,user_jc as --机酒交叉
(
    select distinct `日期`
                   , mdd
                   , uv.user_name
                   , '机酒交叉'      as `渠道`
                   , 0              as user_number
     from uv uv
     left join(
        select to_date(create_time)    as create_date
                , o_qunarusername
                , biz_order_no         as flight_order_no
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2025-12-01'
            and substr(create_time, 1, 10) >= '2025-12-01'  -- 生单时间
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
        ) flight
     on uv.user_name = flight.o_qunarusername
     where flight.create_date >= date_sub(uv.`日期`, 15)
        and flight.create_date <= uv.`日期`
        and flight_order_no is not null
)
,user_xhs as --小红书 宽口径
(
    select distinct uv.`日期`
                   , mdd
                   , uv.user_name
                   , '小红书' as `渠道`
                   , 1  as user_number
    from uv uv
    left join(
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '2025-12-01'
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(`日期`, 7)
       and red.flow_dt <= uv.`日期`
       and red.user_name is not null
)
,user_nr as   --- 内容交叉
(
    select distinct concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) `日期`
            , uv.user_name
            , uv.mdd
            , '内容交叉' as  `渠道`
            , 2         as   user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = '%(DATE)s' and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = '%(FORMAT_DATE)s' and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = '%(DATE)s'
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = '%(DATE)s' and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join (
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= '20251201'
                  and dt <= '%(DATE)s'
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name 
    and concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) = uv.`日期`
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as --暑期活动
(
    select distinct uv.`日期`
                   , uv.mdd
                   , uv.user_name
                   , '营销活动' `渠道`
                   , 3 as       user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as `日期`
                        ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 70)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 70)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 70)
            and dt <= date_sub(current_date, 1)
            and username not like '0000%'
        ) d on d.user_name = uv.user_name
    where d.`日期` >= date_sub(uv.`日期`, 7)
       and d.`日期` <= uv.`日期`
       and d.user_name is not null
)
,user_gnjd as ----国内酒店
(
    select distinct `日期`
                   , uv.mdd
                   , uv.user_name
                   , '国内交叉' as `渠道`
                   , 4          as user_number
    from uv uv
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.`日期`, 365)
       and g.order_date <= uv.`日期`
       and g.user_id is not null
)
,user_channel  as ---流量来源渠道整理 
(
    select distinct `日期`
            , mdd
            , user_name
            , `渠道`
    from (
        select `日期`,
                mdd,
                user_name,
                `渠道`,
                row_number() over (partition by `日期`,user_name order by user_number) as user_level
        from (
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_jc
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_xhs
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_nr
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_hd
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as ----多维度活跃用户汇总
(
    select distinct a.`日期`                                                    as dates
            ,a.user_type
            ,a.mdd
            ,nvl(d.`渠道`, '自然流量')                                        as `渠道`
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.`日期` = d.`日期`
)
,q_uv_info as
(   ---- 流量汇总
    select dates
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,if(grouping(`渠道`)=1,'ALL', `渠道`) as  `渠道`
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(user_id)   `UV`
    from uv_1
    group by dates,cube(user_type , mdd, `渠道`)
) 
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,follow_price_amount
            ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2026-01-01'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'

)
,order_info as ---- 订单汇总
(
    select t1.order_date
          ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
          ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
          ,if(grouping(coalesce(t2.`渠道`,'null'))=1,'ALL',coalesce(t2.`渠道`,'null')) as  `渠道`
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(init_gmv)     as `GMV`
          ,sum(coupon_substract_summary)     as `券额`
          ,sum(ld_yj)     as `离店佣金`
          ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
          ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
          ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from q_order t1
    left join (select distinct dates,user_id,`渠道` from uv_1) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,cube(t1.mdd,t1.user_type,coalesce(t2.`渠道`,'null'))
)


select t1.dates   `日期`
        ,t1.mdd
        ,t1.user_type  `新老客`
        ,t1.`渠道`
        ,nvl(t1.`UV`, 0)   as UV
        ,COALESCE(t2.`间夜量`, 0)     as `间夜量`
        ,COALESCE(t2.`订单量`, 0)      as `订单量`
        ,COALESCE(t2.`下单用户量`, 0)      as `下单用户量`
        ,concat(round(COALESCE(t1.`UV` / t3.`UV`, 0) * 100, 1), '%')   as `流量占比`
        ,concat(round(COALESCE(t2.`订单量` / t1.`UV`, 0) * 100, 1), '%')  as `CR`
        ,concat(round(COALESCE(t2.`Q_用券订单量`, 0) / nvl(t2.`订单量`, 0) * 100, 1), '%') as `用券订单占比`
        ,COALESCE(t2.`GMV`, 0)      as `GMV`
        ,COALESCE(t2.`券额`, 0)      as `券额`
        ,COALESCE(t2.`离店佣金`, 0)      as `离店佣金`
        ,COALESCE(t2.`Q_高星间夜量`, 0)      as `Q_高星间夜量`
        ,COALESCE(t2.`Q_中星间夜量`, 0)      as `Q_中星间夜量`
        ,COALESCE(t2.`Q_低星间夜量`, 0)      as `Q_低星间夜量`
from q_uv_info t1 
left join order_info t2 on t1.dates=t2.order_date and t1.mdd=t2.mdd 
        and t1.user_type=t2.user_type and t1.`渠道`=t2.`渠道`
left join (  --- 计算流量占比
    select dates,mdd,user_type,`渠道`,`UV`
    from q_uv_info 
    where user_type = 'ALL' and `渠道` = 'ALL'
) t3 on t1.dates=t3.dates and t1.mdd=t3.mdd 
;

--- 3、Q分fromforlog
with user_type as (
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ---D页离店日期在春节期间
(
    select distinct dt 
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,case when action_entrance_map['fromforlog'] in ('4104','4106') then '二屏内容贴' 
            --   when action_entrance_map['fromforlog']='200000081' then '二屏商卡' 
            --   when action_entrance_map['fromforlog']='200000083' then '市场活动去使用' 
            --   when action_entrance_map['fromforlog']='200000105' then '天天领券任务' 
            --   when action_entrance_map['fromforlog']='200000121' then '答题领积分任务'  
            --   when action_entrance_map['fromforlog']='200000118' then '国酒活动去使用' 
            --   when action_entrance_map['fromforlog']='200000119' then '机票实时短信' 
            --   when action_entrance_map['fromforlog']='200000120' then '带参数push' 
              when action_entrance_map['fromforlog']='200000122' then '国酒大搜落地页商卡' 
            --   when action_entrance_map['fromforlog']='200000123' then '带参数短信' 
              when action_entrance_map['fromforlog']='671' then '大搜落地页-酒店tab' 
              when action_entrance_map['fromforlog']='96' then '大搜' 
            --   when action_entrance_map['fromforlog']='4626' then '我的页面弹窗（机酒用户）' 
              when action_entrance_map['fromforlog']='913' then 'App首页宫格-酒店频道-海外酒店tab' 
              when action_entrance_map['fromforlog']='914' then 'App首页-海外酒店' 
              when action_entrance_map['fromforlog']='4604' then '国际酒店H页快筛标签' 
              when action_entrance_map['fromforlog']='824' then '收藏跳转到酒店详情页'
        else '其他' end as fromforlog_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-01'
        and checkout_date >= '2026-02-15' and  checkout_date <= '2026-02-23'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
)
,q_uv_info as(   ---- 流量汇总
    select dt
            ,if(grouping(fromforlog_type)=1,'ALL', fromforlog_type) as  fromforlog_type
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  new_mdd
            ,count(user_id)   `UV`
    from uv
    group by dt,cube(user_type,fromforlog_type,new_mdd)
) 
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name else '其他' end as new_mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2026-01-01'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)
,order_info as ---- 订单汇总
(
    select t1.order_date
          ,if(grouping(fromforlog_type)=1,'ALL', fromforlog_type) as  fromforlog_type
          ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
          ,if(grouping(t1.new_mdd)=1,'ALL', t1.new_mdd) as  new_mdd
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(init_gmv)     as `GMV`
          ,sum(ld_yj)     as `离店佣金`
          ,sum(coupon_substract_summary)     as `券额`
          ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
          ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
          ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from q_order t1
    left join (select distinct dt,user_id,fromforlog_type from uv) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dt
    group by t1.order_date,cube(t1.user_type,fromforlog_type,new_mdd)
)


select t1.dt   `日期`
        ,t1.user_type  `新老客`
        ,t1.fromforlog_type
        ,t1.new_mdd mdd
        ,nvl(t1.`UV`, 0)   as UV
        ,COALESCE(t2.`间夜量`, 0)     as `间夜量`
        ,COALESCE(t2.`订单量`, 0)      as `订单量`
        ,COALESCE(t2.`下单用户量`, 0)      as `下单用户量`
        ,concat(round(COALESCE(t1.`UV` / t3.`UV`, 0) * 100, 1), '%')   as `流量占比`
        ,concat(round(COALESCE(t2.`订单量` / t1.`UV`, 0) * 100, 1), '%')  as `CR`
        ,concat(round(COALESCE(t2.`Q_用券订单量`, 0) / nvl(t2.`订单量`, 0) * 100, 1), '%') as `用券订单占比`
        ,COALESCE(t2.`GMV`, 0)      as `GMV`
        ,COALESCE(t2.`券额`, 0)      as `券额`
        ,COALESCE(t2.`离店佣金`, 0)      as `离店佣金`
        ,COALESCE(t2.`Q_高星间夜量`, 0)      as `Q_高星间夜量`
        ,COALESCE(t2.`Q_中星间夜量`, 0)      as `Q_中星间夜量`
        ,COALESCE(t2.`Q_低星间夜量`, 0)      as `Q_低星间夜量`
from q_uv_info t1 
left join order_info t2 on t1.dt=t2.order_date and t1.fromforlog_type=t2.fromforlog_type 
        and t1.user_type=t2.user_type  and t1.new_mdd=t2.new_mdd
left join (  --- 计算流量占比
    select dt,new_mdd,user_type,fromforlog_type,`UV`
    from q_uv_info 
    where user_type = 'ALL' and fromforlog_type = 'ALL'
) t3 on t1.dt=t3.dt and t1.new_mdd=t3.new_mdd 
;


--- 4、SDBO链路对比
with c_user_type as(
    select user_id
        , ubt_user_id
        , substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
) 
,q_user_type as (
    select user_id 
        , min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
)
,q_uv as (
    select dt  
        , case when province_name in ('澳门','香港') then province_name 
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
            when c.area in ('欧洲','亚太','美洲') then c.area
            else '其他' end as mdd
        ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
        , case when dt > b.min_order_date then '老客' else '新客' end as user_type
        , a.user_id 
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    left join q_user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-01' and dt<= date_sub(current_date,1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and device_id is not null
        and device_id <> ''
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' 
    group by 1,2,3,4,5
)
,q_b_uv as (
    select  dt 
            ,a.user_id
    from ihotel_default.dw_user_app_log_booking_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-01-01'
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' 
    group by 1,2
)
,q_order as (
    select order_date 
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,checkout_date,order_no,room_night
           ,final_gmv
           ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
           ,a.user_id
           ,hotel_grade
           ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2026-01-01'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)
,c_uv as (
    select dt
            ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when provincename in ('澳门','香港') then '港澳'  when a.countryname in ('泰国','日本','韩国') then a.countryname  else '其他' end as new_mdd
            , case when dt> b.min_order_date then '老客' else '新客' end as user_type 
            ,a.uid
            ,max(detail_dingclick_cnt) detail_dingclick_cnt
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where dt >= '2026-01-01'
        and device_chl = 'app'
        and page_short_domain = 'dbo'
        and check_out >= '2026-02-15' and check_out <= '2026-02-23' 
    group by 1,2,3,4,5
) 
,c_order as(
    select substr(order_date,1,10) as order_date 
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
        ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'
               when extend_info['COUNTRY'] in ('泰国','日本','韩国') then extend_info['COUNTRY']
               else '其他' end as new_mdd
        ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        ,order_no
        ,extend_info['room_night'] room_night
        ,extend_info['STAR'] star
        ,comission
        ,room_fee
        ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
        ,o.user_id
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C' 
        and terminal_channel_type = 'app' 
        and substr(order_date,1,10) >= '2026-01-01' 
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
)

select t1.dt `预定日期`
        ,t1.mdd
        ,t1.user_type
        ,date_format(t1.dt,'u') as `星期`
        ,q_d2b / (c_b_uv / c_uv) d2b_qc
        ,(`Q_订单量` / q_b_uv) / (`C_订单量` / c_b_uv) b2o_qc

        ,q_uv
        ,q_b_uv
        ,q_d2b
        ,`Q_订单量` / q_b_uv q_b2o

        ,c_uv
        ,c_b_uv
        ,c_b_uv / c_uv c_d2b
        ,`C_订单量` / c_b_uv c_b2o

        ,`Q_订单量`
        ,`C_订单量`
        
from  (
    select t1.dt
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(t1.user_id) q_uv
        ,count(t2.user_id) q_b_uv
        ,count(t2.user_id) / count(t1.user_id)  q_d2b
    from  q_uv t1
    left join q_b_uv t2 on t1.dt=t2.dt and t1.user_id=t2.user_id
    group by t1.dt,cube(new_mdd,user_type)
) t1
left join (
    select order_date
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(distinct user_id) `Q_生单uv`
        ,count(distinct order_no) `Q_订单量`
        ,sum(room_night) `Q_间夜量`
        ,sum(final_gmv) `Q_GMV`
        ,sum(ld_yj) `Q_收益额`
        ,sum(coupon_substract_summary) `Q_券额`
    from  q_order 
    group by order_date,cube(new_mdd,user_type)
) t2 on t1.dt = t2.order_date and t1.mdd=t2.mdd and t1.user_type=t2.user_type
left join (
    select dt
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(uid) c_uv
        ,count(case when detail_dingclick_cnt > 0 then uid end ) c_b_uv
    from  c_uv 
    group by dt,cube(new_mdd,user_type)
) t3 on t1.dt = t3.dt and t1.mdd=t3.mdd and t1.user_type=t3.user_type
left join (
    select order_date
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(distinct user_id) `C_生单uv`
        ,count(distinct order_no) `C_订单量`
        ,sum(room_night) `C_间夜量`
        ,sum(room_fee) `C_GMV`
        ,sum(comission) `C_收益额`
        ,sum(cqe) `C_券额`
    from  c_order 
    group by order_date,cube(new_mdd,user_type)
) t4 on t1.dt = t4.order_date and t1.mdd=t4.mdd and t1.user_type=t4.user_type

order by 1,2 
;


--- 5、预定顺畅度
--- pv角度
with q_user_type as (
    select user_id 
        , min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = '20260222'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
)
select a.datas as `日期`
    ,a.user_type
    ,pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`,
    `L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
    `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
    `B2O-房态房价一致率`,
    round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas
        ,user_type,
        round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
        round((1-e/a)*100,2) as `L2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
    from(
        select a.dt as  datas
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e
        from (
            select dt,log_id,
                ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                -- 20240927 是否符合人数条件
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                action_entrance_map['fromforlog'] as is_list  
                ,checkin_date
                checkout_date
                ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
            left join q_user_type b on a.user_id = b.user_id 
            where dt between '2025-01-24' and '2025-02-22'
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
                and  checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
        ) a
        where match_adult != 'false' or match_adult is null
        group by dt,cube(user_type)
    ) a
) a
left join(
    select a.booking_date
        ,user_type
        ,round((1-b/c)*100,2) as `D2B-房态一致率`
        ,round((1-a/(c-b))*100,2) as `D2B-房价一致率`
        ,round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select dt as  booking_date
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then q_trace_id else null end)) as a,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as b,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as c
        from(
            select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) dt
                  ,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange
                  ,case when concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type
            from view_dw_user_app_booking_qta_di  a
            left join q_user_type b on a.user_id = b.user_id 
            where  dt between '20250124' and '20260222'
                and source='app_intl'
                and platform in ('adr','ios')
                and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                and  checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
        )a
        group by dt,cube(user_type)
    ) a
) c on a.datas=c.booking_date and a.user_type=c.user_type
left join(
    select booking_date,user_type,
        round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
    from(
        select booking_date
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),true,null)) as total_submit_fail,
            count(if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,true,null)) as total_submit_coupon,
            count(if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,true,null)) as total_submit_count
        from (
            select to_date(log_time) as booking_date
                    ,case when to_date(log_time) > b.min_order_date then '老客' else '新客' end as user_type
                    ,ret,country_name,province_name,err_message
            from dw_user_app_submit_qta_di a
            left join q_user_type b on a.user_id = b.user_id  
            where dt between '20250124' and '20260222' 
                and source='app_intl'
                and platform in ('adr','ios','AndroidPhone','iPhone')
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
                and err_code not in( '-98','784','785')
                and  checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'
        )
        group by booking_date,cube(user_type)
    ) y
) d on a.datas=d.booking_date and a.user_type=d.user_type
order by `日期` desc
;



--- 6、锁定春节离店抓取支付价beat深度
with qc_price as (
    select order_date
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            ,case   when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('泰国','日本','韩国') then a.country_name  
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260101' 
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            and check_out >= '2026-02-01' and check_out <= '2026-02-05'
    )t
    group by order_date,cube(mdd,user_type)
)

select order_date
      ,mdd
      ,user_type
      ,`支付价lose率`
      ,`支付价lose深度`
      ,`支付价beat深度`
      ,`支付价beat数`       /   `支付价抓取次数`  `beat率`
      ,`支付价beat0-3%次数`   / `支付价抓取次数`  `支付价beat0-3%率`
      ,`支付价beat3-4%次数`   / `支付价抓取次数`  `支付价beat3-4%率`
      ,`支付价beat4-5%次数`   / `支付价抓取次数`  `支付价beat4-5%率`
      ,`支付价beat5-6%次数`   / `支付价抓取次数`  `支付价beat5-6%率`
      ,`支付价beat6-7%次数`   / `支付价抓取次数`  `支付价beat6-7%率`
      ,`支付价beat7-8%次数`   / `支付价抓取次数`  `支付价beat7-8%率`
      ,`支付价beat8%以上次数` /  `支付价抓取次数`  `支付价beat8%以上率`
from qc_price
order by 1,2,3
;



--- 7、不同临期订抓取支付价beat深度
with qc_price as (
    select order_date
        ,if(grouping(early_day)=1,'ALL', early_day) as  early_day
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            ,case   when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('泰国','日本','韩国') then a.country_name  
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
            ,case 
                when datediff(check_in,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) <= 0 then '凌晨&当天订'
                when datediff(check_in,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 1 and 7 then '1-7天'
                when datediff(check_in,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14 then '8-14天'
                when datediff(check_in,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 21 then '15-21天'
                when datediff(check_in,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 22 and 28 then '22-28天'
                when datediff(check_in,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) >= 29 then '29+天'
                else '其他' end as early_day
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20251201' 
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            -- and check_out >= '2026-02-15' and check_out <= '2026-02-23'
    )t
    group by order_date,cube(early_day,mdd,user_type)
)

select order_date
      ,early_day
      ,mdd
      ,user_type
      ,`支付价lose率`
      ,`支付价lose深度`
      ,`支付价beat数`       /   `支付价抓取次数`  `beat率`
      ,`支付价beat0-3%次数`   / `支付价抓取次数`  `支付价beat0-3%率`
      ,`支付价beat3-4%次数`   / `支付价抓取次数`  `支付价beat3-4%率`
      ,`支付价beat4-5%次数`   / `支付价抓取次数`  `支付价beat4-5%率`
      ,`支付价beat5-6%次数`   / `支付价抓取次数`  `支付价beat5-6%率`
      ,`支付价beat6-7%次数`   / `支付价抓取次数`  `支付价beat6-7%率`
      ,`支付价beat7-8%次数`   / `支付价抓取次数`  `支付价beat7-8%率`
      ,`支付价beat8%以上次数` /  `支付价抓取次数`  `支付价beat8%以上率`
from qc_price
order by 1,2,3
;


--- 8、分货源顺畅度
with wrapper_mapping as 
(select distinct id 
                    , supplier_wrapper_group as wrapper_id 
                from ihotel_default.ods_qta_supplier
                where dt='20260224'
                    and inter_flag=1
)
,qc_room_mapping as (
    select distinct
        dt,
        product_id,
        partner_product_id
    from ihotel_default.dwd_supply_qc_product_mapping_di
    where dt between '2026-01-01' and '2026-02-22'
)
,is_agent_mapping as (
  select distinct
      d,
      product_id as room,
      grouptype
  from default.ceq_three_sync_pull_ctrip_qunar_adm_cq_fenxiao_detail a
  left join qc_room_mapping b on a.room = b.partner_product_id and concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) = b.dt
  where d between '20260101' and '20260222'
)

select a.datas as `日期`
       ,a.supplier
       ,`L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
       `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
       `B2O-房态房价一致率`,
       round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`

from
    (select datas,
            supplier,
            round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
            round((1-e/a)*100,2) as `L2D-房态一致率`,
            round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
     from
         (select a.dt as  datas,
                 supplier,
                 count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
                 count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b,
                 count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e
          from
              (select dt,log_id,
                      case 
                        when b.grouptype = 'DC' then 'DC'
                        when split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                        when d.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                        else '其他'
                      end as supplier,
                      ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                      ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                      regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                      get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                      get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                      split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] as supplier_id,
                      split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[0] as room_id,
                      action_entrance_map['fromforlog'] as is_list
                       ,checkin_date,
                          checkout_date
               from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
               left join is_agent_mapping b on split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[0] = b.room and a.dt = concat(substr(b.d,1,4),'-',substr(b.d,5,2),'-',substr(b.d,7,2))
               left join wrapper_mapping c on split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] = c.id
               left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) d on c.wrapper_id = d.wrapper_id
               where dt between '2026-01-01' and '2026-02-22'
                 and source='hotel'
                 and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                 and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                 and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                 and (country_name!='中国' or province_name in('香港','澳门','台湾'))
                 and  checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  ---限定26年春节
                 and user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874','27117935','1263291304','1475831104','1534870051','175004090','223703725','428927726','1005465130','134486580','1534045148','169408570','185495343','185711487','263070154','125896658','140775252','1424343583','1554251482','1070931535','137263924','162660539','273860152','1409683183','1607050360','139741136','196432845')
              ) a
          where match_adult != 'false' or match_adult is null
          group by 1,2
          ) a
    ) a

    left join

    (select a.booking_date,
            supplier,
            round((1-b/c)*100,2) as `D2B-房态一致率`,
            round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
            round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
     from
         (select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date, 
                  supplier,
                 round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then q_trace_id else null end)) as a,
                 count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as b,
                 count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as c
          from
              (select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,
                case
                  when b.grouptype = 'DC' then 'DC'
                  when supplier_id in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                  when c.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                  else '其他'
                end as supplier
               from view_dw_user_app_booking_qta_di a
               left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and a.dt = b.d
               left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id

               where  dt between '20260101' and '20260222'
                 and source='app_intl'
                 and platform in ('adr','ios')
                 and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                 and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                 and q_trace_id not like 'f_inter_autotest%'
                 and  checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  ---限定26年春节
              )a
          group by 1,2
          ) a
      ) b
    on a.datas=b.booking_date and a.supplier = b.supplier

    left join

    (select booking_date,
            supplier,
            round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
     from
         (select to_date(log_time) as booking_date,
                case
                  when b.grouptype = 'DC' then 'DC'
                  when supplier_id in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                  when c.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                  else '其他'
                end as supplier,
                 count(if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),true,null)) as total_submit_fail,
                 count(if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,true,null)) as total_submit_coupon,
                 count(if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,true,null)) as total_submit_count
          from dw_user_app_submit_qta_di a
          left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and to_date(log_time) = concat(substr(b.d,1,4),'-',substr(b.d,5,2),'-',substr(b.d,7,2))
          left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id

          where  dt between '20260101' and '20260222'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and  checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  ---限定26年春节
          group by 1,2
          ) y
    ) c
    on a.datas=c.booking_date and a.supplier = c.supplier
order by `日期` desc
;



--- 8、商旅订单占比
with q_user_type as (
    select user_id 
        , min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
)
,inv_order as (
    select order_no
        ,case when quality_type in ('1', '2') then  'Y' else 'N' end is_bus_gov
    from fuwu.dwd_xcd_htl_complete_di
    where is_international = 1
    and dt >= '2024-01-01'
    group by 1,2
)
,q_order as (
    select order_date 
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,checkout_date,a.order_no,room_night
           ,final_gmv
           ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
           ,a.user_id
           ,hotel_grade
           ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
           ,case  when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
            ,case when f.order_no is not null then 'Y' else 'N' end is_inv
            ,case when is_bus_gov = 'Y' then  'Y' else 'N'  end is_bus_inv
    from default.mdw_order_v3_international a
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join inv_order f on a.order_no=f.order_no
    where dt = '%(DATE)s' 
        and terminal_channel_type = 'app' 
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)

        -- and checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05'
)

select order_date
    ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
    ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
    ,count(distinct user_id) `Q_生单uv`
    ,count(distinct order_no) `Q_订单量`
    ,sum(room_night) `Q_间夜量`
    ,sum(final_gmv) `Q_GMV`
    ,sum(ld_yj) `Q_收益额`
    ,sum(coupon_substract_summary) `Q_券额`
    ,count(distinct case when coupon_substract_summary > 0 then order_no end) `Q_用券订单量`
    ,count(distinct case when is_inv = 'Y' then order_no end) `Q_开票订单量`
    ,count(distinct case when is_bus_inv = 'Y' then order_no end) `Q_企业开票订单量`
from  q_order 
group by order_date,cube(mdd,user_type)
order by 1
;