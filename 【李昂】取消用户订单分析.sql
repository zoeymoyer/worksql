---- 每日领取取消挽留红包的用户，最后仍有40%的订单被取消，需要分析下这部分用户取消订单后是否有重订
---- 分析用户的取消时间间隔，距离checkin 多少天取消，距离预定多少天取消
with user_type as
(
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
,q_order as (----订单明细表包含取消  分目的地、新老维度 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id,hotel_seq
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,init_gmv / room_night adr
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 180) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_cashback_cancel as (--- 领取返现红包且取消用户
    select user_name,order_no,room_night,init_gmv,order_date,checkin_date,checkout_date,substr(first_cancelled_time,1,10) first_cancelled_time
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') fx
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                    else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,hotel_seq,hotel_grade,init_gmv / room_night adr
        ,datediff(first_cancelled_time,order_date) cancel_order_date
        ,datediff(checkin_date,first_cancelled_time) cancel_checkin_date
    FROM default.mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    WHERE dt = '%(DATE)s'
    and is_valid = 1
    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    -- and order_date between date_sub(current_date, 60) and date_sub(current_date, 1)
    and order_status in ('CANCELLED', 'REJECTED')
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null 
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') <> 0
)

select t1.*
        ,t2.`取消7日内再次下单UV`
        ,t2.`取消7日内再次下单订单量`
        ,t2.`取消7日内再次下单订单量(低adr)`
        ,t2.`取消7日内再次下单订单量(同一目的地)`
        ,t2.`取消7日内再次下单订单量(同一目的地低adr)`
        ,t2.`取消7日内再次下单订单量(同一酒店)`
        ,t2.`取消7日内再次下单订单量(同一星级)`
from (
    select   case when cancel_order_date >= 0 and cancel_order_date <= 7 then '1.[0,7]' 
                when cancel_order_date >= 8 and cancel_order_date <= 14 then '2.[8,14]' 
                when cancel_order_date >= 15 and cancel_order_date <= 30 then '3.[15,30]' 
                when cancel_order_date >= 31 and cancel_order_date <= 60 then '4.[31,60]' 
                else '5.60+' end diff_type
            ,count(distinct t1.order_no) `取消订单量`
            ,count(distinct t1.user_name) `取消订单UV` 
            ,sum(t1.init_gmv)  `取消订单GMV`
            ,sum(t1.final_commission_after)  `取消订单佣金`
            ,sum(t1.fx)  `取消订单返现金额`
            
    from q_cashback_cancel t1 
    group by 1
) t1 
left join (
    select case when cancel_order_date >= 0 and cancel_order_date <= 7 then '1.[0,7]' 
                when cancel_order_date >= 8 and cancel_order_date <= 14 then '2.[8,14]' 
                when cancel_order_date >= 15 and cancel_order_date <= 30 then '3.[15,30]' 
                when cancel_order_date >= 31 and cancel_order_date <= 60 then '4.[31,60]' 
                else '5.60+' end diff_type
            ,count(distinct t2.user_name) `取消7日内再次下单UV`
            ,count(distinct t2.order_no)  `取消7日内再次下单订单量`
            ,count(distinct case when t2.adr < t1.adr then t2.order_no end)  `取消7日内再次下单订单量(低adr)`
            ,count(distinct case when t1.mdd=t2.mdd then t2.order_no end)  `取消7日内再次下单订单量(同一目的地)`
            ,count(distinct case when t1.mdd=t2.mdd and t2.adr < t1.adr then t2.order_no end)  `取消7日内再次下单订单量(同一目的地低adr)`
            ,count(distinct case when t1.hotel_seq=t2.hotel_seq then t2.order_no end)  `取消7日内再次下单订单量(同一酒店)`
            ,count(distinct case when t1.hotel_grade=t2.hotel_grade then t2.order_no end)  `取消7日内再次下单订单量(同一星级)`
    from q_cashback_cancel t1 
    left join q_order t2 on t1.user_name=t2.user_name and datediff(t2.order_date,t1.first_cancelled_time) between 1 and 7
    where t2.user_name is not null
    group by 1
)t2 on t1.diff_type=t2.diff_type
;


---- 分析用户重订两次入住时间间隔
with user_type as
(
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
,q_order as (----订单明细表包含取消  分目的地、新老维度 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id,hotel_seq
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,init_gmv / room_night adr,checkin_date
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 180) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_cashback_cancel as (--- 领取返现红包且取消用户
    select user_name,order_no,room_night,init_gmv,order_date,checkin_date,checkout_date,substr(first_cancelled_time,1,10) first_cancelled_time
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') fx
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                    else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,hotel_seq,hotel_grade,init_gmv / room_night adr
        ,datediff(first_cancelled_time,order_date) cancel_order_date
        ,datediff(checkin_date,first_cancelled_time) cancel_checkin_date
    FROM default.mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    WHERE dt = '%(DATE)s'
    and is_valid = 1
    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    -- and order_date between date_sub(current_date, 60) and date_sub(current_date, 1)
    and order_status in ('CANCELLED', 'REJECTED')
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null 
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') <> 0
)


    select  case when abs(datediff(t2.checkin_date,t1.checkin_date)) between 0 and 7 then '1.[0,7]'
                 when abs(datediff(t2.checkin_date,t1.checkin_date)) between 8 and 14 then '2.[8,14]'
                 when abs(datediff(t2.checkin_date,t1.checkin_date)) between 15 and 30 then '3.[15,30]'
                 when abs(datediff(t2.checkin_date,t1.checkin_date)) between 31 and 60 then '4.[31,60]'
                 else '5.60+' end  `取消7日内再次下单两次入住时间差`
            ,count(distinct t2.user_name) `取消7日内再次下单UV`
            ,count(distinct t2.order_no)  `取消7日内再次下单订单量`
            ,count(distinct case when t2.adr < t1.adr then t2.order_no end)  `取消7日内再次下单订单量(低adr)`
            ,count(distinct case when t1.mdd=t2.mdd then t2.order_no end)  `取消7日内再次下单订单量(同一目的地)`
            ,count(distinct case when t1.mdd=t2.mdd and t2.adr < t1.adr then t2.order_no end)  `取消7日内再次下单订单量(同一目的地低adr)`
            ,count(distinct case when t1.hotel_seq=t2.hotel_seq then t2.order_no end)  `取消7日内再次下单订单量(同一酒店)`
            ,count(distinct case when t1.hotel_grade=t2.hotel_grade then t2.order_no end)  `取消7日内再次下单订单量(同一星级)`
    from q_cashback_cancel t1 
    left join q_order t2 on t1.user_name=t2.user_name and datediff(t2.order_date,t1.first_cancelled_time) between 1 and 7
    where t2.user_name is not null
    group by 1
;












---- 大盘
with user_type as
(
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
,q_order as (----订单明细表包含取消  分目的地、新老维度 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id,hotel_seq
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 180) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_cashback_cancel as (--- 领取返现红包且取消用户
    select user_name,order_no,room_night,init_gmv,order_date,checkin_date,checkout_date,substr(first_cancelled_time,1,10) first_cancelled_time
        ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') fx
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                    else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,hotel_seq,hotel_grade
        ,datediff(first_cancelled_time,order_date) cancel_order_date
        ,datediff(checkin_date,first_cancelled_time) cancel_checkin_date
    FROM default.mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    WHERE dt = '%(DATE)s'
    and is_valid = 1
    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    and order_date between date_sub(current_date, 60) and date_sub(current_date, 30)
    and order_status in ('CANCELLED', 'REJECTED')
    
)

select t1.*
        ,t2.`取消7日内再次下单UV`
        ,t2.`取消7日内再次下单订单量`
        ,t2.`取消7日内再次下单订单量(同一目的地)`
        ,t2.`取消7日内再次下单订单量(同一酒店)`
        ,t2.`取消7日内再次下单订单量(同一星级)`
from (
    select   case when cancel_order_date >= 0 and cancel_order_date <= 7 then '1.[0,7]' 
                when cancel_order_date >= 8 and cancel_order_date <= 14 then '2.[8,14]' 
                when cancel_order_date >= 15 and cancel_order_date <= 30 then '3.[15,30]' 
                when cancel_order_date >= 31 and cancel_order_date <= 60 then '4.[31,60]' 
                else '5.60+' end diff_type
            ,count(distinct t1.order_no) `取消订单量`
            ,count(distinct t1.user_name) `取消订单UV` 
            ,sum(t1.init_gmv)  `取消订单GMV`
            ,sum(t1.final_commission_after)  `取消订单佣金`
            ,sum(t1.fx)  `取消订单返现金额`
            
    from q_cashback_cancel t1 
    group by 1
) t1 
left join (
    select case when cancel_order_date >= 0 and cancel_order_date <= 7 then '1.[0,7]' 
                when cancel_order_date >= 8 and cancel_order_date <= 14 then '2.[8,14]' 
                when cancel_order_date >= 15 and cancel_order_date <= 30 then '3.[15,30]' 
                when cancel_order_date >= 31 and cancel_order_date <= 60 then '4.[31,60]' 
                else '5.60+' end diff_type
            ,count(distinct t2.user_name) `取消7日内再次下单UV`
            ,count(distinct t2.order_no)  `取消7日内再次下单订单量`
            ,count(distinct case when t1.mdd=t2.mdd then t2.order_no end)  `取消7日内再次下单订单量(同一目的地)`
            ,count(distinct case when t1.hotel_seq=t2.hotel_seq then t2.order_no end)  `取消7日内再次下单订单量(同一酒店)`
            ,count(distinct case when t1.hotel_grade=t2.hotel_grade then t2.order_no end)  `取消7日内再次下单订单量(同一星级)`
    from q_cashback_cancel t1 
    left join q_order t2 on t1.user_name=t2.user_name and datediff(t2.order_date,t1.first_cancelled_time) between 1 and 7
    where t2.user_name is not null
    group by 1
)t2 on t1.diff_type=t2.diff_type
;

---- 重订用户前后对比
select *
from (
    select user_name
        ,count(distinct order_no) `订单量`
        ,sum(init_gmv)  `GMV`
        ,sum(room_night)`间夜量`
        ,sum(init_gmv) / sum(room_night) `ADR`
        ,sum(fx) `返现金额`
        ,sum(final_commission_after) `佣金`
    from q_cashback_cancel
    group by 1
)t1 left join (
    select  user_name
            ,count(distinct order_no) `订单量-重订`
            ,sum(init_gmv)      `GMV-重订`
            ,sum(room_night)    `间夜量-重订`
            ,sum(init_gmv) / sum(room_night) `ADR-重订`
            ,sum(final_commission_after) `佣金-重订`
    from (
        select t1.user_name,t2.order_no,init_gmv,room_night,t2.order_date,final_commission_after
        from (select user_name,first_cancelled_time from  q_cashback_cancel group by 1,2) t1 
        left join q_order t2 on t1.user_name=t2.user_name and datediff(t2.order_date,t1.first_cancelled_time) between 0 and 7
        where t2.user_name is not null
        group by 1,2,3,4,5,6
    )
    group by 1
) t2 on t1.user_name=t2.user_name
where t2.user_name is not null
;


select  count(distinct t2.user_name) `取消7日内再次下单UV`
        ,count(distinct t2.order_no)  `取消7日内再次下单订单量`
        ,count(distinct case when t2.adr < t1.adr then t2.order_no end)  `取消7日内再次下单订单量(低adr)`
        ,count(distinct case when t1.mdd=t2.mdd then t2.order_no end)  `取消7日内再次下单订单量(同一目的地)`
        ,count(distinct case when t1.mdd=t2.mdd and t2.adr < t1.adr then t2.order_no end)  `取消7日内再次下单订单量(同一目的地低adr)`
        ,count(distinct case when t1.hotel_seq=t2.hotel_seq then t2.order_no end)  `取消7日内再次下单订单量(同一酒店)`
        ,count(distinct case when t1.hotel_grade=t2.hotel_grade then t2.order_no end)  `取消7日内再次下单订单量(同一星级)`
        ,(sum(t1.adr) - sum(t2.adr))/count(1)
from (select user_name from  q_cashback_cancel group by 1) t1 
left join q_order t2 on t1.user_name=t2.user_name and datediff(t2.order_date,t1.first_cancelled_time) between 0 and 7
where t2.user_name is not null