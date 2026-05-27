with user_type as (
    select user_id, min(order_date) as min_order_date
    from default.mdw_order_v3_international   
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by user_id
)
,q_order_app as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,final_gmv
            ,batch_series,hotel_grade,coupon_id,checkout_date,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as commission
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >= '2025-01-01' and checkout_date <= '2026-04-26'
        and order_no <> '103576132435'
)
,q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,final_gmv
            ,batch_series,hotel_grade,coupon_id,checkout_date,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as commission
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >= '2025-01-01' and checkout_date <= '2026-04-26'
        and order_no <> '103576132435'
)
,q_app_order_all as (
    select checkout_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,room_night,order_no
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and checkout_date >= '2025-01-01' and checkout_date <= '2026-04-26'
        and order_no <> '103576132435'
)
,q_order_all as (
    select checkout_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        ,room_night,order_no
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and checkout_date >= '2025-01-01' and checkout_date <= '2026-04-26'
        and order_no <> '103576132435'
)
,q_app_order_all_info as (
    select checkout_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(room_night) as q_rn_app_all 
         ,count(distinct order_no) as q_on_app_all 
    from q_app_order_all t1
    group by checkout_date,cube(t1.mdd,t1.user_type)
)
,q_order_all_info as (
    select checkout_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(room_night) as q_rn_all 
         ,count(distinct order_no) as q_on_all 
    from q_order_all t1
    group by checkout_date,cube(t1.mdd,t1.user_type)
)
,order_info_app as (
    select checkout_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as q_commission_app 
         ,sum(final_gmv) as q_gmv_app 
         ,sum(coupon_substract_summary) as q_coupon_amount_app 
         ,count(distinct order_no) as q_order_cnt_app 
         ,count(distinct t1.user_id) as q_order_user_cnt_app 
         ,sum(room_night) as q_room_night_app 
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) as q_coupon_order_cnt_app 
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_high_star_rn_app 
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_mid_star_rn_app 
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_low_star_rn_app 
    from q_order_app t1
    group by checkout_date,cube(t1.mdd,t1.user_type)
)
,order_info as (
    select checkout_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as q_commission 
         ,sum(final_gmv) as q_gmv 
         ,sum(coupon_substract_summary) as q_coupon_amount 
         ,count(distinct order_no) as q_order_cnt 
         ,count(distinct t1.user_id) as q_order_user_cnt 
         ,sum(room_night) as q_room_night 
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) as q_coupon_order_cnt 
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_high_star_rn 
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_mid_star_rn 
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_low_star_rn 
    from q_order t1
    group by checkout_date,cube(t1.mdd,t1.user_type)
)
,q_data_info as (
    select t1.checkout_date
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.q_room_night, 0) as q_room_night 
            ,coalesce(t1.q_order_cnt, 0)  as q_order_cnt 
            ,coalesce(t1.q_order_user_cnt, 0)   as q_order_user_cnt 
            ,coalesce(t1.q_gmv, 0)      as q_gmv 
            ,coalesce(t1.q_commission, 0)      as q_commission 
            ,coalesce(t1.q_coupon_amount, 0)      as q_coupon_amount 
            ,coalesce(t1.q_high_star_rn, 0)      as q_high_star_rn 
            ,coalesce(t1.q_mid_star_rn, 0)      as q_mid_star_rn 
            ,coalesce(t1.q_low_star_rn, 0)      as q_low_star_rn 

            ,coalesce(t1.q_room_night, 0) / nullif(coalesce(t1.q_order_cnt, 0), 0)  as q_avg_rn_per_order 
            ,coalesce(t1.q_commission, 0) / nullif(coalesce(t1.q_gmv, 0), 0)  as q_take_rate 
            ,coalesce(t1.q_coupon_amount, 0) / nullif(coalesce(t1.q_gmv, 0), 0)  as q_subsidy_rate 
            ,coalesce(t1.q_gmv, 0) / nullif(coalesce(t1.q_room_night, 0), 0)  as q_adr 
            ,coalesce(t1.q_coupon_order_cnt, 0) / nullif(coalesce(t1.q_order_cnt, 0), 0)  as q_coupon_order_rate 

            ,coalesce(t4.q_room_night_app, 0)  as q_room_night_app 
            ,coalesce(t4.q_order_cnt_app, 0)  as q_order_cnt_app 
            ,coalesce(t4.q_order_user_cnt_app, 0) as q_order_user_cnt_app 
            ,coalesce(t4.q_gmv_app, 0)      as q_gmv_app 
            ,coalesce(t4.q_commission_app, 0)      as q_commission_app 
            ,coalesce(t4.q_coupon_amount_app, 0)      as q_coupon_amount_app 
            ,coalesce(t4.q_high_star_rn_app, 0)      as q_high_star_rn_app 
            ,coalesce(t4.q_mid_star_rn_app, 0)      as q_mid_star_rn_app 
            ,coalesce(t4.q_low_star_rn_app, 0)      as q_low_star_rn_app 
            ,coalesce(t4.q_room_night_app, 0) / nullif(coalesce(t4.q_order_cnt_app, 0), 0) as q_avg_rn_per_order_app 
            ,coalesce(t4.q_commission_app, 0)  / nullif(coalesce(t4.q_gmv_app, 0), 0)   as q_take_rate_app 
            ,coalesce(t4.q_coupon_amount_app, 0)  / nullif(coalesce(t4.q_gmv_app, 0), 0)   as q_subsidy_rate_app 
            ,coalesce(t4.q_gmv_app, 0)  / nullif(coalesce(t4.q_room_night_app, 0), 0) as q_adr_app 
            ,coalesce(t4.q_coupon_order_cnt_app, 0) / nullif(coalesce(t4.q_order_cnt_app, 0), 0)  as q_coupon_order_rate_app 

            ,coalesce(t5.q_rn_all, 0)  as q_rn_all 
            ,coalesce(t5.q_on_all, 0)  as q_on_all 
            ,coalesce(t6.q_rn_app_all, 0)  as q_rn_app_all 
            ,coalesce(t6.q_on_app_all, 0)  as q_on_app_all 
    from order_info t1
    left join order_info_app t4 on t1.checkout_date=t4.checkout_date and t1.mdd=t4.mdd and t1.user_type=t4.user_type 
    left join q_order_all_info t5 on t1.checkout_date=t5.checkout_date and t1.mdd=t5.mdd and t1.user_type=t5.user_type
    left join q_app_order_all_info t6 on t1.checkout_date=t6.checkout_date and t1.mdd=t6.mdd and t1.user_type=t6.user_type
)
,c_user_type as(
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,c_app_order as (
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe
            ,substr(checkout_date,1,10) as checkout_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and terminal_channel_type = 'app'
        and order_status <> 'C'
        and substr(checkout_date,1,10) >= '2025-01-01' and substr(checkout_date,1,10) <= '2026-04-26'
)
,c_app_order_all as (
    select substr(checkout_date,1,10) as checkout_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,order_no
            ,extend_info['room_night'] room_night
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(checkout_date,1,10) >= '2025-01-01' and substr(checkout_date,1,10) <= '2026-04-26'
)
,c_order_info as(
    select checkout_date
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,count(distinct order_no) as c_order_cnt 
           ,sum(room_night) as c_room_night 
           ,sum(room_fee)as c_gmv 
           ,sum(comission) as c_commission 
           ,sum(cqe) as c_coupon_amount 
           ,count(distinct user_id)  c_order_user_cnt 
           ,sum(case when star in (4,5) then room_night else 0 end) as c_high_star_rn 
           ,sum(case when star in (3) then room_night else 0 end) as c_mid_star_rn 
           ,sum(case when star not in (3,4,5) then room_night else 0 end) as c_low_star_rn 
    from c_app_order
    group by checkout_date,cube(user_type, mdd)
)
,c_order_all_info as(
    select checkout_date
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,count(distinct order_no) as c_order_cnt_all 
           ,sum(room_night) as c_room_night_all 
    from c_app_order_all
    group by checkout_date,cube(user_type, mdd)
)
,c_data_info as (
    select t1.checkout_date   
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.c_room_night, 0) as c_room_night 
            ,coalesce(t1.c_order_cnt, 0)  as c_order_cnt 
            ,coalesce(t1.c_order_user_cnt, 0)   as c_order_user_cnt 
            ,coalesce(t1.c_gmv, 0)      as c_gmv 
            ,coalesce(t1.c_commission, 0)      as c_commission 
            ,coalesce(t1.c_coupon_amount, 0)      as c_coupon_amount 
            ,coalesce(t1.c_high_star_rn, 0)      as c_high_star_rn 
            ,coalesce(t1.c_mid_star_rn, 0)      as c_mid_star_rn 
            ,coalesce(t1.c_low_star_rn, 0)      as c_low_star_rn 
            ,coalesce(t1.c_room_night, 0) / nullif(coalesce(t1.c_order_cnt, 0), 0)  as c_avg_rn_per_order 
            ,coalesce(t1.c_commission, 0) / nullif(coalesce(t1.c_gmv, 0), 0)  as c_take_rate 
            ,coalesce(t1.c_coupon_amount, 0) / nullif(coalesce(t1.c_gmv, 0), 0)  as c_subsidy_rate 
            ,coalesce(t1.c_gmv, 0) / nullif(coalesce(t1.c_room_night, 0), 0)  as c_adr 
            ,coalesce(t2.c_room_night_all, 0)  as c_room_night_all 
            ,coalesce(t2.c_order_cnt_all, 0)  as c_order_cnt_all 
    from c_order_info t1 
    left join c_order_all_info t2 on t1.checkout_date=t2.checkout_date and t1.mdd=t2.mdd  and t1.user_type=t2.user_type
)


select t1.checkout_date as dt
      ,t1.mdd
      ,t1.user_type

      ,q_room_night_app / nullif(c_room_night, 0)  as `间夜QC` 
      ,q_avg_rn_per_order_app / nullif(c_avg_rn_per_order, 0)  as `单间夜QC` 
      ,q_commission_app / nullif(c_commission, 0)  as `收益QC` 
      ,(q_take_rate_app - c_take_rate)   as `收益率差` 
      ,q_adr_app / nullif(c_adr, 0)    as `ADR_QC` 
      ,(q_subsidy_rate_app - c_subsidy_rate)   as `补贴率差` 
      ,q_order_cnt_app / nullif(c_order_cnt, 0)   as `订单量QC` 

      ,q_room_night  `间夜量`
      ,q_room_night_app  `间夜量_app`
      ,c_room_night  `C间夜量`
      ,q_commission `收益率`
      ,c_commission `C收益率`
      ,q_commission_app `收益率_app`

      ,q_take_rate     `收益率` 
      ,q_take_rate_app `收益率_app`
      ,c_take_rate     `C收益率`

      ,q_subsidy_rate     `补贴率`
      ,q_subsidy_rate_app `补贴率_app`
      ,c_subsidy_rate     `C补贴率`
      ,q_order_cnt `订单量`
      ,q_order_cnt_app `订单量_app`
      ,c_order_cnt `C订单量`

      ,q_gmv `GMV`
      ,q_gmv_app `GMV_app`
      ,c_gmv `CGMV`

      ,q_coupon_amount `补贴金额`
      ,q_coupon_amount_app `补贴金额_app`
      ,c_coupon_amount `C补贴金额`

      ,q_order_user_cnt `生单用户数`
      ,q_order_user_cnt_app `生单用户数_app`
      ,c_order_user_cnt `C生单用户数`

      ,q_adr `ADR`
      ,q_adr_app `ADR_app`
      ,c_adr `CADR`

      ,q_coupon_order_rate `用券订单占比`
      ,q_coupon_order_rate_app `用券订单占比_app`

      ,q_high_star_rn `高星间夜量`
      ,q_high_star_rn_app `高星间夜量_app`
      ,c_high_star_rn `C高星间夜量`

      ,q_mid_star_rn `中星间夜量`
      ,q_mid_star_rn_app `中星间夜量_app`
      ,c_mid_star_rn `C中星间夜量`

      ,q_low_star_rn `低星间夜量`
      ,q_low_star_rn_app `低星间夜量_app`
      ,c_low_star_rn `C低星间夜量`

      ,q_avg_rn_per_order `单间夜`
      ,q_avg_rn_per_order_app `单间夜_app`
      ,c_avg_rn_per_order `C单间夜`
      
      ,1- q_room_night / nullif(q_rn_all, 0) as `取消率` 
      ,1- q_room_night_app / nullif(q_rn_app_all, 0) as `取消率_app` 
      ,1- c_room_night / nullif(c_room_night_all, 0) as `C取消率` 
      ,q_rn_all `总间夜量`
      ,q_on_all `总订单量`
      ,q_rn_app_all `总间夜量_app`
      ,q_on_app_all `总订单量_app`
      ,c_room_night_all `C总间夜量`
      ,c_order_cnt_all `C总订单量`
      
from (
    select checkout_date,mdd,user_type
           ,q_room_night ,q_order_cnt ,q_order_user_cnt ,q_gmv ,q_commission ,q_coupon_amount ,q_high_star_rn ,q_mid_star_rn ,q_low_star_rn ,q_avg_rn_per_order ,q_take_rate ,q_subsidy_rate ,q_adr ,q_coupon_order_rate ,q_room_night_app ,q_order_cnt_app ,q_order_user_cnt_app ,q_gmv_app ,q_commission_app ,q_coupon_amount_app ,q_high_star_rn_app ,q_mid_star_rn_app ,q_low_star_rn_app ,q_avg_rn_per_order_app ,q_take_rate_app ,q_subsidy_rate_app ,q_adr_app ,q_coupon_order_rate_app ,q_rn_all ,q_on_all ,q_rn_app_all ,q_on_app_all
    from q_data_info
) t1
left join c_data_info t2 on t1.checkout_date=t2.checkout_date and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by dt  
        ,case when mdd = '香港'  then 1 when mdd = '澳门'  then 2 when mdd = '泰国'  then 3 when mdd = '日本'  then 4 when mdd = '韩国'  then 5 when mdd = '马来西亚'  then 6 when mdd = '新加坡'  then 7 when mdd = '美国'  then 8 when mdd = '印度尼西亚'  then 9 when mdd = '俄罗斯'  then 10 when mdd = '欧洲'  then 11 when mdd = '亚太'  then 12 when mdd = '美洲'  then 13 when mdd = '其他'  then 14 when mdd = 'ALL'  then 0 end asc
        ,case when user_type = 'ALL' then 1 when user_type = '新客' then 2 when user_type = '老客' then 3 end asc