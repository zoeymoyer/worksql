with user_type as(
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
,c_user_type as(   --- 用于判定c新老客
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
,q_order_app_checkout as (----订单明细表表包含取消  分目的地、新老维度 app
    select checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade
            ,case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                  then (final_commission_after+coalesce(coupon_substract_summary ,0)) 
                  when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') 
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0)) 
                else final_commission_after+coalesce(ext_plat_certificate,0) 
                end as  final_commission_after
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
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >= '2024-01-01' and checkout_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order_checkout as (----订单明细表表包含取消  分目的地、新老维度
    select checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade
            ,case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                  then (final_commission_after+coalesce(coupon_substract_summary ,0)) 
                  when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') 
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0)) 
                else final_commission_after+coalesce(ext_plat_certificate,0) 
                end as  final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >=  '2024-01-01'  and checkout_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_order_checkout as (  --- c订单明细
    select substr(checkout_date,1,10) as checkout_date
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and order_status <> 'C'
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL')
      and substr(checkout_date,1,10) >= '2024-01-01' 
      and substr(checkout_date,1,10) <= date_sub(current_date, 1)
)
,order_info_app_checkout as ( --- q离店订单汇总 app
    select t1.checkout_date 
         ,if(grouping(t1.mdd)=1,'ALL' ,t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL' ,t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金_app_ld`
         ,sum(init_gmv) as               `Q_GMV_app_ld`
         ,count(distinct order_no) as    `Q_订单量_app_ld`
         ,count(distinct t1.user_id) as  `Q_下单用户_app_ld`
         ,sum(room_night) as             `Q_间夜量_app_ld`
         ,sum(coupon_substract_summary) as `Q_券额_app_ld`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量_app_ld`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as   `Q_中星间夜量_app_ld`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量_app_ld`
    from q_order_app_checkout t1
    group by t1.checkout_date,cube(t1.mdd,t1.user_type)
)
,order_info_checkout as ( --- q离店订单汇总 
    select t1.checkout_date 
         ,if(grouping(t1.mdd)=1,'ALL' ,t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL' ,t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金_ld`
         ,sum(init_gmv) as               `Q_GMV_ld`
         ,count(distinct order_no) as    `Q_订单量_ld`
         ,count(distinct t1.user_id) as  `Q_下单用户_ld`
         ,sum(room_night) as             `Q_间夜量_ld`
         ,sum(coupon_substract_summary) as `Q_券额_ld`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量_ld`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as   `Q_中星间夜量_ld`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量_ld`
    from q_order_checkout t1
    group by t1.checkout_date,cube(t1.mdd,t1.user_type)
)
,c_order_info_checkout as(  ---- c离店订单汇总
    select checkout_date
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,count(order_no) as `C_订单量_ld`
           ,sum(room_night) as `C_间夜量_ld`
           ,sum(room_fee)as `C_GMV_ld`
           ,sum(comission) as `C_佣金_ld`
           ,count(distinct user_id) as  `C_下单用户_ld`
           ,sum(cqe)   as `C_券额_ld`
           ,sum(case when star in (4,5) then room_night else 0 end) as `C_高星间夜量_ld`
           ,sum(case when star in (3) then room_night else 0 end) as `C_中星间夜量_ld`
           ,sum(case when star not in (3,4,5) then room_night else 0 end) as `C_低星间夜量_ld`
    from c_order_checkout
    group by checkout_date,cube(user_type, mdd)
)



select t1.checkout_date,t1.mdd,t1.user_type
    ,`Q_间夜量_ld`
    ,`Q_订单量_ld`
    ,`Q_GMV_ld`
    ,`Q_佣金_ld`
    ,`Q_下单用户_ld`
    ,`Q_佣金_ld` / `Q_GMV_ld` as `佣金率`
    ,`Q_GMV_ld` / `Q_间夜量_ld` as `ADR`

    ,`Q_间夜量_app_ld` / `C_间夜量_ld` as  `间夜QC`
    ,`Q_订单量_app_ld` / `C_订单量_ld`  as  `订单量QC`
    ,(`Q_佣金_app_ld` / `Q_GMV_app_ld` ) / (`C_佣金_ld` / `C_GMV_ld`) as  `佣金率QC`
    ,(`Q_GMV_app_ld` / `Q_间夜量_app_ld`) / (`C_GMV_ld` / `C_间夜量_ld`) as   `ADRQC`
    ,`Q_GMV_app_ld` / `C_GMV_ld`   as  `GMVQC`
    ,`Q_佣金_app_ld` / `C_佣金_ld`  as  `佣金QC`
    ,`Q_下单用户_app_ld` / `C_下单用户_ld`  as  `下单用户QC`

    ,`Q_间夜量_app_ld`
    ,`Q_GMV_app_ld`
    ,`Q_佣金_app_ld`
    ,`Q_订单量_app_ld`
    ,`Q_下单用户_app_ld`
    ,`Q_佣金_app_ld` / `Q_GMV_app_ld` as `佣金率_app`
    ,`Q_GMV_app_ld` / `Q_间夜量_app_ld` as `ADR_app`

    ,`C_间夜量_ld`
    ,`C_GMV_ld`
    ,`C_佣金_ld`
    ,`C_订单量_ld`
    ,`C_下单用户_ld`
    ,`C_佣金_ld` / `C_GMV_ld` as `佣金率_c`
    ,`C_GMV_ld` / `C_间夜量_ld` as `ADR_c`

    ,`Q_券额_ld` / `Q_GMV_ld` as `券补率`
    ,`Q_券额_app_ld` / `Q_GMV_app_ld` as `券补率_app`
    ,`C_券额_ld` / `C_GMV_ld` as `券补率_c`
    ,(`Q_券额_app_ld` / `Q_GMV_app_ld`) - (`C_券额_ld` / `C_GMV_ld`) as `券补率_qc`
    ,`Q_券额_ld`
    ,`Q_券额_app_ld`
    ,`C_券额_ld`
    
from order_info_checkout t1
left join order_info_app_checkout t2 on t1.checkout_date=t2.checkout_date and t1.mdd=t2.mdd and t1.user_type=t2.user_type
left join c_order_info_checkout t3 on t1.checkout_date=t3.checkout_date and t1.mdd=t3.mdd and t1.user_type=t3.user_type
order by t1.checkout_date ,case when mdd = '香港'  then 1
           when mdd = '澳门'  then 2
           when mdd = '泰国'  then 3
           when mdd = '日本'  then 4
           when mdd = '韩国'  then 5
           when mdd = '马来西亚'  then 6
           when mdd = '新加坡'  then 7
           when mdd = '美国'  then 8
           when mdd = '印度尼西亚'  then 9
           when mdd = '俄罗斯'  then 10
           when mdd = '欧洲'  then 11
           when mdd = '亚太'  then 12
           when mdd = '美洲'  then 13
           when mdd = '其他'  then 14
           when mdd = 'ALL'  then 0
      end asc
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when  user_type = '老客' then 3 end asc
;