with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= date_sub(current_date, 30)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after_new+COALESCE(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
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
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度 全端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after_new+COALESCE(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
/**************** c相关数据 ****************/ 
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
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
,c_uv as
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= date_sub(current_date, 30) and dt<= date_sub(current_date, 1)
    group by 1,2,3
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
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
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= date_sub(current_date, 30)
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)


select t1.dt
       ,t1.DAU     
       ,t1.newDAU 
       ,t1.oldDAU
       ---- 全端
       ,t2.q_room_nights `Q_间夜量` 
       ,t2.q_yj  `Q佣金`  
       ,t2.q_gmv  `Q_GMV` 
       ,t2.q_orders `Q_订单量` 
       ---- QC对比
       ,t3.q_room_nights_app / t5.c_room_nights_app as `间夜QC`
       ,t1.DAU  / t4.C_DAU as `流量QC`
       ,(t3.q_orders_app / t1.DAU)  / (t5.c_orders_app / t4.C_DAU) as `转化QC`
       ,(t3.q_room_nights_app / t3.q_orders_app)  / (t5.c_room_nights_app / t5.c_orders_app)  as `单间夜QC`
       ,(t3.q_yj_app)  / (t5.c_yj_app) as `收益QC`

       ,t3.q_yj_app / t3.q_gmv_app as `Q_佣金率_app`
       ,t5.c_yj_app / t5.c_gmv_app as  `C_佣金率`
       ,(t3.q_yj_app / t3.q_gmv_app) - (t5.c_yj_app / t5.c_gmv_app) as  `收益率QC差`
       ,(t3.q_yj_byc_app / t3.q_gmv_app) - (t5.c_yj_app / t5.c_gmv_app) as  `收益率QC差(C视角)`
       ,t3.q_qe_app / t3.q_gmv_app as `Q_补贴率_app`
       ,t5.c_qe_app / t5.c_gmv_app as  `C_补贴率`
       ,(t3.q_qe_app / t3.q_gmv_app) - (t5.c_qe_app / t5.c_gmv_app) as  `券补贴率QC差`
       ,t3.q_gmv_app / t3.q_room_nights_app as `Q_ADR_app`
       ,t5.c_gmv_app / t5.c_room_nights_app as  `C_ADR`
       ,(t3.q_gmv_app / t3.q_room_nights_app) / (t5.c_gmv_app / t5.c_room_nights_app) as `ADR_QC`
       ,t3.q_room_nights_app as `Q_间夜量_app`
       ,t5.c_room_nights_app as `C_间夜量`
       ,t3.q_yj_app as  `Q佣金_app` 
       ,t5.c_yj_app  as  `C佣金` 
       ,t3.q_gmv_app as `Q_GMV_app`  
       ,t5.c_gmv_app as  `C_GMV` 
       ,t4.C_DAU
       ,t4.newC_DAU
       ,t4.oldC_DAU

       ---- 全端
       ,t2.q_orders / t1.DAU as  `Q_转化CR` 
       ,t2.q_room_nights / t2.q_orders as `Q_单间夜` 
       ,t2.q_yj / t2.q_gmv as `Q_佣金率` 
       ,t2.q_qe / t2.q_gmv as `Q_补贴率`
       ,t2.q_gmv / t2.q_room_nights as `Q_ADR`
       ,t2.q_order_uv          `Q_下单用户`
       ,t2.q_room_night_high   `Q_高星间夜量`
       ,t2.q_room_night_middle `Q_中星间夜量`
       ,t2.q_room_night_low    `Q_低星间夜量`
       ,t2.q_yj_byc  `Q佣金C视角`
       ,t2.q_qe   `Q券额`
       ,t2.q_order_use_conpon `Q用券订单量`
       ----- APPQC对比
       ,t3.q_orders_app `Q_订单量_app`
       ,t5.c_orders_app `C_订单量`
       ,t3.q_orders_app / t1.DAU as `Q_转化CR_app` 
       ,t5.c_orders_app / t4.C_DAU as  `C_转化CR`
       ,t3.q_room_nights_app / t3.q_orders_app as `Q_单间夜_app`
       ,t5.c_room_nights_app / t5.c_orders_app as  `C_单间夜`
       ,t3.q_order_uv_app   `Q_下单用户_app`
       ,t5.c_order_uv_app   `C_下单用户`
       ,t3.q_room_night_high_app   `Q_高星间夜量_app`
       ,t5.c_room_night_high_app `C_高星间夜量`
       ,t3.q_room_night_middle_app `Q_中星间夜量_app`
       ,t5.c_room_night_middle_app `C_中星间夜量`
       ,t3.q_room_night_low_app    `Q_低星间夜量_app`
       ,t5.c_room_night_low_app `C_低星间夜量`
       ,t3.q_qe_app  `Q券额_app`
       ,t5.c_qe_app  `C_券额`

       ,t3.q_yj_byc_app   `Q佣金C视角_app`
       ,t3.q_order_use_conpon_app `Q用券订单量_app`
from (-- Q流量
    select dt
        ,count(user_id) DAU
        ,count(case when user_type = '新客' then user_id end) newDAU
        ,count(case when user_type != '新客' then user_id end) oldDAU
    from uv
    group by 1
)t1
left join (-- Q订单 全端
    select order_date
            ,sum(final_commission_after) as q_yj
            ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_yj_byc
            ,sum(init_gmv) as q_gmv
            ,sum(coupon_substract_summary) as q_qe
            ,count(distinct order_no) as q_orders
            ,count(distinct user_id) as q_order_uv
            ,sum(room_night) as q_room_nights
            ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_order_use_conpon
            ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_room_night_high
            ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_room_night_middle
            ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_room_night_low
    from q_order
    group by 1
)t2 on t1.dt=t2.order_date
left join (-- Q订单 APP端
    select order_date
            ,sum(final_commission_after) as q_yj_app
            ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_yj_byc_app
            ,sum(init_gmv) as q_gmv_app
            ,sum(coupon_substract_summary) as q_qe_app
            ,count(distinct order_no) as q_orders_app
            ,count(distinct user_id) as q_order_uv_app
            ,sum(room_night) as q_room_nights_app
            ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_order_use_conpon_app
            ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_room_night_high_app
            ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_room_night_middle_app
            ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_room_night_low_app
    from q_app_order
    group by 1
)t3 on t1.dt=t3.order_date
left join (-- C流量
    select dt
            ,sum(c_uv) as C_DAU
            ,sum(case when user_type = '新客' then c_uv end) newC_DAU
            ,sum(case when user_type != '新客' then c_uv end) oldC_DAU
    from c_uv
    group by 1
)t4 on t1.dt=t4.dt
left join (-- C订单 APP端
    select dt
            ,sum(comission) as c_yj_app
            ,sum(room_fee) as c_gmv_app
            ,sum(cqe) as c_qe_app
            ,count(distinct order_no) as c_orders_app
            ,count(distinct user_id) as c_order_uv_app
            ,sum(room_night) as c_room_nights_app
            ,sum(case when star in (4,5) then room_night else 0 end ) as c_room_night_high_app
            ,sum(case when star in (3) then room_night else 0 end ) as c_room_night_middle_app
            ,sum(case when star not in (3,4,5) then room_night else 0 end ) as c_room_night_low_app
    from c_order
    group by 1
)t5 on t1.dt=t5.dt
order by t1.dt 
;




/************************** 分目的地 **************************/
with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= date_sub(current_date, 60)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after_new+COALESCE(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
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
        and order_date >= date_sub(current_date, 60) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度 全端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after_new+COALESCE(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 60) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
/**************** c相关数据 ****************/ 
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
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
,c_uv as
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= date_sub(current_date, 60) and dt<= date_sub(current_date,1)
    group by 1,2,3
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
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
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= date_sub(current_date, 60)
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)


select t1.dt,t1.mdd
       ,t1.DAU     
       ,t1.newDAU 
       ,t1.oldDAU
       ---- 全端
       ,t2.q_room_nights `Q_间夜量` 
       ,t2.q_yj  `Q佣金`  
       ,t2.q_gmv  `Q_GMV` 
       ,t2.q_orders `Q_订单量` 
       ---- QC对比
       ,t3.q_room_nights_app / t5.c_room_nights_app as `间夜QC`
       ,t1.DAU  / t4.C_DAU as `流量QC`
       ,(t3.q_orders_app / t1.DAU)  / (t5.c_orders_app / t4.C_DAU) as `转化QC`
       ,(t3.q_room_nights_app / t3.q_orders_app)  / (t5.c_room_nights_app / t5.c_orders_app)  as `单间夜QC`
       ,(t3.q_yj_app)  / (t5.c_yj_app) as `收益QC`

       ,t3.q_yj_app / t3.q_gmv_app as `Q_佣金率_app`
       ,t5.c_yj_app / t5.c_gmv_app as  `C_佣金率`
       ,(t3.q_yj_app / t3.q_gmv_app) - (t5.c_yj_app / t5.c_gmv_app) as  `收益率QC差`
       ,(t3.q_yj_byc_app / t3.q_gmv_app) - (t5.c_yj_app / t5.c_gmv_app) as  `收益率QC差(C视角)`
       ,t3.q_qe_app / t3.q_gmv_app as `Q_补贴率_app`
       ,t5.c_qe_app / t5.c_gmv_app as  `C_补贴率`
       ,(t3.q_qe_app / t3.q_gmv_app) - (t5.c_qe_app / t5.c_gmv_app) as  `券补贴率QC差`
       ,t3.q_gmv_app / t3.q_room_nights_app as `Q_ADR_app`
       ,t5.c_gmv_app / t5.c_room_nights_app as  `C_ADR`
       ,(t3.q_gmv_app / t3.q_room_nights_app) / (t5.c_gmv_app / t5.c_room_nights_app) as `ADR_QC`
       ,t3.q_room_nights_app as `Q_间夜量_app`
       ,t5.c_room_nights_app as `C_间夜量`
       ,t3.q_yj_app as  `Q佣金_app` 
       ,t5.c_yj_app  as  `C佣金` 
       ,t3.q_gmv_app as `Q_GMV_app`  
       ,t5.c_gmv_app as  `C_GMV` 
       ,t4.C_DAU
       ,t4.newC_DAU
       ,t4.oldC_DAU

       ---- 全端
       ,t2.q_orders / t1.DAU as  `Q_转化CR` 
       ,t2.q_room_nights / t2.q_orders as `Q_单间夜` 
       ,t2.q_yj / t2.q_gmv as `Q_佣金率` 
       ,t2.q_qe / t2.q_gmv as `Q_补贴率`
       ,t2.q_gmv / t2.q_room_nights as `Q_ADR`
       ,t2.q_order_uv          `Q_下单用户`
       ,t2.q_room_night_high   `Q_高星间夜量`
       ,t2.q_room_night_middle `Q_中星间夜量`
       ,t2.q_room_night_low    `Q_低星间夜量`
       ,t2.q_yj_byc  `Q佣金C视角`
       ,t2.q_qe   `Q券额`
       ,t2.q_order_use_conpon `Q用券订单量`
       ----- APPQC对比
       ,t3.q_orders_app `Q_订单量_app`
       ,t5.c_orders_app `C_订单量`
       ,t3.q_orders_app / t1.DAU as `Q_转化CR_app` 
       ,t5.c_orders_app / t4.C_DAU as  `C_转化CR`
       ,t3.q_room_nights_app / t3.q_orders_app as `Q_单间夜_app`
       ,t5.c_room_nights_app / t5.c_orders_app as  `C_单间夜`
       ,t3.q_order_uv_app   `Q_下单用户_app`
       ,t5.c_order_uv_app   `C_下单用户`
       ,t3.q_room_night_high_app   `Q_高星间夜量_app`
       ,t5.c_room_night_high_app `C_高星间夜量`
       ,t3.q_room_night_middle_app `Q_中星间夜量_app`
       ,t5.c_room_night_middle_app `C_中星间夜量`
       ,t3.q_room_night_low_app    `Q_低星间夜量_app`
       ,t5.c_room_night_low_app `C_低星间夜量`
       ,t3.q_qe_app  `Q券额_app`
       ,t5.c_qe_app  `C_券额`

       ,t3.q_yj_byc_app   `Q佣金C视角_app`
       ,t3.q_order_use_conpon_app `Q用券订单量_app`
from (-- Q流量
    select dt,mdd
        ,count(user_id) DAU
        ,count(case when user_type = '新客' then user_id end) newDAU
        ,count(case when user_type != '新客' then user_id end) oldDAU
    from uv
    group by 1,2
)t1
left join (-- Q订单 全端
    select order_date,mdd
            ,sum(final_commission_after) as q_yj
            ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_yj_byc
            ,sum(init_gmv) as q_gmv
            ,sum(coupon_substract_summary) as q_qe
            ,count(distinct order_no) as q_orders
            ,count(distinct user_id) as q_order_uv
            ,sum(room_night) as q_room_nights
            ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_order_use_conpon
            ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_room_night_high
            ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_room_night_middle
            ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_room_night_low
    from q_order
    group by 1,2
)t2 on t1.dt=t2.order_date and t1.mdd=t2.mdd
left join (-- Q订单 APP端
    select order_date,mdd
            ,sum(final_commission_after) as q_yj_app
            ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_yj_byc_app
            ,sum(init_gmv) as q_gmv_app
            ,sum(coupon_substract_summary) as q_qe_app
            ,count(distinct order_no) as q_orders_app
            ,count(distinct user_id) as q_order_uv_app
            ,sum(room_night) as q_room_nights_app
            ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_order_use_conpon_app
            ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_room_night_high_app
            ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_room_night_middle_app
            ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_room_night_low_app
    from q_app_order
    group by 1,2
)t3 on t1.dt=t3.order_date and t1.mdd=t3.mdd
left join (-- C流量
    select dt,mdd
            ,sum(c_uv) as C_DAU
            ,sum(case when user_type = '新客' then c_uv end) newC_DAU
            ,sum(case when user_type != '新客' then c_uv end) oldC_DAU
    from c_uv
    group by 1,2
)t4 on t1.dt=t4.dt and t1.mdd=t4.mdd
left join (-- C订单 APP端
    select dt,mdd
            ,sum(comission) as c_yj_app
            ,sum(room_fee) as c_gmv_app
            ,sum(cqe) as c_qe_app
            ,count(distinct order_no) as c_orders_app
            ,count(distinct user_id) as c_order_uv_app
            ,sum(room_night) as c_room_nights_app
            ,sum(case when star in (4,5) then room_night else 0 end ) as c_room_night_high_app
            ,sum(case when star in (3) then room_night else 0 end ) as c_room_night_middle_app
            ,sum(case when star not in (3,4,5) then room_night else 0 end ) as c_room_night_low_app
    from c_order
    group by 1,2
)t5 on t1.dt=t5.dt and t1.mdd=t5.mdd
order by t1.dt 
;