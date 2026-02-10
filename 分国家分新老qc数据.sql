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

,q_uv_info as
(   ---- 流量汇总
    select dt
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,count(user_id)   uv
    from uv
    group by dt,cube(user_type, mdd)
) 

,q_order_app as (----订单明细表表包含取消  分目的地、新老维度 app
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
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after_new+coalesce(ext_plat_certificate,0) end as qyj  --- Q佣金
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
,q_order as (----订单明细表表包含取消  分目的地、新老维度 
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
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after_new+coalesce(ext_plat_certificate,0) end as qyj  --- Q佣金
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
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info_app as ( --- q app 订单汇总
    select t1.order_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金_app`
         ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as `Q_佣金（C视角）_app`
         ,sum(init_gmv) as `Q_GMV_app`
         ,sum(coupon_substract_summary) as `Q_券额_app`
         ,count(distinct order_no) as `Q_订单量_app`
         ,count(distinct t1.user_id) as `Q_下单用户_app`
         ,sum(room_night) as `Q_间夜量_app`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量_app`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量_app`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量_app`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量_app`
    from q_order_app t1
    group by t1.order_date,cube(t1.mdd,t1.user_type)
)
,order_info as ( --- q 订单汇总
    select t1.order_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金`
         ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as `Q_佣金（C视角）`
         ,sum(init_gmv) as `Q_GMV`
         ,sum(coupon_substract_summary) as `Q_券额`
         ,count(distinct order_no) as `Q_订单量`
         ,count(distinct t1.user_id) as `Q_下单用户`
         ,sum(room_night) as `Q_间夜量`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from q_order t1
    group by t1.order_date,cube(t1.mdd,t1.user_type)
)

/******************************** Q分区域分新老分渠道结果数据 ********************************/ 
,q_data_info as (
    select t1.dt
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.uv, 0)   as uv
            ,coalesce(t2.`Q_间夜量`, 0) as `Q_间夜量`
            ,coalesce(t2.`Q_订单量`, 0)  as `Q_订单量`
            ,coalesce(t2.`Q_下单用户`, 0)   as `Q_下单用户`
            ,coalesce(t2.`Q_GMV`, 0)      as `Q_GMV`
            ,coalesce(t2.`Q_佣金`, 0)      as `Q_佣金`
            ,coalesce(t2.`Q_券额`, 0)      as `Q_券额`
            ,coalesce(t2.`Q_高星间夜量`, 0)      as `Q_高星间夜量`
            ,coalesce(t2.`Q_中星间夜量`, 0)      as `Q_中星间夜量`
            ,coalesce(t2.`Q_低星间夜量`, 0)      as `Q_低星间夜量`
            ,concat(round(coalesce(t1.uv / t3.uv, 0) * 100, 2), '%')   as `Q_流量占比`
            ,coalesce(t2.`Q_订单量` / t1.uv, 0)  as `Q_CR`
            ,coalesce(t2.`Q_间夜量`, 0) / coalesce(t2.`Q_订单量`, 0)  as `Q_单间夜`
            ,coalesce(t2.`Q_佣金`, 0) / coalesce(t2.`Q_GMV`, 0)  as `Q_收益率`
            ,coalesce(t2.`Q_券额`, 0) / coalesce(t2.`Q_GMV`, 0)  as `Q_券补贴率`
            ,coalesce(t2.`Q_GMV`, 0) / coalesce(t2.`Q_间夜量`, 0)  as `Q_ADR`
            ,concat(round(coalesce(t2.`Q_用券订单量`, 0) / coalesce(t2.`Q_订单量`, 0) * 100, 1), '%') as `Q_用券订单占比`

            ,coalesce(t4.`Q_间夜量_app`, 0)  as `Q_间夜量_app`
            ,coalesce(t4.`Q_订单量_app`, 0)  as `Q_订单量_app`
            ,coalesce(t4.`Q_下单用户_app`, 0) as `Q_下单用户_app`
            ,coalesce(t4.`Q_GMV_app`, 0)      as `Q_GMV_app`
            ,coalesce(t4.`Q_佣金_app`, 0)      as `Q_佣金_app`
            ,coalesce(t4.`Q_券额_app`, 0)      as `Q_券额_app`
            ,coalesce(t4.`Q_高星间夜量_app`, 0)      as `Q_高星间夜量_app`
            ,coalesce(t4.`Q_中星间夜量_app`, 0)      as `Q_中星间夜量_app`
            ,coalesce(t4.`Q_低星间夜量_app`, 0)      as `Q_低星间夜量_app`
            ,coalesce(t4.`Q_订单量_app` / t1.uv, 0)  as `Q_CR_app`
            ,coalesce(t4.`Q_间夜量_app`, 0) / coalesce(t4.`Q_订单量_app`, 0) as `Q_单间夜_app`
            ,coalesce(t4.`Q_佣金_app`, 0)  /  coalesce(t4.`Q_GMV_app`, 0)   as `Q_收益率_app`
            ,coalesce(t4.`Q_券额_app`, 0)  /  coalesce(t4.`Q_GMV_app`, 0)   as `Q_券补贴率_app`
            ,coalesce(t4.`Q_GMV_app`, 0)  /  coalesce(t4.`Q_间夜量_app`, 0) as `Q_ADR_app`
            ,concat(round(coalesce(t4.`Q_用券订单量_app`, 0) / coalesce(t4.`Q_订单量_app`, 0) * 100, 1), '%') as `Q_用券订单占比_app`
    from q_uv_info t1 
    left join order_info t2 on t1.dt=t2.order_date and t1.mdd=t2.mdd 
    and t1.user_type=t2.user_type 
    left join order_info_app t4 on t1.dt=t4.order_date and t1.mdd=t4.mdd 
    and t1.user_type=t4.user_type 
    left join (  --- 计算流量占比
        select dt,uv
        from q_uv_info 
        where user_type = 'ALL' and mdd='ALL'
    ) t3 on t1.dt=t3.dt 
)

/**************************************** c相关数据 ****************************************/ 
,c_user_type as
(   --- 用于判定c新老客
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
,c_uv as
(   --- C 流量 目的地加和
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
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= date_sub(current_date, 30)
      and substr(order_date,1,10) <= date_sub(current_date, 1)

)
,c_uv_info as(  ---- c流量汇总
    select dt
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,sum(c_uv) as c_uv
    from c_uv
    group by dt,cube(user_type, mdd)
)
,c_order_info as(  ---- c订单汇总
    select dt
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,count(order_no) as `C_订单量`
           ,sum(room_night) as `C_间夜量`
           ,sum(room_fee)as `C_GMV`
           ,sum(comission) as `C_佣金`
           ,sum(cqe) as `C_券额`
           ,count(distinct user_id)  `C_下单用户`
           ,sum(case when star in (4,5) then room_night else 0 end) as `C_高星间夜量`
           ,sum(case when star in (3) then room_night else 0 end) as `C_中星间夜量`
           ,sum(case when star not in (3,4,5) then room_night else 0 end) as `C_低星间夜量`
    from c_order
    group by dt,cube(user_type, mdd)
)
/******************************** C分区域分新老结果数据 ********************************/ 
,c_data_info as (
select t1.dt   
        ,t1.mdd
        ,t1.user_type  
        ,coalesce(t1.c_uv, 0)   as c_uv
        ,coalesce(t2.`C_间夜量`, 0) as `C_间夜量`
        ,coalesce(t2.`C_订单量`, 0)  as `C_订单量`
        ,coalesce(t2.`C_下单用户`, 0)   as `C_下单用户`
        ,coalesce(t2.`C_GMV`, 0)      as `C_GMV`
        ,coalesce(t2.`C_佣金`, 0)      as `C_佣金`
        ,coalesce(t2.`C_券额`, 0)      as `C_券额`
        ,coalesce(t2.`C_高星间夜量`, 0)      as `C_高星间夜量`
        ,coalesce(t2.`C_中星间夜量`, 0)      as `C_中星间夜量`
        ,coalesce(t2.`C_低星间夜量`, 0)      as `C_低星间夜量`
        ,coalesce(t2.`C_订单量` / t1.c_uv, 0)  as `C_CR`
        ,coalesce(t2.`C_间夜量`, 0) / coalesce(t2.`C_订单量`, 0)  as `C_单间夜`
        ,coalesce(t2.`C_佣金`, 0) / coalesce(t2.`C_GMV`, 0)  as `C_收益率`
        ,coalesce(t2.`C_券额`, 0) / coalesce(t2.`C_GMV`, 0)  as `C_券补贴率`
        ,coalesce(t2.`C_GMV`, 0) / coalesce(t2.`C_间夜量`, 0)  as `C_ADR`
from c_uv_info t1 
left join c_order_info t2 on t1.dt=t2.dt and t1.mdd=t2.mdd 
and t1.user_type=t2.user_type
)

/******************************** QC分区域分新老结果数据 ********************************/ 
select t1.dt
        ,t1.mdd
        ,t1.user_type
        ,concat(round(`Q_间夜量_app` / `C_间夜量` *100, 2), '%') as `间夜QC`
        ,concat(round(uv / c_uv *100, 2), '%')  as `流量QC`
        ,concat(round(`Q_CR_app` / `C_CR` *100, 2), '%') as `转化QC`
        ,concat(round(`Q_佣金_app` / `C_佣金` *100, 2), '%') as `收益QC`
        ,concat(round(`Q_收益率_app` / `C_收益率` *100, 2), '%')  as `收益率QC`
        ,concat(round(`Q_券补贴率_app` / `C_券补贴率` *100, 2), '%')   as `券补贴率QC`
        ,concat(round(`Q_ADR_app` / `C_ADR` *100, 2), '%')   as `ADR_QC`
        ,concat(round(`Q_订单量_app` / `C_订单量` *100, 2), '%')  as `订单量QC`
        ,concat(round(`Q_单间夜_app` / `C_单间夜` *100, 2), '%') as `单间夜QC`


        ,uv
        ,`Q_流量占比`
        ,`Q_间夜量`
        ,`Q_订单量`
        ,`Q_下单用户`
        ,`Q_GMV`
        ,`Q_佣金`
        ,`Q_券额`
        ,concat(round(`Q_CR` * 100, 2), '%') `Q_CR`
        ,concat(round(`Q_收益率` * 100, 2), '%')  `Q_收益率`  --佣金率
        ,concat(round(`Q_券补贴率` * 100, 2), '%') `Q_券补贴率`
        ,`Q_ADR`
        ,`Q_用券订单占比`
        ,`Q_高星间夜量`
        ,`Q_中星间夜量`
        ,`Q_低星间夜量`
        ,`Q_单间夜`
        
        ,c_uv
        ,`C_间夜量`
        ,`C_订单量`
        ,`C_下单用户`
        ,`C_GMV`
        ,`C_佣金`
        ,`C_券额`
        ,concat(round(`C_CR` * 100, 2), '%') `C_CR`
        ,concat(round(`C_收益率` * 100, 2), '%')  `C_收益率`  --佣金率
        ,concat(round(`C_券补贴率` * 100, 2), '%') `C_券补贴率`
        ,`C_ADR`

        ,`C_中星间夜量`
        ,`C_低星间夜量`
        ,`C_单间夜`
from (
    select dt, mdd,user_type,uv
           ,`Q_间夜量`
           ,`Q_订单量`
           ,`Q_下单用户`
           ,`Q_GMV`
           ,`Q_佣金`
           ,`Q_券额`
           ,`Q_高星间夜量`
           ,`Q_中星间夜量`
           ,`Q_低星间夜量`
           ,`Q_流量占比`
           ,`Q_CR`
           ,`Q_单间夜`
           ,`Q_收益率`
           ,`Q_券补贴率`
           ,`Q_ADR`
           ,`Q_用券订单占比`

           ,`Q_间夜量_app`
           ,`Q_订单量_app`
           ,`Q_下单用户_app`
           ,`Q_GMV_app`
           ,`Q_佣金_app`
           ,`Q_券额_app`
           ,`Q_高星间夜量_app`
           ,`Q_中星间夜量_app`
           ,`Q_低星间夜量_app`
           ,`Q_CR_app`
           ,`Q_单间夜_app`
           ,`Q_收益率_app`
           ,`Q_券补贴率_app`
           ,`Q_ADR_app`
           ,`Q_用券订单占比_app`

    from q_data_info
) t1 left join c_data_info t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by t1.dt ,case when mdd = '香港'  then 1
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
      end asc,case when user_type = 'ALL' then 1 when user_type = '新客' then 2 when  user_type = '老客' then 3 end asc
;


