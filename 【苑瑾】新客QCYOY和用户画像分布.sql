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
     where dt >= '2024-01-01'
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
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
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
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
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
            ,concat(round(coalesce(t4.`Q_间夜量_app` / t5.`Q_间夜量_app`, 0) * 100, 2), '%')   as `Q_间夜占比_app`
            ,concat(round(coalesce(t4.`Q_订单量_app` / t5.`Q_订单量_app`, 0) * 100, 2), '%')   as `Q_订单量占比_app`
            ,concat(round(coalesce(t4.`Q_GMV_app` /   t5.`Q_GMV_app`, 0) * 100, 2), '%')   as `Q_GMV占比_app`
            ,concat(round(coalesce(t4.`Q_佣金_app` /   t5.`Q_佣金_app`, 0) * 100, 2), '%')   as `Q_佣金占比_app`
            ,concat(round(coalesce(t4.`Q_券额_app` /   t5.`Q_券额_app`, 0) * 100, 2), '%')   as `Q_券额占比_app`

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
    left join (  --- 计算订单占比
        select order_date,`Q_佣金_app`,`Q_GMV_app`,`Q_订单量_app`,`Q_券额_app`,`Q_间夜量_app`
        from order_info_app 
        where user_type = 'ALL' and mdd='ALL'
    ) t5 on t1.dt=t5.order_date 
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
    and  dt>= '2024-01-01' and dt<= date_sub(current_date, 1)
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
      and substr(order_date,1,10) >= '2024-01-01'
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
        ,concat(round((`Q_收益率_app` - `C_收益率`) *100, 2), '%')  as `收益率QC差`
        ,concat(round((`Q_券补贴率_app` - `C_券补贴率`) *100, 2), '%')  as `券补贴率QC差`
        ,concat(round(`Q_ADR_app` / `C_ADR` *100, 2), '%')   as `ADR_QC`
        ,concat(round(`Q_订单量_app` / `C_订单量` *100, 2), '%')  as `订单量QC`
        ,concat(round(`Q_单间夜_app` / `C_单间夜` *100, 2), '%') as `单间夜QC`


        ,uv
        ,`Q_流量占比`
        ,`Q_间夜占比_app`
        ,`Q_订单量占比_app`
        ,`Q_GMV占比_app`
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
           ,`Q_间夜占比_app`
           ,`Q_订单量占比_app`
           ,`Q_GMV占比_app`
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


---- 用户画像
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
     where dt >= '2024-08-01'
       and dt <= '2024-11-03'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
     
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
        and order_date >= '2024-08-01' and order_date <= '2024-11-03'
        and order_no <> '103576132435'
)
,user_profile as (
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)
,order_result as (
    select user_id,user_type,order_no,gender,city_name,prov_name,city_level
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.order_no,user_type
            ,o.user_id
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('20241103', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from q_order o
        join user_profile u on u.user_id = o.user_id
    )
)

,uv_result as (
    select user_id,user_type,gender,city_name,prov_name,city_level
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.user_id,user_type
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from uv o
        join user_profile u on u.user_id = o.user_id
    )
)


select t1.city_lev,t1.age_level,t1.gender,o_uv,f_uv
from (
    select city_lev,age_level,gender,count(distinct user_id) o_uv
    from order_result
    where user_type = '新客'
    group by 1,2,3
) t1 left join (
    select city_lev,age_level,gender,count(distinct user_id) f_uv
    from uv_result
    where user_type = '新客'
    group by 1,2,3
) t2 on t1.city_lev=t2.city_lev and t1.age_level=t2.age_level and t1.gender=t2.gender
;


---- 24年春节离店用户画像
with user_type as
(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '20250205'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when order_date >= '2025-01-28' and order_date <= '2025-02-04' then '春节期间预定'
                  when datediff('2025-01-28', order_date) between 1 and 3 then '提前订1-3天'
                  when datediff('2025-01-28', order_date) between 4 and 7 then '提前订4-7天'
                  when datediff('2025-01-28', order_date) between 8 and 14 then '提前订8-14天'
                  when datediff('2025-01-28', order_date) between 15 and 30 then '提前订15-30天'
                  when datediff('2025-01-28', order_date) between 31 and 60 then '提前订31-60天'
                  else '提前订61天+' end per_type
     
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '20250205'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 24年春节
        and order_date <= '2025-02-04'
        and order_no <> '103576132435'
 
)
,user_profile as (
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)
,order_result as (
    select user_id,user_type,order_no,gender,city_name,prov_name,city_level,room_night,per_type,mdd
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.order_no,user_type,room_night,per_type,mdd
            ,o.user_id
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from q_order o
        left join user_profile u on u.user_id = o.user_id
    )
)

select mdd,sum(room_night) room_night
      ,sum(sum(room_night)) over() all
      ,sum(room_night) / sum(sum(room_night)) over() rate
     
from order_result
where user_type = '新客'
group by 1
;


select city_lev,city_level,sum(room_night) room_night
      ,sum(case when per_type != '春节期间预定' then room_night end) `提前订间夜`
      ,sum(case when per_type = '提前订1-3天' then room_night end) `提前订1-3天`
      ,sum(case when per_type = '提前订4-7天' then room_night end) `提前订4-7天`
      ,sum(case when per_type = '提前订8-14天' then room_night end) `提前订8-14天`
      ,sum(case when per_type = '提前订15-30天' then room_night end) `提前订15-30天`
      ,sum(case when per_type = '提前订31-60天' then room_night end) `提前订31-60天`
      ,sum(case when per_type = '提前订61天+' then room_night end)   `提前订61天+`
      ,sum(case when age_level = '年轻' then room_night end)   `年轻`
      ,sum(case when age_level = '成熟' then room_night end)   `成熟`
      ,sum(case when age_level = '中老年' then room_night end)   `中老年`
from order_result
where user_type = '新客' and city_lev != '未知'
group by 1,2
;



---- 24和25年年轻新客目的地变化情况
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
     where dt >= '2024-08-01'
    --    and dt <= '2024-11-03'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,user_profile as (
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)

,uv_result as (
    select user_id,user_type,gender,city_name,prov_name,city_level,mdd,dt
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.user_id,user_type,mdd,dt
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from uv o
        join user_profile u on u.user_id = o.user_id
    )
)

select t1.city_lev
      ,t1.mdd
      ,f_uv_24
      ,f_uv_25
      ,per_24
      ,per_25
from (
    select city_lev,mdd
        ,count(distinct user_id) f_uv_24
        ,sum(count(distinct user_id)) over(partition by city_lev) cnt_24
        ,count(distinct user_id)  / sum(count(distinct user_id)) over(partition by city_lev) per_24
    from uv_result
    where user_type = '新客' and age_level = '年轻' and city_lev != '未知'
        and dt between '2024-08-01' and '2024-11-03'
    group by 1,2
) t1 
left join (
    select city_lev,mdd
        ,count(distinct user_id) f_uv_25
        ,sum(count(distinct user_id)) over(partition by city_lev) cnt_25
        ,count(distinct user_id)  / sum(count(distinct user_id)) over(partition by city_lev) per_25
    from uv_result
    where user_type = '新客' and age_level = '年轻' and city_lev != '未知'
        and dt between '2025-08-01' and '2025-11-03'
    group by 1,2
)t2 on t1.city_lev=t2.city_lev and t1.mdd=t2.mdd

order by city_lev desc,f_uv_24 desc
;



--- 前往目的地是日本、马来、香港的新客 X 客源地（市省）
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
,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
     
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
        and order_date >= '2024-01-01' 
        -- and order_date <= '2024-10-31'
        and order_no <> '103576132435'
)
,user_profile as (
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)
,order_result as (
    select user_id,user_type,order_no,gender,city_name,prov_name,city_level,order_date,mdd,room_night
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.order_no,user_type,order_date,mdd,room_night
            ,o.user_id
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('20251107', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from q_order o
        join user_profile u on u.user_id = o.user_id
    )
)

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

,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,user_cityname
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
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
      and substr(order_date,1,10) >= '2024-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)

)


---- 省份年度
select  t1.prov_name,t1.mdd
        ,q_room_night
        ,q_all,q_rate
        ,c_room_night
        ,q_room_night / c_room_night room_night_qc
        ,c_all,c_rate
        ,q_rn,c_rn
from (
    select a.prov_name
          ,a.mdd
          ,a.q_room_night
          ,b.q_room_night  q_all
          ,a.q_room_night / b.q_room_night q_rate
          ,row_number() over(partition by a.mdd order by a.q_room_night desc) q_rn
    from (
        select prov_name
            ,mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2
    )a
    left join (
        select mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1
    )b on  a.mdd=b.mdd
) t1 
left join (
    select prov_name,mdd
          ,c_room_night,sum(c_room_night) over(partition by mdd) c_all
          ,c_room_night / sum(c_room_night) over(partition by mdd) c_rate
          ,row_number() over(partition by mdd order by c_room_night desc) c_rn
    from (
    select prov_name,mdd
          ,sum(room_night) c_room_night
    from (
        select substr(dt,1,7) mth
            ,user_cityname
            ,mdd,order_no,room_night,prov_name
        from c_order t1
        left join (select prov_name,city_name from order_result group by 1,2) t2 on t1.user_cityname=t2.city_name
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
    )a 
    group by 1,2
    )
) t2 on  t1.prov_name=t2.prov_name and t1.mdd=t2.mdd
;


----- 城市Top30 年度
select  t1.city_name,t1.mdd,q_room_night
        ,q_all,q_rate
        ,c_room_night
        ,q_room_night / c_room_night room_night_qc
        ,c_all,c_rate
        ,q_rn
        ,c_rn
from (
    select a.city_name,a.mdd,a.q_room_night
          ,b.q_room_night q_all
          ,a.q_room_night / b.q_room_night q_rate
          ,row_number() over(partition by a.mdd order by a.q_room_night desc) q_rn
    from (
        select city_name
            ,mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2
    )a left join (
        select mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1
    )b on  a.mdd=b.mdd
) t1 
left join (
    select a.user_cityname,a.mdd,a.c_room_night
          ,b.c_room_night c_all
          ,a.c_room_night / b.c_room_night  c_rate
          ,row_number() over(partition by a.mdd order by a.c_room_night desc) c_rn
    from (
        select user_cityname
            ,mdd
            ,count(distinct order_no)  c_order_no
            ,sum(room_night) c_room_night
           
        from c_order
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2
    )a left join (
        select mdd
             ,count(distinct order_no)  c_order_no
             ,sum(room_night) c_room_night
        from c_order
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1
    ) b on a.mdd=b.mdd
) t2 on t1.city_name=t2.user_cityname and t1.mdd=t2.mdd
where t2.mdd is not null 
and t1.q_rn <= 30  --- 取每个目的地Top30间夜量城市
order by 1, q_room_night desc,q_rn asc
;


---- 省份月度
select  t1.mth,t1.prov_name,t1.mdd
        ,q_room_night
        ,q_all,q_rate
        ,c_room_night
        ,q_room_night / c_room_night room_night_qc
        ,c_all,c_rate
        ,q_rn,c_rn
from (
    select a.mth
          ,a.prov_name
          ,a.mdd
          ,a.q_room_night
          ,b.q_room_night  q_all
          ,a.q_room_night / b.q_room_night q_rate
          ,row_number() over(partition by a.mth,a.mdd order by a.q_room_night desc) q_rn
    from (
        select substr(order_date,1,7) mth
            ,prov_name
            ,mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2,3
    )a
    left join (
        select substr(order_date,1,7) mth
            ,mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2
    )b on a.mth=b.mth and a.mdd=b.mdd
) t1 
left join (
    select mth,prov_name,mdd
          ,c_room_night,sum(c_room_night) over(partition by mth,mdd) c_all
          ,c_room_night / sum(c_room_night) over(partition by mth,mdd) c_rate
          ,row_number() over(partition by mth,mdd order by c_room_night desc) c_rn
    from (
    select mth,prov_name,mdd
          ,sum(room_night) c_room_night
    from (
        select substr(dt,1,7) mth
            ,user_cityname
            ,mdd,order_no,room_night
            ,case when  user_cityname in ('晋江','福清','闽侯','石狮') then '福建'
                  when  user_cityname in ('惠东') then '广东' 
                  when  user_cityname in ('三河') then '河北' 
                  when  user_cityname in ('长沙县') then '湖南' 
                  when  user_cityname in ('吉林市') then '吉林' 
                  when  user_cityname in ('江阴','常熟','张家港','丹阳','溧阳') then '江苏' 
                  when  user_cityname in ('胶州') then '山东' 
                  when  user_cityname in ('简阳') then '四川' 
                  when  user_cityname in ('大理市') then '云南' 
                  when  user_cityname in ('慈溪','海宁','嘉善','东阳','安吉','乐清','温岭','德清','永康','长兴') then '浙江' 
                  else prov_name end prov_name
        from c_order t1
        left join (select prov_name,city_name from order_result group by 1,2) t2 on t1.user_cityname=t2.city_name
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
    )a 
    group by 1,2,3
    )
) t2 on t1.mth=t2.mth and t1.prov_name=t2.prov_name and t1.mdd=t2.mdd
;



----- 城市Top30
select  t1.mth,t1.city_name,t1.mdd,q_room_night
        ,q_all,q_rate
        ,c_room_night
        ,q_room_night / c_room_night room_night_qc
        ,c_all,c_rate
        ,q_rn
        ,c_rn
from (
    select a.mth,a.city_name,a.mdd,a.q_room_night
          ,b.q_room_night q_all
          ,a.q_room_night / b.q_room_night q_rate
          ,row_number() over(partition by a.mth,a.mdd order by a.q_room_night desc) q_rn
    from (
        select substr(order_date,1,7) mth
            ,city_name
            ,mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2,3
    )a left join (
        select substr(order_date,1,7) mth
            ,mdd
            ,count(distinct order_no)  q_order_no
            ,sum(room_night) q_room_night
        from order_result
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2
    )b on a.mth=b.mth and a.mdd=b.mdd
) t1 
left join (
    select a.mth,a.user_cityname,a.mdd,a.c_room_night
          ,b.c_room_night c_all
          ,a.c_room_night / b.c_room_night  c_rate
          ,row_number() over(partition by a.mth,a.mdd order by a.c_room_night desc) c_rn
    from (
        select substr(dt,1,7) mth
            ,user_cityname
            ,mdd
            ,count(distinct order_no)  c_order_no
            ,sum(room_night) c_room_night
           
        from c_order
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2,3
    )a left join (
        select substr(dt,1,7) mth
            ,mdd
            ,count(distinct order_no)  c_order_no
            ,sum(room_night) c_room_night
        from c_order
        where user_type = '新客' and mdd in ('香港','日本','马来西亚')
        group by 1,2
    ) b on a.mth=b.mth and a.mdd=b.mdd
) t2 on t1.mth=t2.mth and t1.city_name=t2.user_cityname and t1.mdd=t2.mdd
where t2.mth is not null 
and t1.q_rn <= 30  --- 取每月每个目的地Top30间夜量城市
order by 1, q_room_night desc,q_rn asc
;




---- 锁定春节离店流量
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

select dt,sum(uv)uv
from (
select dt 
        ,case when province_name in ('澳门','香港') then province_name
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚') then a.country_name  
                when a.country_name in ('美国','阿联酋','俄罗斯','土耳其','澳大利亚','西班牙','意大利','法国','英国','德国') then '其他'
                else 'other' end as mdd
        ,case when concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type
        ,count (distinct a.user_id) as uv
    from default.dw_user_app_detail_visit_di_v3 a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 

    where dt between '20240101' and '20250204'
        -- and checkout_date between '2024-10-01' and '2024-10-07'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 24年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2
) t1 where user_type = '新客' group by 1
order by 1