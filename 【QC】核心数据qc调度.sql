with user_type as (
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----分日去重活跃用户
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
    where dt >= date_sub(current_date, 30)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_uv_info as
(   ---- 流量汇总
    select dt
        ,if(grouping(mdd)=1 ,'ALL' ,mdd) as  mdd
        ,if(grouping(user_type)=1 ,'ALL' ,user_type) as  user_type
        ,count(user_id)   uv
    from uv
    group by dt,cube(user_type, mdd)
) 
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
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
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
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
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order as (----订单明细表包含取消  分目的地、新老维度 
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
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
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
         ,sum(final_commission_after) as q_commission_app -- Q_佣金_app
         ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_commission_c_view_app -- Q_佣金（C视角）_app
         ,sum(init_gmv) as q_gmv_app -- Q_GMV_app
         ,sum(coupon_substract_summary) as q_coupon_amount_app -- Q_券额_app
         ,count(distinct order_no) as q_order_cnt_app -- Q_订单量_app
         ,count(distinct t1.user_id) as q_order_user_cnt_app -- Q_下单用户_app
         ,sum(room_night) as q_room_night_app -- Q_间夜量_app
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_coupon_order_cnt_app -- Q_用券订单量_app
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_high_star_rn_app -- Q_高星间夜量_app
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_mid_star_rn_app -- Q_中星间夜量_app
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_low_star_rn_app -- Q_低星间夜量_app
    from q_order_app t1
    group by t1.order_date,cube(t1.mdd,t1.user_type)
)
,order_info as ( --- q 订单汇总
    select t1.order_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as q_commission -- Q_佣金
         ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_commission_c_view -- Q_佣金（C视角）
         ,sum(init_gmv) as q_gmv -- Q_GMV
         ,sum(coupon_substract_summary) as q_coupon_amount -- Q_券额
         ,count(distinct order_no) as q_order_cnt -- Q_订单量
         ,count(distinct t1.user_id) as q_order_user_cnt -- Q_下单用户
         ,sum(room_night) as q_room_night -- Q_间夜量
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_coupon_order_cnt -- Q_用券订单量
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_high_star_rn -- Q_高星间夜量
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_mid_star_rn -- Q_中星间夜量
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_low_star_rn -- Q_低星间夜量
    from q_order t1
    group by t1.order_date,cube(t1.mdd,t1.user_type)
)

/******************************** 预定口径Q分区域分新老结果数据 ********************************/ 
,q_data_info as (
    select t1.dt
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.uv, 0)   as uv
            ,coalesce(t2.q_room_night, 0) as q_room_night -- Q_间夜量
            ,coalesce(t2.q_order_cnt, 0)  as q_order_cnt -- Q_订单量
            ,coalesce(t2.q_order_user_cnt, 0)   as q_order_user_cnt -- Q_下单用户
            ,coalesce(t2.q_gmv, 0)      as q_gmv -- Q_GMV
            ,coalesce(t2.q_commission, 0)      as q_commission -- Q_佣金
            ,coalesce(t2.q_coupon_amount, 0)      as q_coupon_amount -- Q_券额
            ,coalesce(t2.q_high_star_rn, 0)      as q_high_star_rn -- Q_高星间夜量
            ,coalesce(t2.q_mid_star_rn, 0)      as q_mid_star_rn -- Q_中星间夜量
            ,coalesce(t2.q_low_star_rn, 0)      as q_low_star_rn -- Q_低星间夜量
            ,coalesce(t1.uv / t3.uv, 0)   as q_traffic_rate -- Q_流量占比
            ,coalesce(t4.q_room_night_app / t5.q_room_night_app, 0)   as q_rn_rate_app -- Q_间夜占比_app
            ,coalesce(t4.q_order_cnt_app / t5.q_order_cnt_app, 0)   as q_order_cnt_rate_app -- Q_订单量占比_app
            ,coalesce(t4.q_gmv_app /   t5.q_gmv_app, 0)   as q_gmv_rate_app -- Q_GMV占比_app
            ,coalesce(t4.q_commission_app /   t5.q_commission_app, 0)   as q_commission_rate_app -- Q_佣金占比_app
            ,coalesce(t4.q_coupon_amount_app /   t5.q_coupon_amount_app, 0)   as q_coupon_amt_rate_app -- Q_券额占比_app

            ,coalesce(t2.q_room_night / t6.q_room_night, 0)   as q_rn_rate -- Q_间夜占比
            ,coalesce(t2.q_order_cnt / t6.q_order_cnt, 0)   as q_order_cnt_rate -- Q_订单量占比
            ,coalesce(t2.q_gmv /   t6.q_gmv, 0)   as q_gmv_rate -- Q_GMV占比
            ,coalesce(t2.q_commission /   t6.q_commission, 0)   as q_commission_rate -- Q_佣金占比
            ,coalesce(t2.q_coupon_amount /   t6.q_coupon_amount, 0)   as q_coupon_amt_rate -- Q_券额占比

            ,coalesce(t2.q_order_cnt / t1.uv, 0)  as q_cr -- Q_CR
            ,coalesce(t2.q_room_night, 0) / coalesce(t2.q_order_cnt, 0)  as q_avg_rn_per_order -- Q_单间夜
            ,coalesce(t2.q_commission, 0) / coalesce(t2.q_gmv, 0)  as q_take_rate -- Q_收益率
            ,coalesce(t2.q_coupon_amount, 0) / coalesce(t2.q_gmv, 0)  as q_subsidy_rate -- Q_券补贴率
            ,coalesce(t2.q_gmv, 0) / coalesce(t2.q_room_night, 0)  as q_adr -- Q_ADR
            ,coalesce(t2.q_coupon_order_cnt, 0) / coalesce(t2.q_order_cnt, 0)  as q_coupon_order_rate -- Q_用券订单占比

            ,coalesce(t4.q_room_night_app, 0)  as q_room_night_app -- Q_间夜量_app
            ,coalesce(t4.q_order_cnt_app, 0)  as q_order_cnt_app -- Q_订单量_app
            ,coalesce(t4.q_order_user_cnt_app, 0) as q_order_user_cnt_app -- Q_下单用户_app
            ,coalesce(t4.q_gmv_app, 0)      as q_gmv_app -- Q_GMV_app
            ,coalesce(t4.q_commission_app, 0)      as q_commission_app -- Q_佣金_app
            ,coalesce(t4.q_coupon_amount_app, 0)      as q_coupon_amount_app -- Q_券额_app
            ,coalesce(t4.q_high_star_rn_app, 0)      as q_high_star_rn_app -- Q_高星间夜量_app
            ,coalesce(t4.q_mid_star_rn_app, 0)      as q_mid_star_rn_app -- Q_中星间夜量_app
            ,coalesce(t4.q_low_star_rn_app, 0)      as q_low_star_rn_app -- Q_低星间夜量_app
            ,coalesce(t4.q_order_cnt_app / t1.uv, 0)  as q_cr_app -- Q_CR_app
            ,coalesce(t4.q_room_night_app, 0) / coalesce(t4.q_order_cnt_app, 0) as q_avg_rn_per_order_app -- Q_单间夜_app
            ,coalesce(t4.q_commission_app, 0)  /  coalesce(t4.q_gmv_app, 0)   as q_take_rate_app -- Q_收益率_app
            ,coalesce(t4.q_coupon_amount_app, 0)  /  coalesce(t4.q_gmv_app, 0)   as q_subsidy_rate_app -- Q_券补贴率_app
            ,coalesce(t4.q_gmv_app, 0)  /  coalesce(t4.q_room_night_app, 0) as q_adr_app -- Q_ADR_app
            ,coalesce(t4.q_coupon_order_cnt_app, 0) / coalesce(t4.q_order_cnt_app, 0)  as q_coupon_order_rate_app -- Q_用券订单占比_app
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
    left join (  --- 计算订单占比 APP
        select order_date,q_commission_app,q_gmv_app,q_order_cnt_app,q_coupon_amount_app,q_room_night_app
        from order_info_app 
        where user_type = 'ALL' and mdd='ALL'
    ) t5 on t1.dt=t5.order_date 
    left join (  --- 计算订单占比 全端
        select order_date,q_commission,q_gmv,q_order_cnt,q_coupon_amount,q_room_night
        from order_info
        where user_type = 'ALL' and mdd='ALL'
    ) t6 on t1.dt=t6.order_date 
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
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
        ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
        ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
        ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= date_sub(current_date, 30) and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            -- ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
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
           ,count(uid) as c_uv
    from c_uv
    group by dt,cube(user_type, mdd)
)
,c_order_info as(  ---- c订单汇总
    select dt
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,count(order_no) as c_order_cnt -- C_订单量
           ,sum(room_night) as c_room_night -- C_间夜量
           ,sum(room_fee)as c_gmv -- C_GMV
           ,sum(comission) as c_commission -- C_佣金
           ,sum(cqe) as c_coupon_amount -- C_券额
           ,count(distinct user_id)  c_order_user_cnt -- C_下单用户
           ,sum(case when star in (4,5) then room_night else 0 end) as c_high_star_rn -- C_高星间夜量
           ,sum(case when star in (3) then room_night else 0 end) as c_mid_star_rn -- C_中星间夜量
           ,sum(case when star not in (3,4,5) then room_night else 0 end) as c_low_star_rn -- C_低星间夜量
    from c_order
    group by dt,cube(user_type, mdd)
)
/******************************** C分区域分新老结果数据 ********************************/ 
,c_data_info as (
select t1.dt   
        ,t1.mdd
        ,t1.user_type  
        ,coalesce(t1.c_uv, 0)   as c_uv
        ,coalesce(t2.c_room_night, 0) as c_room_night -- C_间夜量
        ,coalesce(t2.c_order_cnt, 0)  as c_order_cnt -- C_订单量
        ,coalesce(t2.c_order_user_cnt, 0)   as c_order_user_cnt -- C_下单用户
        ,coalesce(t2.c_gmv, 0)      as c_gmv -- C_GMV
        ,coalesce(t2.c_commission, 0)      as c_commission -- C_佣金
        ,coalesce(t2.c_coupon_amount, 0)      as c_coupon_amount -- C_券额
        ,coalesce(t2.c_high_star_rn, 0)      as c_high_star_rn -- C_高星间夜量
        ,coalesce(t2.c_mid_star_rn, 0)      as c_mid_star_rn -- C_中星间夜量
        ,coalesce(t2.c_low_star_rn, 0)      as c_low_star_rn -- C_低星间夜量
        ,coalesce(t2.c_order_cnt / t1.c_uv, 0)  as c_cr -- C_CR
        ,coalesce(t2.c_room_night, 0) / coalesce(t2.c_order_cnt, 0)  as c_avg_rn_per_order -- C_单间夜
        ,coalesce(t2.c_commission, 0) / coalesce(t2.c_gmv, 0)  as c_take_rate -- C_收益率
        ,coalesce(t2.c_coupon_amount, 0) / coalesce(t2.c_gmv, 0)  as c_subsidy_rate -- C_券补贴率
        ,coalesce(t2.c_gmv, 0) / coalesce(t2.c_room_night, 0)  as c_adr -- C_ADR
from c_uv_info t1 
left join c_order_info t2 on t1.dt=t2.dt and t1.mdd=t2.mdd 
and t1.user_type=t2.user_type
)

,qc_sdbo as (
    select t1.dt
        ,t1.mdd
        ,t1.user_type
        ,s_all_UV
        ,d_s_UV
        ,b_ds_UV
        ,o_ds_order
        ,d_s_UV / s_all_UV   s2d
        ,b_ds_UV / d_s_UV   d2b
        ,o_ds_order / b_ds_UV  b2o
        ,o_ds_order / s_all_UV  s2o

        ,s_all_UV_c
        ,d_s_UV_c
        ,b_ds_UV_c
        ,o_ds_order_c
        ,d_s_UV_c / s_all_UV_c s2d_c
        ,b_ds_UV_c / d_s_UV_c d2b_c
        ,o_ds_order_c / b_ds_UV_c b2o_c
        ,o_ds_order_c / s_all_UV_c s2o_c

        ,(d_s_UV / s_all_UV) / (d_s_UV_c / s_all_UV_c)   s2d_qc
        ,(b_ds_UV / d_s_UV) / (b_ds_UV_c / d_s_UV_c)    d2b_qc
        ,(o_ds_order / b_ds_UV) / (o_ds_order_c / b_ds_UV_c)   b2o_qc
        ,(o_ds_order / s_all_UV) / (o_ds_order_c / s_all_UV_c)   s2o_qc

        ,s_all_UV / s_all_UV_c  s_uv_qc
        ,d_s_UV / d_s_UV_c      d_uv_qc
        ,b_ds_UV / b_ds_UV_c    b_uv_qc
        ,o_ds_order / o_ds_order_c  o_uv_qc

    from(
        select dt
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,sum(s_all_UV) s_all_UV
            ,sum(d_all_UV) d_all_UV
            ,sum(b_all_UV) b_all_UV
            ,sum(d_s_UV) d_s_UV
            ,sum(b_ds_UV) b_ds_UV
            ,sum(o_ds_order) o_ds_order
            ,sum(q_uv) q_uv
            ,sum(order_user_cnt) order_user_cnt
        from (
            select 
                a.dt
                ,a.mdd
                ,a.user_type
                ,count(distinct case when search_pv >0 then  a.user_id else null end )s_all_UV
                ,count(distinct case when detail_pv >0 then  a.user_id else null end )d_all_UV
                ,count(distinct case when booking_pv >0 then a.user_id else null end )b_all_UV
                ,count(distinct case when order_pv >0 then   a.user_id else null end )o_UV
                ,count(distinct case when search_pv >0 or detail_pv>0 then  a.user_id else null end )sd_UV

                ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
                ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end ) b_ds_UV
                ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order

                ,count(distinct case when detail_pv >0 and search_pv <=0 then  a.user_id else null end )  d_z_UV
                ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv <=0 then  a.user_id else null end )b_dz_UV
                ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv <=0 then order_no else null end )o_dz_order
                ,count(distinct a.user_id) q_uv
                ,count(distinct b.user_id) order_user_cnt
            from  uv a  -- 流量表
            left join q_order_app b on a.dt=b.order_date and a.user_id=b.user_id and a.mdd=b.mdd   -- 订单表
            group by 1,2,3
        ) a 
        group by dt,cube(user_type, mdd)
    )t1   
    left join (
        select dt
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,sum(s_all_UV_c) s_all_UV_c
            ,sum(d_s_UV_c) d_s_UV_c
            ,sum(b_ds_UV_c) b_ds_UV_c
            ,sum(o_ds_order_c) o_ds_order_c
            ,sum(c_uv) c_uv
            ,sum(order_user_cnt_c) order_user_cnt_c
        from (
            select t1.dt
                ,t1.mdd
                ,t1.user_type
                ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
                ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
                ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
                ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
                ,count(distinct uid) as c_uv
                ,count(distinct t2.ubt_user_id) as order_user_cnt_c
            from c_uv t1 
            left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id and t1.mdd=t2.mdd
            group by 1,2,3
        ) t 
        group by dt,cube(user_type, mdd)
    )t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
)

/******************************** QC分区域分新老结果数据 ********************************/ 
select t1.dt
        ,t1.mdd
        ,t1.user_type

        ,q_room_night_app / c_room_night  as qc_rn_rate -- 间夜QC
        ,uv / c_uv   as qc_traffic_rate -- 流量QC
        ,q_cr_app / c_cr  as qc_cr -- 转化QC
        ,q_commission_app / c_commission  as qc_revenue -- 收益QC
        ,(q_take_rate_app - c_take_rate)   as qc_take_rate_diff -- 收益率QC差
        ,(q_subsidy_rate_app - c_subsidy_rate)   as qc_subsidy_rate_diff -- 券补贴率QC差
        ,q_adr_app / c_adr    as qc_adr -- ADR_QC
        ,q_order_cnt_app / c_order_cnt   as qc_order_cnt -- 订单量QC
        ,q_avg_rn_per_order_app / c_avg_rn_per_order  as qc_avg_rn -- 单间夜QC

        ,uv
        ,c_uv
        ,q_room_night -- Q_间夜量
        ,q_room_night_app -- Q_间夜量_app
        ,c_room_night -- C_间夜量
        ,q_commission -- Q_佣金
        ,c_commission -- C_佣金
        ,q_commission_app -- Q_佣金_app

        ,q_traffic_rate -- Q_流量占比
        ,q_cr      q_cr -- Q_CR
        ,q_cr_app  q_cr_app -- Q_CR_app
        ,c_cr      c_cr -- C_CR
        ,q_take_rate     q_take_rate     -- Q_收益率 --佣金率
        ,q_take_rate_app q_take_rate_app -- Q_收益率_app --佣金率
        ,c_take_rate     c_take_rate     -- C_收益率 --佣金率

        ,q_rn_rate_app -- Q_间夜占比_app
        ,q_order_cnt_rate_app -- Q_订单量占比_app
        ,q_gmv_rate_app -- Q_GMV占比_app
        ,q_commission_rate_app -- Q_佣金占比_app
        ,q_coupon_amt_rate_app -- Q_券额占比_app
        ,q_rn_rate -- Q_间夜占比
        ,q_order_cnt_rate -- Q_订单量占比
        ,q_gmv_rate -- Q_GMV占比
        ,q_commission_rate -- Q_佣金占比
        ,q_coupon_amt_rate -- Q_券额占比   

        ,q_subsidy_rate     q_subsidy_rate -- Q_券补贴率
        ,q_subsidy_rate_app q_subsidy_rate_app -- Q_券补贴率_app
        ,c_subsidy_rate     c_subsidy_rate -- C_券补贴率
        ,q_order_cnt -- Q_订单量
        ,q_order_cnt_app -- Q_订单量_app
        ,c_order_cnt -- C_订单量

        ,q_gmv -- Q_GMV
        ,q_gmv_app -- Q_GMV_app
        ,c_gmv -- C_GMV

        ,q_coupon_amount -- Q_券额
        ,q_coupon_amount_app -- Q_券额_app
        ,c_coupon_amount -- C_券额

        ,q_order_user_cnt -- Q_下单用户
        ,q_order_user_cnt_app -- Q_下单用户_app
        ,c_order_user_cnt -- C_下单用户

        ,q_adr -- Q_ADR
        ,q_adr_app -- Q_ADR_app
        ,c_adr -- C_ADR

        ,q_coupon_order_rate -- Q_用券订单占比
        ,q_coupon_order_rate_app -- Q_用券订单占比_app

        ,q_high_star_rn -- Q_高星间夜量
        ,q_high_star_rn_app -- Q_高星间夜量_app
        ,c_high_star_rn -- C_高星间夜量

        ,q_mid_star_rn -- Q_中星间夜量
        ,q_mid_star_rn_app -- Q_中星间夜量_app
        ,c_mid_star_rn -- C_中星间夜量

        ,q_low_star_rn -- Q_低星间夜量
        ,q_low_star_rn_app -- Q_低星间夜量_app
        ,c_low_star_rn -- C_低星间夜量

        ,q_avg_rn_per_order -- Q_单间夜
        ,q_avg_rn_per_order_app -- Q_单间夜_app
        ,c_avg_rn_per_order -- C_单间夜

        ,s_all_UV
        ,d_s_UV
        ,b_ds_UV
        ,o_ds_order
        , s2d
        , d2b
        , b2o
        , s2o

        ,s_all_UV_c
        ,d_s_UV_c
        ,b_ds_UV_c
        ,o_ds_order_c
        ,s2d_c
        ,d2b_c
        ,b2o_c
        ,s2o_c

        , s2d_qc
        , d2b_qc
        , b2o_qc
        , s2o_qc


from (---- 预定口径Q数据
    select dt, mdd,user_type,uv
           ,q_room_night -- Q_间夜量
           ,q_order_cnt -- Q_订单量
           ,q_order_user_cnt -- Q_下单用户
           ,q_gmv -- Q_GMV
           ,q_commission -- Q_佣金
           ,q_coupon_amount -- Q_券额
           ,q_high_star_rn -- Q_高星间夜量
           ,q_mid_star_rn -- Q_中星间夜量
           ,q_low_star_rn -- Q_低星间夜量
           ,q_traffic_rate -- Q_流量占比
           ,q_rn_rate_app -- Q_间夜占比_app
           ,q_order_cnt_rate_app -- Q_订单量占比_app
           ,q_gmv_rate_app -- Q_GMV占比_app
           ,q_commission_rate_app -- Q_佣金占比_app
           ,q_coupon_amt_rate_app -- Q_券额占比_app
           ,q_rn_rate -- Q_间夜占比
           ,q_order_cnt_rate -- Q_订单量占比
           ,q_gmv_rate -- Q_GMV占比
           ,q_commission_rate -- Q_佣金占比
           ,q_coupon_amt_rate -- Q_券额占比
           ,q_cr -- Q_CR
           ,q_avg_rn_per_order -- Q_单间夜
           ,q_take_rate -- Q_收益率
           ,q_subsidy_rate -- Q_券补贴率
           ,q_adr -- Q_ADR
           ,q_coupon_order_rate -- Q_用券订单占比
           ,q_room_night_app -- Q_间夜量_app
           ,q_order_cnt_app -- Q_订单量_app
           ,q_order_user_cnt_app -- Q_下单用户_app
           ,q_gmv_app -- Q_GMV_app
           ,q_commission_app -- Q_佣金_app
           ,q_coupon_amount_app -- Q_券额_app
           ,q_high_star_rn_app -- Q_高星间夜量_app
           ,q_mid_star_rn_app -- Q_中星间夜量_app
           ,q_low_star_rn_app -- Q_低星间夜量_app
           ,q_cr_app -- Q_CR_app
           ,q_avg_rn_per_order_app -- Q_单间夜_app
           ,q_take_rate_app -- Q_收益率_app
           ,q_subsidy_rate_app -- Q_券补贴率_app
           ,q_adr_app -- Q_ADR_app
           ,q_coupon_order_rate_app -- Q_用券订单占比_app
    from q_data_info
) t1
left join c_data_info t2   --- 预定口径C数据
on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
left join qc_sdbo t3
on t1.dt=t3.dt and t1.mdd=t3.mdd and t1.user_type=t3.user_type
order by t1.dt 
        ,case when mdd = '香港'  then 1
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