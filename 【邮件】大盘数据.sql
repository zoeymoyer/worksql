-- 1_2025大盘数据

with q_user_type as (
    select user_id
         ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
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

   ,q_uv as (
    select dt as `日期`
         ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
         ,count(distinct if((search_pv + detail_pv + booking_pv + order_pv)>0,a.user_id,null)) as q_uv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
             left join q_user_type b on a.user_id = b.user_id
             left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= trunc(date_sub(current_date,1), 'MM')  -- 改为当月第一天
      and dt<= date_sub(current_date,1)   -- 前一天
      and business_type = 'hotel'
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
      and (search_pv + detail_pv + booking_pv + order_pv)>0
    group by 1,2,3
)

   ,c_uv as (
    select dt as `日期`
         ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
         ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
             left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name
             left join c_user_type b on a.uid=b.ubt_user_id
    where device_chl='app'
      and dt >= trunc(date_sub(current_date,1), 'MM')  -- 改为当月第一天
      and dt <= date_sub(current_date, 1)  -- 改为前一天
    group by 1,2,3
)

   ,q_app_order as (
    select order_date as `日期`
         ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type
         ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                       then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                   else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
         ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                       then (init_commission_after_new+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                   else init_commission_after_new+nvl(ext_plat_certificate,0) end) --Q_佣金
        + sum(case when nvl(four_a, third_a) is not null and dt <= "20221124" then round(nvl(((nvl(second_a, first_a) - nvl(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - nvl(four_a, third_a)) * room_night)),2)
                   when nvl(four_a, third_a) is not null and order_date <= "2024-03-29" then (nvl(four_a_reduce, third_a_reduce)*room_night)
                   else nvl(cashbackmap['follow_price_amount']*room_night,0) end) --追价补
        + sum(nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night) --协议补
        + sum(nvl(cashbackmap['framework_amount'],0)) --券补
        as `Q_佣金（C视角）`
         ,sum(init_gmv) as `Q_GMV`
         ,sum(case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                   else nvl(coupon_substract_summary,0) end) as `Q_券额`
         ,count(order_no) as `Q_订单量`
         ,count(distinct a.user_id) as `Q_下单用户`
         ,sum(room_night) as `Q_间夜量`
         ,sum(case when qta_supplier_id = '1615667' then room_night else 0 end ) as `Q_C2Q间夜量`
         ,sum(case when qta_supplier_id in ('800000191','800000650','1617596','1617599','800000218','800000227','800000221','800000224','1625282') then room_night else 0 end ) as `Q_ABE间夜量`
         ,sum(case when qta_supplier_id not in ('1615667','800000191','800000650','1617596','1617599','800000218','800000227','800000221','800000224','1625282') then room_night else 0 end ) as `Q_其余代理间夜量`
         ,sum(if(promotion_ids_set is not null,room_night,null)) as promotion_count
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from default.mdw_order_v3_international a
             left join q_user_type b on a.user_id = b.user_id
             left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
      --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
      and terminal_channel_type = 'app'
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
      and (first_rejected_time is null or date(first_rejected_time) > order_date)
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid='1'
      and order_date >= trunc(date_sub(current_date,1), 'MM')  -- 改为当月第一天
      and order_date <= date_sub(current_date, 1)  -- 改为前一天
      and order_no <> '103576132435'
    group by 1,2,3
)

   ,q_order as (
    select order_date as `日期`
         ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type
         ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                       then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                   else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
         ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                       then (init_commission_after_new+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                   else init_commission_after_new+nvl(ext_plat_certificate,0) end) --Q_佣金
        + sum(case when nvl(four_a, third_a) is not null and dt <= "20221124" then round(nvl(((nvl(second_a, first_a) - nvl(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - nvl(four_a, third_a)) * room_night)),2)
                   when nvl(four_a, third_a) is not null and order_date <= "2024-03-29" then (nvl(four_a_reduce, third_a_reduce)*room_night)
                   else nvl(cashbackmap['follow_price_amount']*room_night,0) end) --追价补
        + sum(nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night) --协议补
        + sum(nvl(cashbackmap['framework_amount'],0)) --券补
        as `Q_佣金（C视角）`
         ,sum(room_night) as `Q_间夜量`
    from default.mdw_order_v3_international a
             left join q_user_type b on a.user_id = b.user_id
             left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
      --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
      and terminal_channel_type in ('www','app','touch')
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
      and (first_rejected_time is null or date(first_rejected_time) > order_date)
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid='1'
      and order_date >= trunc(date_sub(current_date,1), 'MM')  -- 改为当月第一天
      and order_date <= date_sub(current_date, 1)  -- 改为前一天
      and order_no <> '103576132435'
    group by 1,2,3
)

   ,c_app_order as(
    select
        substr(order_date,1,10) `日期`
         ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as `目的地`
         ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
         ,count(order_no) as `C_订单量`
         ,sum(extend_info['room_night']) as `C_间夜量`
         ,sum(room_fee)as `C_GMV`
         ,sum(comission) as `C_佣金`
         ,sum( get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount')) as `C_券额`
         ,sum(case when extend_info['STAR'] in (4,5) then extend_info['room_night'] else 0 end ) as `C_高星间夜量`
         ,sum(case when extend_info['STAR'] in (3) then extend_info['room_night'] else 0 end ) as `C_中星间夜量`
         ,sum(case when extend_info['STAR'] not in (3,4,5) then extend_info['room_night'] else 0 end ) as `C_低星间夜量`
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
      and substr(order_date,1,10) >= trunc(date_sub(current_date,1), 'MM')  -- 改为当月第一天
      and substr(order_date,1,10) <= date_sub(current_date, 1)  -- 改为前一天
    group by 1,2,3
)

select  `日期`
     ,date_format(`日期`,'u')`星期`
     ,sum(`Q_all_间夜量`)`间夜量`
     ,concat(round(sum(`Q_all_佣金`)/10000,1),'万')`收益额`
     ,concat(round(sum(`Q_间夜量`)/sum(`C_间夜量`)*100,1),'%')`间夜QC`
     ,concat(round(sum(q_uv)/sum(c_uv)*100,1),'%')`流量QC`
     ,concat(round((sum(`Q_订单量`)/sum(q_uv))/(sum(`C_订单量`)/sum(c_uv))*100,1),'%')`转化QC`
     ,concat(round((sum(`Q_间夜量`)/sum(`Q_订单量`))/(sum(`C_间夜量`)/sum(`C_订单量`))*100,1),'%')`单间夜QC`
     ,concat(round(sum(`Q_佣金`)/sum(`C_佣金`)*100,1),'%')`收益QC`
     ,concat(round(sum(`Q_佣金`)/sum(`Q_GMV`)*100,2),'%')`Q_收益率`
     ,concat(round(sum(`C_佣金`)/sum(`C_GMV`)*100,2),'%')`C_收益率`
     ,concat(round(((sum(`Q_佣金`)/sum(`Q_GMV`))-(sum(`C_佣金`)/sum(`C_GMV`)))*100,2),'%')`收益率QC差`
     ,concat(round(((sum(`Q_佣金（C视角）`)/sum(`Q_GMV`))-(sum(`C_佣金`)/sum(`C_GMV`)))*100,2),'%')`收益率QC差(C视角)`
     ,concat(round(sum(`Q_券额`)/sum(`Q_GMV`)*100,2),'%')`Q_券补贴率`
     ,concat(round(sum(`C_券额`)/sum(`C_GMV`)*100,2),'%')`C_券补贴率`
     ,concat(round(((sum(`Q_券额`)/sum(`Q_GMV`))-(sum(`C_券额`)/sum(`C_GMV`)))*100,2),'%')`券补贴率QC差`
     ,CAST(round(sum(`Q_GMV`)/sum(`Q_间夜量`),0) as int)`Q_ADR`
     ,CAST(round(sum(`C_GMV`)/sum(`C_间夜量`),0) as int)`C_ADR`
     ,concat(round((sum(`Q_GMV`)/sum(`Q_间夜量`))/(sum(`C_GMV`)/sum(`C_间夜量`))*100,1),'%') `ADR_QC`
-- , concat(round(sum(`Q_高星间夜量`)/sum(`C_高星间夜量`)*100, 1), '%')`高星间夜QC`
-- , concat(round(sum(`Q_中星间夜量`)/sum(`C_中星间夜量`)*100, 1), '%')`中星间夜QC`
-- , concat(round(sum(`Q_低星间夜量`)/sum(`C_低星间夜量`)*100, 1), '%')`低星间夜QC`
     ,sum(`Q_间夜量`) `Q_间夜量`
     ,sum(`C_间夜量`) `C_间夜量`
     ,sum(`Q_佣金`) `Q_佣金`
     ,sum(`C_佣金`) `C_佣金`
     ,sum(`Q_GMV`) `Q_GMV`
     ,sum(`C_GMV`) `C_GMV`
from
    (
        select  a.`日期`
             ,a.user_type
             ,a.`目的地`
             ,a.q_uv
             ,b.c_uv
             ,c.`Q_订单量`
             ,c.`Q_间夜量`
             ,c.`Q_佣金`
             ,c.`Q_佣金（C视角）`
             ,c.`Q_GMV`
             ,c.`Q_券额`
             ,c.`Q_高星间夜量`
             ,c.`Q_中星间夜量`
             ,c.`Q_低星间夜量`
             ,d.`C_订单量`
             ,d.`C_间夜量`
             ,d.`C_佣金`
             ,d.`C_GMV`
             ,d.`C_券额`
             ,d.`C_高星间夜量`
             ,d.`C_中星间夜量`
             ,d.`C_低星间夜量`
             ,c.`Q_C2Q间夜量`
             ,c.`Q_ABE间夜量`
             ,c.`Q_其余代理间夜量`
             ,e.`Q_间夜量` as `Q_all_间夜量`
             ,e.`Q_佣金` as `Q_all_佣金`
        from q_uv a
                 left join c_uv b
                           on a.`日期` = b.`日期` and a.`目的地` = b.`目的地` and a.user_type = b.user_type
                 left join q_app_order c
                           on a.`日期` = c.`日期` and a.`目的地` = c.`目的地` and a.user_type = c.user_type
                 left join c_app_order d
                           on a.`日期` = d.`日期` and a.`目的地` = d.`目的地` and a.user_type = d.user_type
                 left join q_order e
                           on a.`日期` = e.`日期` and a.`目的地` = e.`目的地` and a.user_type = e.user_type
    )a
group by  1
       ,2
order by  a.`日期`;