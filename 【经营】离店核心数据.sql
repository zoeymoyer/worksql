with q_user_type as 
    (select user_id 
        , min(order_date) as min_order_date
    from mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
    )

, c_user_type as 
    (select user_id
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

, q_app_order as 
    (select checkout_date as `日期`
        , case 
            when province_name in ('澳门','香港') then province_name 
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
            when e.area in ('欧洲','亚太','美洲') then e.area 
            else '其他' end as `目的地`
        , case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        , sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                else final_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
        , sum(final_gmv) as `Q_GMV`
        , sum(case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                else nvl(coupon_substract_summary,0) end) as `Q_券额`
        , count(order_no) as `Q_订单量` 
        , count(distinct a.user_id) as `Q_下单用户` 
        , sum(room_night) as `Q_间夜量`
        , sum(case when qta_supplier_id = '1615667' then room_night else 0 end ) as `Q_C2Q间夜量`
        , sum(case when qta_supplier_id in ('800000191','800000650','1617596','1617599','800000218','800000227','800000221','800000224','1625282') then room_night else 0 end ) as `Q_ABE间夜量`
        , sum(case when qta_supplier_id not in ('1615667','800000191','800000650','1617596','1617599','800000218','800000227','800000221','800000224','1625282') then room_night else 0 end ) as `Q_其余代理间夜量`
        , sum(if(promotion_ids_set is not null,room_night,null)) as promotion_count
        , sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
        , sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
        , sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`

        , sum(case when chain_tag = 'Key Chain' then room_night end) as `Q_KC间夜量`
        , sum(case when chain_tag = 'Strategic Chain' then room_night end) as `Q_SC间夜量`
        , sum(case when chain_tag = 'Regional Chain' then room_night end) as `Q_RC间夜量`
        , sum(case when chain_tag = 'Local Chain' then room_night end) as `Q_LC间夜量`
        , sum(case when chain_tag not in ('Local Chain','Key Chain','Strategic Chain','Regional Chain','Local Chain') or chain_tag is null then room_night end) as `Q_IH间夜量`

    from mdw_order_v3_international a 

    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name

    left join
        (select hotel_seq
            , max(ctrip_group_id) as ctrip_group_id
        from default.dim_hotel_info_intl_v3
        where dt = '%(DATE)s'
            and hotel_operating_status = '营业中'
        group by 1
        ) h
    on a.hotel_seq = h.hotel_seq

    left join temp.temp_yuchen_shen_chainhotel_id_list_20250702 as ci
    on h.ctrip_group_id = ci.chain_id 

    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date between date_sub(current_date,15) and date_sub(current_date,1)
        and order_no <> '103576132435'
    group by 1,2,3
    )

, q_order as 
    (select checkout_date as `日期`
        , case 
            when province_name in ('澳门','香港') then province_name 
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
            when e.area in ('欧洲','亚太','美洲') then e.area 
            else '其他' end as `目的地`
        , case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        , sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                else final_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
        , sum(room_night) as `Q_间夜量`
    from mdw_order_v3_international a 
    
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date between date_sub(current_date,15) and date_sub(current_date,1)
        and order_no <> '103576132435'
    group by 1,2,3
    )

, c_app_order as 
    (select substr(checkout_date,1,10) `日期`
        , case 
            when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
            when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
            when c.area in ('欧洲','亚太','美洲') then c.area
            else '其他' end as `目的地`
        , case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        , count(order_no) as `C_订单量`
        , sum(extend_info['room_night']) as `C_间夜量`
        , sum(room_fee)as `C_GMV`
        , sum(comission) as `C_佣金`
        , sum( get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount')) as `C_券额`
        , sum(case when extend_info['STAR'] in (4,5) then extend_info['room_night'] else 0 end ) as `C_高星间夜量`
        , sum(case when extend_info['STAR'] in (3) then extend_info['room_night'] else 0 end ) as `C_中星间夜量`
        , sum(case when extend_info['STAR'] not in (3,4,5) then extend_info['room_night'] else 0 end ) as `C_低星间夜量`

        , sum(case when chain_tag = 'Key Chain' then extend_info['room_night'] end) as `C_KC间夜量`
        , sum(case when chain_tag = 'Strategic Chain' then extend_info['room_night'] end) as `C_SC间夜量`
        , sum(case when chain_tag = 'Regional Chain' then extend_info['room_night'] end) as `C_RC间夜量`
        , sum(case when chain_tag = 'Local Chain' then extend_info['room_night'] end) as `C_LC间夜量`
        , sum(case when chain_tag not in ('Local Chain','Key Chain','Strategic Chain','Regional Chain','Local Chain') or chain_tag is null then extend_info['room_night'] end) as `C_IH间夜量`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o

    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 

    left join (select distinct hotel_seq, partner_hotel_id from ihotel_default.dim_hotel_mapping_intl_v3 where dt = '%(DATE)s' and partner = 'ctrip') m
    on o.hotel_seq = m.partner_hotel_id

    left join
        (select hotel_seq
            , max(ctrip_group_id) as ctrip_group_id
        from default.dim_hotel_info_intl_v3
        where dt = '%(DATE)s'
        and hotel_operating_status = '营业中'
        group by 1
        ) h
    on m.hotel_seq = h.hotel_seq

    left join temp.temp_yuchen_shen_chainhotel_id_list_20250702 as ci
    on h.ctrip_group_id = ci.chain_id

    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        -- and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
        and terminal_channel_type = 'app'
        and order_status <> 'C'
        and substr(checkout_date,1,10) between date_sub(current_date,15) and date_sub(current_date,1)
    group by 1,2,3
    )

, q_app_order_all as 
    (select checkout_date as `日期`
        , case 
            when province_name in ('澳门','香港') then province_name 
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
            when e.area in ('欧洲','亚太','美洲') then e.area 
            else '其他' end as `目的地`
        , case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
        , sum(room_night) as `Q_间夜量`
    from mdw_order_v3_international a 
    
    left join q_user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
        and is_valid = '1'
        and checkout_date between date_sub(current_date,15) and date_sub(current_date,1)
        and order_no <> '103576132435'
    group by 1,2,3
    )

, c_app_order_all as 
    (select substr(checkout_date,1,10) `日期`
        , case 
            when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
            when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
            when c.area in ('欧洲','亚太','美洲') then c.area
            else '其他' end as `目的地`
        , case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        , sum(extend_info['room_night']) as `C_间夜量`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o

    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 

    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(checkout_date,1,10) between date_sub(current_date,15) and date_sub(current_date,1)
    group by 1,2,3
    )


select `日期`
    , date_format(`日期`, 'u')`星期`
    , sum(`Q_all_间夜量`)`间夜量`
    , concat(round(sum(`Q_all_佣金`)/10000, 1), '万')`收益额`
    , concat(round(sum(`Q_间夜量`)/sum(`C_间夜量`)*100, 1), '%')`间夜QC`
    , concat(round((sum(`Q_间夜量`)/sum(`Q_订单量`))/(sum(`C_间夜量`)/sum(`C_订单量`))*100, 1), '%')`单间夜QC`
    , concat(round(sum(`Q_佣金`)/sum(`C_佣金`)*100, 1), '%')`收益QC`
    , concat(round(sum(`Q_佣金`)/sum(`Q_GMV`)*100, 2), '%')`Q_收益率`
    , concat(round(sum(`C_佣金`)/sum(`C_GMV`)*100, 2), '%')`C_收益率`

    , concat(round(((sum(`Q_佣金`)/sum(`Q_GMV`))-(sum(`C_佣金`)/sum(`C_GMV`)))*100, 2), '%')`收益率QC差`

    , concat(round(sum(`Q_券额`)/sum(`Q_GMV`)*100, 2), '%')`Q_券补贴率`
    , concat(round(sum(`C_券额`)/sum(`C_GMV`)*100, 2), '%')`C_券补贴率`
    , concat(round(((sum(`Q_券额`)/sum(`Q_GMV`))-(sum(`C_券额`)/sum(`C_GMV`)))*100, 2), '%')`券补贴率QC差`

    , CAST(round(sum(`Q_GMV`)/sum(`Q_间夜量`), 0) as int)`Q_ADR`
    , CAST(round(sum(`C_GMV`)/sum(`C_间夜量`), 0) as int)`C_ADR`
    , concat(round((sum(`Q_GMV`)/sum(`Q_间夜量`))/(sum(`C_GMV`)/sum(`C_间夜量`))*100, 1), '%') `ADR_QC`

    , concat(round((1 - sum(`Q_间夜量`)/sum(`Q_间夜_非当日取消`))*100, 1),'%') as `Q_取消率`
    , concat(round((1 - sum(`C_间夜量`)/sum(`C_间夜_非当日取消`))*100, 1),'%') as `C_取消率`
    , concat(round((sum(`C_间夜量`)/sum(`C_间夜_非当日取消`)-sum(`Q_间夜量`)/sum(`Q_间夜_非当日取消`))*100, 1), 'pp') `取消率_QC差`

    , concat(round(sum(case when user_type = '新客' then `Q_间夜量` end)/sum(case when user_type = '新客' then `C_间夜量` end)*100, 1), '%') as `间夜QC_新客`
    , concat(round(sum(case when user_type = '老客' then `Q_间夜量` end)/sum(case when user_type = '老客' then `C_间夜量` end)*100, 1), '%') as `间夜QC_老客`

    , concat(round(sum(case when `目的地` = '日本' then `Q_间夜量` end)/sum(case when `目的地` = '日本' then `C_间夜量` end)*100, 1), '%') as `间夜QC_日本`
    , concat(round(sum(case when `目的地` = '泰国' then `Q_间夜量` end)/sum(case when `目的地` = '泰国' then `C_间夜量` end)*100, 1), '%') as `间夜QC_泰国`
    , concat(round(sum(case when `目的地` = '香港' then `Q_间夜量` end)/sum(case when `目的地` = '香港' then `C_间夜量` end)*100, 1), '%') as `间夜QC_香港`
    , concat(round(sum(case when `目的地` = '韩国' then `Q_间夜量` end)/sum(case when `目的地` = '韩国' then `C_间夜量` end)*100, 1), '%') as `间夜QC_韩国`

    , concat(round(sum(`Q_KC间夜量`)/sum(`C_KC间夜量`)*100, 1), '%') as `间夜QC_KC`
    , concat(round(sum(`Q_SC间夜量`)/sum(`C_SC间夜量`)*100, 1), '%') as `间夜QC_SC`
    , concat(round(sum(`Q_RC间夜量`)/sum(`C_RC间夜量`)*100, 1), '%') as `间夜QC_RC`
    , concat(round(sum(`Q_LC间夜量`)/sum(`C_LC间夜量`)*100, 1), '%') as `间夜QC_LC`
    , concat(round(sum(`Q_IH间夜量`)/sum(`C_IH间夜量`)*100, 1), '%') as `间夜QC_IH`

from
    (select a.`日期`
        , a.user_type
        , a.`目的地`
        , a.`Q_订单量`
        , a.`Q_间夜量`
        , a.`Q_佣金`
        , a.`Q_GMV`
        , a.`Q_券额`
        , a.`Q_高星间夜量`
        , a.`Q_中星间夜量`
        , a.`Q_低星间夜量`
        , a.`Q_KC间夜量`
        , a.`Q_SC间夜量`
        , a.`Q_RC间夜量`
        , a.`Q_LC间夜量`
        , a.`Q_IH间夜量`
        , d.`C_订单量`
        , d.`C_间夜量`
        , d.`C_佣金`
        , d.`C_GMV`
        , d.`C_券额`
        , d.`C_高星间夜量`
        , d.`C_中星间夜量`
        , d.`C_低星间夜量`
        , d.`C_KC间夜量`
        , d.`C_SC间夜量`
        , d.`C_RC间夜量`
        , d.`C_LC间夜量`
        , d.`C_IH间夜量`  
        , a.`Q_C2Q间夜量`
        , a.`Q_ABE间夜量`
        , a.`Q_其余代理间夜量`   
        , e.`Q_间夜量` as `Q_all_间夜量`
        , e.`Q_佣金` as `Q_all_佣金`
        , f.`Q_间夜量` as `Q_间夜_非当日取消`
        , g.`C_间夜量` as `C_间夜_非当日取消`
    from q_app_order a
    left join c_app_order d on a.`日期` = d.`日期` and a.`目的地` = d.`目的地` and a.user_type = d.user_type
    left join q_order e on a.`日期` = e.`日期` and a.`目的地` = e.`目的地` and a.user_type = e.user_type
    left join q_app_order_all f on a.`日期` = f.`日期` and a.`目的地` = f.`目的地` and a.user_type = f.user_type
    left join c_app_order_all g on a.`日期` = g.`日期` and a.`目的地` = g.`目的地` and a.user_type = g.user_type
    ) a
group by 1, 2
order by a.`日期` desc


;



