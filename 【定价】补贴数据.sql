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

, q_order as 
    (select order_date as `日期`
        , user_type

        , sum(`Q_间夜`) as `Q_间夜`
        , sum(`Q_佣金`) as `Q_佣金`
        , sum(`Q_gmv`) as `Q_gmv`
        , sum(`Q_补贴前佣金`) as `Q_补贴前佣金`
        , sum(`Q_补贴前gmv`) as `Q_补贴前gmv`

        , sum(`变现提`) as `变现提`
        , sum(`定价补`) as `定价补`
        , sum(`协议补`) as `协议补`
        , sum(`平台补`) as `平台补`
        , sum(`追价补`) as `追价补`
        , sum(`券补`) as `券补`
        , sum(`积分补`) as `积分补`
    from
        (select distinct order_date
            , order_no
            , case when order_date = b.min_order_date then '新客' else '老客' end as user_type 

            , room_night as `Q_间夜`
            , init_gmv as `Q_gmv`
            , case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                else init_commission_after+nvl(ext_plat_certificate,0) end as `Q_佣金`
            
            , case when get_json_object(extendinfomap,'$.v2_c_incr') is not null then init_gmv * get_json_object(extendinfomap,'$.v2_c_incr') end as `Q_补贴前佣金`
            , case when get_json_object(extendinfomap,'$.v2_c_incr') is not null then init_gmv end as `Q_补贴前gmv`
            , get_json_object(extendinfomap,'$.v2_c_incr') as `锚C佣金率`

            , coalesce(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) * room_night as `变现提`

            , coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night as `定价补`
            , coalesce(get_json_object(extendinfomap,'$.frame_amount'),0) * room_night + coalesce(cashbackmap['framework_amount'],0) as `协议补`
            , coalesce(get_json_object(extendinfomap,'$.platform_amount'),0) * room_night as `平台补`
            , case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
                 then coalesce(follow_price_amount,0) end as `追价补`
            , case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                else nvl(coupon_substract_summary,0) end as `券补`
            , coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfo.exchangeAmount'), 0) as `积分补`

        from mdw_order_v3_international a 

        left join q_user_type b on a.user_id = b.user_id 

        where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
            and terminal_channel_type = 'app'
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid='1'
            and order_date between date_sub(current_date,15) and date_sub(current_date,1)
            and order_no <> '103576132435'
        ) z
    group by 1,2
    )

, c_order as 
    (select substr(order_date,1,10) `日期`
        , case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
        , count(order_no) as `C_订单量`
        , sum(extend_info['room_night']) as `C_间夜量`
        , sum(room_fee)as `C_gmv`
        , sum(comission) as `C_佣金`
        , sum(get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount')) as `C_券额`

    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o

    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 

    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        -- and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(order_date,1,10) between date_sub(current_date,15) and date_sub(current_date,1)
    group by 1,2
    )


select q.`日期`
    , date_format(q.`日期`, 'u') as `星期`

    , concat(round(sum(`Q_佣金`)/sum(`Q_gmv`)*100,2),'%') as `Q佣金率`
    , concat(round(sum(`C_佣金`)/sum(`C_gmv`)*100,2),'%') as `C佣金率`
    , concat(round((sum(`Q_佣金`)/sum(`Q_gmv`)-sum(`C_佣金`)/sum(`C_gmv`))*100,2),'%') as `QC佣金差`

    , concat(round(sum(`Q_补贴前佣金`)/sum(`Q_补贴前gmv`)*100,2),'%') as `Q_补贴前佣金率`
    , concat(round(sum(`C_佣金`+`C_券额`)/sum(`C_gmv`+`C_券额`)*100,2),'%') as `C_补贴前佣金率`
    , concat(round((sum(`Q_补贴前佣金`)/sum(`Q_补贴前gmv`) - sum(`C_佣金`+`C_券额`)/sum(`C_gmv`+`C_券额`))*100,2),'%') as `订单结构差`

    , concat(round(sum(`变现提`)/sum(`Q_gmv`)*100,2),'%') as `Q总变现`
    , concat(round(-sum(`定价补`+`追价补`+`券补`+`积分补`)/sum(`Q_gmv`)*100,2),'%') as `Q总补贴`
    , concat(round(sum(`变现提`-(`定价补`+`追价补`+`券补`+`积分补`))/sum(`Q_gmv`)*100,2),'%') as `变现补贴差`

    , concat(round(sum(`定价补`)/sum(`Q_gmv`)*100,2),'%') as `Q定价补`
    , concat(round(sum(`券补`)/sum(`Q_gmv`)*100,2),'%') as `Q券补`
    , concat(round(sum(`积分补`)/sum(`Q_gmv`)*100,2),'%') as `Q积分补`
    , concat(round(sum(`追价补`)/sum(`Q_gmv`)*100,2),'%') as `Q追价补`
    , concat(round(sum(`协议补`)/sum(`Q_gmv`)*100,2),'%') as `Q协议补`
    , concat(round(sum(`平台补`)/sum(`Q_gmv`)*100,2),'%') as `Q平台补`
    
    , concat(round(sum(`C_券额`)/sum(`C_gmv`)*100,2),'%') as `C券补`
   

from q_order q
left join c_order c on q.`日期` = c.`日期` and q.user_type = c.user_type
group by 1,2
order by q.`日期` desc
