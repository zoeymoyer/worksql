select '离店口径' as `口径`
    , q.checkout_date as `离店日期`
    , `Q间夜`
    , `Qgmv`
    , `Q佣金`
    , `QApp_间夜`
    , `QApp_gmv`
    , `QApp_佣金`
    , `CApp_间夜量`
    , `CApp_Cgmv`
    , `CApp_C佣金`
    , `C间夜量`
    , `Cgmv`
    , `C佣金`
    , '' as `Q_佣金（C视角）`
    , '' as `QApp_gmv_hs`
    , '' as `QApp_佣金_hs`
    , '' as `Q_佣金（C视角）_hs`
    , '' as `QApp_gmv_ms`
    , '' as `QApp_佣金_ms`
    , '' as `Q_佣金（C视角）_ms`
    , '' as `QApp_gmv_ls`
    , '' as `QApp_佣金_ls`
    , '' as `Q_佣金（C视角）_ls`
    , '' as `CApp_Cgmv_hs`
    , '' as `CApp_C佣金_hs`
    , '' as `CApp_Cgmv_ms`
    , '' as `CApp_C佣金_ms`
    , '' as `CApp_Cgmv_ls`
    , '' as `CApp_C佣金_ls`
from
    (select checkout_date
        , sum(room_night) as `Q间夜`
        , sum(init_gmv) as `Qgmv`
        , sum(case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') then (final_commission_after+nvl(coupon_substract_summary ,0)) 
            when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0)) 
            else final_commission_after+nvl(ext_plat_certificate,0) end) as `Q佣金`

        , sum(case when terminal_channel_type = 'app' then room_night end) as `QApp_间夜`
        , sum(case when terminal_channel_type = 'app' then init_gmv end) as `QApp_gmv`
        , sum(case when terminal_channel_type = 'app' and a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') then (final_commission_after+nvl(coupon_substract_summary ,0)) 
            when terminal_channel_type = 'app' and (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') then (final_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0)) 
            when terminal_channel_type = 'app' then final_commission_after+nvl(ext_plat_certificate,0) end) as `QApp_佣金`
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
        -- and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and checkout_date >= '2024-01-01'
        and checkout_date <= date_add('%(FORMAT_DATE)s',124)
        and a.order_no <> '103576132435'
    group by 1
    ) q

left join
    (select substr(checkout_date,1,10) as checkout_date
        , sum(extend_info['room_night'] ) as `C间夜量`
        , sum(room_fee)as `Cgmv`
        , sum(comission) as `C佣金`

        , sum(case when terminal_channel_type = 'app' then extend_info['room_night'] end) as `CApp_间夜量`
        , sum(case when terminal_channel_type = 'app' then room_fee end) as `CApp_Cgmv`
        , sum(case when terminal_channel_type = 'app' then comission end) as `CApp_C佣金`

    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = '%(FORMAT_DATE)s'
    and extend_info['IS_IBU'] = '0'
    and extend_info['book_channel'] = 'Ctrip'
    and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
    and order_status <> 'C'
    -- and terminal_channel_type = 'app'
    and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL')
    and substr(checkout_date,1,10) >= '2024-01-01' 
    and substr(checkout_date,1,10) <= date_add(dt,124)
    group by 1
    ) c
on q.checkout_date = c.checkout_date

union all

select '预定口径' as `口径`
    , q.order_date as `预定日期`
    , `Q间夜`
    , `Qgmv`
    , `Q佣金`
    , `QApp_间夜`
    , `QApp_gmv`
    , `QApp_佣金`
    , `CApp_间夜量`
    , `CApp_Cgmv`
    , `CApp_C佣金`
    , `C间夜量`
    , `Cgmv`
    , `C佣金`
    , `Q_佣金（C视角）`
    , `QApp_gmv_hs`
    , `QApp_佣金_hs`
    , `Q_佣金（C视角）_hs`
    , `QApp_gmv_ms`
    , `QApp_佣金_ms`
    , `Q_佣金（C视角）_ms`
    , `QApp_gmv_ls`
    , `QApp_佣金_ls`
    , `Q_佣金（C视角）_ls`
    , `CApp_Cgmv_hs`
    , `CApp_C佣金_hs`
    , `CApp_Cgmv_ms`
    , `CApp_C佣金_ms`
    , `CApp_Cgmv_ls`
    , `CApp_C佣金_ls`

from
    (select order_date
        , sum(room_night) as `Q间夜`
        , sum(init_gmv) as `Qgmv`
        , sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q佣金`

        , sum(case when terminal_channel_type = 'app' then room_night end) as `QApp_间夜`
        , sum(case when terminal_channel_type = 'app' then init_gmv end) as `QApp_gmv`
        , sum(case when terminal_channel_type = 'app' and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' then init_commission_after+nvl(ext_plat_certificate,0) end) as `QApp_佣金`

        , sum(case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                   then (init_commission_after+nvl(coupon_substract_summary ,0)) 
                   when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') 
                   then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0)) 
                else init_commission_after+nvl(ext_plat_certificate,0) end)
        +sum(nvl(follow_price_amount,0))
        +sum(nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night)
        +sum(nvl(cashbackmap['framework_amount'],0))
                as `Q_佣金（C视角）`

        , sum(case when terminal_channel_type = 'app' and hotel_grade in (4,5) then init_gmv end) as `QApp_gmv_hs`
        , sum(case when terminal_channel_type = 'app' and hotel_grade in (4,5) and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' and hotel_grade in (4,5) then init_commission_after+nvl(ext_plat_certificate,0) end) as `QApp_佣金_hs`
        , sum(case when terminal_channel_type = 'app' and hotel_grade in (4,5) and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after_new+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' and hotel_grade in (4,5) then init_commission_after_new+nvl(ext_plat_certificate,0) end) --Q_佣金
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (4,5) and nvl(four_a, third_a) is not null and dt <= "20221124" then round(nvl(((nvl(second_a, first_a) - nvl(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - nvl(four_a, third_a)) * room_night)),2)
            when terminal_channel_type = 'app' and hotel_grade in (4,5) and nvl(four_a, third_a) is not null and order_date <= "2024-03-29" then (nvl(four_a_reduce, third_a_reduce)*room_night)
            when terminal_channel_type = 'app' and hotel_grade in (4,5) then nvl(cashbackmap['follow_price_amount']*room_night,0) end) --追价补
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (4,5) then nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night end) --协议补
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (4,5) then nvl(cashbackmap['framework_amount'],0) end) --券补
            as `Q_佣金（C视角）_hs`

        , sum(case when terminal_channel_type = 'app' and hotel_grade in (3) then init_gmv end) as `QApp_gmv_ms`
        , sum(case when terminal_channel_type = 'app' and hotel_grade in (3) and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' and hotel_grade in (3) then init_commission_after+nvl(ext_plat_certificate,0) end) as `QApp_佣金_ms`
        , sum(case when terminal_channel_type = 'app' and hotel_grade in (3) and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after_new+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' and hotel_grade in (3) then init_commission_after_new+nvl(ext_plat_certificate,0) end) --Q_佣金
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (3) and nvl(four_a, third_a) is not null and dt <= "20221124" then round(nvl(((nvl(second_a, first_a) - nvl(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - nvl(four_a, third_a)) * room_night)),2)
            when terminal_channel_type = 'app' and hotel_grade in (3) and nvl(four_a, third_a) is not null and order_date <= "2024-03-29" then (nvl(four_a_reduce, third_a_reduce)*room_night)
            when terminal_channel_type = 'app' and hotel_grade in (3) then nvl(cashbackmap['follow_price_amount']*room_night,0) end) --追价补
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (3) then nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night end) --协议补
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (3) then nvl(cashbackmap['framework_amount'],0) end) --券补
            as `Q_佣金（C视角）_ms`

        , sum(case when terminal_channel_type = 'app' and hotel_grade in (0,1,2) then init_gmv end) as `QApp_gmv_ls`
        , sum(case when terminal_channel_type = 'app' and hotel_grade in (0,1,2) and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' and hotel_grade in (0,1,2) then init_commission_after+nvl(ext_plat_certificate,0) end) as `QApp_佣金_ls`
        , sum(case when terminal_channel_type = 'app' and hotel_grade in (0,1,2) and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after_new+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' and hotel_grade in (0,1,2) then init_commission_after_new+nvl(ext_plat_certificate,0) end) --Q_佣金
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (0,1,2) and nvl(four_a, third_a) is not null and dt <= "20221124" then round(nvl(((nvl(second_a, first_a) - nvl(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - nvl(four_a, third_a)) * room_night)),2)
            when terminal_channel_type = 'app' and hotel_grade in (0,1,2) and nvl(four_a, third_a) is not null and order_date <= "2024-03-29" then (nvl(four_a_reduce, third_a_reduce)*room_night)
            when terminal_channel_type = 'app' and hotel_grade in (0,1,2) then nvl(cashbackmap['follow_price_amount']*room_night,0) end) --追价补
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (0,1,2) then nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night end) --协议补
            + sum(case when terminal_channel_type = 'app' and hotel_grade in (0,1,2) then nvl(cashbackmap['framework_amount'],0) end) --券补
            as `Q_佣金（C视角）_ls`
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
        -- and terminal_channel_type = 'app'
        -- and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2024-01-01'
        and a.order_no <> '103576132435'
    group by 1
    ) q

left join
    (select substr(order_date,1,10) as order_date
        , sum(extend_info['room_night'] ) as `C间夜量`
        , sum(room_fee)as `Cgmv`
        , sum(comission) as `C佣金`

        , sum(case when terminal_channel_type = 'app' then extend_info['room_night'] end) as `CApp_间夜量`
        , sum(case when terminal_channel_type = 'app' then room_fee end) as `CApp_Cgmv`
        , sum(case when terminal_channel_type = 'app' then comission end) as `CApp_C佣金`

        , sum(case when terminal_channel_type = 'app' and extend_info['STAR'] in (4,5) then room_fee end) as `CApp_Cgmv_hs`
        , sum(case when terminal_channel_type = 'app' and extend_info['STAR'] in (4,5) then comission end) as `CApp_C佣金_hs`
        , sum(case when terminal_channel_type = 'app' and extend_info['STAR'] in (3) then room_fee end) as `CApp_Cgmv_ms`
        , sum(case when terminal_channel_type = 'app' and extend_info['STAR'] in (3) then comission end) as `CApp_C佣金_ms`
        , sum(case when terminal_channel_type = 'app' and extend_info['STAR'] in (0,1,2) then room_fee end) as `CApp_Cgmv_ls`
        , sum(case when terminal_channel_type = 'app' and extend_info['STAR'] in (0,1,2) then comission end) as `CApp_C佣金_ls`

    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = '%(FORMAT_DATE)s'
    and extend_info['IS_IBU'] = '0'
    and extend_info['book_channel'] = 'Ctrip'
    and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
    -- and terminal_channel_type = 'app'
    and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
    and substr(order_date,1,10) >= '2024-01-01'
    group by 1
    ) c
on q.order_date = c.order_date


union all



select '当日预定拆解' as `口径`
    , case when q.checkout_date is not null then q.checkout_date else c.checkout_date end as `离店日期`
    , `Q间夜`
    , `Qgmv`
    , `Q佣金`
    , `QApp_间夜`
    , `QApp_gmv`
    , `QApp_佣金`
    , `CApp_间夜量`
    , `CApp_Cgmv`
    , `CApp_C佣金`
    , `C间夜量`
    , `Cgmv`
    , `C佣金`
    , `Q间夜_日本` as `Q_佣金（C视角）or 日本`
    , `Q间夜_泰国` as `QApp_gmv_hs or 泰国`
    , `Q间夜_韩国` as `QApp_佣金_hs or 韩国`
    , `Q间夜_香港` as `Q_佣金（C视角）_hs or 香港`
    , `Q间夜_澳门` as `QApp_gmv_ms or 澳门`
    , '' as `QApp_佣金_ms`
    , '' as `Q_佣金（C视角）_ms`
    , '' as `QApp_gmv_ls`
    , '' as `QApp_佣金_ls`
    , '' as `Q_佣金（C视角）_ls`
    , '' as `CApp_Cgmv_hs`
    , '' as `CApp_C佣金_hs`
    , '' as `CApp_Cgmv_ms`
    , '' as `CApp_C佣金_ms`
    , '' as `CApp_Cgmv_ls`
    , '' as `CApp_C佣金_ls`

from
    (select case when checkout_date <= date_add('%(FORMAT_DATE)s',124) then checkout_date else '远期离店' end as checkout_date
        , sum(room_night) as `Q间夜`
        , sum(case when country_name = '日本' then room_night end) as `Q间夜_日本`
        , sum(case when country_name = '泰国' then room_night end) as `Q间夜_泰国`
        , sum(case when country_name = '韩国' then room_night end) as `Q间夜_韩国`
        , sum(case when province_name = '香港' then room_night end) as `Q间夜_香港`
        , sum(case when province_name = '澳门' then room_night end) as `Q间夜_澳门`
        , sum(init_gmv) as `Qgmv`
        , sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q佣金`
        , sum(case when terminal_channel_type = 'app' then room_night end) as `QApp_间夜`
        , sum(case when terminal_channel_type = 'app' then init_gmv end) as `QApp_gmv`
        , sum(case when terminal_channel_type = 'app' and (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            when terminal_channel_type = 'app' then init_commission_after+nvl(ext_plat_certificate,0) end) as `QApp_佣金`
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
        -- and terminal_channel_type = 'app'
        -- and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date = '%(FORMAT_DATE)s'
        and a.order_no <> '103576132435'
    group by 1
    ) q

full outer join
    (select case when substr(checkout_date,1,10) <= date_add('%(FORMAT_DATE)s',124) then substr(checkout_date,1,10) else '远期离店' end as checkout_date
        , sum(extend_info['room_night'] ) as `C间夜量`
        , sum(room_fee)as `Cgmv`
        , sum(comission) as `C佣金`
        , sum(case when terminal_channel_type = 'app' then extend_info['room_night'] end) as `CApp_间夜量`
        , sum(case when terminal_channel_type = 'app' then room_fee end) as `CApp_Cgmv`
        , sum(case when terminal_channel_type = 'app' then comission end) as `CApp_C佣金`
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = '%(FORMAT_DATE)s'
    and extend_info['IS_IBU'] = '0'
    and extend_info['book_channel'] = 'Ctrip'
    and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
    -- and terminal_channel_type = 'app'
    and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
    and substr(order_date,1,10) = '%(FORMAT_DATE)s'
    group by 1
    ) c
on q.checkout_date = c.checkout_date
