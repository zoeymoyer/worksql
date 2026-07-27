select  order_date
    ,a.order_no
    ,room_night as `Q_间夜`
    ,init_gmv as `Q_gmv`  --- 支付价
    ,bp * room_night as `折后底价`  -- 原始底价（采购成本）
    ,sup_base_price * room_num as `原始底价` -- 折后底价（商促折扣后）
    ,case when batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%'
            then init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + nvl(ext_plat_certificate, 0)
            else init_commission_after + nvl(ext_plat_certificate, 0)
        end as `Q_佣金`  --- 2种计算逻辑：①gmv-折后底价 ②补贴前佣金+多倍积分+变现提-定价补-追价补-券补-积分补
    ,case when get_json_object(extendinfomap, '$.adjustBasedPayOnCommission') is not null and get_json_object(extendinfomap, '$.adjustBasedPayOnCommission') != '' then 0
            else coalesce(get_json_object(extendinfomap, '$.bp_adv_amount_realized') * room_night, 0)
        end as `变现提` --- 折后底价QC差 Q90 C100 变现提10
    ,coalesce(get_json_object(extendinfomap, '$.V2_BEAT_AMOUNT_AF'), 0) * room_night as `定价补`  --- 定价补=协议补+平台补
    ,coalesce(get_json_object(extendinfomap, '$.frame_amount'), 0) * room_night + coalesce(cashbackmap['framework_amount'], 0) as `协议补`
    ,coalesce(get_json_object(extendinfomap, '$.platform_amount'), 0) * room_night as `平台补`
    ,case when coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%' then 0
            else nvl(coupon_substract_summary, 0)
        end as `券补`   --- 券补=券补_竞争+券补_叠加+券补_其他+券补_非BU市场+券补_券包
    ,coupon_amount1 + coupon_amount2 + coupon_amount6  as `券补_竞争`
    ,coupon_amount4 + coupon_amount5   as `券补_叠加`
    ,coupon_amount0 + coupon_amount7 + coupon_amount8  as `券补_其他`
    ,coupon_amount9 + coupon_amount10   as `券补_非BU市场`
    ,coupon_amount3 - coalesce(cashbackmap['voucher_pack_price'], 0) as `券补_券包` -- coupon_amount3-券包收入
    ,case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
            then coalesce(follow_price_amount, 0)
        end as `追价补`
    ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) as `积分补`
    ,case when array_contains(supplier_promotion_code, '2913') and qta_supplier_id = '1615667' then coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0)
        end as `多倍积分补`
   -- ,case when get_json_object(extendinfomap, '$.v2_c_incr') is not null then init_gmv * get_json_object(extendinfomap, '$.v2_c_incr') end as `Q_补贴前佣金旧` --- 弃用
    ,coalesce(get_json_object(extendinfomap,'$.basedCommissionAmount'),0) * room_night as `Q_补贴前佣金`
    ,case when get_json_object(extendinfomap, '$.v2_c_incr') is not null then init_gmv end as `Q_补贴前gmv`
    ,get_json_object(extendinfomap, '$.v2_c_incr') as `锚C佣金率`
    ,coalesce(cashbackmap['voucher_amount'], 0) as `券包主券补`
    ,coalesce(cashbackmap['voucher_pack_price'], 0) as `券包收入`
    ,coupon_amount  --- 券补总额=
    ,coupon_amount0
    ,coupon_amount1
    ,coupon_amount2
    ,coupon_amount3
    ,coupon_amount4
    ,coupon_amount5
    ,coupon_amount6
    ,coupon_amount7
    ,coupon_amount8
    ,coupon_amount9
    ,coupon_amount10
from mdw_order_v3_international a
left join (
    select order_no
        ,sum(coupon_amount) as coupon_amount
        ,coalesce(sum(case when coupon_type = 0 then coupon_amount end), 0) as coupon_amount0
        ,coalesce(sum(case when coupon_type = 1 then coupon_amount end), 0) as coupon_amount1
        ,coalesce(sum(case when coupon_type = 2 then coupon_amount end), 0) as coupon_amount2
        ,coalesce(sum(case when coupon_type = 3 then coupon_amount end), 0) as coupon_amount3
        ,coalesce(sum(case when coupon_type = 4 then coupon_amount end), 0) as coupon_amount4
        ,coalesce(sum(case when coupon_type = 5 then coupon_amount end), 0) as coupon_amount5
        ,coalesce(sum(case when coupon_type = 6 then coupon_amount end), 0) as coupon_amount6
        ,coalesce(sum(case when coupon_type = 7 then coupon_amount end), 0) as coupon_amount7
        ,coalesce(sum(case when coupon_type = 8 then coupon_amount end), 0) as coupon_amount8
        ,coalesce(sum(case when coupon_type = 9 then coupon_amount end), 0) as coupon_amount9
        ,coalesce(sum(case when coupon_type = 10 then coupon_amount end), 0) as coupon_amount10
    from (
        select order_no
            ,coalesce(group_code, 0) as coupon_serie
            ,coalesce(cast(split(coupon_substract, ',')[pos] as int), 0) as coupon_amount
            ,coalesce(coupon_detail[group_code], 0) as coupon_type
        from (
            select coupon_detail
                ,batch_series
                ,coupon_substract
                ,coupon_substract_summary
                ,coalesce(cashbackmap['voucher_amount'], 0) as voucher_amount
                ,coalesce(cashbackmap['voucher_pack_price'], 0) as voucher_pack_price
                ,order_no
                ,order_date
                ,init_gmv
            from default.mdw_order_v3_international a
            where dt = '%(DATE)s'
                and order_date between date_add('%(FORMAT_DATE)s', -14) and '%(FORMAT_DATE)s'
                and order_date >= '2026-06-07'
                and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
                and terminal_channel_type = 'app'
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
                and (first_rejected_time is null or date(first_rejected_time) > order_date)
                and (refund_time is null or date(refund_time) > order_date)
                and is_valid = '1'
                and order_no <> '103576132435'
        ) q
        lateral view posexplode(split(batch_series, ',')) t as pos, group_code
    ) z
    group by 1
) c on a.order_no = c.order_no
where dt = '%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
    and (first_rejected_time is null or date(first_rejected_time) > order_date)
    and (refund_time is null or date(refund_time) > order_date)
    and is_valid = '1'
    and order_date between date_sub(current_date, 1) and date_sub(current_date, 1)
    and a.order_no <> '103576132435'