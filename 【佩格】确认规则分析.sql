with display_new as (--- 新报价表
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
        ,get_json_object(extendinfomap, '$.traceId') as traceId
        ,substring_index(room_id, '_', 1) as room_id
        ,physical_room_id
        ,case  when get_json_object(extendInfoMap, '$.homogenizationKey') like '%NO_INSTANCE_CONFIRM%' then '非立即确认'
                when get_json_object(extendInfoMap, '$.homogenizationKey') like '%INSTANCE_CONFIRM%' then '立即确认'
            else '其他' end as confirmation_type  -- 确认规则
        ,product_room_index  -- 产品房型排序
        ,hotel_seq,user_name
    from ihotel_default.dw_hotel_price_display_v2
    where dt = replace(date_sub(current_date, 1),'-','')
        and get_json_object(extendinfomap, '$.traceId') is not null
        and substring_index(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7,8
)
,q_order as (
    select  order_date
        ,get_json_object(extendinfomap,'$.traceId') as traceId
        ,user_info['orig_device_id'] as orig_device_id
        ,hotel_seq
        ,room_night
        ,a.order_no
        ,qta_product_id
        ,physical_room_id
        ,max_c
        ,unix_timestamp(first_confirmed_time) - unix_timestamp(order_time) as confirm_cost_seconds -- 确认耗时(秒)
    from default.mdw_order_v3_international a
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date = date_sub(current_date, 1)
        and get_json_object(extendinfomap,'$.traceId') is not null
)
,compare_result as (--- 产品力比价结果 
    select  concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2)) as order_date
        , qunar_price_info['traceId'] as trace_id
        , a.id
        , split(qunar_product_room_id,'_')[0] as qunar_product_room_id
        , pay_price_compare_result
        , -pay_price_diff as pay_price_diff
        , ctrip_pay_price
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    where a.dt = replace(date_sub(current_date, 1),'-','')
        and business_type='intl_crawl_cq_api_userview'
        and compare_type='SIMILAR_PRODUCT_LOWEST' --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
        and room_type_cover='Qmeet'
        and ctrip_room_status='true'
        and qunar_room_status='true'
)
,display_total as (--- 当日整体报价曝光
    select dt
        ,count(distinct traceId) as display_pv_all
        ,count(distinct user_name) as display_uv_all
    from display_new
    group by 1
)
,display_metric as (--- 报价曝光及产品位次
    select dt
        ,confirmation_type
        ,count(distinct traceId) as display_pv
        ,count(distinct user_name) as display_uv
        ,avg(product_room_index) as avg_product_room_index
    from display_new
    group by 1,2
)
,display_order_concat as (--- 报价曝光与产单数据关联
    select t1.dt
        ,t1.confirmation_type
        ,t2.order_no
        ,t2.room_night
        ,t2.confirm_cost_seconds
    from display_new t1 
    left join q_order t2 on t1.traceId = t2.traceId and t1.physical_room_id = t2.physical_room_id 
        and t1.room_id = t2.qta_product_id and t1.hotel_seq = t2.hotel_seq and t1.dt = t2.order_date
)
,order_metric as (--- 确认体验及产单指标
    select dt
        ,confirmation_type
        ,avg(case when confirm_cost_seconds >= 0 then confirm_cost_seconds end) / 60.0 as avg_confirm_cost_min
        ,percentile_approx(case when confirm_cost_seconds >= 0 then confirm_cost_seconds end,0.8) / 60.0 as p80_confirm_cost_min
        ,percentile_approx(case when confirm_cost_seconds >= 0 then confirm_cost_seconds end,0.5) / 60.0 as p50_confirm_cost_min
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
    from display_order_concat
    group by 1,2
)
,order_total as (--- 当日整体匹配订单
    select dt
        ,sum(room_night) as room_night_all
    from display_order_concat
    group by 1
)
,display_compare_concat as (--- 报价与比价结果匹配
    select t1.dt,t1.confirmation_type,t2.id,t2.pay_price_compare_result,t2.pay_price_diff,t2.ctrip_pay_price
    from display_new t1
    left join compare_result t2 on t1.traceId = t2.trace_id and t1.room_id = t2.qunar_product_room_id 
)
,compare_metric as (--- 产品力指标
    select dt
        ,confirmation_type
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as pay_price_lose_rate --支付价lose率
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as pay_price_beat_depth  --支付价beat深度
    from display_compare_concat
    group by 1,2
)
,order_all as (
    select order_date as dt
        ,count(order_no) as order_all
    from q_order
    group by 1
)

select t1.dt `日期`
    ,t1.confirmation_type `分类`
    ,round(t2.avg_confirm_cost_min, 2) `平均确认时长(min)`
    ,round(t2.p80_confirm_cost_min, 2) `80分位确认时长(min)`
    ,round(t2.p50_confirm_cost_min, 2) `50分位确认时长(min)`

    ,round(t1.display_pv / t3.display_pv_all,6) as `报价曝光占比pv`
    ,round(t1.display_uv / t3.display_uv_all,6) as `报价曝光占比uv`
    ,round(t1.avg_product_room_index, 2) `平均位次`
    ,round(t4.pay_price_lose_rate, 6) `支付价lose率`
    ,-round(t4.pay_price_beat_depth, 6) `支付价beat深度`
    ,round(t2.room_night / t5.room_night_all,6) as `间夜占比`
    ,t2.room_night `间夜量`
    ,round(t2.order_cnt / t1.display_uv,6) as `报价转化率uv`
    ,round(t2.order_cnt / t1.display_pv,6) as `报价转化率pv`

    ,t1.display_pv `报价曝光量`
    ,t1.display_uv `报价曝光UV`
    ,t2.order_cnt `订单量`
    ,t5.room_night_all `总间夜量`
    ,t3.display_pv_all `整体报价曝光量`
    ,t3.display_uv_all `整体报价曝光UV`  
    ,t6.order_all  `大盘预定订单量`
    ,round(t2.order_cnt / t6.order_all,6)  `匹配率`
from display_metric t1
left join order_metric t2 on t1.dt = t2.dt and t1.confirmation_type = t2.confirmation_type
left join display_total t3 on t1.dt = t3.dt
left join compare_metric t4 on t1.dt = t4.dt and t1.confirmation_type = t4.confirmation_type
left join order_total t5 on t1.dt = t5.dt
left join order_all t6 on t1.dt = t6.dt
order by 1 desc
    ,case when t1.confirmation_type = '立即确认' then 1
          when t1.confirmation_type = '非立即确认' then 2
          else 99 end
;

--- 2、服务赔付数据
with display_new as (--- 新报价表
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
        ,get_json_object(extendinfomap, '$.traceId') as traceId
        ,substring_index(room_id, '_', 1) as room_id
        ,physical_room_id
        ,case  when get_json_object(extendInfoMap, '$.homogenizationKey') like '%NO_INSTANCE_CONFIRM%' then '非立即确认'
                when get_json_object(extendInfoMap, '$.homogenizationKey') like '%INSTANCE_CONFIRM%' then '立即确认'
            else '其他' end as confirmation_type  -- 确认规则
        ,product_room_index  -- 产品房型排序
        ,hotel_seq,user_name
    from ihotel_default.dw_hotel_price_display_v2
    where dt >= '20260515' and dt <= '20260530'
        and get_json_object(extendinfomap, '$.traceId') is not null
        and substring_index(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7,8
)
,q_order as (
    select  order_date
        ,get_json_object(extendinfomap,'$.traceId') as traceId
        ,user_info['orig_device_id'] as orig_device_id
        ,hotel_seq
        ,room_night
        ,a.order_no
        ,qta_product_id
        ,physical_room_id
        ,max_c
        ,unix_timestamp(first_confirmed_time) - unix_timestamp(order_time) as confirm_cost_seconds -- 确认耗时(秒)
    from default.mdw_order_v3_international a
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= '2026-05-15'
        and get_json_object(extendinfomap,'$.traceId') is not null
)
,display_order_concat as (--- 报价曝光与产单数据关联
    select t1.dt
        ,t1.confirmation_type
        ,t2.order_no
        ,t2.room_night
        ,t2.confirm_cost_seconds
    from display_new t1 
    left join q_order t2 on t1.traceId = t2.traceId and t1.physical_room_id = t2.physical_room_id 
        and t1.room_id = t2.qta_product_id and t1.hotel_seq = t2.hotel_seq and t1.dt = t2.order_date
)
,compensate_detail as (--- 赔付数据
    select aa.id as compensate_id
        ,substr(aa.pay_time, 1, 10) as pay_date
        ,aa.order_no
        ,aa.total_amount
    from fuwu.dwd_compensate_htl_finish_to_flight_di aa
    where aa.dt between '2023-01-01' and date_sub(current_date, 1)
        and substr(aa.pay_time, 1, 10) >= '2026-05-15'
        and substr(aa.pay_time, 1, 10) <= date_sub(current_date, 1)
)
,display_ord_compensate as (
    select t1.confirmation_type,t1.order_no,t2.pay_date,t1.dt,t2.total_amount
    from display_order_concat t1
    left join compensate_detail t2 on t1.order_no = t2.order_no
)
,serviceDefectRate as (-- 服务缺陷率
    select confirmation_type
        ,total as `产单量`
        ,concat(round(((a / total * 3.5) + (b / total * 0.2) + (c / total) + (d / total) + (e / total * 0.3) + (f / total * 0.3)) * 100, 2), '%') as `加权缺陷率`
        ,round(a / total * 3.5 * 100, 2) as `到店无房率`
        ,round(b / total * 0.2 * 100, 2) as `到店无预订率`
        -- ,round((e + f) / total * 0.3 * 100, 2) as `确认前推翻率`
        -- ,round((c + d) / total * 100, 2) as `确认后推翻率`
        ,round(e / total * 0.3 * 100, 2) as `确认前满房率`
        ,round(f / total * 100, 2) as `确认前涨价率`
        ,round(c / total * 0.3 * 100, 2) as `确认后满房率`
        ,round(d / total * 100, 2) as `确认后涨价率`
        ,a as `到店无房`
        ,b as `到店无预订`
        -- ,(e + f) as `确认前推翻`
        -- ,(c + d) as `确认后推翻`
        ,e as `确认前满房`
        ,f as `确认前涨价`
        ,c as `确认后满房`
        ,d as `确认后涨价`
        ,total - a - b - c - d - e - f as `无拒单`
    from (
        select confirmation_type
            ,count(distinct case when complain_type = '到店无房' then aa.order_no else null end) as a
            ,count(distinct case when complain_type = '到店无预订' then aa.order_no else null end) as b
            ,count(distinct case when complain_type = '确认后满房' then aa.order_no else null end) as c
            ,count(distinct case when complain_type = '确认后涨价' then aa.order_no else null end) as d
            ,count(distinct case when complain_type = '确认前满房' then aa.order_no else null end) as e
            ,count(distinct case when complain_type = '确认前涨价' then aa.order_no else null end) as f
            ,count(distinct case when complain_type = '无拒单' then aa.order_no else null end) as i
            ,count(distinct aa.order_no) as total
        from (
            select order_no
                ,hotel_id
                ,complain_type
                ,checkin_date
                ,country
                ,province
                ,balance_type
                ,is_guarantee
                ,pay_status
                ,case when defect_type is null then complain_type else defect_type end as complain_type_new
            from fuwu.dwd_ord_htl_servicequality_di
            where dt between '%(FORMAT_DATE_365)s' and '%(FORMAT_DATE)s'
                and sale_channel = 'Q2Q' -- 勿动
                and is_international = '1' -- 勿动
                and order_status <> 'DELETE'
                and (((balance_type = 'PROXY' or is_guarantee = 1) and pay_status not in ('PAY', 'PAY_FAILED')) or (balance_type = 'CASH' and is_guarantee = '0')) -- 勿动
                and checkin_date between '2026-05-15' and date_sub(current_date, 1)
        ) aa
        left join display_order_concat o on o.order_no = aa.order_no
        group by 1
    ) bb
)

select t1.confirmation_type
    ,t1.order_cnt as `产单量`
    ,t1.compensate_order_cnt as `赔付单量`
    ,concat(round(t1.compensate_order_rate * 100, 2), '%') as `赔付单占比`
    ,round(t1.compensate_amount, 2) as `赔付金额`
    ,t2.`产单量`
    ,t2.`加权缺陷率`
    ,t2.`到店无房率`
    ,t2.`到店无预订率`
    ,t2.`确认前满房率`
    ,t2.`确认前涨价率`
    ,t2.`确认后满房率`
    ,t2.`确认后涨价率`
    ,t2.`到店无房`
    ,t2.`到店无预订`
    ,t2.`确认前满房`
    ,t2.`确认前涨价`
    ,t2.`确认后满房`
    ,t2.`确认后涨价`
    ,t2.`无拒单`
from (
    select confirmation_type
        ,count(distinct order_no) as order_cnt
        ,count(distinct case when pay_date is not null then order_no end) as compensate_order_cnt
        ,round(count(distinct case when pay_date is not null then order_no end) / count(distinct order_no), 6) as compensate_order_rate
        ,round(sum(total_amount), 2) as compensate_amount
    from display_ord_compensate
    group by 1
)t1 left join serviceDefectRate t2 on t1.confirmation_type = t2.confirmation_type
;




--- 赔付数据
with order_detail as (
    select checkout_date as datee
        ,case when physical_room_name = '待确认房型' then '特价房' else '非特价房' end as room_type
        ,order_no
        ,room_night
        ,case when batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%'
              then final_commission_after
                 + coalesce(cast(split(coupon_info['23base_ZK_728810'], '_')[1] as double), 0)
                 + coalesce(cast(split(coupon_info['23extra_ZK_ce6f99'], '_')[1] as double), 0)
                 + coalesce(ext_plat_certificate, 0)
              else final_commission_after + coalesce(ext_plat_certificate, 0) end as final_commission_after
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
        and checkout_date >= date_sub(current_date, 30)
        and checkout_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info as (
    select datee
        ,room_type
        ,count(distinct order_no) as order_nos
        ,sum(room_night) as room_nights
        ,sum(final_commission_after) as order_commission
    from order_detail
    group by 1,2
    union all
    select datee
        ,'总量' as room_type
        ,count(distinct order_no) as order_nos
        ,sum(room_night) as room_nights
        ,sum(final_commission_after) as order_commission
    from order_detail
    group by 1
)
,day_total_order as (
    select datee
        ,sum(room_night) as total_room_nights
    from order_detail
    group by 1
)
,compensate_detail as (
    select distinct r1.compensate_id
        ,r1.pay_date as datee
        ,r1.order_no
        ,r3.room_type
        ,r1.total_amount
    from (
        select aa.id as compensate_id
            ,substr(aa.pay_time, 1, 10) as pay_date
            ,aa.order_no
            ,aa.total_amount
        from fuwu.dwd_compensate_htl_finish_to_flight_di aa
        where aa.dt between '2023-01-01' and date_sub(current_date, 1)
            and substr(aa.pay_time, 1, 10) >= date_sub(current_date, 30)
            and substr(aa.pay_time, 1, 10) <= date_sub(current_date, 1)
    ) r1
    inner join (
        select order_no
            ,case when physical_room_name = '待确认房型' then '特价房' else '非特价房' end as room_type
        from default.mdw_order_v3_international
        where dt = '%(DATE)s'
    ) r3
        on r1.order_no = r3.order_no
)
,compensate_info as (
    select datee
        ,room_type
        ,count(distinct order_no) as compensate_order_nos
        ,sum(total_amount) as compensate_amount
    from compensate_detail
    group by 1,2
    union all
    select datee
        ,'总量' as room_type
        ,count(distinct order_no) as compensate_order_nos
        ,sum(total_amount) as compensate_amount
    from compensate_detail
    group by 1
)

select o.datee as `日期`
    ,o.room_type as `房型`
    ,o.order_nos as `订单量`
    ,o.room_nights as `间夜量`
    ,concat(round(case when nvl(t.total_room_nights, 0) = 0 then 0 else o.room_nights / t.total_room_nights * 100 end, 2), '%') as `间夜占比`
    ,round(o.order_commission, 2) as `订单收益额`
    ,nvl(c.compensate_order_nos, 0) as `赔付单量`
    ,concat(round(case when nvl(o.order_nos, 0) = 0 then 0 else nvl(c.compensate_order_nos, 0) / o.order_nos * 100 end, 2), '%') as `赔付单占比`
    ,round(nvl(c.compensate_amount, 0), 2) as `赔付金额`
    ,round(case when nvl(o.room_nights, 0) = 0 then 0 else nvl(c.compensate_amount, 0) / o.room_nights end, 2) as `单间夜赔付金额`
from order_info o
left join compensate_info c
    on o.datee = c.datee
    and o.room_type = c.room_type
left join day_total_order t
    on o.datee = t.datee
order by o.datee desc
    ,case when o.room_type = '总量' then 0
          when o.room_type = '特价房' then 1
          when o.room_type = '非特价房' then 2
          else 3 end
;

--- 加权缺陷路
with serviceDefectRate as (
    select checkin_date as `入住日期`
        ,if_tj
        ,total as `产单量`
        ,concat(round(((a / total * 3.5) + (b / total * 0.2) + (c / total) + (d / total) + (e / total * 0.3) + (f / total * 0.3)) * 100, 2), '%') as `加权缺陷率`
        ,round(a / total * 3.5 * 100, 2) as `到店无房率`
        ,round(b / total * 0.2 * 100, 2) as `到店无预订率`
        -- ,round((e + f) / total * 0.3 * 100, 2) as `确认前推翻率`
        -- ,round((c + d) / total * 100, 2) as `确认后推翻率`
        ,round(e / total * 0.3 * 100, 2) as `确认前满房率`
        ,round(f / total * 100, 2) as `确认前涨价率`
        ,round(c / total * 0.3 * 100, 2) as `确认后满房率`
        ,round(d / total * 100, 2) as `确认后涨价率`
        ,a as `到店无房`
        ,b as `到店无预订`
        -- ,(e + f) as `确认前推翻`
        -- ,(c + d) as `确认后推翻`
        ,e as `确认前满房`
        ,f as `确认前涨价`
        ,c as `确认后满房`
        ,d as `确认后涨价`
        ,total - a - b - c - d - e - f as `无拒单`
    from (
        select aa.checkin_date
            ,case when physical_room_name = '待确认房型' then '特价房' else '非特价房' end as if_tj
            ,count(distinct case when complain_type = '到店无房' then aa.order_no else null end) as a
            ,count(distinct case when complain_type = '到店无预订' then aa.order_no else null end) as b
            ,count(distinct case when complain_type = '确认后满房' then aa.order_no else null end) as c
            ,count(distinct case when complain_type = '确认后涨价' then aa.order_no else null end) as d
            ,count(distinct case when complain_type = '确认前满房' then aa.order_no else null end) as e
            ,count(distinct case when complain_type = '确认前涨价' then aa.order_no else null end) as f
            ,count(distinct case when complain_type = '无拒单' then aa.order_no else null end) as i
            ,count(distinct aa.order_no) as total
        from (
            select order_no
                ,hotel_id
                ,complain_type
                ,checkin_date
                ,country
                ,province
                ,balance_type
                ,is_guarantee
                ,pay_status
                ,case when defect_type is null then complain_type else defect_type end as complain_type_new
            from fuwu.dwd_ord_htl_servicequality_di
            where dt between '%(FORMAT_DATE_365)s' and '%(FORMAT_DATE)s'
                and sale_channel = 'Q2Q' -- 勿动
                and is_international = '1' -- 勿动
                and order_status <> 'DELETE'
                and (((balance_type = 'PROXY' or is_guarantee = 1) and pay_status not in ('PAY', 'PAY_FAILED')) or (balance_type = 'CASH' and is_guarantee = '0')) -- 勿动
                and checkin_date between date_sub(current_date, 30) and date_sub(current_date, 1)
        ) aa
        left join default.mdw_order_v3_international o
            on o.dt = '%(DATE)s'
            and o.order_no = aa.order_no
        group by 1,2
    ) bb
)
select *
from serviceDefectRate
;

--- 3、产品力数据
with qc_price as (
    select order_date
        ,business_type_name
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result,business_type,check_out,check_in
            ,case when business_type = 'intl_crawl_cq_spa' then '抓取'
                  when business_type = 'intl_crawl_cq_api_order' then '生单'
                  when business_type = 'intl_crawl_cq_api_userview_acc' then '主站模拟券后'
                  else '其他' end as business_type_name
            -- 【新增】: 使用解析后的 order_date 和 check_in 日期计算提前订分布
            ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) < 0 or datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) = 0 then '凌晨订&当天订'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 1 and 3    then '提前订1-3天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 4 and 7    then '提前订4-7天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14   then '提前订8-14天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
             end as per_type
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260410' and dt <= replace(date_sub(current_date, 1),'-','')
            -- and business_type = 'intl_crawl_cq_spa'  -- intl_crawl_cq_spa 抓取  intl_crawl_cq_api_order 生单  intl_crawl_cq_api_userview 主站（流量） intl_crawl_cq_api_userview_acc 主站模拟券后价
            and business_type in ('intl_crawl_cq_spa', 'intl_crawl_cq_api_order', 'intl_crawl_cq_api_userview_acc')
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
    )t
    group by 1,2,cube(user_type,mdd)
)

select order_date
      ,business_type_name
      ,mdd
      ,user_type
      ,`支付价lose率`
      ,`支付价lose深度`
      ,`支付价beat深度`
      ,`支付价beat数`         / `支付价抓取次数`  `beat率`
      ,`支付价beat0-3%次数`   / `支付价抓取次数`  `支付价beat0-3%率`
      ,`支付价beat3-4%次数`   / `支付价抓取次数`  `支付价beat3-4%率`
      ,`支付价beat4-5%次数`   / `支付价抓取次数`  `支付价beat4-5%率`
      ,`支付价beat5-6%次数`   / `支付价抓取次数`  `支付价beat5-6%率`
      ,`支付价beat6-7%次数`   / `支付价抓取次数`  `支付价beat6-7%率`
      ,`支付价beat7-8%次数`   / `支付价抓取次数`  `支付价beat7-8%率`
      ,`支付价beat8%以上次数`  / `支付价抓取次数`  `支付价beat8%以上率`
from qc_price
order by 1,2,3,4
;



with display_new as (--- 新报价表
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
        ,get_json_object(extendinfomap, '$.traceId') as traceId
        ,substring_index(room_id, '_', 1) as room_id
        ,physical_room_id
        ,case  when get_json_object(extendInfoMap, '$.homogenizationKey') like '%NO_INSTANCE_CONFIRM%' then '非立即确认'
                when get_json_object(extendInfoMap, '$.homogenizationKey') like '%INSTANCE_CONFIRM%' then '立即确认'
            else '其他' end as confirmation_type  -- 确认规则
        ,product_room_index  -- 产品房型排序
        ,hotel_seq,user_name
    from ihotel_default.dw_hotel_price_display_v2
    where dt >= replace(date_sub(current_date, 3),'-','') and dt <= replace(date_sub(current_date, 1),'-','')
        and get_json_object(extendinfomap, '$.traceId') is not null
        and substring_index(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7,8
)
,q_order as (
    select  order_date
        ,get_json_object(extendinfomap,'$.traceId') as traceId
        ,user_info['orig_device_id'] as orig_device_id
        ,hotel_seq
        ,room_night
        ,a.order_no
        ,qta_product_id
        ,physical_room_id
        ,max_c
        ,unix_timestamp(first_confirmed_time) - unix_timestamp(order_time) as confirm_cost_seconds -- 确认耗时(秒)
        ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) <= 7   then '提前0-7天'
                when datediff(checkin_date, order_date) between 8 and 15   then '提前订8-15天'
                else '提前订15天以上' end as pre_order_days
    from default.mdw_order_v3_international a
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= date_sub(current_date, 3) and order_date <= date_sub(current_date, 1)
        and get_json_object(extendinfomap,'$.traceId') is not null
)
,display_total as (--- 当日整体报价曝光
    select dt
        ,count(distinct traceId) as display_pv_all
        ,count(distinct user_name) as display_uv_all
    from display_new
    group by 1
)

,display_order_concat as (--- 报价曝光与产单数据关联
    select t1.dt
        ,t1.confirmation_type
        ,t2.order_no
        ,t2.room_night
        ,t2.confirm_cost_seconds
        ,t2.pre_order_days
    from display_new t1 
    left join q_order t2 on t1.traceId = t2.traceId and t1.physical_room_id = t2.physical_room_id 
        and t1.room_id = t2.qta_product_id and t1.hotel_seq = t2.hotel_seq and t1.dt = t2.order_date
)
,order_metric as (--- 确认体验及产单指标
    select dt
        ,confirmation_type,pre_order_days
        ,avg(case when confirm_cost_seconds >= 0 then confirm_cost_seconds end) / 60.0 as avg_confirm_cost_min
        ,percentile_approx(case when confirm_cost_seconds >= 0 then confirm_cost_seconds end,0.8) / 60.0 as p80_confirm_cost_min
        ,percentile_approx(case when confirm_cost_seconds >= 0 then confirm_cost_seconds end,0.5) / 60.0 as p50_confirm_cost_min
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
    from display_order_concat
    group by 1,2,3
)


select t1.dt `日期`
    ,t1.confirmation_type `分类`
    ,pre_order_days `提前订分布`
    ,round(t1.avg_confirm_cost_min, 2) `平均确认时长(min)`
    ,round(t1.p80_confirm_cost_min, 2) `80分位确认时长(min)`
    ,round(t1.p50_confirm_cost_min, 2) `50分位确认时长(min)`
    ,t2.room_night `间夜量`
    ,t2.order_cnt `订单量`
from order_metric t1
order by 1 desc
    ,case when t1.confirmation_type = '立即确认' then 1
          when t1.confirmation_type = '非立即确认' then 2
          else 99 end
;




--- 报价生单人数不一致统计
with display_new as (--- 新报价表
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
        ,get_json_object(extendinfomap, '$.traceId') as traceId
        ,substring_index(room_id, '_', 1) as room_id
        ,physical_room_id
        ,hotel_seq,adults_num
    from ihotel_default.dw_hotel_price_display_v2
    where dt >= replace(date_sub(current_date, 1),'-','') and dt <= replace(date_sub(current_date, 1),'-','')
        and get_json_object(extendinfomap, '$.traceId') is not null
        and substring_index(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6
)
,q_order as (
    select  order_date
        ,get_json_object(extendinfomap,'$.traceId') as traceId
        ,user_info['orig_device_id'] as orig_device_id
        ,hotel_seq
        ,room_night
        ,a.order_no
        ,qta_product_id
        ,physical_room_id
        ,max_c
        ,unix_timestamp(first_confirmed_time) - unix_timestamp(order_time) as confirm_cost_seconds -- 确认耗时(秒)
    from default.mdw_order_v3_international a
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= date_sub(current_date, 3) and order_date <= date_sub(current_date, 1)
        and get_json_object(extendinfomap,'$.traceId') is not null
)
,display_order_concat as (--- 报价曝光与产单数据关联
    select t1.dt,t1.adults_num,t2.max_c
        ,t2.order_no
        ,t2.room_night
    from display_new t1 
    left join q_order t2 on t1.traceId = t2.traceId and t1.physical_room_id = t2.physical_room_id 
        and t1.room_id = t2.qta_product_id and t1.hotel_seq = t2.hotel_seq and t1.dt = t2.order_date
)