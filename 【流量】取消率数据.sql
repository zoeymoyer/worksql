--- 1、T0取消率
with user_type as (-----新老客
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

,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,count(distinct order_no) order_no_q
            ,count(distinct case when (first_cancelled_time is null or date(first_cancelled_time) > order_date)  
                          and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                          and (refund_time is null or date(refund_time) > order_date)
                    then order_no end) no_t0_cancel_order_no_q
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1
)
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
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
        --    ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
        --        when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
        --        when c.area in ('欧洲','亚太','美洲') then c.area
        --        else '其他' end as mdd
        --     ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,count(distinct order_no) order_no_c
            ,count(distinct case when (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
                    then order_no end) no_t0_cancel_order_no_c
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and substr(order_date,1,10) >= '2025-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1
)


select t1.order_date
      ,order_no_q
      ,no_t0_cancel_order_no_q
      ,1 - no_t0_cancel_order_no_q / order_no_q  cancel_rate_q
      ,order_no_c
      ,no_t0_cancel_order_no_c
      ,1 - no_t0_cancel_order_no_c / order_no_c cancel_rate_c
from q_app_order t1
left join c_order t2
on t1.order_date=t2.dt
order by 1
;

--- 2、离店取消率
with q_order as(
    select substr(checkout_date,1,10) as checkout_date
        , count(distinct order_no) as `Q订单`
        , count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end)) as `Q未取消订单-当日`
        , count (distinct (case when order_status = 'CHECKED_OUT' then order_no end)) as `Q已离店订单-总共`
        , (1 - count (distinct (case when order_status = 'CHECKED_OUT' then order_no end))/count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end))) as `Q取消率(不含当日取消）`
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and checkout_date between date_sub(current_date, 90) and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
    group by 1
)

,c_order as(
    SELECT 
        substr(o.checkout_date, 1, 10) AS checkout_date
    ,COUNT(DISTINCT o.order_no) AS `C订单`
    ,COUNT(DISTINCT CASE 
            WHEN o.extend_info['CANCEL_TIME'] IS NULL 
                OR o.extend_info['CANCEL_TIME'] = 'NULL' 
                OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) 
            THEN o.order_no 
        END) AS `C未取消订单-当日`
        ,COUNT(DISTINCT CASE WHEN o.order_status <> 'C' THEN o.order_no END) AS `C已离店订单-总共`
        ,(1 - COUNT(DISTINCT CASE WHEN o.order_status <> 'C' THEN o.order_no END)/COUNT(DISTINCT CASE WHEN o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) THEN o.order_no END)) as `C取消率（不含当日取消）`
    FROM ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    WHERE 
        o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between date_sub(current_date, 90) and date_sub(current_date, 1) -- 退房日期范围
    GROUP BY 1
)
,q_cashback as
(
    SELECT checkout_date,
        count(distinct order_no) as `领取返现订单量`,
        sum (case 
                        when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
            else init_commission_after+nvl(ext_plat_certificate,0) 
        end) as `领取返现订单佣金`,sum(get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount'))as `返现`
    FROM default.mdw_order_v3_international
    WHERE dt = '%(DATE)s'
    and order_status = 'CHECKED_OUT'
    and is_valid = 1
    and checkout_date between date_sub(current_date, 90) and date_sub(current_date, 1)
    and get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount') is not null
    group by 1
    order by checkout_date desc
)
select 
    a.checkout_date
    ,a.`Q订单`
    ,a.`Q未取消订单-当日`
    ,a.`Q已离店订单-总共`
    ,CONCAT(ROUND(a.`Q取消率(不含当日取消）` * 100, 2), '%') AS `Q取消率(不含当日取消）`
    ,b.`C订单`
    ,b.`C未取消订单-当日`
    ,b.`C已离店订单-总共`
    ,CONCAT(ROUND(b.`C取消率（不含当日取消）` * 100, 2), '%') AS `C取消率(不含当日取消）`
    ,CONCAT( ROUND((a.`Q取消率(不含当日取消）`/b.`C取消率（不含当日取消）`) * 100,2),'%') AS `取消率QC`
    ,c.`领取返现订单量`
    ,ROUND(c.`领取返现订单佣金`) as `领取返现订单佣金`
    ,ROUND(c.`返现`) as `返现`
    ,CONCAT( ROUND((c.`领取返现订单量`/a.`Q已离店订单-总共`) * 100,2),'%') AS `领取返现订单占比`
from q_order a
left join c_order b on a.checkout_date = b.checkout_date 
left join q_cashback c on a.checkout_date = c.checkout_date 
order by a.checkout_date desc
;

--- 3、预定取消率
with q_order as(
    select order_date
        , count(distinct order_no) as `Q订单`
        , count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end)) as `Q未取消订单-当日`
        , count (distinct (case when order_status in ('CANCELLED', 'REJECTED') then order_no end)) as `Q取消订单-总共`
        , count (distinct (case when order_status in ('CANCELLED', 'REJECTED') then order_no end))/count(distinct order_no) as `Q取消率`
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and order_date between date_sub(current_date, 90) and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
    group by 1
)

,c_order as(
    SELECT 
        substr(o.order_date, 1, 10) AS order_date
        ,COUNT(DISTINCT o.order_no) AS `C订单`
        ,COUNT(DISTINCT CASE 
                WHEN o.extend_info['CANCEL_TIME'] IS NULL 
                    OR o.extend_info['CANCEL_TIME'] = 'NULL' 
                    OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) 
                THEN o.order_no 
            END) AS `C未取消订单-当日`
        ,COUNT(DISTINCT CASE WHEN o.order_status = 'C' THEN o.order_no END) AS `C取消订单-总共`
        ,COUNT(DISTINCT CASE WHEN o.order_status = 'C' THEN o.order_no END) / COUNT(DISTINCT o.order_no) as `C取消率`
    FROM ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    WHERE 
        o.dt = date_sub(current_date, 1)  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(order_date,1,10) between date_sub(current_date, 90) and date_sub(current_date, 1)
    GROUP BY 1
)

select 
    a.order_date
    ,a.`Q订单`
    ,a.`Q未取消订单-当日`
    ,a.`Q取消订单-总共`
    ,CONCAT(ROUND(a.`Q取消率` * 100, 2), '%') AS `Q取消率`
    ,b.`C订单`
    ,b.`C未取消订单-当日`
    ,b.`C取消订单-总共`
    ,CONCAT(ROUND(b.`C取消率` * 100, 2), '%') AS `C取消率`
    ,CONCAT( ROUND((a.`Q取消率`/b.`C取消率`) * 100,2),'%') AS `取消率QC`

from q_order a
left join c_order b on a.order_date = b.order_date 
order by a.order_date desc
;


---- 分目的地新老取消率