with user_type -----用户首单日
as (
        select user_id,user_name
                , min(order_date) as min_order_date
        from mdw_order_v3_international   --- 海外订单表
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and order_status not in ('CANCELLED', 'REJECTED')
            and is_valid = '1'
        group by 1,2
)
,uv as ----流量
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= date_sub(current_date, 1)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,a.user_name
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,1) and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
)
,flow_left_order as ( --- 流量 LEFT JOIN 订单（同日+同用户）
    select t1.dt,t1.user_id,t1.user_name,t1.user_type,t2.user_type ouser_type,t2.user_id ouid,CASE WHEN t2.user_id IS NOT NULL THEN 1 ELSE 0 END AS has_order_today
    from uv t1
    left join q_order t2 on t1.user_id=t2.user_id and t1.dt=t2.order_date
)
,platform_new as (--- 判定平台新
    select distinct dt,
                    user_pk,
                    user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 1)  and dt <= date_sub(current_date, 1)
        and dict_type = 'pncl_wl_username'
)

SELECT 'flow_left_order' AS src,
    t.dt,
    t.has_order_today,
    t.bucket,
    COUNT(DISTINCT t.user_id) AS users
FROM (
    SELECT
        flo.dt,
        flo.user_id,
        flo.has_order_today,
        CASE
            WHEN (q.min_order_date IS NULL OR flo.dt <= q.min_order_date) THEN
                CASE WHEN pn.user_pk IS NOT NULL THEN '平台新客' ELSE '业务新客' END
            ELSE '老客'
        END AS bucket
    FROM flow_left_order flo
    LEFT JOIN user_type q   ON flo.user_id = q.user_id
    LEFT JOIN platform_new pn ON pn.dt = flo.dt AND pn.user_pk = flo.user_name
) t
GROUP BY 1,2,3,4
union all 
SELECT
    'order_only' AS src,
    t.order_date AS dt,
    1            AS has_order_today,         -- 仅订单口径自然都是当日有单
    t.bucket,
    COUNT(DISTINCT t.user_id) AS users
FROM (
    SELECT
        oi.order_date,
        oi.user_id,
        CASE
            WHEN (q.min_order_date IS NULL OR oi.order_date = q.min_order_date) THEN
                CASE WHEN pn.user_pk IS NOT NULL THEN '平台新客' ELSE '业务新客' END
            ELSE '老客'
        END AS bucket
    FROM q_order oi
    LEFT JOIN user_type q   ON oi.user_id = q.user_id
    LEFT JOIN platform_new pn ON pn.dt = oi.order_date AND pn.user_pk = oi.user_name
) t
GROUP BY 1,2,3,4

;
