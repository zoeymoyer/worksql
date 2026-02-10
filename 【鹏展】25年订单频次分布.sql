with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,row_number() over(partition by order_date,a.user_id order by order_time) rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,big_order_info as (--- 多单用户
    select user_id
    from (
        select user_id,count(distinct t1.order_no)order_no
        from q_order t1
        group by 1
    ) where order_no >= 7
)
,overlap_user_q as  (  --- 搬单订单逻辑：若A订单与B订单入离时间存在重叠，判断为搬单订单
    select distinct order_no
          , user_id
          , checkin_date
          , checkout_date_next
          , checkout_date
          , checkout_date_last
          , case when checkout_date_next <= checkout_date then 1 
               when checkout_date_last >= checkout_date then 1 
               else 0 end as overlap_ord
    from (
        select distinct user_id
            , order_no
            , checkin_date
            , checkout_date
            , lead(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_next
            , lag(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_last
        from mdw_order_v3_international 
        where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
            -- and terminal_channel_type in ('www','app','touch')
            and terminal_channel_type = 'app'
            -- and order_status not in ('CANCELLED','REJECTED')
            and is_valid = '1'
            and checkout_date between '2025-01-01' and '%(FORMAT_DATE)s'
            and order_no <> '103576132435'
    ) z
)

select order_no
        ,count(1) uv
        ,count(1) / sum(count(1)) over () rate
from (
    select user_id,count(distinct t1.order_no)order_no
    from q_order t1
    left join (
            select t1.user_id 
            from big_order_info t1
            left join overlap_user_q t2 on t1.user_id=t2.user_id
            where t2.overlap_ord = 1
            and t2.user_id is not null
            group by 1
    ) t2 on t1.user_id=t2.user_id
    where t1.user_type = '老客'
    and t2.user_id is not null
    group by 1
)t group by 1
order by 1
;


---- 实际使用
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,row_number() over(partition by order_date,a.user_id order by order_time) rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,overlap_user_q as  (  --- 搬单订单逻辑：若A订单与B订单入离时间存在重叠，判断为搬单订单
    select distinct order_no
          , user_id
          , checkin_date
          , checkout_date_next
          , checkout_date
          , checkout_date_last
          , case when checkout_date_next <= checkout_date then 1 
               when checkout_date_last >= checkout_date then 1 
               else 0 end as overlap_ord
    from (
        select distinct user_id
            , order_no
            , checkin_date
            , checkout_date
            , lead(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_next
            , lag(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_last
        from mdw_order_v3_international 
        where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
            -- and terminal_channel_type in ('www','app','touch')
            and terminal_channel_type = 'app'
            -- and order_status not in ('CANCELLED','REJECTED')
            and is_valid = '1'
            and checkout_date between '2025-01-01' and '2026-02-04'
            and order_no <> '103576132435'
    ) z
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= date_sub(current_date, 30)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3
)



select count(distinct  t1.user_id) uv,count(distinct t2.user_id)
        
from (--- 老客订单数量-剔除搬单订单
    select user_id
          ,count(distinct t1.order_no) order_no
    from q_order t1
    left join (  --- 搬单订单
            select order_no 
            from overlap_user_q
            where overlap_ord = 1
            group by 1
    ) t2 on t1.order_no=t2.order_no
    where t1.user_type = '老客'
    and t2.order_no is null
    group by 1
)t1 
left join (--- 近期活跃
    select  a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= date_sub(current_date, 30)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2
) t2 on  t1.user_id=t2.user_id
where t1.order_no >= 6
;



---- tagger标签建设
select t1.user_name key, 1 value
        
from (--- 老客订单数量-剔除搬单订单
    select user_name
          ,count(distinct t1.order_no) order_no
    from (----订单明细表
        select order_date
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
                ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
                ,a.user_id,user_name,order_no,init_gmv,room_night
        from default.mdw_order_v3_international a 
        left join (-----新老客
                select user_id
                        ,min(order_date) as min_order_date
                from default.mdw_order_v3_international   --- 海外订单表
                where dt = '$QDATE(-1,'yyyyMMdd')' 
                    and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                    and terminal_channel_type in ('www', 'app', 'touch')
                    and order_status not in ('CANCELLED', 'REJECTED')
                    and is_valid = '1'
                group by 1
        ) b on a.user_id = b.user_id 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where dt = '$QDATE(-1,'yyyyMMdd')' 
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and order_status not in ('CANCELLED','REJECTED')
            and order_no <> '103576132435'
            and order_date >= '2025-01-01' and order_date <= '$QDATE(-1,'yyyy-MM-dd')' 
    ) t1
    left join (  --- 搬单订单
            select order_no 
            from (  --- 搬单订单逻辑：若A订单与B订单入离时间存在重叠，判断为搬单订单
                select distinct order_no
                    , user_id
                    , checkin_date
                    , checkout_date_next
                    , checkout_date
                    , checkout_date_last
                    , case when checkout_date_next <= checkout_date then 1 
                        when checkout_date_last >= checkout_date then 1 
                        else 0 end as overlap_ord
                from (
                    select distinct user_id
                        , order_no
                        , checkin_date
                        , checkout_date
                        , lead(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_next
                        , lag(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_last
                    from default.mdw_order_v3_international 
                    where dt = '$QDATE(-1,'yyyyMMdd')' 
                        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
                        and terminal_channel_type = 'app'
                        and is_valid = '1'
                        and checkout_date >= '2025-01-01' and checkout_date <= '$QDATE(-1,'yyyy-MM-dd')' 
                        and order_no <> '103576132435'
                ) z
            )z
            where overlap_ord = 1
            group by 1
    ) t2 on t1.order_no=t2.order_no
    where t1.user_type = '老客'
    and t2.order_no is null
    group by 1
)t1 
left join (
    select  a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= '$QDATE(-30,'yyyy-MM-dd')' 
       and dt <= '$QDATE(-1,'yyyy-MM-dd')' 
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2
) t2 on  t1.user_name=t2.user_name
LEFT JOIN ( --排除之前打开过该push的user_name
    select 
        username as user_name
    from pp_pub.dws_usertouch_username_send_arrive_click_order_label_di 
    where dt > '2026-02-05'
        and task_category in('营销push')
        and is_click = '1'
        and task_id IN (346730) --  填上所有task
    group by 1
) c on t1.user_name = c.user_name
where t1.order_no >= 6 
and t2.user_name is not null
and c.user_name is not null
GROUP by 1
;