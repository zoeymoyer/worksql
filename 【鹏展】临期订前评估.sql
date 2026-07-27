with q_order as ( --- Q侧订单明细打标
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                -- else '提前订31+' 
                else '提前订15+' 
          end as per_type
          ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
        -- 6. 日期分类：holiday、workday、weekend
        ,dd.date_type,dd.holiday_name
        ,order_no,room_night,user_name,init_gmv
        ,case when is_instant_confirm = '0' then '立即确认' else '非立即确认' end as confirm_type
    from default.mdw_order_v3_international a 
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.order_date = dd.date
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
)
,c_order as ( 
    select substr(o.order_date,1,10) as dt
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订15+'  
          end as per_type
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          -- 6. 日期分类：holiday、workday、weekend
          ,dd.date_type,dd.holiday_name
          ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.order_date,1,10) = dd.date
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
)
,q_agg as (
    select substr(order_date,1,7) as mth
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,sum(room_night) as q_rn
          ,sum(case when confirm_type = '立即确认' then room_night else 0 end) as q_rn_confirm
    from q_order t
    group by substr(order_date,1,7),per_type,linqi_type
    grouping sets (
        (substr(order_date,1,7), per_type), -- 核心维度：提前订
        (substr(order_date,1,7),linqi_type)
    )
)
,c_agg as (
    select substr(dt,1,7) as mth
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,sum(room_night) as c_rn
    from c_order t
    group by substr(dt,1,7),per_type,linqi_type
    grouping sets (
        (substr(dt,1,7), per_type), -- 核心维度：提前订
        (substr(dt,1,7),linqi_type)
    )
)

select t1.mth,t1.per_type
        ,t1.q_rn,t1.q_rn_confirm
        ,t2.c_rn
        ,concat(round(t1.q_rn / (t1.q_rn_mth_twice / 2) * 100, 2),'%') as q_rn_ratio
        ,concat(round(t2.c_rn / (t2.c_rn_mth_twice / 2) * 100, 2),'%') as c_rn_ratio
from (
    select mth,per_type,q_rn,q_rn_confirm,sum(q_rn) over(partition by mth) as q_rn_mth_twice
    from q_agg
)t1
left join (
    select mth,per_type,c_rn,sum(c_rn) over(partition by mth) as c_rn_mth_twice
    from c_agg
)t2 on t1.mth = t2.mth and t1.per_type = t2.per_type
order by t1.mth   
        ,case when t1.per_type = '凌晨订&当天订' then 2
          when t1.per_type = '提前订1-3天' then 3
          when t1.per_type = '提前订4-7天' then 4
          when t1.per_type = '提前订8-14天' then 5
          when t1.per_type = '提前订15+' then 6
          when t1.per_type = '临期订' then 0
          when t1.per_type = '非临期订' then 1
          else 99
     end
;




with linqi_price_base as ( --- 昨天临期D页用户看到的酒店最低报价
    select
        concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
        ,user_name
        ,hotel_seq
        ,substr(checkin_date, 1, 10) as checkin_date
        ,substr(checkout_date, 1, 10) as checkout_date
        ,min(price) as price
    from ihotel_default.dw_hotel_price_display_v2
    where dt = '20260609'
        and get_json_object(extendinfomap, '$.traceId') is not null
        and price is not null
        and datediff(substr(checkin_date, 1, 10), concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))) <= 7
    group by 1,2,3,4,5
)

,price_base as ( --- 过去30天酒店最低报价
    select
        concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
        ,hotel_seq
        ,substr(checkin_date, 1, 10) as checkin_date
        ,substr(checkout_date, 1, 10) as checkout_date
        ,min(price) as price
    from ihotel_default.dw_hotel_price_display_v2
    where dt between '20260511' and '20260609'
        and get_json_object(extendinfomap, '$.traceId') is not null
        and price is not null
    group by 1,2,3,4
)

,price_tag as (
    select
        a.dt
        ,a.user_name
        ,a.hotel_seq
        ,a.checkin_date
        ,a.checkout_date
        ,a.price
        ,min(case when b.dt between date_sub(a.dt, 6) and a.dt then b.price end) as min_price_7d
        ,min(case when b.dt between date_sub(a.dt, 14) and a.dt then b.price end) as min_price_15d
        ,min(case when b.dt between date_sub(a.dt, 29) and a.dt then b.price end) as min_price_30d
    from linqi_price_base a
    left join price_base b
        on a.hotel_seq = b.hotel_seq
        and a.checkin_date = b.checkin_date
        and a.checkout_date = b.checkout_date
        and b.dt between date_sub(a.dt, 29) and a.dt
    group by 1,2,3,4,5,6
)

select
    dt
    ,count(1) as quote_uv_hotel_cnt

    ,sum(case when price <= min_price_7d then 1 else 0 end) as is_7d_low_price_cnt
    ,concat(printf('%.2f', 100 * sum(case when price <= min_price_7d then 1 else 0 end) / count(1)), '%') as is_7d_low_price_ratio

    ,sum(case when price <= min_price_15d then 1 else 0 end) as is_15d_low_price_cnt
    ,concat(printf('%.2f', 100 * sum(case when price <= min_price_15d then 1 else 0 end) / count(1)), '%') as is_15d_low_price_ratio

    ,sum(case when price <= min_price_30d then 1 else 0 end) as is_30d_low_price_cnt
    ,concat(printf('%.2f', 100 * sum(case when price <= min_price_30d then 1 else 0 end) / count(1)), '%') as is_30d_low_price_ratio
from price_tag
group by 1
;