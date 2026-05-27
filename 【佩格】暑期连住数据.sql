with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order_app as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade,user_name
            ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
            else '其他' end as order_date_type
            ,datediff(checkout_date, checkin_date) as stay_days
            ,case when datediff(checkout_date, checkin_date) <= 6 then datediff(checkout_date, checkin_date) else '7+' end as stay_days_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
            or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
            or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
            or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
        )
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
)
,c_user_type as(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                   when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                   when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                   when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                   when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                   when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
            else '其他' end as order_date_type
            ,datediff(checkout_date, checkin_date) stay_days
            ,case when datediff(checkout_date, checkin_date) <= 6 then datediff(checkout_date, checkin_date) else '7+' end as stay_days_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
        and terminal_channel_type = 'app'
        and order_status <> 'C'
        and (
            (checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
            or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
            or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
            or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
        )
)

select t1.order_date_type
        ,t1.user_type as user_type1
        ,t1.order_cnt as app_order_cnt
        ,t1.room_night as app_room_night
        ,t1.avg_room_night as app_avg_room_night
        ,t1.conv_ord_cnt as app_conv_ord_cnt
        ,t1.conv_room_night as app_conv_room_night
        ,t1.conv_avg_room_night as app_conv_avg_room_night
        ,concat(round(t1.conv_rate * 100, 2), '%') as app_conv_rate
        ,t2.order_cnt as c_order_cnt
        ,t2.room_night as c_room_night
        ,t2.avg_room_night as c_avg_room_night
        ,t2.conv_ord_cnt as c_conv_ord_cnt
        ,t2.conv_room_night as c_conv_room_night
        ,t2.conv_avg_room_night as c_conv_avg_room_night
        ,concat(round(t2.conv_rate * 100, 2), '%') as c_conv_rate
from (
    select order_date_type
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
        ,sum(room_night) / count(distinct order_no) as avg_room_night
        ,count(distinct case when stay_days >= 2 then order_no end) as conv_ord_cnt  --- 连住订单 
        ,sum(case when stay_days >= 2 then room_night end) as conv_room_night
        ,sum(case when stay_days >= 2 then room_night end) / count(distinct case when stay_days >= 2 then order_no end) as conv_avg_room_night
        ,count(distinct case when stay_days >= 2 then order_no end) / count(distinct order_no) as conv_rate
    from q_order_app
    group by 1
)t1 
left join (
    select order_date_type
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
        ,sum(room_night) / count(distinct order_no) as avg_room_night
        ,count(distinct case when stay_days >= 2 then order_no end) as conv_ord_cnt  --- 连住订单 
        ,sum(case when stay_days >= 2 then room_night end) as conv_room_night
        ,sum(case when stay_days >= 2 then room_night end) / count(distinct case when stay_days >= 2 then order_no end) as conv_avg_room_night
        ,count(distinct case when stay_days >= 2 then order_no end) / count(distinct order_no) as conv_rate
    from c_order
    group by 1 
)t2 on t1.order_date_type = t2.order_date_type
;



select checkout_date,count(distinct order_no) as order_cnt
     ,count(distinct case when order_status not in ('CANCELLED', 'REJECTED') then order_no end) as checkout_cnt
     ,count(distinct case when order_status not in ('CANCELLED', 'REJECTED') then order_no end)  / count(distinct order_no)  as checkout_rate       
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        
        and order_no <> '103576132435'
        and checkout_date >= '2026-05-01' and checkout_date <= '2026-05-14'
    group by 1
order by 1;