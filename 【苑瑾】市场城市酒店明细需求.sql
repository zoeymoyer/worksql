-- 1、近30天有产指定国家城市酒店明细
with q_order as (
    select order_date
            , province_name 
            ,a.country_name
            ,case when city_name in ('西归浦市','济州市')  then '济州岛'
                  when city_name in ('邦拉蒙') then '芭提雅'
                  when city_name in ('西海岸省') then '哥打京那巴鲁'
                  when city_name in ('东北县') then '槟城'
                  when city_name in ('斗湖省') then '仙本那'
                  when city_name in ('新山') then '柔佛'
                  when city_name in ('八打灵县') then '沙巴'
                  when city_name in ('克拉克县') then '拉斯维加斯'
                else city_name end as city_name
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,hotel_seq,hotel_name
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and (
            province_name in ('澳门','香港')
            or a.country_name in ('泰国','韩国','新加坡','马来西亚','美国','越南','印度尼西亚')
        )
        and city_name  in ('澳门','香港','新加坡'
                ,'首尔','济州市','西归浦市','釜山','仁川'
                ,'曼谷','普吉岛','清迈','邦拉蒙','苏梅岛'
                ,'吉隆坡','西海岸省','东北县','斗湖省','新山','八打灵县'
                ,'巴厘岛'
                ,'富国岛'
                ,'纽约','洛杉矶','克拉克县','旧金山','檀香山','橙县','库克县'
            ) 
)
,hotle_score as (  --- 酒店评分
    select obj_seq 
        ,max(reference_score) as hotel_score
        ,max(count) as hotel_comments
    from default.ods_qunar_review_obj_score a
    where dt = '%(DATE)s'
          and tag = '1'
    group by 1
)
,hotel_poi as (
    select hotel_seq,
           concat_ws(',', collect_set(filtername)) as filtername
    from ihotel_default.dw_ihotel_area_hotel_relate_di
    where dt='2026-06-02'
    group by hotel_seq
)
select mdd,city_name,t1.hotel_seq,t1.hotel_name,round(init_gmv/room_night, 2) as adr,hotel_score,hotel_comments,room_night,filtername,hotel_grade,init_gmv,order_count,user_count
from (
    select case when province_name in ('澳门','香港') then province_name else country_name end as mdd
        ,city_name
        ,hotel_seq
        ,hotel_name
        ,hotel_grade
        ,sum(init_gmv) as init_gmv
        ,sum(room_night) as room_night
        ,count(distinct order_no) as order_count
        ,count(distinct user_id) as user_count
    from q_order
    group by 1,2,3,4,5
)t1
left join hotle_score t2 on t1.hotel_seq = t2.obj_seq
left join hotel_poi t3 on t1.hotel_seq = t3.hotel_seq
order by room_night desc
;

--- 2、25年6-12月top15国家&top160城市订单数据-预定、离店
select order_month,mdd,order_count,concat(round(order_count/month_order_count * 100, 2), '%') as order_percent
from (
    select substr(order_date,1,7) as order_month
            ,case when province_name in ('香港','澳门') then '港澳' else a.country_name end as mdd
            ,count(distinct order_no) as order_count
            ,row_number() over(partition by substr(order_date,1,7) order by count(distinct order_no) desc) as order_rank
            ,sum(count(distinct order_no)) over(partition by substr(order_date,1,7)) as month_order_count
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-06-01' and order_date <= '2025-12-31'
        and order_no <> '103576132435'
    group by 1,2
) t where order_rank <= 15
order by order_month desc,order_count desc
;

select checkout_month,mdd,order_count,concat(round(order_count/month_order_count * 100, 2), '%') as order_percent
from (
    select substr(checkout_date,1,7) as checkout_month
            ,case when province_name in ('香港','澳门') then '港澳' else a.country_name end as mdd
            ,count(distinct order_no) as order_count
            ,row_number() over(partition by substr(checkout_date,1,7) order by count(distinct order_no) desc) as order_rank
            ,sum(count(distinct order_no)) over(partition by substr(checkout_date,1,7)) as month_order_count
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >= '2025-06-01' and checkout_date <= '2025-12-31'
        and order_no <> '103576132435'
    group by 1,2
) t where order_rank <= 15
order by checkout_month desc,order_count desc
;


select order_month,mdd,city_name,order_count,concat(round(order_count/month_order_count * 100, 2), '%') as order_percent
from (
    select substr(order_date,1,7) as order_month
            ,case when province_name in ('香港','澳门') then '港澳' else a.country_name end as mdd
            ,city_name
            ,count(distinct order_no) as order_count
            ,row_number() over(partition by substr(order_date,1,7) order by count(distinct order_no) desc) as order_rank
            ,sum(count(distinct order_no)) over(partition by substr(order_date,1,7)) as month_order_count
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-06-01' and order_date <= '2025-12-31'
        and order_no <> '103576132435'
    group by 1,2,3
) t where order_rank <= 160
order by order_month desc,order_count desc
;


select checkout_month,mdd,city_name,order_count,concat(round(order_count/month_order_count * 100, 2), '%') as order_percent
from (
    select substr(checkout_date,1,7) as checkout_month
            ,case when province_name in ('香港','澳门') then '港澳' else a.country_name end as mdd
            ,city_name
            ,count(distinct order_no) as order_count
            ,row_number() over(partition by substr(checkout_date,1,7) order by count(distinct order_no) desc) as order_rank
            ,sum(count(distinct order_no)) over(partition by substr(checkout_date,1,7)) as month_order_count
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >= '2025-06-01' and checkout_date <= '2025-12-31'
        and order_no <> '103576132435'
    group by 1,2,3
) t where order_rank <= 160
order by checkout_month desc,order_count desc
;