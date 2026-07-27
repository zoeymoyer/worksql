
--- 1、近30天活跃且近7天未下单
with d_uv as (--- D页流量用户
    select dt
        ,user_name
        ,root_city_code
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3
)
,q_order_app as (----订单明细表 app
    select order_date
            ,user_name
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

select count(distinct t1.user_name) d_act_uv_7d
from d_uv t1 
left join q_order_app t2 on t1.user_name = t2.user_name 
left join (
    select user_name as key,notification_switch as value 
    from pp_pub.dim_touch_username_switch_da  
    where dt = date_sub(current_date, 1)
    and user_name is not null and user_name not in('',' ')
    and  notification_switch = '1' 
    group by 1,2
) t3 on t1.user_name = t3.key
where t2.user_name is null and t3.key is null
;


--- 1、近30天活跃且近7天未下单-tagger使用
select t1.user_name key,
       t1.root_city_codes value
from (
    select user_name
        ,concat_ws(',', collect_set(root_city_code)) as root_city_codes 
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 30)and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1
) t1 
left join (
    select user_name
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30)and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
) t2 on t1.user_name = t2.user_name 

where t2.user_name is null
;



-- 2、近7天浏览未下单且近7天在对应城市订阅过降价提醒的酒店
with d_uv as (--- D页流量用户
    select user_id
        ,root_city_code,hotel_seq
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3
)
,q_order_app as (----订单明细表 app
    select user_id
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,hotel_price_drop(  -- 订阅酒店降价提醒
    select t.user_id
            ,t.create_time
            ,hotel_seq
            ,subscribe_price / datediff(to_date,from_date) price
            ,row_number() over (partition by t.user_id order by t.create_time desc) as rn
            -- ,from_date,to_date
    from ihotel_default.ods_hotel_intl_greway_hotel_room_price_reduction_tips_hi t
    where t.dt>= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1)
)
,d_uv1d as (--- D页流量用户
    select user_id
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt = date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1
)

select count(distinct t1.user_id),count(1),count(distinct t4.user_id)
from d_uv t1 
left join q_order_app t2 on t1.user_id = t2.user_id
left join hotel_price_drop t3 on t1.user_id = t3.user_id and t1.hotel_seq = t3.hotel_seq
left join d_uv1d t4 on t1.user_id=t4.user_id
where t2.user_id is null and t3.user_id is not null and t3.rn=1
;



-- 2、近30天浏览未下单且近30天在对应城市订阅过降价提醒的酒店 tagger标签
select t1.key
        ,collect_set(concat(t1.root_city_code, '|', t1.hotel_seq, '|', t3.price)) as value
from (--- D页流量用户
    select user_id key
        ,root_city_code
        ,hotel_seq
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1) 
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3
) t1 
left join (----订单明细表 app
    select user_id
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1) 
        and order_no <> '103576132435'
) t2 on t1.key = t2.user_id
left join (  -- 订阅酒店降价提醒
    select user_id,hotel_seq,price
    from (
        select t.user_id
                ,t.create_time
                ,t.hotel_seq
                ,round(subscribe_price / datediff(to_date, from_date),4) price
                ,f.city_code
                ,row_number() over (partition by t.user_id, f.city_code order by t.create_time desc) as rn
                -- ,from_date,to_date
        from ihotel_default.ods_hotel_intl_greway_hotel_room_price_reduction_tips_hi t
        left join default.dim_hotel_info_intl_v3 f on t.hotel_seq = f.hotel_seq
        where t.dt>= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1)  and f.dt=replace(date_sub(current_date, 1), '-', '')
    ) t where rn=1
) t3 on t1.key = t3.user_id and t1.hotel_seq = t3.hotel_seq
where t2.user_id is null and t3.user_id is not null
group by 1
;

-- 3、近30天浏览未下单且近30天未订阅降价提醒且最近30天收藏酒店
select t1.key
        ,collect_set(concat(t1.root_city_code, '|', t1.hotel_seq)) as value
from (--- D页流量用户
    select user_id key
        ,root_city_code
        ,hotel_seq
        ,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 30)  and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4
) t1 
left join (----订单明细表 app
    select user_id
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30)  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
) t2 on t1.key = t2.user_id
left join (  -- 订阅酒店降价提醒
    select user_id,hotel_seq,price
    from (
        select t.user_id
                ,t.create_time
                ,t.hotel_seq
                ,round(subscribe_price / datediff(to_date, from_date),4) price
                ,f.city_code
                ,row_number() over (partition by t.user_id, f.city_code order by t.create_time desc) as rn
                -- ,from_date,to_date
        from ihotel_default.ods_hotel_intl_greway_hotel_room_price_reduction_tips_hi t
        left join default.dim_hotel_info_intl_v3 f on t.hotel_seq = f.hotel_seq
        where t.dt>= date_sub(current_date, 30)  and t.dt<= date_sub(current_date, 1) and f.dt=replace(date_sub(current_date, 1), '-', '')
    ) t where rn=1
) t3 on t1.key = t3.user_id and t1.hotel_seq = t3.hotel_seq
left join (-- 收藏酒店
    select username,business_key as hotel_seq
    from (
        select business_key,username,update_time,f.city_code,row_number() over (partition by t.username, f.city_code order by t.update_time desc) as rn
        from pp_pub.ods_tc_favourite_favourite_di  t
        left join default.dim_hotel_info_intl_v3 f on t.business_key = f.hotel_seq
        where t.dt >= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1) and f.dt=replace(date_sub(current_date, 1), '-', '')
            and business_type='hotel'
    )t where rn=1
)t4 on t1.user_name = t4.username and t1.hotel_seq = t4.hotel_seq
where t2.user_id is null -- 未下单
    and t3.user_id is  null -- 未订酒店降价提醒
    and t4.username is not null  -- 已收藏酒店
group by 1
;



-- 4、近30天浏览未下单且近30天未订阅降价提醒且最近未30天收藏酒店浏览多次的酒店
select t1.key
        ,collect_set(concat(t1.root_city_code, '|', t1.hotel_seq, '|', t5.price)) as value
from (--- D页流量用户
    select key,root_city_code,hotel_seq,user_name,pv,action_time
    from (
        select user_id key
            ,root_city_code
            ,hotel_seq
            ,user_name
            ,count(distinct qtrace_id) pv
            ,max(action_time)action_time
            ,row_number() over(partition by user_id,root_city_code order by count(distinct qtrace_id) desc, max(action_time) desc) rn
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        where dt >= date_sub(current_date, 30)  and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        group by 1,2,3,4
    )t where rn=1
) t1 
left join (----订单明细表 app
    select user_id
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30)  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
) t2 on t1.key = t2.user_id
left join (  -- 订阅酒店降价提醒
    select user_id,hotel_seq,price
    from (
        select t.user_id
                ,t.create_time
                ,t.hotel_seq
                ,round(subscribe_price / datediff(to_date, from_date),4) price
                ,f.city_code
                ,row_number() over (partition by t.user_id, f.city_code order by t.create_time desc) as rn
                -- ,from_date,to_date
        from ihotel_default.ods_hotel_intl_greway_hotel_room_price_reduction_tips_hi t
        left join default.dim_hotel_info_intl_v3 f on t.hotel_seq = f.hotel_seq
        where t.dt>= date_sub(current_date, 30)  and t.dt<= date_sub(current_date, 1) and f.dt=replace(date_sub(current_date, 1), '-', '')
    ) t where rn=1
) t3 on t1.key = t3.user_id and t1.hotel_seq = t3.hotel_seq
left join (-- 收藏酒店
    select username,business_key as hotel_seq
    from (
        select business_key,username,update_time,f.city_code,row_number() over (partition by t.username, f.city_code order by t.update_time desc) as rn
        from pp_pub.ods_tc_favourite_favourite_di  t
        left join default.dim_hotel_info_intl_v3 f on t.business_key = f.hotel_seq
        where t.dt >= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1) and f.dt=replace(date_sub(current_date, 1), '-', '')
            and business_type='hotel'
    )t where rn=1
)t4 on t1.user_name = t4.username and t1.hotel_seq = t4.hotel_seq
left join (-- D页价格
    select a.user_id
            ,hotel_seq,city_code
            ,min(price) price
    from ihotel_default.dw_hotel_price_display a 
    where dt >= replace(date_sub(current_date, 30), '-', '') and dt <= replace(date_sub(current_date, 1), '-', '')
    group by 1,2,3
) t5 on t1.key = t5.user_id and t1.hotel_seq = t5.hotel_seq and t1.root_city_code = t5.city_code
where t2.user_id is null -- 未下单
    and t3.user_id is  null -- 未订酒店降价提醒
    and t4.username is  null  -- 未收藏酒店
    and t5.user_id is not null
group by 1
;



with d_uv_base as ( --- D页近15天流量用户
    select
        dt
        ,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2
)

,d_user_tag as ( --- D页用户打标
    select
        user_name
        ,max(case when dt = date_sub(current_date, 1) then 1 else 0 end) as is_d_1d
        ,max(case when dt between date_sub(current_date, 7) and date_sub(current_date, 2) then 1 else 0 end) as is_d_7d
        ,max(case when dt between date_sub(current_date, 15) and date_sub(current_date, 2) then 1 else 0 end) as is_d_15d
    from d_uv_base
    group by 1
)

,q_order_tag as ( --- app订单用户打标
    select
        user_name
        ,max(case when order_date = date_sub(current_date, 1) then 1 else 0 end) as is_ord_1d
        ,max(case when order_date between date_sub(current_date, 7) and date_sub(current_date, 1) then 1 else 0 end) as is_ord_7d
        ,max(case when order_date between date_sub(current_date, 15) and date_sub(current_date, 1) then 1 else 0 end) as is_ord_15d
    from default.mdw_order_v3_international a 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and order_date >= date_sub(current_date, 15)
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1
)

select
    count(distinct case when d.is_d_1d = 1 then d.user_name end) as d_act_uv_1d

    ,count(distinct case when d.is_d_1d = 1
                          and coalesce(o.is_ord_1d, 0) = 0 then d.user_name end) as d_act_uv_no_ord_1d

    ,count(distinct case when d.is_d_1d = 1
                          and d.is_d_7d = 1
                          and coalesce(o.is_ord_7d, 0) = 0 then d.user_name end) as d_act_uv_7d_no_ord

    ,count(distinct case when d.is_d_1d = 1
                          and d.is_d_15d = 1
                          and coalesce(o.is_ord_15d, 0) = 0 then d.user_name end) as d_act_uv_15d_no_ord
from d_user_tag d
left join q_order_tag o
    on d.user_name = o.user_name
where d.is_d_1d = 1
;




--- H页信息流城市酒店筛选
with hotel_room_night_365d as (
    select  a.city_code
            ,a.hotel_seq
            ,sum(a.room_night) as room_nights
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
        on a.country_name = e.country_name
    where a.dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (a.province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and a.terminal_channel_type = 'app'
        and (a.first_cancelled_time is null or date(a.first_cancelled_time) > a.order_date)
        and (a.first_rejected_time is null or date(a.first_rejected_time) > a.order_date)
        and (a.refund_time is null or date(a.refund_time) > a.order_date)
        and a.is_valid = '1'
        and a.order_date >= date_sub(current_date, 365)
        and a.order_date <= date_sub(current_date, 1)
        and a.order_no <> '103576132435'
    group by 1,2
)
,hotel_room_night_5y as (
    select  a.city_code
            ,a.hotel_seq
            ,sum(a.room_night) as room_nights
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
        on a.country_name = e.country_name
    where a.dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (a.province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and a.terminal_channel_type = 'app'
        and (a.first_cancelled_time is null or date(a.first_cancelled_time) > a.order_date)
        and (a.first_rejected_time is null or date(a.first_rejected_time) > a.order_date)
        and (a.refund_time is null or date(a.refund_time) > a.order_date)
        and a.is_valid = '1'
        and a.order_date >= date_sub(current_date, 365 * 5)
        and a.order_date <= date_sub(current_date, 1)
        and a.order_no <> '103576132435'
    group by 1,2
)
,hotel_dim as (
    select  distinct hotel_seq from temp.temp_xiaohan_song_hexinjiudianchangshi_983
)
,dim_hotel_rank as (  
    select  r.city_code
            ,d.hotel_seq
            ,coalesce(r.room_nights, 0) as room_nights
            ,row_number() over (
                partition by r.city_code
                order by coalesce(r.room_nights, 0) desc, d.hotel_seq
            ) as dim_rn
    from hotel_dim d
    left join hotel_room_night_365d r
        on d.hotel_seq = r.hotel_seq
)
,dim_hotel_top as (--- 1、先取近1年983酒店中Top50
    select  city_code
            ,hotel_seq
            ,room_nights
            ,1 as hotel_type_priority
    from dim_hotel_rank
    where dim_rn <= 50
)
,order_hotel_rank as (--- 2、再取未在近1年983酒店Top50中的酒店
    select  r.city_code
            ,r.hotel_seq
            ,r.room_nights
            ,row_number() over (
                partition by r.city_code
                order by r.room_nights desc
            ) as order_rn
    from hotel_room_night_365d r
    left join dim_hotel_top d
        on r.city_code = d.city_code
        and r.hotel_seq = d.hotel_seq
    where d.hotel_seq is null
)
,hotel_pool as (--- 3、整合983Top50和非983酒店
    select  city_code
            ,hotel_seq
            ,room_nights
            ,hotel_type_priority
    from dim_hotel_top

    union all

    select  city_code
            ,hotel_seq
            ,room_nights
            ,2 as hotel_type_priority
    from order_hotel_rank
)
,hotel_pool_top50 as (--- 4、取整合后的Top50
    select city_code
            ,hotel_seq
            ,room_nights
    from (
        select  city_code
                ,hotel_seq
                ,room_nights
                ,row_number() over (
                    partition by city_code
                    order by hotel_type_priority, room_nights desc, hotel_seq
                ) as rn
        from hotel_pool
    )
    where rn <= 50
)

,city_hotel_cnt as (--- 每个城市的酒店数量
    select  city_code
            ,count(distinct hotel_seq) as hotel_cnt
    from hotel_pool_top50
    group by 1
)

,hotel_room_night_5y_supply as (--- 筛选酒店数量少于4个的城市看近5年产量
    select  r.city_code
            ,r.hotel_seq
            ,r.room_nights
            ,row_number() over (
                partition by r.city_code
                order by r.room_nights desc, r.hotel_seq
            ) as supply_rn
    from hotel_room_night_5y r
    inner join city_hotel_cnt c
        on r.city_code = c.city_code
        and c.hotel_cnt < 4
    left join hotel_pool_top50 t
        on r.city_code = t.city_code
        and r.hotel_seq = t.hotel_seq
    where t.hotel_seq is null
)

,hotel_supply_top as (--- 少于4个酒店的城市从近5年产量补齐至4个酒店
    select  a.city_code
            ,a.hotel_seq
            ,a.room_nights
    from hotel_room_night_5y_supply a
    inner join city_hotel_cnt c
        on a.city_code = c.city_code
    where a.supply_rn <= 4 - c.hotel_cnt
)

,hotel_final as (
    select  city_code
            ,hotel_seq
            ,room_nights
            ,1 as source_priority
    from hotel_pool_top50

    union all

    select  city_code
            ,hotel_seq
            ,room_nights
            ,2 as source_priority
    from hotel_supply_top
)

select  city_code
        ,hotel_seq
        ,room_nights
        ,row_number() over (
            partition by city_code
            order by source_priority, room_nights desc, hotel_seq
        ) as rn
from hotel_final
;



select t1.key
        ,concat_ws(',',collect_set(concat(t1.root_city_code, '|', t1.hotel_seq, '|', t5.price))) as value
from (--- D页流量用户
    select key,root_city_code,hotel_seq,user_name,pv,action_time
    from (
        select user_id key
            ,root_city_code
            ,hotel_seq
            ,user_name
            ,count(distinct qtrace_id) pv
            ,max(action_time)action_time
            ,row_number() over(partition by user_id,root_city_code order by count(distinct qtrace_id) desc, max(action_time) desc) rn
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        group by 1,2,3,4
    )t where rn=1
) t1 
left join (----订单明细表 app
    select user_id
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
) t2 on t1.key = t2.user_id
left join (  -- 订阅酒店降价提醒
    select user_id,hotel_seq,price
    from (
        select t.user_id
                ,t.create_time
                ,t.hotel_seq
                ,round(subscribe_price / datediff(to_date, from_date),4) price
                ,f.city_code
                ,row_number() over (partition by t.user_id, f.city_code order by t.create_time desc) as rn
                -- ,from_date,to_date
        from ihotel_default.ods_hotel_intl_greway_hotel_room_price_reduction_tips_hi t
        left join default.dim_hotel_info_intl_v3 f on t.hotel_seq = f.hotel_seq
        where t.dt>= date_sub(current_date, 30) and t.dt<= date_sub(current_date, 1) and f.dt=replace(date_sub(current_date, 1), '-', '')
    ) t where rn=1
) t3 on t1.key = t3.user_id and t1.hotel_seq = t3.hotel_seq
left join (-- 收藏酒店
    select username,business_key as hotel_seq
    from (
        select business_key,username,update_time,f.city_code,row_number() over (partition by t.username, f.city_code order by t.update_time desc) as rn
        from pp_pub.ods_tc_favourite_favourite_di  t
        left join default.dim_hotel_info_intl_v3 f on t.business_key = f.hotel_seq
        where t.dt >= date_sub(current_date, 30)and t.dt <= date_sub(current_date, 1) and f.dt=replace(date_sub(current_date, 1), '-', '')
            and business_type='hotel'
    )t where rn=1
)t4 on t1.user_name = t4.username and t1.hotel_seq = t4.hotel_seq
left join (-- D页价格
    select a.user_id
            ,hotel_seq,city_code
            ,min(price) price
    from ihotel_default.dw_hotel_price_display a 
    where dt >= replace(date_sub(current_date, 30), '-', '') and dt <= replace(date_sub(current_date, 1), '-', '')
    group by 1,2,3
) t5 on t1.key = t5.user_id and t1.hotel_seq = t5.hotel_seq and t1.root_city_code = t5.city_code
where t2.user_id is null -- 未下单
    and t3.user_id is  null -- 未订酒店降价提醒
    and t4.username is  null  -- 未收藏酒店
    and t5.user_id is not null
    and t1.key = '322011478'
group by 1