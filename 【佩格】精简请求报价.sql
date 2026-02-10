with user_type as
(
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
,q_order_app as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,max_c,hotel_seq
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-10-01' 
        and order_no <> '103576132435'
)
,l_display as (
    select dt,t1.user_id
           ,city_name
           ,t1.hotel_seq
           ,adults_num
           ,children_num
           ,all_num
           ,max_c
           ,order_no
           ,case when max_c = all_num then '一致' 
                 when max_c > all_num then '不一致'
                 when all_num is null then '为空'
                 when max_c < all_num then '小于'
            end num_type
    from (
        select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))  dt 
            ,a.user_id
            ,city_name
            ,hotel_seq
            ,adults_num
            ,children_num
            ,adults_num + children_num all_num
        from ihotel_default.dw_hotel_price_display a 
        where dt>= '20251001'
        -- and user_id = '1526422273'
        group by 1,2,3,4,5,6
    ) t1 
    left join q_order_app t2 on t1.dt=t2.order_date and t1.user_id=t2.user_id and t1.hotel_seq=t2.hotel_seq
)

select t1.dt
       ,count(t1.user_id) `总请求报价pv`
       ,count(distinct t1.order_no) `总订单量`
       ,count(case when num_type =  '为空' then t1.user_id end) `请求报价人数为空pv`
       ,count(distinct case when num_type =  '为空'  then t1.order_no end) `请求报价人数为空的订单量`
       ,count(distinct case when num_type =  '一致'  then t1.order_no end) `请求报价人数和订单入住数量一致的订单量`
       ,count(distinct case when num_type =  '不一致'  then t1.order_no end) `请求报价人数和订单入住数量不一致的订单量`
       ,concat(round(count(distinct case when num_type =  '一致' then t1.order_no end) / count(t1.user_id) * 100, 2), '%') `一致率`
       ,count(distinct case when num_type =  '小于'  then t1.order_no end) `请求报价人数大于订单入住数量的订单量`
from l_display t1
group by 1
order by dt
;




with q_order_app as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,max_c,hotel_seq
    from mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-10-01' 
        and order_no <> '103576132435'
)
,bj_detail as (
    select t1.dt,t1.user_id,t1.hotel_seq,t1.qtrace_id,t1.adults_num,t1.children_num,t1.all_num,t1.order_no
    from (
        select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2))  dt 
            ,a.user_id
            ,hotel_seq
            ,qtrace_id
            ,adults_num
            ,children_num
            ,adults_num + children_num all_num
        from ihotel_default.dw_hotel_price_display a 
        where dt >= '20251001'
        group by 1,2,3,4,5,6,7
    ) t1 left join (--detail
        select distinct dt
                ,pre_qtrace_id
                ,hotel_seq
                ,qtrace_id
        from ihotel_default.dw_user_path_di_v4
        where dt >= '2025-10-01'
        and process_stage = 'detail'
    )t2 on t1.qtrace_id=t2.qtrace_id and t1.dt=t2.dt
    left join ( --booking
        select distinct dt
                ,pre_qtrace_id
                ,hotel_seq
                ,qtrace_id
        from ihotel_default.dw_user_path_di_v4
        where dt >= '2025-10-01'
        and process_stage = 'booking'
    ) booking  on lower(t2.qtrace_id)=lower(booking.pre_qtrace_id) and t2.dt=booking.dt
    left join
        ( --order_submit
        select distinct dt
            ,pre_qtrace_id
            ,hotel_seq
            ,qtrace_id
            ,order_no
        from ihotel_default.dw_user_path_di_v4
        where dt >= '2025-10-01'
        and process_stage = 'order'
    ) ord on lower(booking.qtrace_id)=lower(ord.pre_qtrace_id) and booking.dt = ord.dt
)

,l_display as (
    select dt
           ,t1.user_id
           ,t1.hotel_seq
           ,t1.adults_num
           ,t1.children_num
           ,t1.all_num
           ,t2.max_c
           ,t2.order_no
           ,case when max_c = all_num then '一致' 
                 when max_c > all_num then '不一致'
                 when all_num is null then '为空'
                 when max_c < all_num then '小于'
            end num_type
    from bj_detail t1 
    left join q_order_app t2 on t1.dt=t2.order_date and t1.user_id=t2.user_id and t1.order_no=t2.order_no
)

select t1.dt
       ,count(t1.user_id) `总请求报价pv`
       ,count(distinct t1.order_no) `总订单量`
       ,count(case when num_type =  '为空' then t1.user_id end) `请求报价人数为空pv`
       ,count(distinct case when num_type =  '为空'  then t1.order_no end) `请求报价人数为空的订单量`
       ,count(distinct case when num_type =  '一致'  then t1.order_no end) `请求报价人数和订单入住数量一致的订单量`
       ,count(distinct case when num_type =  '不一致'  then t1.order_no end) `请求报价人数和订单入住数量不一致的订单量`
       ,concat(round(count(distinct case when num_type =  '一致' then t1.order_no end) / count(t1.user_id) * 100, 2), '%') `一致率`
       ,count(distinct case when num_type =  '小于'  then t1.order_no end) `请求报价人数大于订单入住数量的订单量`
from l_display t1
group by 1
order by dt
;
;