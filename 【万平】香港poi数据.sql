

with q_order_app as (
    select order_date
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,checkout_date,init_commission_after
            ,hotel_seq
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
        and province_name = '香港'
)

,hotel_poi as (
    select hotel_seq,filtername
    from ihotel_default.dw_ihotel_area_hotel_relate_di
    where dt='2026-06-09'
    group by 1,2
)

select filtername
      ,order_count
      ,order_all
      ,concat(round((order_count / order_all)*100,2),'%') as order_ratio

from (
    select filtername
        ,count(distinct order_no) as order_count
        --    ,concat(round((count(distinct order_no) / sum(count(distinct order_no)) over())*100,2),'%') order_ratio
        ,count(distinct t1.hotel_seq) as seq_count
        ,1 keys
    from q_order_app t1
    left join hotel_poi t2 on t1.hotel_seq = t2.hotel_seq
    group by 1
) t1
left join (
    select count(disitnct order_no) order_all
            ,1 keys
    from q_order_app
) t2 on t1.keys = t2.keys
order by 2 desc
;



select hotel_seq,hotel_name,hotel_enname,total_room_night,row_number() over (order by total_room_night desc) as rn 
from (
    select a.hotel_seq,a.hotel_name,attrs['enName'] as hotel_enname,sum(room_night) as total_room_night
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join default.dim_hotel_info_intl_v3 b on a.hotel_seq = b.hotel_seq 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')  and b.dt='20260611'
        and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 90)and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and a.province_name = '香港'
    group by 1,2,3
)
order by 4 


--- 香港有产酒店数量
select t1.hotel_seq,t1.hotel_name,t2.attrs['enName'] as hotel_enname,sum(room_night) as total_room_night
from (
    select a.hotel_seq,a.hotel_name,room_night
    from default.mdw_order_v3_international a 
    left join temp.temp_yiq·uny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') 
        and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 90)and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and a.province_name = '香港'
) t1 
left join (select * from default.dim_hotel_info_intl_v3 where dt='20260611') t2 on t1.hotel_seq = t2.hotel_seq
group by 1,2,3
order by 4 desc





select city_name,hotel_name
from (
    select city_name,hotel_name,sum(room_night) rn,row_number() over (partition by city_name order by sum(room_night) desc) as r
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
        and order_date >= '2026-07-01' and order_date <= '2026-08-31'
        and order_no <> '103576132435'
        and city_name in ('柏林','慕尼黑','法兰克福','汉堡','科隆','莫斯科','圣彼得堡','Gorod Irkutsk','海参崴行政区','巴黎','滨海阿尔卑斯','贝尔格莱德','诺维萨德','尼什','巴塞罗那省','马德里','塞维利亚省','罗马','威尼斯','佛罗伦萨','米兰','伦敦','伯明翰','爱丁堡','纽约','洛杉矶','橙县','旧金山','萨福克县','克拉克县','大温哥华','多伦多','蒙特利尔','坎昆'
)

    group by 1,2
) where r <= 100 order by 1,2





--- 香港有产酒店
with hotle_score as (  --- 酒店评分
    select obj_seq 
        ,max(reference_score) as hotel_score
        ,max(count) as hotel_comments
    from default.ods_qunar_review_obj_score a
    where dt = '%(DATE)s'
          and tag = '1'
    group by 1
)

select a.hotel_seq,a.hotel_name,hotel_score,sum(room_night) as total_room_night,count(distinct order_no) as total_orders,sum(init_gmv)init_gmv
       ,round(sum(init_gmv) / sum(room_night)) adr
from default.mdw_order_v3_international a 
left join temp.temp_yiq·uny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
left join hotle_score h on a.hotel_seq = h.obj_seq
where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') 
    and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
    -- and terminal_channel_type = 'app'
    and terminal_channel_type in ('www','app','touch')
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    and is_valid='1'
    and order_date >= '2026-04-01' and order_date <= date_sub(current_date, 1)
    and order_no <> '103576132435'
    and a.province_name = '香港'
order by total_orders desc
;


with q_order as (
    select 
        case 
            when (init_gmv / room_night) <= 200 then '1(0,200]'
            when (init_gmv / room_night) > 200 and (init_gmv / room_night) <= 400 then '2(200,400]'
            when (init_gmv / room_night) > 400 and (init_gmv / room_night) <= 600 then '3(400,600]'
            when (init_gmv / room_night) > 600 and (init_gmv / room_night) <= 800 then '4(600,800]'
            when (init_gmv / room_night) > 800 and (init_gmv / room_night) <= 1000 then '5(800,1000]'
            when (init_gmv / room_night) > 1000 and (init_gmv / room_night) <= 1200 then '6(1000,1200]'
            when (init_gmv / room_night) > 1200 and (init_gmv / room_night) <= 1600 then '7(1200,1600]'
            when (init_gmv / room_night) > 1600 and (init_gmv / room_night) <= 2000 then '8(1600,2000]'
            else '9(2000+]'
        end as adr_level,
        sum(room_night) as q_rn
    from default.mdw_order_v3_international a 
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and province_name = '香港' 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and order_date >= '2026-04-01' 
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1
)
,c_order as (
    select 
        case 
            when (room_fee / extend_info['room_night']) <= 200 then '1(0,200]'
            when (room_fee / extend_info['room_night']) > 200  and (room_fee / extend_info['room_night']) <= 400 then '2(200,400]'
            when (room_fee / extend_info['room_night']) > 400  and (room_fee / extend_info['room_night']) <= 600 then '3(400,600]'
            when (room_fee / extend_info['room_night']) > 600  and (room_fee / extend_info['room_night']) <= 800 then '4(600,800]'
            when (room_fee / extend_info['room_night']) > 800  and (room_fee / extend_info['room_night']) <= 1000 then '5(800,1000]'
            when (room_fee / extend_info['room_night']) > 1000 and (room_fee / extend_info['room_night']) <= 1200 then '6(1000,1200]'
            when (room_fee / extend_info['room_night']) > 1200 and (room_fee / extend_info['room_night']) <= 1600 then '7(1200,1600]'
            when (room_fee / extend_info['room_night']) > 1600 and (room_fee / extend_info['room_night']) <= 2000 then '8(1600,2000]'
            else '9(2000+]'
        end as adr_level,
        sum(extend_info['room_night']) as c_rn
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = date_sub(current_date, 1)
        and extend_info['PROVINCE'] = '香港'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(order_date, 1, 10) >= '2026-04-01'
        and substr(order_date, 1, 10) <= date_sub(current_date, 1)
    group by 1
)
--- 3. 全外连接拼接结果
select 
    coalesce(q.adr_level, c.adr_level) as "ADR分布带",
    coalesce(q.q_rn, 0) as "Q_APP间夜量",
    coalesce(c.c_rn, 0) as "C_APP间夜量",
    concat(round(coalesce(q.q_rn, 0) / coalesce(c.c_rn, 1) * 100, 2), '%') as "间夜QC"
from q_order q
full outer join c_order c on q.adr_level = c.adr_level
order by 1;