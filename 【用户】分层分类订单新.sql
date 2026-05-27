      
with holiday_user as 
     (select user_id 
          , count(distinct case when (checkin_date between '2023-12-31' and '2024-01-02'
          or checkin_date between '2024-02-09' and '2024-02-18'
          or checkin_date between '2024-04-03' and '2024-04-07'
          or checkin_date between '2024-04-30' and '2024-05-06'
          or checkin_date between '2024-06-09' and '2024-06-13'
          or checkin_date between '2024-09-14' and '2024-09-18'
          or checkin_date between '2024-09-30' and '2024-10-08'
          or checkin_date between '2025-01-26' and '2025-02-04'
          or checkout_date between '2023-12-31' and '2024-01-02'
          or checkout_date between '2024-02-09' and '2024-02-18'
          or checkout_date between '2024-04-03' and '2024-04-07'
          or checkout_date between '2024-04-30' and '2024-05-06'
          or checkout_date between '2024-06-09' and '2024-06-13'
          or checkout_date between '2024-09-14' and '2024-09-18'
          or checkout_date between '2024-09-30' and '2024-10-08'
          or checkout_date between '2025-01-26' and '2025-02-04'
          or checkout_date between '2025-04-03' and '2025-04-07'
          or checkout_date between '2025-04-30' and '2025-05-06'
          or checkout_date between '2025-05-29' and '2025-06-03'
          ) then order_no end)/count(distinct order_no) as holiday_ord_rate
     from mdw_order_v3_international
     where dt = '%(DATE)s'
          and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
          and terminal_channel_type in ('www','app','touch') 
          and order_status not in ('CANCELLED','REJECTED')
          and is_valid='1'
          and checkout_date between '2023-10-31' and '2025-07-31'
     group by 1
     ),

invoice as 
     (select distinct order_no 
     from fuwu.dwd_xcd_htl_complete_di
     where is_international = 1
     and dt >= '2025-07-01'
     )



select substr(a.order_date,1,7) as order_month
     -- , gender
     , age_period
     , `adr_period`
     , case when `总入住天数` >= 7 then 1 else 0 end as `长住`
     , overlap_ord as `订房人`
     , case when meeting_cnt > 0 or is_invoice = 1 then 1 else 0 end as `商务`
     , case when child_cnt > 0 then 1 else 0 end as `亲子`
     , case when user_country not in ('中国','未知') then 1 else 0 end as `国际客`
     , case when is_pkg_product = 'true' or `早餐份数` > 0 then 1 else 0 end as `套餐订单`
     , case when commission < 0 then 1 else 0 end as `负佣订单`
     , case when bed_type in ('WIDE','UP_DOWN','SPELL') or hotelSubCategory in (519,520) then 1 else 0 end as `背包客`
     , case when city_level in ('一线','新一线','二线') and age between 25 and 49 and holiday_ord_rate >= 0.8 then 1 else 0 end as `白领`


     , count(distinct a.order_no) as `订单`
     , count(distinct a.user_id) as `用户`

     , sum(`间夜`) as `间夜`
     , sum(`gmv`) as `gmv`
     , sum(commission) as `佣金`


from
     (select order_date
          , a.order_no
          , a.user_id
          -- , case when order_date > b.min_order_date then '老客' else '新客' end as user_newold
          , gender
          , city_level
          , age
          , holiday_ord_rate
          , case when f.order_no is not null then 1 else 0 end as is_invoice
          , case when age between 0 and 24 then '0-24'
               when age between 25 and 35 then '25-35'
               when age between 36 and 49 then '36-49'
               else '50+' end as age_period  
          , user_country
          , room_night as `间夜`
          , init_gmv as `gmv`
          , init_gmv/room_night as `adr`
          , case 
               when init_gmv/room_night <= 100 then '[0,100]'
               when init_gmv/room_night <= 400 then '[100,400]'
               when init_gmv/room_night <= 700 then '[400,700]'
               when init_gmv/room_night <= 1500 then '[700,1500]'
               when init_gmv/room_night <= 2000 then '[1500,2000]'
               else '2000+' end as `adr_period`
          , datediff(checkout_date,checkin_date) as `总入住天数`
          , breakfast as `早餐份数`
          , is_pkg_product
          , init_commission_after as commission
          , bed_type
          , hotelSubCategory
          
     from mdw_order_v3_international a     

     left join
          (select distinct user_id
               , level_desc
               , gender
               , city_level
               , 2025 - substr(birth_year_month,1,4) as age
          from pub.dim_user_profile_nd
          ) c
     on a.user_id = c.user_id

     left join
          (select hotel_seq
               , max(attrs['hotelSubCategory']) as hotelSubCategory --酒店分类
          from ihotel_default.dim_hotel_info_intl_v3
          where dt = '20250731'
          and hotel_operating_status = '营业中'
          group by 1
          ) d
     on a.hotel_seq = d.hotel_seq

     left join holiday_user e
     on a.user_id = e.user_id

     left join invoice f
     on a.order_no = f.order_no

     where dt = '%(DATE)s'
          and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
          and terminal_channel_type in ('www','app','touch') 
          and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
          and (first_rejected_time is null or date(first_rejected_time) > order_date) 
          and (refund_time is null or date(refund_time) > order_date)
          and is_valid='1'
          and order_date between '2025-07-01' and '2025-07-31'
          and a.order_no <> '103576132435'
     ) a

left join
     (select distinct order_no
          , checkin_date
          , checkout_date_next
          , checkout_date
          , checkout_date_last
          , case when checkout_date_next <= checkout_date then 1 
               when checkout_date_last >= checkout_date then 1 
               else 0 end as overlap_ord
     from
          (select distinct user_id
               , order_no
               , checkin_date
               , checkout_date
               , lead(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_next
               , lag(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_last
          from mdw_order_v3_international 
          where dt = '%(DATE)s'
               and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
               and terminal_channel_type in ('www','app','touch') 
               and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
               and (first_rejected_time is null or date(first_rejected_time) > order_date) 
               and (refund_time is null or date(refund_time) > order_date)
               and is_valid='1'
               and order_date between '2025-07-01' and '2025-07-31'
               and order_no <> '103576132435'
          order by user_id, checkin_date
          ) z
     ) b
on a.order_no = b.order_no

left join
     (select -- concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as pt_dt
           user_id
          -- , count(distinct log_id) as cnt
          , count(distinct case when query like '%会议%' then log_id end) as meeting_cnt
          -- , count(distinct case when query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%' then log_id end) as child_cnt
          , count(distinct case when (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) or query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%' then log_id end) as child_cnt
     from dw_user_app_search_di_v3
     where dt between '20250701' and '20250731'
          and device_id is not null
          and device_id <> ''
          and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
          and business_type = 'hotel'
     group by 1
     ) c
on a.user_id = c.user_id
-- and a.order_date = c.pt_dt
group by 1,2,3,4,5,6,7,8,9,10,11,12

    