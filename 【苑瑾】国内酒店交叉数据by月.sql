---- 整体交叉，国内酒店4星以上
with hotel_order as (--- 国内酒店订单
    select user_name,order_dat
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        and order_date >= '2021-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
        and hotel_grade >= 4 --四星及以上
    group by 1,2
)
,user_type as
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
,q_app_order as (---- 国际酒店订单
    select user_name,order_date 
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
        and order_date >= '2023-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
    group by 1,2,3
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2023-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,month_range AS (
    -- 生成需要计算的月份序列（最近23个月）
    SELECT 
        add_months(trunc(current_date(), 'MM'), -n) as month_start
    FROM (
        SELECT explode(sequence(0, 34)) as n
    )
)
,gn_hotel_grade as (
    SELECT  substr(mr.month_start,1,7) mth
        ,COUNT(DISTINCT ft.user_name) as YOU
    FROM month_range mr
    LEFT JOIN hotel_order ft 
        ON ft.order_date >= add_months(mr.month_start, -11) 
        AND ft.order_date < add_months(mr.month_start, 1)  -- 到当月最后一天
    GROUP BY 1
)


select t1.mth
      ,order_uv
      ,order_uv_hotel
      ,uv
      ,uv_hotel
      ,YOU
from (--- 订单交叉  下单用户在过往365天内于国内酒店有下单
    select substr(t1.order_date, 1, 7) mth
            ,count(distinct t1.user_name) order_uv
            ,count(distinct t2.user_name) order_uv_hotel
    from (
        select order_date,user_name
        from q_app_order t1
    )t1 left join (
        select order_date,user_name
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.order_date >=  t2.order_date 
        and datediff(t1.order_date,t2.order_date) >= 365
    group by 1
)t1 left join (--- 流量交叉 用户在过往365天内于国内酒店有下单
    select substr(t1.dt, 1, 7) mth
            ,count(distinct t1.user_name) uv
            ,count(distinct t2.user_name) uv_hotel
    from (
        select dt,user_name
        from uv t1
    )t1 left join (
        select order_date,user_name
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.dt >=  t2.order_date 
        and datediff(t1.dt,t2.order_date) >= 365
    group by 1
)t2 on t1.mth=t2.mth
left join gn_hotel_grade t3 on t1.mth=t3.mth
order by 1 desc
;


WITH month_range AS (
    -- 生成需要计算的月份序列（最近23个月）
    SELECT 
        add_months(trunc(current_date(), 'MM'), -n) as month_start
    FROM (
        SELECT explode(sequence(0, 22)) as n
    )
)
,hotel_order as (--- 国内酒店订单
    select user_name,order_date--,hotel_seq,order_no,room_night
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        and order_date >= '2022-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
        and hotel_grade >= 4 --四星及以上
    group by 1,2
)

SELECT 
    mr.month_start,
    COUNT(DISTINCT ft.user_name) as YOU
FROM month_range mr
LEFT JOIN hotel_order ft 
    ON ft.order_date >= add_months(mr.month_start, -11) 
    AND ft.order_date < add_months(mr.month_start, 1)  -- 到当月最后一天
GROUP BY mr.month_start 
;




---------------------------- 分星级
with hotel_order as (--- 国内酒店订单
    select user_name,order_date
           ,CASE WHEN hotel_grade <= 3 THEN '3星及以下'
            WHEN hotel_grade = 4 THEN '4星'
            WHEN hotel_grade = 5 THEN '5星' end as hotel_grade
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        and order_date >= '2021-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
    group by 1,2,3
)
,q_app_order as (---- 国际酒店订单
    select user_name,order_date--,hotel_seq,order_no,room_night
    from default.mdw_order_v3_international a 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
        and order_date >= '2023-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
    group by 1,2
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            -- ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    --  left join user_type b on a.user_id = b.user_id 
     where dt >= '2023-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,month_range AS (
    -- 生成需要计算的月份序列（最近23个月）
    SELECT 
        add_months(trunc(current_date(), 'MM'), -n) as month_start
    FROM (
        SELECT explode(sequence(0, 34)) as n
    )
)
,gn_hotel_grade as (
    SELECT  substr(mr.month_start,1,7) mth
        ,hotel_grade
        ,COUNT(DISTINCT ft.user_name) as YOU
    FROM month_range mr
    LEFT JOIN hotel_order ft 
        ON ft.order_date >= add_months(mr.month_start, -11) 
        AND ft.order_date < add_months(mr.month_start, 1)  -- 到当月最后一天
    GROUP BY 1,2 
)


select t1.mth
      ,t1.hotel_grade
      ,order_uv
      ,order_uv_hotel
      ,uv
      ,uv_hotel
      ,YOU
from (--- 订单交叉  下单用户在过往365天内于国内酒店有下单
    select substr(t1.order_date, 1, 7) mth,hotel_grade
            ,count(distinct t1.user_name) order_uv
            ,count(distinct t2.user_name) order_uv_hotel
    from (
        select order_date,user_name
        from q_app_order t1
    )t1 left join (
        select order_date,user_name,hotel_grade
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.order_date >=  t2.order_date 
        and datediff(t1.order_date,t2.order_date) >= 365
    group by 1,2
)t1 left join (--- 流量交叉 用户在过往365天内于国内酒店有下单
    select substr(t1.dt, 1, 7) mth,hotel_grade
            ,count(distinct t1.user_name) uv
            ,count(distinct t2.user_name) uv_hotel
    from (
        select dt,user_name
        from uv t1
    )t1 left join (
        select order_date,user_name,hotel_grade
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.dt >=  t2.order_date 
        and datediff(t1.dt,t2.order_date) >= 365
    group by 1,2
)t2 on t1.mth=t2.mth and t1.hotel_grade=t2.hotel_grade
left join gn_hotel_grade t3 on t1.mth=t3.mth and t1.hotel_grade=t3.hotel_grade
order by 1 desc
;


-------------------------- 分ADR
with hotel_order as (--- 国内酒店订单
    select user_name,order_date
           ,CASE WHEN init_gmv/room_night < 100 THEN '100-'
                WHEN init_gmv/room_night >= 100 AND init_gmv/room_night < 200 THEN '100-199'
                WHEN init_gmv/room_night >= 200 AND init_gmv/room_night < 300 THEN '200-299'
                WHEN init_gmv/room_night >= 300 AND init_gmv/room_night < 400 THEN '300-399'
                WHEN init_gmv/room_night >= 400 AND init_gmv/room_night < 500 THEN '400-499'
                WHEN init_gmv/room_night >= 500 AND init_gmv/room_night < 600 THEN '500-599'
                WHEN init_gmv/room_night >= 600 AND init_gmv/room_night < 800 THEN '600-799'
                WHEN init_gmv/room_night >= 800 AND init_gmv/room_night < 1000 THEN '800-999'
                WHEN init_gmv/room_night >= 1000 AND init_gmv/room_night < 1500 THEN 'a1000-1499'
                WHEN init_gmv/room_night >= 1500 AND init_gmv/room_night < 2000 THEN 'a1500-1999'
                WHEN init_gmv/room_night >= 2000 THEN 'a2000+' END as adr
    from (
        select user_name,order_date,order_no
        from hotel.dwd_ord_order_detail_da
        where dt = '%(FORMAT_DATE)s'
            and order_date >= '2021-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
            and is_mainland_china = 1
            and is_valid = 1
            and terminal_channel_type in ('app') 
            and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
            and (rejected_time is null or date(rejected_time) > order_date)
            and (refund_time is null or date(refund_time) > order_date)
            --and is_order_checkout = 1
            and trim(user_name) not in ('','NULL','null')
            and user_name is not null
            and is_distribute = 1
            and pay_time != ''
        group by 1,2,3
    )a 
    join (
        select order_no,init_gmv,room_night
        from hotel.dwd_ord_wide_order_detail_da
        where dt = '%(FORMAT_DATE)s'
        and order_date >= '2021-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
    )b on a.order_no = b.order_no
)
,q_app_order as (---- 国际酒店订单
    select user_name,order_date--,hotel_seq,order_no,room_night
    from default.mdw_order_v3_international a 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
        and order_date >= '2023-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
    group by 1,2
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            -- ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    --  left join user_type b on a.user_id = b.user_id 
     where dt >= '2023-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,month_range AS (
    -- 生成需要计算的月份序列（最近23个月）
    SELECT 
        add_months(trunc(current_date(), 'MM'), -n) as month_start
    FROM (
        SELECT explode(sequence(0, 34)) as n
    )
)
,gn_hotel_grade as (
    SELECT  substr(mr.month_start,1,7) mth
        ,adr
        ,COUNT(DISTINCT ft.user_name) as YOU
    FROM month_range mr
    LEFT JOIN hotel_order ft 
        ON ft.order_date >= add_months(mr.month_start, -11) 
        AND ft.order_date < add_months(mr.month_start, 1)  -- 到当月最后一天
    GROUP BY 1,2 
)


select t1.mth
      ,t1.adr
      ,order_uv
      ,order_uv_hotel
      ,uv
      ,uv_hotel
      ,YOU
from (--- 订单交叉  下单用户在过往365天内于国内酒店有下单
    select substr(t1.order_date, 1, 7) mth,adr
            ,count(distinct t1.user_name) order_uv
            ,count(distinct t2.user_name) order_uv_hotel
    from (
        select order_date,user_name
        from q_app_order t1
    )t1 left join (
        select order_date,user_name,adr
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.order_date >=  t2.order_date 
        and datediff(t1.order_date,t2.order_date) >= 365
    group by 1,2
)t1 left join (--- 流量交叉 用户在过往365天内于国内酒店有下单
    select substr(t1.dt, 1, 7) mth,adr
            ,count(distinct t1.user_name) uv
            ,count(distinct t2.user_name) uv_hotel
    from (
        select dt,user_name
        from uv t1
    )t1 left join (
        select order_date,user_name,adr
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.dt >=  t2.order_date 
        and datediff(t1.dt,t2.order_date) >= 365
    group by 1,2
)t2 on t1.mth=t2.mth and t1.adr=t2.adr
left join gn_hotel_grade t3 on t1.mth=t3.mth and t1.adr=t3.adr
order by 1 desc
;




---- 每日跑过往30天数据
with hotel_order as (
    select user_name,order_date--,hotel_seq,order_no,room_night
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        and order_date >= '2022-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
        and hotel_grade >= 4 --四星及以上
    group by 1,2
)
,date_range as (
    select order_date
    from hotel_order
    where order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
    group by 1 
)

 ,expanded_data as (
    select user_name,order_date, date_add(order_date, pos) AS dt_in_window
    from hotel_order
    LATERAL VIEW POSEXPLODE(SPLIT(SPACE(30), ' ')) t AS pos, dummy 
    where order_date >= '2025-01-01' 
  )


SELECT dt_in_window,count(distinct user_name) 
FROM expanded_data 
group by 1
ORDER BY dt_in_window DESC
;


---- 国内酒店数据
with hotel_order as (
    select user_name,order_date,order_no,room_num,room_night,hotel_grade,is_hours_room,country_name
           ,city_name,province_name,hotel_name,req_productroomname
           ,terminal_channel_type
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        --and order_date >= '2022-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
)
,wide_hotel_order as (
    select order_date,order_no,init_gmv,room_night,room_num,supplier_name
        ,third_supplier_channel,supplier_channel_type,checkin_daynum,final_commission,init_commission,contact_name
        ,terminal_channel_type
    from hotel.dwd_ord_wide_order_detail_da
    where dt = '%(FORMAT_DATE)s'
    --and order_date >= '2022-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
    and is_mainland_china = 1
    and is_valid = 1
    and terminal_channel_type in ('app') 
)


select t1.order_date
      ,count(distinct t1.user_name)  order_uv
      ,count(distinct t1.order_no)  orders
      ,sum(t2.room_num) room_num
      ,sum(t2.room_night) room_night
      ,sum(t2.init_gmv) init_gmv
      ,sum(t2.final_commission) final_commission
      ,sum(t2.init_commission) init_commission
      ,count(distinct case when hotel_grade = 1 then t1.order_no end) orders1
      ,count(distinct case when hotel_grade = 2 then t1.order_no end) orders2
      ,count(distinct case when hotel_grade = 3 then t1.order_no end) orders3
      ,count(distinct case when hotel_grade = 4 then t1.order_no end) orders4
      ,count(distinct case when hotel_grade = 5 then t1.order_no end) orders5
      ,sum(case when hotel_grade = 1 then t2.room_night end) room_night1
      ,sum(case when hotel_grade = 2 then t2.room_night end) room_night2
      ,sum(case when hotel_grade = 3 then t2.room_night end) room_night3
      ,sum(case when hotel_grade = 4 then t2.room_night end) room_night4
      ,sum(case when hotel_grade = 5 then t2.room_night end) room_night5

      ,sum(case when hotel_grade = 1 then t2.init_gmv end) init_gmv1
      ,sum(case when hotel_grade = 2 then t2.init_gmv end) init_gmv2
      ,sum(case when hotel_grade = 3 then t2.init_gmv end) init_gmv3
      ,sum(case when hotel_grade = 4 then t2.init_gmv end) init_gmv4
      ,sum(case when hotel_grade = 5 then t2.init_gmv end) init_gmv5

      ,sum(case when hotel_grade = 1 then t2.final_commission end) final_commission1
      ,sum(case when hotel_grade = 2 then t2.final_commission end) final_commission2
      ,sum(case when hotel_grade = 3 then t2.final_commission end) final_commission3
      ,sum(case when hotel_grade = 4 then t2.final_commission end) final_commission4
      ,sum(case when hotel_grade = 5 then t2.final_commission end) final_commission5

      ,sum(case when hotel_grade = 1 then t2.init_commission end) init_commission1
      ,sum(case when hotel_grade = 2 then t2.init_commission end) init_commission2
      ,sum(case when hotel_grade = 3 then t2.init_commission end) init_commission3
      ,sum(case when hotel_grade = 4 then t2.init_commission end) init_commission4
      ,sum(case when hotel_grade = 5 then t2.init_commission end) init_commission5
from hotel_order t1
left join wide_hotel_order t2 on t1.order_date=t2.order_date and t1.order_no=t2.order_no
group by 1
order by 1 desc;




---- 整体交叉，国内酒店4星以上 分新老
with hotel_order as (--- 国内酒店订单
    select user_name,order_date
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        and order_date >= '2021-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
        and hotel_grade >= 4 --四星及以上
    group by 1,2
)
,user_type as
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
,q_app_order as (---- 国际酒店订单
    select user_name,order_date 
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
        and order_date >= '2023-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
    group by 1,2,3
)
,uv as ----分日去重活跃用户
(
    select distinct a.dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2023-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,month_range AS (
    -- 生成需要计算的月份序列（最近23个月）
    SELECT 
        add_months(trunc(current_date(), 'MM'), -n) as month_start
    FROM (
        SELECT explode(sequence(0, 34)) as n
    )
)
,gn_hotel_grade as (
    SELECT  substr(mr.month_start,1,7) mth
        ,COUNT(DISTINCT ft.user_name) as YOU
    FROM month_range mr
    LEFT JOIN hotel_order ft 
        ON ft.order_date >= add_months(mr.month_start, -11) 
        AND ft.order_date < add_months(mr.month_start, 1)  -- 到当月最后一天
    GROUP BY 1
)


select t1.mth
      ,order_uv
      ,order_uv_hotel
      ,uv
      ,uv_hotel
      ,YOU
from (--- 订单交叉  下单用户在过往365天内于国内酒店有下单
    select substr(t1.order_date, 1, 7) mth
            ,count(distinct t1.user_name) order_uv
            ,count(distinct t2.user_name) order_uv_hotel
    from (
        select order_date,user_name
        from q_app_order t1
    )t1 left join (
        select order_date,user_name
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.order_date >=  t2.order_date 
        and datediff(t1.order_date,t2.order_date) >= 365
    group by 1
)t1 left join (--- 流量交叉 用户在过往365天内于国内酒店有下单
    select substr(t1.dt, 1, 7) mth
            ,count(distinct t1.user_name) uv
            ,count(distinct t2.user_name) uv_hotel
    from (
        select dt,user_name
        from uv t1
    )t1 left join (
        select order_date,user_name
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.dt >=  t2.order_date 
        and datediff(t1.dt,t2.order_date) >= 365
    group by 1
)t2 on t1.mth=t2.mth
left join gn_hotel_grade t3 on t1.mth=t3.mth
order by 1 desc
;


---------------------------- 分星级 分新老
with hotel_order as (--- 国内酒店订单
    select user_name,order_date
           ,CASE WHEN hotel_grade <= 3 THEN '3星及以下'
            WHEN hotel_grade = 4 THEN '4星'
            WHEN hotel_grade = 5 THEN '5星' end as hotel_grade
    from hotel.dwd_ord_order_detail_da
    where dt = '%(FORMAT_DATE)s'
        and order_date >= '2021-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
        and is_mainland_china = 1
        and is_valid = 1
        and terminal_channel_type in ('app') 
        and (cancelled_time is null or date(cancelled_time) > order_date)  --与国际酒店保持同口径，剔除当天取消、拒单的
        and (rejected_time is null or date(rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        --and is_order_checkout = 1
        and trim(user_name) not in ('','NULL','null')
        and user_name is not null
        and is_distribute = 1
        and pay_time != ''
    group by 1,2,3
)
,q_app_order as (---- 国际酒店订单
    select user_name,order_date--,hotel_seq,order_no,room_night
    from default.mdw_order_v3_international a 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
        and order_date >= '2023-01-01' and order_date <= date_sub(current_date, 1)  --近两年订单
    group by 1,2
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            -- ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    --  left join user_type b on a.user_id = b.user_id 
     where dt >= '2023-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,month_range AS (
    -- 生成需要计算的月份序列（最近23个月）
    SELECT 
        add_months(trunc(current_date(), 'MM'), -n) as month_start
    FROM (
        SELECT explode(sequence(0, 34)) as n
    )
)
,gn_hotel_grade as (
    SELECT  substr(mr.month_start,1,7) mth
        ,hotel_grade
        ,COUNT(DISTINCT ft.user_name) as YOU
    FROM month_range mr
    LEFT JOIN hotel_order ft 
        ON ft.order_date >= add_months(mr.month_start, -11) 
        AND ft.order_date < add_months(mr.month_start, 1)  -- 到当月最后一天
    GROUP BY 1,2 
)


select t1.mth
      ,t1.hotel_grade
      ,order_uv
      ,order_uv_hotel
      ,uv
      ,uv_hotel
      ,YOU
from (--- 订单交叉  下单用户在过往365天内于国内酒店有下单
    select substr(t1.order_date, 1, 7) mth,hotel_grade
            ,count(distinct t1.user_name) order_uv
            ,count(distinct t2.user_name) order_uv_hotel
    from (
        select order_date,user_name
        from q_app_order t1
    )t1 left join (
        select order_date,user_name,hotel_grade
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.order_date >=  t2.order_date 
        and datediff(t1.order_date,t2.order_date) >= 365
    group by 1,2
)t1 left join (--- 流量交叉 用户在过往365天内于国内酒店有下单
    select substr(t1.dt, 1, 7) mth,hotel_grade
            ,count(distinct t1.user_name) uv
            ,count(distinct t2.user_name) uv_hotel
    from (
        select dt,user_name
        from uv t1
    )t1 left join (
        select order_date,user_name,hotel_grade
        from hotel_order t1
    ) t2 on t1.user_name=t2.user_name and t1.dt >=  t2.order_date 
        and datediff(t1.dt,t2.order_date) >= 365
    group by 1,2
)t2 on t1.mth=t2.mth and t1.hotel_grade=t2.hotel_grade
left join gn_hotel_grade t3 on t1.mth=t3.mth and t1.hotel_grade=t3.hotel_grade
order by 1 desc
;
