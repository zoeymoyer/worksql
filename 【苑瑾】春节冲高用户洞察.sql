---- 24年春节离店用户画像
with user_type as
(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '20250205'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when order_date >= '2025-01-28' and order_date <= '2025-02-04' then '春节期间预定'
                  when datediff('2025-01-28', order_date) between 1 and 3 then '提前订1-3天'
                  when datediff('2025-01-28', order_date) between 4 and 7 then '提前订4-7天'
                  when datediff('2025-01-28', order_date) between 8 and 14 then '提前订8-14天'
                  when datediff('2025-01-28', order_date) between 15 and 30 then '提前订15-30天'
                  when datediff('2025-01-28', order_date) between 31 and 60 then '提前订31-60天'
                  when datediff('2025-01-28', order_date) between 61 and 180 then '提前订61-180天'
                  else '提前订181天+' end per_type
     
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '20250205'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 24年春节
        and order_date <= '2025-02-04'
        and order_no <> '103576132435'
 
)
,user_profile as (
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)
,order_result as (
    select user_id,user_type,order_no,gender,city_name,prov_name,city_level,room_night,per_type,mdd,order_date,hotel_grade
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.order_no,user_type,room_night,per_type,mdd,order_date,hotel_grade
            ,o.user_id
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from q_order o
        left join user_profile u on u.user_id = o.user_id
    )
)

select if(grouping(t1.city_lev)=1,'ALL', t1.city_lev) as  city_lev
      ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
      ,if(grouping(t1.gender)=1,'ALL', t1.gender) as  gender
      ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
      ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
      ,sum(room_night) room_night
      ,count(distinct order_no) order_no
from order_result t1
group by cube(mdd,user_type,city_lev,age_level,gender)
order by 1
,case when mdd = '香港'  then 1
           when mdd = '澳门'  then 2
           when mdd = '泰国'  then 3
           when mdd = '日本'  then 4
           when mdd = '韩国'  then 5
           when mdd = '马来西亚'  then 6
           when mdd = '新加坡'  then 7
           when mdd = '美国'  then 8
           when mdd = '印度尼西亚'  then 9
           when mdd = '俄罗斯'  then 10
           when mdd = '欧洲'  then 11
           when mdd = '亚太'  then 12
           when mdd = '美洲'  then 13
           when mdd = '其他'  then 14
           when mdd = 'ALL'  then 0
      end asc
,case when user_type = 'ALL' then 1 
    when user_type = '新客' then 2 
    when  user_type = '老客' then 3 end asc
;



---- 24年春节离店用户画像
with user_type as
(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '20250205'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when order_date >= '2025-01-28' and order_date <= '2025-02-04' then '春节期间预定'
                  when datediff('2025-01-28', order_date) between 1 and 3 then '提前订1-3天'
                  when datediff('2025-01-28', order_date) between 4 and 7 then '提前订4-7天'
                  when datediff('2025-01-28', order_date) between 8 and 14 then '提前订8-14天'
                  when datediff('2025-01-28', order_date) between 15 and 30 then '提前订15-30天'
                  when datediff('2025-01-28', order_date) between 31 and 60 then '提前订31-60天'
                  when datediff('2025-01-28', order_date) between 61 and 180 then '提前订61-180天'
                  else '提前订181天+' end per_type
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,case when init_gmv / room_night < 400  then '1[0,400)'
                  when init_gmv / room_night >= 400 and init_gmv / room_night < 800  then '2[400,800)'
                  when init_gmv / room_night >= 800 and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' end adr
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '20250205'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 24年春节
        and order_date <= '2025-02-04'
        and order_no <> '103576132435'
 
)
,user_profile as (
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)
,order_result as (
    select user_id,user_type,order_no,gender,city_name,prov_name,city_level,room_night,per_type,mdd,order_date,hotel_grade,adr
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
    from (
        select o.order_no,user_type,room_night,per_type,mdd,order_date,hotel_grade,adr
            ,o.user_id
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from q_order o
        left join user_profile u on u.user_id = o.user_id
    )
)

select if(grouping(t1.city_lev)=1,'ALL', t1.city_lev) as  city_lev
      ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
      ,if(grouping(t1.gender)=1,'ALL', t1.gender) as  gender
      ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
      ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
      ,if(grouping(t1.per_type)=1,'ALL', t1.per_type) as  per_type
      ,if(grouping(t1.hotel_grade)=1,'ALL', t1.hotel_grade) as  hotel_grade
      ,if(grouping(t1.adr)=1,'ALL', t1.adr) as  adr
      ,sum(room_night) room_night
      ,count(distinct order_no) order_no
from order_result t1
group by cube(mdd,user_type,city_lev,age_level,gender,per_type,hotel_grade,adr)
;







with user_type as (
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '20251207'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order_25 as (
      select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when order_date >= '2026-02-15' and order_date <= '2026-02-23' then '春节期间预定'
                  when datediff('2026-02-15', order_date) between 1 and 3 then '提前订1-3天'
                  when datediff('2026-02-15', order_date) between 4 and 7 then '提前订4-7天'
                  when datediff('2026-02-15', order_date) between 8 and 14 then '提前订8-14天'
                  when datediff('2026-02-15', order_date) between 15 and 30 then '提前订15-30天'
                  when datediff('2026-02-15', order_date) between 31 and 60 then '提前订31-60天'
                  when datediff('2026-02-15', order_date) between 61 and 180 then '提前订61-180天'
                  else '提前订181天+' end per_type
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,case when init_gmv / room_night < 400  then '1[0,400)'
                  when init_gmv / room_night >= 400 and init_gmv / room_night < 800  then '2[400,800)'
                  when init_gmv / room_night >= 800 and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' end adr
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '20251207'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  --- 25年春节
        and order_no <> '103576132435'
        and order_date >= '2025-03-01'
)
,q_order_24 as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when order_date >= '2025-01-28' and order_date <= '2025-02-04' then '春节期间预定'
                  when datediff('2025-01-28', order_date) between 1 and 3 then '提前订1-3天'
                  when datediff('2025-01-28', order_date) between 4 and 7 then '提前订4-7天'
                  when datediff('2025-01-28', order_date) between 8 and 14 then '提前订8-14天'
                  when datediff('2025-01-28', order_date) between 15 and 30 then '提前订15-30天'
                  when datediff('2025-01-28', order_date) between 31 and 60 then '提前订31-60天'
                  when datediff('2025-01-28', order_date) between 61 and 180 then '提前订61-180天'
                  else '提前订181天+' end per_type
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,case when init_gmv / room_night < 400  then '1[0,400)'
                  when init_gmv / room_night >= 400 and init_gmv / room_night < 800  then '2[400,800)'
                  when init_gmv / room_night >= 800 and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' end adr
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '20250205'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 24年春节
        and order_date <= '2025-02-04'
        and order_no <> '103576132435'
 
)


    select order_date
        ,mdd
        ,sum(room_night)room_night
        ,sum(sum(room_night)) over(partition by order_date) rn_all
        ,sum(room_night) / sum(sum(room_night)) over(partition by order_date) rate
    from q_order_25
    group by 1,2

order by 1
;


tmp.temp_yz_cj_24




select *,sum(room_night) over() all_ 
from (
    select case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,sum(room_night) room_night
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '20250205'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 24年春节
        and order_date <= '2024-11-19' and order_date >= '2024-02-12'
        and order_no <> '103576132435'
    group by 1
) order by 2 desc