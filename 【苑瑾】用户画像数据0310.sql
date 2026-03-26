--- 预定春节用户画像
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
,q_order_25_spr as (----锁定25年春节离店
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 25年春节
        and order_date <= '2025-02-04'
        and order_no <> '103576132435'
)
,q_order_26_spr as (----锁定26年春节离店
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
   
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  --- 26年春节
        and order_date <= '2026-02-23'
        and order_no <> '103576132435'
)
,uv_25_spr as ---D页流量25年春节
(
    select case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '2025-02-04'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 25年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4
    union all
    select case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
    from default.dw_user_app_detail_visit_di_v3 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '20241202' and dt >= '20231212'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 25年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4
)
,uv_26_spr as ---D页流量26年春节
(
    select case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '2026-02-23' and dt >= '2025-01-01'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  --- 26年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4
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
,order_result_25_spr as ( --- 订单画像标签
    select user_id,user_type,order_no,city_name,prov_name,city_level,room_night,mdd,order_date,hotel_grade
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
    from (
        select o.order_no,user_type,room_night,mdd,order_date,hotel_grade
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
        from q_order_25_spr o
        left join user_profile u on u.user_id = o.user_id
    )
)
,order_result_26_spr as ( --- 订单画像标签
    select user_id,user_type,order_no,city_name,prov_name,city_level,room_night,mdd,order_date,hotel_grade
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
    from (
        select o.order_no,user_type,room_night,mdd,order_date,hotel_grade
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
        from q_order_26_spr o
        left join user_profile u on u.user_id = o.user_id
    )
)
,uv_result_25_spr as ( --- 流量画像标签
    select user_id,user_type,mdd
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
    from (
        select o.user_id,user_type,mdd
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from uv_25_spr o
        left join user_profile u on u.user_id = o.user_id
    )
)
,uv_result_26_spr as ( --- 流量画像标签
    select user_id,user_type,mdd
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
    from (
        select o.user_id,user_type,mdd
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from uv_26_spr o
        left join user_profile u on u.user_id = o.user_id
    )
)

select t1.city_lev,t1.age_level,t1.gender,t1.user_type
      ,order_uv_26_spr / flow_uv_26_spr CR26
      ,order_uv_25_spr / flow_uv_25_spr CR25
      ,room_night_25_spr,order_no_25_spr,order_uv_25_spr
      ,room_night_26_spr,order_no_26_spr,order_uv_26_spr
      ,flow_uv_25_spr,flow_uv_26_spr
from (
    select if(grouping(t1.city_lev)=1,'ALL', t1.city_lev) as  city_lev
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.gender)=1,'ALL', t1.gender) as  gender
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,sum(room_night) room_night_25_spr
        ,count(distinct order_no) order_no_25_spr
        ,count(distinct user_id) order_uv_25_spr
    from order_result_25_spr t1
    group by cube(user_type,city_lev,age_level,gender)
)t1 left join (
    select if(grouping(t1.city_lev)=1,'ALL', t1.city_lev) as  city_lev
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.gender)=1,'ALL', t1.gender) as  gender
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,sum(room_night) room_night_26_spr
        ,count(distinct order_no) order_no_26_spr
        ,count(distinct user_id) order_uv_26_spr
    from order_result_26_spr t1
    group by cube(user_type,city_lev,age_level,gender)
)t2 on t1.city_lev=t2.city_lev and t1.age_level=t2.age_level and t1.gender=t2.gender and t1.user_type=t2.user_type
left join (
    select if(grouping(t1.city_lev)=1,'ALL', t1.city_lev) as  city_lev
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.gender)=1,'ALL', t1.gender) as  gender
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct user_id) flow_uv_25_spr
    from uv_result_25_spr t1
    group by cube(user_type,city_lev,age_level,gender)
)t3 on t1.city_lev=t3.city_lev and t1.age_level=t3.age_level and t1.gender=t3.gender and t1.user_type=t3.user_type
left join (
    select if(grouping(t1.city_lev)=1,'ALL', t1.city_lev) as  city_lev
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.gender)=1,'ALL', t1.gender) as  gender
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct user_id) flow_uv_26_spr
    from uv_result_26_spr t1
    group by cube(user_type,city_lev,age_level,gender)
)t4 on t1.city_lev=t4.city_lev and t1.age_level=t4.age_level and t1.gender=t4.gender and t1.user_type=t4.user_type
order by 1
,case when user_type = 'ALL' then 1 
    when user_type = '新客' then 2 
    when  user_type = '老客' then 3 end asc
;



--- 分渠道用户画像 25年春节
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
,q_order_25_spr as (----锁定25年春节离店
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 25年春节
        and order_date <= '2025-02-04'
        and order_no <> '103576132435'
)
,uv_25_spr as (---D页流量25年春节
    select dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '2025-02-04' 
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 25年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
    union all
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
    from default.dw_user_app_detail_visit_di_v3 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '20241202' and dt >= '20240101'
        and checkout_date >= '2025-01-28' and checkout_date <= '2025-02-04'  --- 25年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
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
,uv_channel as (--- 渠道
    select dt,user_id,channel ,user_name
    from ihotel_default.dwd_flow_ug_channel_di where dt <= '2026-02-23' 
    group by 1,2,3,4
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_id
           ,c.uid
    from uv_channel a
    left join (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
        select  t.dt,
                t.uid,
                t.username,
                t.category
        from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
        where t.dt >= '2024-12-01' and t.dt <= '2026-02-23' 
            and t.category in ('信息流', '达人')
        group by 1,2,3,4
    ) c on a.user_name=c.username and a.dt=c.dt
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between '2023-12-01' and '2026-02-23' 
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between '2023-12-01' and '2026-02-23' 
    group by 1,2,3
)
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,user_market as (---- 市场投放  宽口径
    select  m.dt
            ,m.user_id
    from ihotel_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2
)
,order_result_25_spr as ( --- 订单画像标签
    select user_id,user_type,order_no,city_name,prov_name,city_level,room_night,mdd,order_date,hotel_grade
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
           ,channel,is_market
    from (
        select o.order_no,user_type,room_night,mdd,order_date,hotel_grade
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
            ,channel
            ,case when t4.user_id is not null then 'Y' else 'N' end is_market
        from q_order_25_spr o
        left join user_profile u on u.user_id = o.user_id
        left join uv_channel t3 on o.user_id=t3.user_id and o.order_date=t3.dt
        left join user_market t4 on o.user_id=t4.user_id and o.order_date=t4.dt
    )
)
,uv_result_25_spr as ( --- 流量画像标签
    select dt,user_id,user_type,mdd
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
            ,channel,is_market
    from (
        select o.user_id,user_type,mdd,o.dt
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
            ,channel
            ,case when t4.user_id is not null then 'Y' else 'N' end is_market
        from uv_25_spr o
        left join user_profile u on u.user_id = o.user_id
        left join uv_channel t3 on o.user_id=t3.user_id and o.dt=t3.dt
        left join user_market t4 on o.user_id=t4.user_id and o.dt=t4.dt
    )
)


select t1.order_date,t1.age_level,t1.user_type,t1.channel,t1.is_market
      ,room_night_25_spr,order_no_25_spr,order_uv_25_spr
      ,flow_uv_25_spr
      ,order_uv_25_spr/flow_uv_25_spr cr_25
from (
    select order_date
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,if(grouping(t1.is_market)=1,'ALL', t1.is_market) as  is_market
        ,sum(room_night) room_night_25_spr
        ,count(distinct order_no) order_no_25_spr
        ,count(distinct user_id) order_uv_25_spr
    from order_result_25_spr t1
    group by order_date, cube(user_type,age_level,is_market,channel)
)t1 
left join (
    select dt
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.is_market)=1,'ALL', t1.is_market) as  is_market
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,count(distinct user_id) flow_uv_25_spr
    from uv_result_25_spr t1
    group by dt, cube(user_type,age_level,is_market,channel)
)t3 on t1.age_level=t3.age_level and t1.user_type=t3.user_type and t1.order_date=t3.dt and t1.is_market=t3.is_market and t1.channel=t3.channel
order by 1
,case when user_type = 'ALL' then 1 
    when user_type = '新客' then 2 
    when  user_type = '老客' then 3 end asc
;



--- 分渠道用户画像 26年春节
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
,q_order_26_spr as (----锁定26年春节离店
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
   
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  --- 26年春节
        and order_date <= '2026-02-23'
        and order_no <> '103576132435'
)
,uv_26_spr as (---D页流量26年春节

    select dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '2026-02-23'
        and checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23'  --- 26年春节
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
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
,uv_channel as (--- 渠道
    select dt,user_id,channel ,user_name
    from ihotel_default.dwd_flow_ug_channel_di where dt <= '2026-02-23' 
    group by 1,2,3,4
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_id
           ,c.uid
    from uv_channel a
    left join (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
        select  t.dt,
                t.uid,
                t.username,
                t.category
        from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
        where t.dt >= '2025-01-01' and t.dt <= '2026-02-23' 
            and t.category in ('信息流', '达人')
        group by 1,2,3,4
    ) c on a.user_name=c.username and a.dt=c.dt
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between '2024-12-01' and '2026-02-23' 
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between '2024-12-01' and '2026-02-23' 
    group by 1,2,3
)
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,user_market as (---- 市场投放  宽口径
    select  m.dt
            ,m.user_id
    from ihotel_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2
)

,order_result_26_spr as ( --- 订单画像标签
    select user_id,user_type,order_no,city_name,prov_name,city_level,room_night,mdd,order_date,hotel_grade
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
           ,channel,is_market
    from (
        select o.order_no,user_type,room_night,mdd,order_date,hotel_grade
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
            ,channel
            ,case when t4.user_id is not null then 'Y' else 'N' end is_market
        from q_order_26_spr o
        left join user_profile u on u.user_id = o.user_id
        left join uv_channel t3 on o.user_id=t3.user_id and o.order_date=t3.dt
        left join user_market t4 on o.user_id=t4.user_id and o.order_date=t4.dt
    )
)

,uv_result_26_spr as ( --- 流量画像标签
    select dt,user_id,user_type,mdd
           ,birth_year_month
           ,age
           ,gender
           ,case when city_level in ('一线','新一线','二线','三线','四线','五线')  then city_level else  '未知' end as  city_lev
           ,case when age <= 18 then '(0,18]'
                 when age >= 19 and age <= 24 then '[19,24]'
                 when age >= 25 and age <= 30 then '[25,30]'
                 when age >= 31 and age <= 35 then '[31,35]'
                 when age >= 36 and age <= 40 then '[36,40]'
                 when age >= 41 and age <= 45 then '[41,45]'
                 when age >= 46 and age <= 50 then '[46,50]'
                 when age >= 51 then '[51+)'
            else '未知' end as age_level
            ,channel,is_market
    from (
        select o.user_id,user_type,mdd,o.dt
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('%(DATE)s', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
            ,channel
            ,case when t4.user_id is not null then 'Y' else 'N' end is_market
        from uv_26_spr o
        left join user_profile u on u.user_id = o.user_id
        left join uv_channel t3 on o.user_id=t3.user_id and o.dt=t3.dt
        left join user_market t4 on o.user_id=t4.user_id and o.dt=t4.dt
    )
)

select t1.order_date,t1.age_level,t1.user_type,t1.channel,t1.is_market
      ,room_night_26_spr,order_no_26_spr,order_uv_26_spr
      ,flow_uv_26_spr
from (
    select order_date
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,if(grouping(t1.is_market)=1,'ALL', t1.is_market) as  is_market
        ,sum(room_night) room_night_26_spr
        ,count(distinct order_no) order_no_26_spr
        ,count(distinct user_id) order_uv_26_spr
    from order_result_26_spr t1
    group by order_date, cube(user_type,age_level,is_market,channel)
)t1 
left join (
    select dt
        ,if(grouping(t1.age_level)=1,'ALL', t1.age_level) as  age_level
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.is_market)=1,'ALL', t1.is_market) as  is_market
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,count(distinct user_id) flow_uv_26_spr
    from uv_result_26_spr t1
    group by dt, cube(user_type,age_level,is_market,channel)
)t3 on t1.age_level=t3.age_level and t1.user_type=t3.user_type and t1.order_date=t3.dt and t1.is_market=t3.is_market and t1.channel=t3.channel
order by 1
,case when user_type = 'ALL' then 1 
    when user_type = '新客' then 2 
    when  user_type = '老客' then 3 end asc
;
