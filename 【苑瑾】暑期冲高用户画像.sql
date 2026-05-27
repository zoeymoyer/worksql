--- 1、暑期、五一、平日离店用户画像分析
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
,q_order as (----订单明细表表包含取消  
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  else '其他' end as checkout_date_type
            ,checkout_date,mobile_platform
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
        )
        and order_no <> '103576132435'
)
,search_child as (
    select user_id
    from default.dw_user_app_search_di_v3
    where dt >= '20240101' and dt <= '20250831'
        and device_id is not null
        and device_id <> ''
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and business_type = 'hotel'
        and (
                (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%')
            )
    group by 1
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
    select user_id
           ,user_type  -- 用户类型
           ,order_no   -- 订单号
           ,gender     -- 性别
           ,city_name,prov_name
           ,city_level -- 城市等级
           ,mdd        -- 目的地
           ,checkout_date_type -- 离店类型
           ,hotel_grade     -- 酒店星级
           ,birth_year_month -- 出生年月
           ,age      -- 年龄
           ,case when age <= 18 then '1(0,18]'
                 when age >= 19 and age <= 24 then '2[19,24]'
                 when age >= 25 and age <= 30 then '3[25,30]'
                 when age >= 31 and age <= 35 then '4[31,35]'
                 when age >= 36 and age <= 40 then '5[36,40]'
                 when age >= 41 and age <= 45 then '6[41,45]'
                 when age >= 46 and age <= 50 then '7[46,50]'
                 when age > 50 then '8[51+)'
            else '未知' end as age_level  -- 年龄段
            ,mobile_platform  -- 手机平台
            ,is_child  -- 亲子
    from (
        select o.order_no
            ,user_type
            ,mdd
            ,hotel_grade
            ,o.user_id,o.mobile_platform,checkout_date_type
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,case when birth_year_month is null then '未知'
                else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int)
                end as age
            ,case when s.user_id is not null then '亲子' else '非亲子' end as is_child
        from q_order o
        left join user_profile u on u.user_id = o.user_id
        left join search_child s on s.user_id = o.user_id
    )t
)
-- =========================================================
-- 以下为数据聚合与交叉分析部分
-- =========================================================
select 
     checkout_date_type 
    ,nvl(mdd, 'ALL') as mdd
    ,nvl(age_level, 'ALL') as age_level
    ,nvl(gender, 'ALL') as gender
    ,nvl(mobile_platform, 'ALL') as mobile_platform
    ,nvl(city_level, 'ALL') as city_level
    ,nvl(is_child, 'ALL') as is_child
    
    -- 核心画像指标
    ,count(distinct user_id) as uv 
    ,count(distinct order_no) as order_cnt
    
    -- 场景标签映射（严格使用 grouping 判定，避免多场景混淆）
    ,case 
        -- 0. 大盘汇总
        when grouping(mdd)=1 and grouping(is_child)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '0. 大盘汇总：离店类型'
        
        -- 1. 原有多维交叉场景
        when grouping(mdd)=1 and grouping(is_child)=1 and grouping(age_level)=0 and grouping(gender)=0 and grouping(mobile_platform)=0 and grouping(city_level)=0 then '场景1：年龄x性别x机型x城市级别x离店类型'
        when grouping(mdd)=0 and grouping(is_child)=1 and grouping(age_level)=0 and grouping(gender)=0 and grouping(mobile_platform)=0 and grouping(city_level)=0 then '场景2：年龄x性别x机型x城市级别x目的地x离店类型'
        when grouping(mdd)=0 and grouping(is_child)=0 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '场景3：目的地x亲子x离店类型'
        when grouping(mdd)=0 and grouping(age_level)=0 and grouping(is_child)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '场景4：目的地x年龄段x离店类型'
        
        -- 2. 新增单维交叉场景
        when grouping(gender)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：性别x离店类型'
        when grouping(age_level)=0 and grouping(mdd)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：年龄段x离店类型'
        when grouping(city_level)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(is_child)=1 then '单维：城市线级x离店类型'
        when grouping(mobile_platform)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：手机平台x离店类型'
        when grouping(is_child)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '单维：亲子x离店类型'
        when grouping(mdd)=0 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：目的地x离店类型'
        
     end as cross_scenario
from order_result 
group by 
     checkout_date_type
    ,mdd
    ,age_level
    ,gender
    ,mobile_platform
    ,city_level
    ,is_child
grouping sets (
    -- 0. 大盘纯离店类型汇总 (相当于为总体表现定基准)
    (checkout_date_type),

    -- 1. 原始复杂的交叉场景
    (checkout_date_type, age_level, gender, mobile_platform, city_level),
    (checkout_date_type, age_level, gender, mobile_platform, city_level, mdd),
    (checkout_date_type, mdd, is_child),
    (checkout_date_type, mdd, age_level),
    
    -- 2. 新增的单个维度与离店类型交叉场景
    (checkout_date_type, gender),           -- 性别 x 离店类型
    (checkout_date_type, age_level),        -- 年龄段 x 离店类型
    (checkout_date_type, city_level),       -- 城市线级 x 离店类型
    (checkout_date_type, mobile_platform),  -- 手机平台 x 离店类型
    (checkout_date_type, is_child),         -- 亲子 x 离店类型
    (checkout_date_type, mdd)               -- 目的地 x 离店类型
)
;


--- 2、25年暑期和25年五一预定分布
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
,q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  else '其他' end as order_date_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
        )
        and order_no <> '103576132435'
)
,uv as (---D页流量25年春节
    select dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  else '其他' end as checkout_date_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '2025-08-17' 
        and (
            (checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
        )
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6
    union all
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  else '其他' end as checkout_date_type
    from default.dw_user_app_detail_visit_di_v3 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '20241202' and dt >= '20240101'
        and (
            (checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
        )
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6
)

-- =======================================================
-- 统一整合底层数据，便于一套逻辑同时算出流量与订单指标
-- =======================================================
,base_data as (
    select order_date_type as checkout_date_type -- 统一定义为离店类型
          ,order_date as action_date             -- 统一定义为行为日期
          ,mdd
          ,user_type
          ,user_id
          ,order_no
          ,1 as is_order
          ,0 as is_uv
    from q_order
    
    union all
    
    select checkout_date_type
          ,dt as action_date
          ,mdd
          ,user_type
          ,user_id
          ,null as order_no
          ,0 as is_order
          ,1 as is_uv
    from uv
)
select 
     checkout_date_type
    ,action_date
    ,nvl(mdd, 'ALL') as mdd
    ,nvl(user_type, 'ALL') as user_type
    
    -- 输出核心指标
    ,count(distinct case when is_uv = 1 then user_id end) as uv
    ,count(distinct case when is_order = 1 then user_id end) as order_uv
    ,count(distinct case when is_order = 1 then order_no end) as order_cnt

    -- 场景标签映射
    ,case 
        when grouping(mdd) = 1 and grouping(user_type) = 1 then '场景1：离店类型 x 日期'
        when grouping(mdd) = 0 and grouping(user_type) = 1 then '场景2：离店类型 x 日期 x 目的地'
        when grouping(mdd) = 1 and grouping(user_type) = 0 then '场景3：离店类型 x 日期 x 新老客'
     end as cross_scenario
     
from base_data
group by 
     checkout_date_type
    ,action_date
    ,mdd
    ,user_type
grouping sets (
    (checkout_date_type, action_date),
    (checkout_date_type, action_date, mdd),
    (checkout_date_type, action_date, user_type)
)
order by 
    checkout_date_type, 
    action_date, 
    cross_scenario
;


--- 3、平日
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
,q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  else '其他' end as checkout_date_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and order_date >= '2024-07-07' and order_date <= '2025-08-17'  --- 25年平时
        and order_no <> '103576132435'
)
,uv as (---D页流量25年春节
    select dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  else '其他' end as checkout_date_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '2025-08-17' 
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6
    union all
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  else '其他' end as checkout_date_type
    from default.dw_user_app_detail_visit_di_v3 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt <= '20241202' and dt >= '20240707'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6
)
,uv_data as (
    select dt
        ,if(grouping(mdd) = 1, 'ALL', mdd) as mdd
        ,if(grouping(user_type) = 1, 'ALL', user_type) as user_type
        ,count(distinct user_id) as uv
        ,count(distinct case when checkout_date_type = '25年暑期' then user_id end) as summer_uv
    from uv
    group by 
        dt
        ,mdd
        ,user_type
    grouping sets (
        (dt),
        (dt, mdd),
        (dt, user_type)
    )
)
,order_data as (
    select order_date
        ,if(grouping(mdd) = 1, 'ALL', mdd) as mdd
        ,if(grouping(user_type) = 1, 'ALL', user_type) as user_type
        ,count(distinct order_no) as order_cnt
        ,count(distinct case when checkout_date_type = '25年暑期' then order_no end) as summer_order_cnt
        ,count(distinct user_id) as order_uv
        ,count(distinct case when checkout_date_type = '25年暑期' then user_id end) as summer_order_uv
    from q_order
    group by 
        order_date
        ,mdd
        ,user_type
    grouping sets (
        (order_date),
        (order_date, mdd),
        (order_date, user_type)
    )
)

select t1.dt
    ,t1.mdd
    ,t1.user_type
    ,t1.uv
    ,t1.summer_uv
    ,t2.order_cnt
    ,t2.summer_order_cnt
    ,t2.order_uv
    ,t2.summer_order_uv
    ,summer_uv/uv as summer_uv_ratio
    ,summer_order_cnt / order_cnt as summer_order_ratio
    ,summer_order_uv / order_uv as summer_order_uv_ratio
from uv_data t1 
left join order_data t2 on t1.dt = t2.order_date and t1.mdd = t2.mdd and t1.user_type = t2.user_type
order by t1.dt, t1.mdd, t1.user_type
;



--- 4、价格带x目的地分析
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
,q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when init_gmv / room_night < 400  then '1[0,400)'
                  when init_gmv / room_night >= 400 and init_gmv / room_night < 800  then '2[400,800)'
                  when init_gmv / room_night >= 800 and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' end adr_level
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17'
        and order_no <> '103576132435'
)

select order_date
    ,if(grouping(mdd) = 1, 'ALL', mdd) as mdd
    ,if(grouping(user_type) = 1, 'ALL', user_type) as user_type
    ,if(grouping(adr_level) = 1, 'ALL', adr_level) as adr_level
    ,count(distinct order_no) as order_cnt
    ,count(distinct case when checkout_date_type = '25年暑期' then order_no end) as summer_order_cnt
    ,count(distinct user_id) as order_uv
    ,count(distinct case when checkout_date_type = '25年暑期' then user_id end) as summer_order_uv
from q_order
group by 
    order_date
    ,mdd
    ,user_type,adr_level
grouping sets (
    (order_date,adr_level),
    (order_date, mdd,adr_level),
    (order_date, user_type,adr_level)
)
;






--- 1、暑期、五一、平日离店用户画像分析
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
,q_order as (----订单明细表表包含取消  
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
            ,checkout_date,mobile_platform
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
            or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
            or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
            or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
        )
        and order_no <> '103576132435'
)
,search_child as (
    select user_id
    from default.dw_user_app_search_di_v3
    where dt >= '20240101' and dt <= '20260505'
        and device_id is not null
        and device_id <> ''
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and business_type = 'hotel'
        and (
                (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%')
            )
    group by 1
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
    select user_id
           ,user_type  -- 用户类型
           ,order_no   -- 订单号
           ,gender     -- 性别
           ,city_name,prov_name
           ,city_level -- 城市等级
           ,mdd        -- 目的地
           ,checkout_date_type -- 离店类型
           ,hotel_grade     -- 酒店星级
           ,birth_year_month -- 出生年月
           ,age      -- 年龄
           ,case when age <= 18 then '1(0,18]'
                 when age >= 19 and age <= 24 then '2[19,24]'
                 when age >= 25 and age <= 30 then '3[25,30]'
                 when age >= 31 and age <= 35 then '4[31,35]'
                 when age >= 36 and age <= 40 then '5[36,40]'
                 when age >= 41 and age <= 45 then '6[41,45]'
                 when age >= 46 and age <= 50 then '7[46,50]'
                 when age > 50 then '8[51+)'
            else '未知' end as age_level  -- 年龄段
            ,mobile_platform  -- 手机平台
            ,is_child  -- 亲子
    from (
        select o.order_no
            ,user_type
            ,mdd
            ,hotel_grade
            ,o.user_id,o.mobile_platform,checkout_date_type
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,case when birth_year_month is null then '未知'
                else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int)
                end as age
            ,case when s.user_id is not null then '亲子' else '非亲子' end as is_child
        from q_order o
        left join user_profile u on u.user_id = o.user_id
        left join search_child s on s.user_id = o.user_id
    )t
)
-- =========================================================
-- 以下为数据聚合与交叉分析部分
-- =========================================================
select 
     checkout_date_type 
    ,nvl(mdd, 'ALL') as mdd
    ,nvl(age_level, 'ALL') as age_level
    ,nvl(gender, 'ALL') as gender
    ,nvl(mobile_platform, 'ALL') as mobile_platform
    ,nvl(city_level, 'ALL') as city_level
    ,nvl(is_child, 'ALL') as is_child
    
    -- 核心画像指标
    ,count(distinct user_id) as uv 
    ,count(distinct order_no) as order_cnt
    
    -- 场景标签映射（严格使用 grouping 判定，避免多场景混淆）
    ,case 
        -- 0. 大盘汇总
        when grouping(mdd)=1 and grouping(is_child)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '0. 大盘汇总：离店类型'
        
        -- 1. 原有多维交叉场景
        when grouping(mdd)=1 and grouping(is_child)=1 and grouping(age_level)=0 and grouping(gender)=0 and grouping(mobile_platform)=0 and grouping(city_level)=0 then '场景1：年龄x性别x机型x城市级别x离店类型'
        when grouping(mdd)=0 and grouping(is_child)=1 and grouping(age_level)=0 and grouping(gender)=0 and grouping(mobile_platform)=0 and grouping(city_level)=0 then '场景2：年龄x性别x机型x城市级别x目的地x离店类型'
        when grouping(mdd)=0 and grouping(is_child)=0 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '场景3：目的地x亲子x离店类型'
        when grouping(mdd)=0 and grouping(age_level)=0 and grouping(is_child)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '场景4：目的地x年龄段x离店类型'
        
        -- 2. 新增单维交叉场景
        when grouping(gender)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：性别x离店类型'
        when grouping(age_level)=0 and grouping(mdd)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：年龄段x离店类型'
        when grouping(city_level)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(is_child)=1 then '单维：城市线级x离店类型'
        when grouping(mobile_platform)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：手机平台x离店类型'
        when grouping(is_child)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '单维：亲子x离店类型'
        when grouping(mdd)=0 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：目的地x离店类型'
        
     end as cross_scenario
from order_result 
group by 
     checkout_date_type
    ,mdd
    ,age_level
    ,gender
    ,mobile_platform
    ,city_level
    ,is_child
grouping sets (
    -- 0. 大盘纯离店类型汇总 (相当于为总体表现定基准)
    (checkout_date_type),

    -- 1. 原始复杂的交叉场景
    (checkout_date_type, age_level, gender, mobile_platform, city_level),
    (checkout_date_type, age_level, gender, mobile_platform, city_level, mdd),
    (checkout_date_type, mdd, is_child),
    (checkout_date_type, mdd, age_level),
    
    -- 2. 新增的单个维度与离店类型交叉场景
    (checkout_date_type, gender),           -- 性别 x 离店类型
    (checkout_date_type, age_level),        -- 年龄段 x 离店类型
    (checkout_date_type, city_level),       -- 城市线级 x 离店类型
    (checkout_date_type, mobile_platform),  -- 手机平台 x 离店类型
    (checkout_date_type, is_child),         -- 亲子 x 离店类型
    (checkout_date_type, mdd)               -- 目的地 x 离店类型
)
;



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
,q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade,user_name
            ,case when checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  else '其他' end as order_date_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-07' and checkout_date <= '2025-08-17')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
        )
        and order_no <> '103576132435'
)
,platform_new as (--- 判定平台新
    select  dt,user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-01-01' and dt <= '2025-08-17'
        and dict_type = 'pncl_wl_username'
    group by 1,2
)

select order_date_type
        ,case
            when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
            when t1.user_type = '新客' then '平台老业务新'
            else '老客'
        end as user_type1
        ,count(distinct t1.user_id) as uv
from q_order t1 
left join platform_new t2 on t1.user_name=t2.user_pk and t1.order_date = t2.dt
group by 1,2
;


--- 1、暑期、五一、平日离店用户画像分析
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
,q_order as (----订单明细表表包含取消  
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            
            ,checkout_date,mobile_platform
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,search_child as (
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt,user_id
    from default.dw_user_app_search_di_v3
    where dt >= '20250101' and dt <= '20260510'
        and device_id is not null
        and device_id <> ''
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and business_type = 'hotel'
        and (
                (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%')
            )
    group by 1,2
)



select t1.order_date,sum(room_night) as rn
        ,sum(case when t2.user_id is not null then room_night end) as child_rn
        ,sum(case when t2.user_id is not null then room_night end) / sum(room_night) as child_rn_ratio
from q_order t1
left join search_child t2 on t1.user_id = t2.user_id and t1.order_date = t2.dt
group by 1
order by 1 
;



with q_order as (----订单明细表表包含取消  
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            
            ,checkout_date,mobile_platform
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and ext_flag_map['ord_children_num'] > 0 --- 包含儿童订单
)
,search_child as (
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt,user_id
    from default.dw_user_app_search_di_v3
    where dt >= '20260101' and dt <= '20260510'
        and device_id is not null
        and device_id <> ''
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and business_type = 'hotel'
        and (
                (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%')
            )
    group by 1,2
)

select t1.order_date,count(distinct t1.user_id) as order_uv
        ,count(distinct case when t2.user_id is not null then t1.user_id end) as child_order_uv
        ,count(distinct case when t2.user_id is not null then t1.user_id end) / count(distinct t1.user_id) as child_order_uv_ratio
from q_order t1 
left join search_child t2 on t1.user_id = t2.user_id and t1.order_date = t2.dt
group by 1
order by 1 desc
;



-- 最新亲子逻辑
--- 暑期、五一、平日离店用户画像分析0512
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
,q_order as (----订单明细表表包含取消  
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
            ,checkout_date,mobile_platform
            ,ext_flag_map['ord_children_num'] ord_children_num
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
            or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
            or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
            or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
        )
        and order_no <> '103576132435'
)
,search_child as (
    select checkout_date_type,user_id
    from (
        select  user_id
                ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
        from default.dw_user_app_search_di_v3
        where dt >= '20240101' and dt <= '20260505'
            and device_id is not null
            and device_id <> ''
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and business_type = 'hotel'
            and (   --- 亲子标签筛选
                    (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                    or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%'  or query like '%亲子%')
                )
            and (
                (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
                or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
                or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
                or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
                or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
                or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
            )
        group by 1,2
        union all
        select user_id
                ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        where dt <= '2026-05-05' and dt >= '2024-01-01'        
            and guestinfos['child_num'] > 0
            and (
                    (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
                    or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
                    or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
                    or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
                    or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
                    or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
                )
        group by 1,2
    ) group by 1,2
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
    select user_id
           ,user_type  -- 用户类型
           ,order_no   -- 订单号
           ,gender     -- 性别
           ,city_name,prov_name
           ,city_level -- 城市等级
           ,mdd        -- 目的地
           ,checkout_date_type -- 离店类型
           ,hotel_grade     -- 酒店星级
           ,birth_year_month -- 出生年月
           ,age      -- 年龄
           ,case when age <= 18 then '1(0,18]'
                 when age >= 19 and age <= 24 then '2[19,24]'
                 when age >= 25 and age <= 30 then '3[25,30]'
                 when age >= 31 and age <= 35 then '4[31,35]'
                 when age >= 36 and age <= 40 then '5[36,40]'
                 when age >= 41 and age <= 45 then '6[41,45]'
                 when age >= 46 and age <= 50 then '7[46,50]'
                 when age > 50 then '8[51+)'
            else '未知' end as age_level  -- 年龄段
            ,mobile_platform  -- 手机平台
            ,is_child  -- 亲子
    from (
        select o.order_no
            ,user_type
            ,mdd
            ,hotel_grade
            ,o.user_id,o.mobile_platform,checkout_date_type
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,case when birth_year_month is null then '未知'
                else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int)
                end as age
            ,case when (s.user_id is not null or o.ord_children_num > 0) then '亲子' else '非亲子' end as is_child
        from q_order o
        left join user_profile u on u.user_id = o.user_id
        left join search_child s on s.user_id = o.user_id and s.checkout_date_type=o.checkout_date_type
    )t
)
-- =========================================================
-- 以下为数据聚合与交叉分析部分
-- =========================================================
select 
     checkout_date_type 
    ,nvl(mdd, 'ALL') as mdd
    ,nvl(age_level, 'ALL') as age_level
    ,nvl(gender, 'ALL') as gender
    ,nvl(mobile_platform, 'ALL') as mobile_platform
    ,nvl(city_level, 'ALL') as city_level
    ,nvl(is_child, 'ALL') as is_child
    
    -- 核心画像指标
    ,count(distinct user_id) as uv 
    ,count(distinct order_no) as order_cnt
    
    -- 场景标签映射（严格使用 grouping 判定，避免多场景混淆）
    ,case 
        -- 0. 大盘汇总
        when grouping(mdd)=1 and grouping(is_child)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '0. 大盘汇总：离店类型'
        
        -- 1. 原有多维交叉场景
        when grouping(mdd)=1 and grouping(is_child)=1 and grouping(age_level)=0 and grouping(gender)=0 and grouping(mobile_platform)=0 and grouping(city_level)=0 then '场景1：年龄x性别x机型x城市级别x离店类型'
        when grouping(mdd)=0 and grouping(is_child)=1 and grouping(age_level)=0 and grouping(gender)=0 and grouping(mobile_platform)=0 and grouping(city_level)=0 then '场景2：年龄x性别x机型x城市级别x目的地x离店类型'
        when grouping(mdd)=0 and grouping(is_child)=0 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '场景3：目的地x亲子x离店类型'
        when grouping(mdd)=0 and grouping(age_level)=0 and grouping(is_child)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '场景4：目的地x年龄段x离店类型'
        
        -- 2. 新增单维交叉场景
        when grouping(gender)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：性别x离店类型'
        when grouping(age_level)=0 and grouping(mdd)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：年龄段x离店类型'
        when grouping(city_level)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(is_child)=1 then '单维：城市线级x离店类型'
        when grouping(mobile_platform)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：手机平台x离店类型'
        when grouping(is_child)=0 and grouping(mdd)=1 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 then '单维：亲子x离店类型'
        when grouping(mdd)=0 and grouping(age_level)=1 and grouping(gender)=1 and grouping(mobile_platform)=1 and grouping(city_level)=1 and grouping(is_child)=1 then '单维：目的地x离店类型'
        
     end as cross_scenario
from order_result 
group by 
     checkout_date_type
    ,mdd
    ,age_level
    ,gender
    ,mobile_platform
    ,city_level
    ,is_child
grouping sets (
    -- 0. 大盘纯离店类型汇总 (相当于为总体表现定基准)
    (checkout_date_type),

    -- 1. 原始复杂的交叉场景
    (checkout_date_type, age_level, gender, mobile_platform, city_level),
    (checkout_date_type, age_level, gender, mobile_platform, city_level, mdd),
    (checkout_date_type, mdd, is_child),
    (checkout_date_type, mdd, age_level),
    
    -- 2. 新增的单个维度与离店类型交叉场景
    (checkout_date_type, gender),           -- 性别 x 离店类型
    (checkout_date_type, age_level),        -- 年龄段 x 离店类型
    (checkout_date_type, city_level),       -- 城市线级 x 离店类型
    (checkout_date_type, mobile_platform),  -- 手机平台 x 离店类型
    (checkout_date_type, is_child),         -- 亲子 x 离店类型
    (checkout_date_type, mdd)               -- 目的地 x 离店类型
)
;


-- 最新亲子逻辑
with q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07' then '25年国庆'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
            ,checkout_date,mobile_platform
            ,ext_flag_map['ord_children_num'] ord_children_num
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
            or (checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07')  --- 25年国庆
            or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
            or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
            or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
        )
        and order_no <> '103576132435'
)
,search_child as (
    select checkout_date_type,user_id
    from (
        select  user_id
                ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07' then '25年国庆'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
        from default.dw_user_app_search_di_v3
        where dt >= '20240101' and dt <= '20260505'
            and device_id is not null
            and device_id <> ''
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and business_type = 'hotel'
            and (   --- 亲子标签筛选
                    (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                    or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%'  or query like '%亲子%')
                )
            and (
                (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
                or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
                or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
                or (checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07')  --- 25年国庆
                or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
                or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
                or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
            )
        group by 1,2
        union all
        select user_id
                ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07' then '25年国庆'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        where dt <= '2026-05-05' and dt >= '2024-01-01'        
            and guestinfos['child_num'] > 0
            and (
                (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
                or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
                or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
                or (checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07')  --- 25年国庆
                or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
                or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
                or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
            )
        group by 1,2
    ) group by 1,2
)

,order_result as (
    select o.order_no
        ,o.mdd
        ,o.user_id,o.checkout_date_type
        ,case when (s.user_id is not null or o.ord_children_num > 0) then '亲子' else '非亲子' end as is_child
    from q_order o
    left join search_child s on s.user_id = o.user_id  and s.checkout_date_type=o.checkout_date_type
)

select 
     checkout_date_type 
    ,nvl(mdd, 'ALL') as mdd
    ,nvl(is_child, 'ALL') as is_child

    -- 核心画像指标
    ,count(distinct user_id) as uv 
    ,count(distinct order_no) as order_cnt

from order_result 
group by 
     checkout_date_type
    ,mdd
    ,is_child
grouping sets (
    (checkout_date_type),
    (checkout_date_type, mdd, is_child),
    (checkout_date_type, is_child),         -- 亲子 x 离店类型
    (checkout_date_type, mdd)               -- 目的地 x 离店类型
)
;


-- 预定口径亲子间夜
with q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,ext_flag_map['ord_children_num'] ord_children_num
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,search_child as (
    select dt,user_id
    from (
        select  user_id
                ,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
        from default.dw_user_app_search_di_v3
        where dt >= '20250101' and dt <= '20260512'
            and device_id is not null
            and device_id <> ''
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and business_type = 'hotel'
            and (   --- 亲子标签筛选
                    (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                    or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%'  or query like '%亲子%')
                )
        group by 1,2
        union all
        select user_id
                ,dt
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        where  dt >= '2025-01-01' and dt <= '2026-05-12'   
            and guestinfos['child_num'] > 0
        group by 1,2
    ) 
    group by 1,2
)
,order_result as (
    select t1.order_date,t1.user_id
            ,case when ord_children_num > 0 or t2.user_id is not null then '亲子' else '非亲子' end as is_child
    from q_order t1
    left join search_child t2 on t1.user_id = t2.user_id and t1.order_date = t2.dt
    group by 1,2,3
)

select t1.order_date
       ,nvl(mdd, 'ALL') as mdd
       ,sum(room_night) as total_room_night
       ,sum(case when t2.is_child='亲子' then room_night end) as child_room_night
       ,sum(case when t2.is_child='亲子' then room_night end) / sum(room_night) as child_room_night_ratio
from q_order t1
left join order_result t2 on t1.user_id = t2.user_id and t1.order_date = t2.order_date
group by 1,2 
grouping sets (
    (order_date, mdd),
    (order_date)
)
order by 1,2
;


--- 暑期、五一、平日离店用户星级占比情况
with q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night,hotel_grade
            ,case when checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31' then '25年暑期'
                  when checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05' then '25年五一'
                  when checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20' then '25年平时'
                  when checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07' then '25年国庆'
                  when checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23' then '26年春节'
                  when checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06' then '26年清明'
                  when checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
            ,checkout_date,mobile_platform
            ,ext_flag_map['ord_children_num'] ord_children_num
            ,case when hotel_grade in (4,5) then '高星' 
                  when hotel_grade in (3) then '中星' 
                  else '低星' end as hotel_grade_type
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and (
            (checkout_date >= '2025-07-01' and checkout_date <= '2025-08-31')  --- 25年暑期
            or (checkout_date >= '2025-05-01' and checkout_date <= '2025-05-05')  --- 25年五一
            or (checkout_date >= '2025-06-06' and checkout_date <= '2025-06-20')  --- 25年平时
            or (checkout_date >= '2025-10-01' and checkout_date <= '2025-10-07')  --- 25年国庆
            or (checkout_date >= '2026-02-15' and checkout_date <= '2026-02-23')  --- 26年春节
            or (checkout_date >= '2026-04-04' and checkout_date <= '2026-04-06')  --- 26年清明
            or (checkout_date >= '2026-05-01' and checkout_date <= '2026-05-05')  --- 26年五一
        )
        and order_no <> '103576132435'
)
,c_order as (--- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'
                when extend_info['COUNTRY'] in ('泰国','日本','韩国') then extend_info['COUNTRY']
                else '其他' end as new_mdd
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,case when extend_info['STAR'] in (4,5) then '高星' 
                  when extend_info['STAR'] in (3) then '中星' 
                  else '低星' end as hotel_grade_type
            ,substr(checkout_date,1,10) checkout_date
            ,case when substr(checkout_date,1,10) >= '2025-07-01' and substr(checkout_date,1,10) <= '2025-08-31' then '25年暑期'
                  when substr(checkout_date,1,10) >= '2025-05-01' and substr(checkout_date,1,10) <= '2025-05-05' then '25年五一'
                  when substr(checkout_date,1,10) >= '2025-06-06' and substr(checkout_date,1,10) <= '2025-06-20' then '25年平时'
                  when substr(checkout_date,1,10) >= '2025-10-01' and substr(checkout_date,1,10) <= '2025-10-07' then '25年国庆'
                  when substr(checkout_date,1,10) >= '2026-02-15' and substr(checkout_date,1,10) <= '2026-02-23' then '26年春节'
                  when substr(checkout_date,1,10) >= '2026-04-04' and substr(checkout_date,1,10) <= '2026-04-06' then '26年清明'
                  when substr(checkout_date,1,10) >= '2026-05-01' and substr(checkout_date,1,10) <= '2026-05-05' then '26年五一'
                  else '其他' end as checkout_date_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and order_status <> 'C'
      and (
            (substr(checkout_date,1,10) >= '2025-07-01' and substr(checkout_date,1,10) <= '2025-08-31')  --- 25年暑期
            or (substr(checkout_date,1,10) >= '2025-05-01' and substr(checkout_date,1,10) <= '2025-05-05')  --- 25年五一
            or (substr(checkout_date,1,10) >= '2025-06-06' and substr(checkout_date,1,10) <= '2025-06-20')  --- 25年平时
            or (substr(checkout_date,1,10) >= '2025-10-01' and substr(checkout_date,1,10) <= '2025-10-07')  --- 25年国庆
            or (substr(checkout_date,1,10) >= '2026-02-15' and substr(checkout_date,1,10) <= '2026-02-23')  --- 26年春节
            or (substr(checkout_date,1,10) >= '2026-04-04' and substr(checkout_date,1,10) <= '2026-04-06')  --- 26年清明
            or (substr(checkout_date,1,10) >= '2026-05-01' and substr(checkout_date,1,10) <= '2026-05-05')  --- 26年五一
        )
)

select t1.checkout_date_type,t1.hotel_grade_type
       ,t1.total_room_night as qunar_room_night
       ,t2.total_room_night as ctrip_room_night
from (
    select checkout_date_type
        ,hotel_grade_type
        ,sum(room_night) as total_room_night
    from q_order
    group by 1,2
)t1 left join (
    select checkout_date_type
        ,hotel_grade_type
        ,sum(room_night) as total_room_night
    from c_order
    group by 1,2
)t2 on t1.checkout_date_type = t2.checkout_date_type and t1.hotel_grade_type = t2.hotel_grade_type
;
