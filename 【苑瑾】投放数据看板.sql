------------------------------------- sql1新客
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,platform_new as (--- 判定平台新
    select distinct dt,
                user_pk,
                user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3

    union all

    select date(click_time) as dt,
         ad_name, --specialkey
         uid
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.specialkey,
            t.merge_specialkey_name,
            t.platform,
            t.ad_name,
            t.site_set,
            t.category
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category = '信息流'
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type,
            m.ad_name
    from market_active m
    left join market_click i on m.uid = i.uid --and m.specialkey = i.ad_name
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        --i.dt = m.dt
        and i.uid is not null
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,ad_name,user_type
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (  ----订单表 
    select order_date,
        case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_1 as (
    select distinct a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`广告引流到APP活跃uv`
       ,gjuv.`引流新客UV`
       ,concat(round(gjuv.`引流新客UV` /  uv.`新客UV` * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流新客UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量`) ADR


       ,muv.`广告引流到APP活跃uv_窄口径`
       ,gjuv.`引流新客UV_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流新客UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径`) ADR
from (--- 广告投放数据
    select  dt
           ,count(distinct username) `广告引流到APP活跃uv`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `广告引流到APP活跃uv_窄口径`
    from  market_uv_1 a
    where user_type = '新客'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流新客UV`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流新客UV_窄口径`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type='新客'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t1.user_type='新客'  --- 筛选流量用户类型
    and t2.username is not null
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
          ,count(distinct case when is_gj = 'Y' then t1.user_name end) `生单用户量_窄口径`
          ,count(distinct case when is_gj = 'Y' then t1.order_no end) `订单量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.room_night end) `间夜量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.init_gmv end) `GMV_窄口径`
    from q_order_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type='新客'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t1.user_type='新客'   --- 筛选订单用户类型
    and t2.username is not null
    group by 1
)order  on  muv.dt =order.order_date 
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) `新客UV`
    from uv_1 t1 
    where t1.user_type='新客'  --- 筛选流量用户类型
    group by 1
)uv on  uv.dt =muv.dt 
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    where t1.user_type='新客'  --- 筛选订单用户类型
    group by 1
)ouv on  ouv.order_date =muv.dt 
order by `日期` desc
;


--------------------------------------- sql2业务新
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,platform_new as (--- 判定平台新
    select distinct dt,
                user_pk,
                user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3

    union all

    select date(click_time) as dt,
         ad_name, --specialkey
         uid
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.specialkey,
            t.merge_specialkey_name,
            t.platform,
            t.ad_name,
            t.site_set,
            t.category
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category = '信息流'
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type,
            m.ad_name
    from market_active m
    left join market_click i on m.uid = i.uid --and m.specialkey = i.ad_name
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        --i.dt = m.dt
        and i.uid is not null
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,ad_name,user_type
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (  ----订单表 
    select order_date,
        case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_1 as (
    select distinct a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`广告引流到APP活跃uv`
       ,gjuv.`引流新客UV`
       ,concat(round(gjuv.`引流新客UV` /  uv.`新客UV` * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流新客UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量`) ADR


       ,muv.`广告引流到APP活跃uv_窄口径`
       ,gjuv.`引流新客UV_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流新客UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径`) ADR
from (--- 广告投放数据
    select  dt
           ,count(distinct username) `广告引流到APP活跃uv`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `广告引流到APP活跃uv_窄口径`
    from  market_uv_1 a
    where user_type_new = '平台老业务新'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流新客UV`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流新客UV_窄口径`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type_new = '平台老业务新'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t1.user_type_new = '平台老业务新'  --- 筛选流量用户类型
    and t2.username is not null
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
          ,count(distinct case when is_gj = 'Y' then t1.user_name end) `生单用户量_窄口径`
          ,count(distinct case when is_gj = 'Y' then t1.order_no end) `订单量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.room_night end) `间夜量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.init_gmv end) `GMV_窄口径`
    from q_order_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type_new = '平台老业务新'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t1.user_type_new = '平台老业务新'   --- 筛选订单用户类型
    and t2.username is not null
    group by 1
)order  on  muv.dt =order.order_date 
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) `新客UV`
    from uv_1 t1 
    where t1.user_type_new = '平台老业务新'  --- 筛选流量用户类型
    group by 1
)uv on  uv.dt =muv.dt 
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    where t1.user_type_new = '平台老业务新'  --- 筛选订单用户类型
    group by 1
)ouv on  ouv.order_date =muv.dt 
order by `日期` desc
;


--------------------------------------- sql3平台新
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,platform_new as (--- 判定平台新
    select distinct dt,
                user_pk,
                user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3

    union all

    select date(click_time) as dt,
         ad_name, --specialkey
         uid
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.specialkey,
            t.merge_specialkey_name,
            t.platform,
            t.ad_name,
            t.site_set,
            t.category
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category = '信息流'
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type,
            m.ad_name
    from market_active m
    left join market_click i on m.uid = i.uid --and m.specialkey = i.ad_name
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        --i.dt = m.dt
        and i.uid is not null
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,ad_name,user_type
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (  ----订单表 
    select order_date,
        case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_1 as (
    select distinct a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`广告引流到APP活跃uv`
       ,gjuv.`引流新客UV`
       ,concat(round(gjuv.`引流新客UV` /  uv.`新客UV` * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流新客UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量`) ADR


       ,muv.`广告引流到APP活跃uv_窄口径`
       ,gjuv.`引流新客UV_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流新客UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径`) ADR
from (--- 广告投放数据
    select  dt
           ,count(distinct username) `广告引流到APP活跃uv`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `广告引流到APP活跃uv_窄口径`
    from  market_uv_1 a
    where user_type_new = '平台新业务新'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流新客UV`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流新客UV_窄口径`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type_new = '平台新业务新'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t1.user_type_new = '平台新业务新'  --- 筛选流量用户类型
    and t2.username is not null
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
          ,count(distinct case when is_gj = 'Y' then t1.user_name end) `生单用户量_窄口径`
          ,count(distinct case when is_gj = 'Y' then t1.order_no end) `订单量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.room_night end) `间夜量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.init_gmv end) `GMV_窄口径`
    from q_order_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type_new = '平台新业务新'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t1.user_type_new = '平台新业务新'   --- 筛选订单用户类型
    and t2.username is not null
    group by 1
)order  on  muv.dt =order.order_date 
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) `新客UV`
    from uv_1 t1 
    where t1.user_type_new = '平台新业务新'  --- 筛选流量用户类型
    group by 1
)uv on  uv.dt =muv.dt 
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    where t1.user_type_new = '平台新业务新'  --- 筛选订单用户类型
    group by 1
)ouv on  ouv.order_date =muv.dt 
order by `日期` desc
;


------------------------------------- sql4老客
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,platform_new as (--- 判定平台新
    select distinct dt,
                user_pk,
                user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3

    union all

    select date(click_time) as dt,
         ad_name, --specialkey
         uid
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.specialkey,
            t.merge_specialkey_name,
            t.platform,
            t.ad_name,
            t.site_set,
            t.category
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category = '信息流'
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type,
            m.ad_name
    from market_active m
    left join market_click i on m.uid = i.uid --and m.specialkey = i.ad_name
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        --i.dt = m.dt
        and i.uid is not null
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,ad_name,user_type
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (  ----订单表 
    select order_date,
        case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_1 as (
    select distinct a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`广告引流到APP活跃uv`
       ,gjuv.`引流老客UV`
       ,concat(round(gjuv.`引流老客UV` /  uv.`老客UV` * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流老客UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR


       ,muv.`广告引流到APP活跃uv_窄口径`
       ,gjuv.`引流老客UV_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流老客UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径` ) ADR
from (--- 广告投放数据
    select  dt
           ,count(distinct username) `广告引流到APP活跃uv`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `广告引流到APP活跃uv_窄口径`
    from  market_uv_1 a
    where user_type = '老客'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流老客UV`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流老客UV_窄口径`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type='老客'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t1.user_type='老客'  --- 筛选流量用户类型
    and t2.username is not null
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
          ,count(distinct case when is_gj = 'Y' then t1.user_name end) `生单用户量_窄口径`
          ,count(distinct case when is_gj = 'Y' then t1.order_no end) `订单量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.room_night end) `间夜量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.init_gmv end) `GMV_窄口径`
    from q_order_1 t1 
    left join (
        select dt,username
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        where user_type='老客'  --- 筛选市场用户类型
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t1.user_type='老客'   --- 筛选订单用户类型
    and t2.username is not null
    group by 1
)order  on  muv.dt =order.order_date 
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) `老客UV`
    from uv_1 t1 
    where t1.user_type='老客'  --- 筛选流量用户类型
    group by 1
)uv on  uv.dt =muv.dt 
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    where t1.user_type='老客'  --- 筛选订单用户类型
    group by 1
)ouv on  ouv.order_date =muv.dt 
order by `日期` desc
;




------------------------------------- 多维度
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,platform_new as (--- 判定平台新
    select distinct dt,
                user_pk,
                user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3

    union all

    select date(click_time) as dt,
         ad_name, --specialkey
         uid
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.specialkey,
            t.merge_specialkey_name,
            t.platform,
            t.ad_name,
            t.site_set,
            t.category
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category = '信息流'
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type,
            m.ad_name
    from market_active m
    left join market_click i on m.uid = i.uid --and m.specialkey = i.ad_name
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        --i.dt = m.dt
        and i.uid is not null
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,ad_name,user_type
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (  ----订单表 
    select order_date,
        case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_1 as (
    select distinct a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


select  muv.dt `日期`
       ,muv.user_type_new `用户类型`
       ,muv.platform `平台`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`广告引流到APP活跃uv`
       ,gjuv.`引流UV`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR


       ,muv.`广告引流到APP活跃uv_窄口径`
       ,gjuv.`引流UV_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径` ) ADR
from (--- 广告投放数据
    select  dt,user_type_new,platform
           ,count(distinct username) `广告引流到APP活跃uv`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `广告引流到APP活跃uv_窄口径`
    from  market_uv_1 a
    group by 1,2,3
) muv
left join (--- 流量数据
    select t1.dt,user_type_new,platform
          ,count(t1.user_name) `引流UV`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流UV_窄口径`
    from uv_1 t1 
    left join (
        select dt,username,platform
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        group by 1,2,3
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1,2,3
)gjuv on  gjuv.dt =muv.dt and muv.user_type_new=gjuv.user_type_new and muv.platform=gjuv.platform
left join (--- 订单数据
    select order_date,user_type_new,platform
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
          ,count(distinct case when is_gj = 'Y' then t1.user_name end) `生单用户量_窄口径`
          ,count(distinct case when is_gj = 'Y' then t1.order_no end) `订单量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.room_night end) `间夜量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.init_gmv end) `GMV_窄口径`
    from q_order_1 t1 
    left join (
        select dt,username,platform
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        group by 1,2,3
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1,2,3
)order  on  muv.dt =order.order_date  and muv.user_type_new=order.user_type_new and muv.platform=order.platform
left join (--- 国酒流量数据 算占比
    select dt,user_type_new
          ,count(t1.user_name) UV
    from uv_1 t1 
    -- where t1.user_type='老客'  --- 筛选流量用户类型
    group by 1,2
)uv on  uv.dt =muv.dt  and muv.user_type_new=uv.user_type_new
left join (--- 国酒订单数据 算占比
    select order_date,user_type_new
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    where t1.user_type='老客'  --- 筛选订单用户类型
    group by 1,2
)ouv on  ouv.order_date =muv.dt and muv.user_type_new=ouv.user_type_new
order by `日期` desc
;



------------------------------------- 多维度new
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,platform_new as (--- 判定平台新
    select distinct dt,
                user_pk,
                user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3

    union all

    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 30) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.specialkey,
            t.merge_specialkey_name,
            t.platform,
            t.ad_name,
            t.site_set,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category = '信息流'
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type,
            m.ad_name
    from market_active m
    left join market_click i on m.uid = i.uid --and m.specialkey = i.ad_name
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        --i.dt = m.dt
        and i.uid is not null
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,ad_name,user_type
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (  ----订单表 
    select order_date,
        case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_1 as (
    select distinct a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


select  muv.dt `日期`
       ,muv.user_type_new `用户类型`
       ,muv.platform `平台`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`广告引流到APP活跃uv`
       ,gjuv.`引流UV`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR


       ,muv.`广告引流到APP活跃uv_窄口径`
       ,gjuv.`引流UV_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径` ) ADR
from (--- 广告投放数据
    select  dt,user_type_new,platform
           ,count(distinct username) `广告引流到APP活跃uv`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `广告引流到APP活跃uv_窄口径`
    from  market_uv_1 a
    group by 1,2,3
) muv
left join (--- 流量数据
    select t1.dt,user_type_new,platform
          ,count(t1.user_name) `引流UV`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流UV_窄口径`
    from uv_1 t1 
    left join (
        select dt,username,platform
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        group by 1,2,3
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1,2,3
)gjuv on  gjuv.dt =muv.dt and muv.user_type_new=gjuv.user_type_new and muv.platform=gjuv.platform
left join (--- 订单数据
    select order_date,user_type_new,platform
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
          ,count(distinct case when is_gj = 'Y' then t1.user_name end) `生单用户量_窄口径`
          ,count(distinct case when is_gj = 'Y' then t1.order_no end) `订单量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.room_night end) `间夜量_窄口径`
          ,sum(case when is_gj = 'Y' then t1.init_gmv end) `GMV_窄口径`
    from q_order_1 t1 
    left join (
        select dt,username,platform
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        group by 1,2,3
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1,2,3
)order  on  muv.dt =order.order_date  and muv.user_type_new=order.user_type_new and muv.platform=order.platform
left join (--- 国酒流量数据 算占比
    select dt,user_type_new
          ,count(t1.user_name) UV
    from uv_1 t1 
    -- where t1.user_type='老客'  --- 筛选流量用户类型
    group by 1,2
)uv on  uv.dt =muv.dt  and muv.user_type_new=uv.user_type_new
left join (--- 国酒订单数据 算占比
    select order_date,user_type_new
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    where t1.user_type='老客'  --- 筛选订单用户类型
    group by 1,2
)ouv on  ouv.order_date =muv.dt and muv.user_type_new=ouv.user_type_new
order by `日期` desc
;






---- 订单QC数据，小程序和app
with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when terminal_channel_type = 'app' then 'app' when user_tracking_data['inner_channel'] = 'smart_app' then 'wechat' else 'else' end as channel
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
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
            ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,case when terminal_channel_type = 'app' then 'app' when extend_info['IS_WEBCHATAPP'] = 'T' then 'wechat' else 'else' end as channel
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
    --   and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2024-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)

select t1.mth
       ,order_no_app_q
       ,order_no_app_c
       ,concat(round(order_no_app_q / order_no_app_c * 100, 1),'%') order_no_app_qc
       ,order_no_app_nu_q
       ,order_no_app_nu_c
       ,concat(round(order_no_app_nu_q / order_no_app_nu_c * 100, 1),'%') order_no_app_nu_qc


       ,order_no_wechat_q
       ,order_no_wechat_c
       ,concat(round(order_no_wechat_q / order_no_wechat_c * 100, 1),'%') order_no_wechat_qc
       ,order_no_wechat_nu_q
       ,order_no_wechat_nu_c
       ,concat(round(order_no_wechat_nu_q / order_no_wechat_nu_c * 100, 1),'%') order_no_wechat_nu_qc

from (
    select substr(order_date,1,7) mth
           ,round(sum(order_no_app_q) / count(1),0) order_no_app_q
           ,round(sum(order_no_wechat_q) / count(1),0) order_no_wechat_q
           ,round(sum(order_no_app_nu_q) / count(1),0) order_no_app_nu_q
           ,round(sum(order_no_wechat_nu_q) / count(1),0) order_no_wechat_nu_q
    from (
        select order_date
            ,count(distinct case when channel='app' then  order_no end) order_no_app_q
            ,count(distinct case when channel='wechat' then  order_no end) order_no_wechat_q
            ,count(distinct case when channel='app' and user_type = '新客' then  order_no end) order_no_app_nu_q
            ,count(distinct case when channel='wechat' and user_type = '新客' then  order_no end) order_no_wechat_nu_q
        from q_order
        group by 1
    )a group by 1
) t1 
left join (
    select substr(dt,1,7) mth
           ,round(sum(order_no_app_c) / count(1),0) order_no_app_c
           ,round(sum(order_no_wechat_c) / count(1),0) order_no_wechat_c
           ,round(sum(order_no_app_nu_c) / count(1),0) order_no_app_nu_c
           ,round(sum(order_no_wechat_nu_c) / count(1),0) order_no_wechat_nu_c
    from (
        select dt
            ,count(distinct case when channel='app' then  order_no end) order_no_app_c
            ,count(distinct case when channel='wechat' then  order_no end) order_no_wechat_c
            ,count(distinct case when channel='app' and user_type = '新客' then  order_no end) order_no_app_nu_c
            ,count(distinct case when channel='wechat' and user_type = '新客' then  order_no end) order_no_wechat_nu_c
        from q_order
        group by 1
    )a group by 1
) t2 on t1.mth=t2.mth 
order by 1 desc;


