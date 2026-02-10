with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)

--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

       ,muv.`活跃uv_窄口径`
       ,gjuv.`引流UV_窄口径`
       ,gjuv.`引流UV_新激活_窄口径`
       ,gjuv.`引流UV_老激活_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径` ) ADR
       ,order7.`生单用户量_窄口径` `生单用户量_窄口径7`
       ,order7.`订单量_窄口径` `订单量_窄口径7`
       ,order7.`间夜量_窄口径` `间夜量_窄口径7`
       ,concat(round(order7.`订单量_窄口径` / gjuv.`引流UV_窄口径` * 100, 2), '%') CR7
       ,round(order7.`GMV_窄口径`  /  order7.`间夜量_窄口径` ) ADR7
from (--- 48h广告投放数据
    select  dt
         
           ,count(distinct username) `活跃uv`
        --    ,count(distinct case when is_new_active = '新激活' then username end) `活跃uv-新激活`
        --    ,count(distinct case when is_new_active = '老激活' then username end) `活跃uv-老激活`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `活跃uv_窄口径`
        --    ,count(distinct case when ad_name like '%国际酒店%' and is_new_active = '新激活' then username end) `活跃uv_窄口径-新激活`
        --    ,count(distinct case when ad_name like '%国际酒店%' and is_new_active = '老激活' then username end) `活跃uv_窄口径-老激活`
    from  market_uv_1 a
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流UV_窄口径`
          ,count(case when is_gj = 'Y' and is_new_active = '新激活' then t1.user_name end) `引流UV_新激活_窄口径`
          ,count(case when is_gj = 'Y' and is_new_active = '老激活' then t1.user_name end) `引流UV_老激活_窄口径`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        -- and t1.user_type = '新客'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
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
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
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
        from  market_uv_7
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
    from uv_1 t1 
    -- where t1.user_type='老客'  --- 筛选流量用户类型
    group by 1
)uv on  uv.dt =muv.dt  
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    -- where t1.user_type='老客'  --- 筛选订单用户类型
    group by 1
)ouv on  ouv.order_date =muv.dt 
order by `日期` desc
;



---- 分维度
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.category ,muv.user_type_new ,muv.platform
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

       ,muv.`活跃uv_窄口径`
       ,gjuv.`引流UV_窄口径`
       ,gjuv.`引流UV_新激活_窄口径`
       ,gjuv.`引流UV_老激活_窄口径`
       ,order.`生单用户量_窄口径`
       ,order.`订单量_窄口径`
       ,order.`间夜量_窄口径`
       ,concat(round(order.`订单量_窄口径` / gjuv.`引流UV_窄口径` * 100, 2), '%') CR
       ,round(order.`GMV_窄口径`  /  order.`间夜量_窄口径` ) ADR
       ,order7.`生单用户量_窄口径` `生单用户量_窄口径7`
       ,order7.`订单量_窄口径` `订单量_窄口径7`
       ,order7.`间夜量_窄口径` `间夜量_窄口径7`
       ,concat(round(order7.`订单量_窄口径` / gjuv.`引流UV_窄口径` * 100, 2), '%') CR7
       ,round(order7.`GMV_窄口径`  /  order7.`间夜量_窄口径` ) ADR7
from (--- 48h广告投放数据
    select  dt
           ,category ,user_type_new ,platform
           ,count(distinct username) `活跃uv`
        --    ,count(distinct case when is_new_active = '新激活' then username end) `活跃uv-新激活`
        --    ,count(distinct case when is_new_active = '老激活' then username end) `活跃uv-老激活`
           ,count(distinct case when ad_name like '%国际酒店%'  then username end) `活跃uv_窄口径`
        --    ,count(distinct case when ad_name like '%国际酒店%' and is_new_active = '新激活' then username end) `活跃uv_窄口径-新激活`
        --    ,count(distinct case when ad_name like '%国际酒店%' and is_new_active = '老激活' then username end) `活跃uv_窄口径-老激活`
    from  market_uv_1 a
    group by 1,2,3,4
) muv
left join (--- 流量数据
    select t1.dt
          ,category,user_type_new,platform
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
          ,count(case when is_gj = 'Y' then t1.user_name end) `引流UV_窄口径`
          ,count(case when is_gj = 'Y' and is_new_active = '新激活' then t1.user_name end) `引流UV_新激活_窄口径`
          ,count(case when is_gj = 'Y' and is_new_active = '老激活' then t1.user_name end) `引流UV_老激活_窄口径`
    from uv_1 t1 
    left join (
        select dt,username
               ,category ,platform
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        group by 1,2,3,4
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        -- and t1.user_type = '新客'
    group by 1,2,3,4
)gjuv on  gjuv.dt =muv.dt  and muv.category=gjuv.category and muv.user_type_new=gjuv.user_type_new and muv.platform=gjuv.platform
left join (--- 48h订单数据
    select order_date,user_type_new,platform,category
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
        select dt,username,platform,category
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_1
        group by 1,2,3,4
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1,2,3,4
)order  on  muv.dt =order.order_date  and muv.user_type_new=order.user_type_new and muv.platform=order.platform and muv.category=order.category
left join (--- 7天订单数据
    select order_date,user_type_new,platform,category
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
        select dt,username,platform,category
               ,max(case when  ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
        from  market_uv_7
        group by 1,2,3,4
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
    group by 1,2,3,4
)order7  on  muv.dt =order7.order_date  and muv.user_type_new=order7.user_type_new and muv.platform=order7.platform and muv.category=order7.category
left join (--- 国酒流量数据 算占比
    select dt,user_type_new
          ,count(t1.user_name) UV
    from uv_1 t1 
    -- where t1.user_type='老客'  --- 筛选流量用户类型
    group by 1,2
)uv on  uv.dt =muv.dt  and uv.user_type_new=muv.user_type_new
left join (--- 国酒订单数据 算占比
    select order_date,user_type_new
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    -- where t1.user_type='老客'  --- 筛选订单用户类型
    group by 1,2
)ouv on  ouv.order_date =muv.dt and muv.user_type_new=ouv.user_type_new
order by `日期` desc
;



---- sql1、新客整体-宽口径
with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

from (--- 48h广告投放活跃数据
    select  dt
           ,count(distinct username) `活跃uv`
    from  market_uv_1 a
    where user_type = '新客'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type = '新客'
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type = '新客'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_1
        where user_type = '新客'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type = '新客'
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_7
        where user_type = '新客'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type = '新客'
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
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


---- sql2、业务新-宽口径
with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

from (--- 48h广告投放活跃数据
    select  dt
           ,count(distinct username) `活跃uv`
    from  market_uv_1 a
    where user_type_new = '平台老业务新'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type_new = '平台老业务新'
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台老业务新'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_1
        where user_type_new = '平台老业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台老业务新'
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_7
        where user_type_new = '平台老业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台老业务新'
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
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


---- sql3、平台新-宽口径
with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

from (--- 48h广告投放活跃数据
    select  dt
           ,count(distinct username) `活跃uv`
    from  market_uv_1 a
    where user_type_new = '平台新业务新'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type_new = '平台新业务新'
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台新业务新'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_1
        where user_type_new = '平台新业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台新业务新'
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_7
        where user_type_new = '平台新业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台新业务新'
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
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



---- sql4、新客整体-窄口径
with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
        and ad_name like '%国际酒店%'    ---- 窄口径
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

from (--- 48h广告投放活跃数据
    select  dt
           ,count(distinct username) `活跃uv`
    from  market_uv_1 a
    where user_type = '新客'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type = '新客'
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type = '新客'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_1
        where user_type = '新客'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type = '新客'
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_7
        where user_type = '新客'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type = '新客'
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
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



---- sql5、业务新-窄口径
with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
        and ad_name like '%国际酒店%'    ---- 窄口径
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

from (--- 48h广告投放活跃数据
    select  dt
           ,count(distinct username) `活跃uv`
    from  market_uv_1 a
    where user_type_new = '平台老业务新'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type_new = '平台老业务新'
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台老业务新'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_1
        where user_type_new = '平台老业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台老业务新'
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_7
        where user_type_new = '平台老业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台老业务新'
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
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


---- sql6、平台新-窄口径
with user_type as (--- 判定业务新
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
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15) 
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓、小程序
    select  date(click_time) as dt,
          ad_name, --specialkey
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name, --specialkey
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
,market_active as (--市场设备活跃信息  -- 区分一下新老
    select distinct
            t.dt,
            case when t.dt > t1.min_order_date then '老客' else '新客' end as user_type,
            t.uid,
            t.username,
            t.platform,
            t.ad_name,
            t.category,first_active_time
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    left join user_type t1 on t.username=t1.user_name
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
        and ad_name like '%国际酒店%'    ---- 窄口径
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.first_active_time) - 172800 and i.click_time <= m.first_active_time 
        and i.uid is not null
)
-- 将市场活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (
    select distinct
            m.dt,
            m.uid,
            m.username,
            m.platform,
            m.user_type
            ,m.ad_name
            ,category
    from market_active m
    left join market_click i on m.uid = i.uid 
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
)
,new_active as (  --- 新激活
    select logdate,uid
    from pub.dwd_flow_first_accapp_xxl_dr_mi
    where logdate >= date_sub(current_date, 90)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  存在部分用户有多个ad_name，user_type，platform
    select distinct a.dt,username,category,user_type,ad_name
           ,platform --平台安卓或者IOS
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join platform_new b on a.dt = b.dt and a.username=b.user_pk 
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.username is not null and a.username not in ('null', 'NULL', '', ' ')
)
,uv as (----分日去重活跃用户
    select  dt 
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
    group by 1,2,3,4,5
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
    select  a.dt,user_name,a.user_id,user_type,mdd
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk
    group by 1,2,3,4,5,6  
)
,q_order_1 as (
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


--- 整体
select  muv.dt `日期`
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as `星期`
       ,muv.`活跃uv`
       ,gjuv.`引流UV`
       ,gjuv.`引流UV_新激活`
       ,gjuv.`引流UV_老激活`
       ,concat(round(gjuv.`引流UV` /  uv.UV * 100, 2), '%')  `UV占比`
       ,order.`生单用户量`
       ,order.`订单量`
       ,concat(round(order.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比`
       ,order.`间夜量`
       ,concat(round(order.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比`
       ,concat(round(order.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR
       ,round(order.`GMV`  /  order.`间夜量` ) ADR

       ,order7.`生单用户量` `生单用户量7`
       ,order7.`订单量` `订单量7`
       ,concat(round(order7.`订单量` /  ouv.`订单量` * 100, 2), '%') `订单量占比7`
       ,order7.`间夜量` `间夜量7`
       ,concat(round(order7.`间夜量` /  ouv.`间夜量` * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.`订单量` / gjuv.`引流UV` * 100, 2), '%') CR7
       ,round(order7.`GMV`  /  order7.`间夜量` ) ADR7

from (--- 48h广告投放活跃数据
    select  dt
           ,count(distinct username) `活跃uv`
    from  market_uv_1 a
    where user_type_new = '平台新业务新'
    group by 1
) muv
left join (--- 流量数据
    select t1.dt
          ,count(t1.user_name) `引流UV`
          ,count(case when is_new_active = '新激活' then t1.user_name end) `引流UV_新激活`
          ,count(case when is_new_active = '老激活' then t1.user_name end) `引流UV_老激活`
    from uv_1 t1 
    left join (
        select dt,username
               ,max(case when ad_name like '%国际酒店%' then 'Y' else 'N' end) is_gj
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type_new = '平台新业务新'
        group by 1,2
    ) t2  on t1.dt=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台新业务新'
    group by 1
)gjuv on  gjuv.dt =muv.dt 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_1
        where user_type_new = '平台新业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台新业务新'
    group by 1
)order  on  muv.dt =order.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) `生单用户量`
          ,count(distinct t1.order_no) `订单量`
          ,sum(t1.room_night) `间夜量`
          ,sum(t1.init_gmv) `GMV`
    from q_order_1 t1 
    left join (
        select dt,username
        from  market_uv_7
        where user_type_new = '平台新业务新'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.username
    where t2.username is not null
        and t1.user_type_new = '平台新业务新'
    group by 1
)order7  on  muv.dt =order7.order_date  
left join (--- 国酒流量数据 算占比
    select dt
          ,count(t1.user_name) UV
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
