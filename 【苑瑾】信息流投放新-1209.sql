--- sql1  新客
with user_type as (--- 判定业务新
    select user_id,user_name
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,ldbo_uv as (  --- sdbo 小时级
    select dt,user_id,user_name,concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)) action_time
    from (
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date,15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
    ) group by 1,2,3,4
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
,market_active as (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
    select  t.dt,
            t.uid,
            t.username,
            t.platform,
            t.category,
            case when ad_name like '%国际酒店%' then 'Y' else 'N' end is_ihotel
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
        -- and t.ad_name like '%国际酒店%'   --- 窄口径
    group by 1,2,3,4,5,6
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_id
           ,a.user_name
           ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
           ,c.uid,c.platform,c.is_ihotel,c.category
           ,a.action_time
    from ldbo_uv a
    left join user_type b on a.user_id = b.user_id 
    left join market_active c on a.user_name=c.username and a.dt=c.dt
)
,ihotel_act_uv as (-- 最终国酒活跃明细表-分维度
    select a.dt,a.user_id,user_name,action_time,user_type,uid,platform,is_ihotel,category
          ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from ihotel_uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk 
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
-- 将活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (---- 国酒活跃48h有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.user_type_new
            ,m.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.click_time >= m.action_time - interval '48' hour and i.click_time <= m.action_time 
       -- unix_timestamp(i.click_time) >= unix_timestamp(m.action_time) - 172800 and i.click_time <= m.action_time 
        and i.uid is not null
    group by 1,2,3,4,5,6,7,8
)
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (---- 国酒活跃7天有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.user_type_new
            ,m.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2,3,4,5,6,7,8
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
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,user_type_new,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,user_type_new,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
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
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,40) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order_1 as (--- 订单区分平台新老
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


---- 新客
select  muv.dt 
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as wkd
       ,muv.gj_uv `引流UV`
       ,muv.gj_uv_new  `引流UV-新激活`
       ,muv.gj_uv_old  `引流UV-老激活`
       ,concat(round(muv.gj_uv /  uv.uv * 100, 2), '%') `UV占比`
       ,q_ord.order_uv   `生单用户量`
       ,q_ord.order_no   `订单量`
       ,concat(round(q_ord.order_no/  ouv.order_no * 100, 2), '%') `订单量占比`
       ,q_ord.room_night  `间夜量`
       ,concat(round(q_ord.room_night /  ouv.room_night * 100, 2), '%') `间夜量占比`
       ,concat(round(q_ord.order_no/ muv.gj_uv * 100, 2), '%') CR
       ,round(q_ord.GMV  /  q_ord.room_night ) ADR

       ,order7.order_uv7    `生单用户量7`
       ,order7.order_no7     `订单量7`
       ,concat(round(order7.order_no7 /  ouv.order_no* 100, 2), '%') `订单量占比7`
       ,order7.room_night7  `间夜量7`
       ,concat(round(order7.room_night7 /  ouv.room_night * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.order_no7 / muv.gj_uv * 100, 2), '%') CR7
       ,round(order7.GMV7  /  order7.room_night7 ) ADR7

from (--- 48h流量数据
    select t1.dt
          ,count(t1.user_name) gj_uv
          ,count(case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
    from  (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type = '新客'
        group by 1,2
    ) t1
    group by 1
)muv 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no)  order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
    from q_order_1 t1 
    left join (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
        from  market_uv_1
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null 
        and user_type = '新客'
    group by 1
)q_ord  on  muv.dt =q_ord.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) order_uv7
          ,count(distinct t1.order_no)  order_no7
          ,sum(t1.room_night) room_night7
          ,sum(t1.init_gmv) GMV7
    from q_order_1 t1 
    left join (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
        from  market_uv_7
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null 
        and user_type = '新客'
    group by 1
)order7  on muv.dt =order7.order_date  
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
    from q_order_1 t1
    where user_type = '新客' 
    group by 1
)ouv on  ouv.order_date =muv.dt 
left join (--- 国酒流量 算占比
    select dt
          ,count(distinct user_name) uv
    from ihotel_act_uv
    where user_type = '新客' 
    group by 1
) uv on uv.dt =muv.dt 
order by dt desc
;

--- sql2  新客-窄口径
with user_type as (--- 判定业务新
    select user_id,user_name
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,ldbo_uv as (  --- sdbo 小时级
    select dt,user_id,user_name,concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)) action_time
    from (
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date,15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
    ) group by 1,2,3,4
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
,market_active as (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
    select  t.dt,
            t.uid,
            t.username,
            t.platform,
            t.category,
            case when ad_name like '%国际酒店%' then 'Y' else 'N' end is_ihotel
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
        -- and t.ad_name like '%国际酒店%'   --- 窄口径
    group by 1,2,3,4,5,6
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_id
           ,a.user_name
           ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
           ,c.uid,c.platform,c.is_ihotel,c.category
           ,a.action_time
    from ldbo_uv a
    left join user_type b on a.user_id = b.user_id 
    left join market_active c on a.user_name=c.username and a.dt=c.dt
)
,ihotel_act_uv as (-- 最终国酒活跃明细表-分维度
    select a.dt,a.user_id,user_name,action_time,user_type,uid,platform,is_ihotel,category
          ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from ihotel_uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk 
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
-- 将活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (---- 国酒活跃48h有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.user_type_new
            ,m.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.click_time >= m.action_time - interval '48' hour and i.click_time <= m.action_time 
       -- unix_timestamp(i.click_time) >= unix_timestamp(m.action_time) - 172800 and i.click_time <= m.action_time 
        and i.uid is not null
    group by 1,2,3,4,5,6,7,8
)
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (---- 国酒活跃7天有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.user_type_new
            ,m.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2,3,4,5,6,7,8
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
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,user_type_new,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,user_type_new,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
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
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,40) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order_1 as (--- 订单区分平台新老
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)


---- 新客
select  muv.dt 
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as wkd
       ,muv.gj_uv `引流UV`
       ,muv.gj_uv_new  `引流UV-新激活`
       ,muv.gj_uv_old  `引流UV-老激活`
       ,concat(round(muv.gj_uv /  uv.uv * 100, 2), '%') `UV占比`
       ,q_ord.order_uv   `生单用户量`
       ,q_ord.order_no   `订单量`
       ,concat(round(q_ord.order_no/  ouv.order_no * 100, 2), '%') `订单量占比`
       ,q_ord.room_night  `间夜量`
       ,concat(round(q_ord.room_night /  ouv.room_night * 100, 2), '%') `间夜量占比`
       ,concat(round(q_ord.order_no/ muv.gj_uv * 100, 2), '%') CR
       ,round(q_ord.GMV  /  q_ord.room_night ) ADR

       ,order7.order_uv7    `生单用户量7`
       ,order7.order_no7     `订单量7`
       ,concat(round(order7.order_no7 /  ouv.order_no* 100, 2), '%') `订单量占比7`
       ,order7.room_night7  `间夜量7`
       ,concat(round(order7.room_night7 /  ouv.room_night * 100, 2), '%') `间夜量占比7`
       ,concat(round(order7.order_no7 / muv.gj_uv * 100, 2), '%') CR7
       ,round(order7.GMV7  /  order7.room_night7 ) ADR7

from (--- 48h流量数据
    select t1.dt
          ,count(t1.user_name) gj_uv
          ,count(case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
    from  (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
               ,max(is_new_active) is_new_active
        from  market_uv_1
        where user_type = '新客'
        and is_ihotel = 'Y'
        group by 1,2
    ) t1
    group by 1
)muv 
left join (--- 48h订单数据
    select order_date
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no)  order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
    from q_order_1 t1 
    left join (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
        from  market_uv_1
        where  is_ihotel = 'Y'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null 
        and user_type = '新客'
    group by 1
)q_ord  on  muv.dt =q_ord.order_date  
left join (--- 7天订单数据
    select order_date
          ,count(distinct t1.user_name) order_uv7
          ,count(distinct t1.order_no)  order_no7
          ,sum(t1.room_night) room_night7
          ,sum(t1.init_gmv) GMV7
    from q_order_1 t1 
    left join (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
        from  market_uv_7
        where  is_ihotel = 'Y'
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null 
        and user_type = '新客'
    group by 1
)order7  on muv.dt =order7.order_date  
left join (--- 国酒订单数据 算占比
    select order_date
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
    from q_order_1 t1
    where user_type = '新客' 
    group by 1
)ouv on  ouv.order_date =muv.dt 
left join (--- 国酒流量 算占比
    select dt
          ,count(distinct user_name) uv
    from ihotel_act_uv
    where user_type = '新客' 
    group by 1
) uv on uv.dt =muv.dt 
order by dt desc
;

---- 分维度数据
with user_type as (--- 判定业务新
    select user_id,user_name
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,ldbo_uv as (  --- sdbo 小时级
    select dt,user_id,user_name,concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)) action_time
    from (
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date,15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
        union
        select  dt 
                ,a.user_id,user_name
                ,action_time
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        group by 1,2,3,4
    ) group by 1,2,3,4
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
,market_active as (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
    select  t.dt,
            t.uid,
            t.username,
            t.platform,
            t.category,
            case when ad_name like '%国际酒店%' then 'Y' else 'N' end is_ihotel
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
        -- and t.ad_name like '%国际酒店%'   --- 窄口径
    group by 1,2,3,4,5,6
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_id
           ,a.user_name
           ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
           ,c.uid,c.platform,c.is_ihotel,c.category
           ,a.action_time
    from ldbo_uv a
    left join user_type b on a.user_id = b.user_id 
    left join market_active c on a.user_name=c.username and a.dt=c.dt
)
,ihotel_act_uv as (-- 最终国酒活跃明细表-分维度
    select a.dt,a.user_id,user_name,action_time,user_type,uid,platform,is_ihotel,category
          ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from ihotel_uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk 
)
,market_click as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 25) and date_sub(current_date, 1)
    group by 1,2,3
)
-- 将活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (---- 国酒活跃48h有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.user_type_new
            ,m.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.click_time >= m.action_time - interval '48' hour and i.click_time <= m.action_time 
       -- unix_timestamp(i.click_time) >= unix_timestamp(m.action_time) - 172800 and i.click_time <= m.action_time 
        and i.uid is not null
    group by 1,2,3,4,5,6,7,8
)
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (---- 国酒活跃7天有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.user_type_new
            ,m.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2,3,4,5,6,7,8
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
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,user_type_new,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,user_type_new,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
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
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,40) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order_1 as (--- 订单区分平台新老
    select  order_date,user_name,a.user_id,user_type,order_no,room_night,init_gmv,final_commission_after
           ,case when (a.user_type = '新客' and b.user_id is not null) then '平台新业务新'
                 when a.user_type = '新客' then '平台老业务新'
            else '老客' end as user_type_new
    from  q_order a
    left join platform_new b on a.order_date = b.dt and a.user_name=b.user_pk  
)

---- 分维度
select  muv.dt 
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as wkd
       ,muv.user_type_new
       ,muv.category
       ,muv.platform
       ,muv.gj_uv 
       ,muv.gj_uv_new 
       ,muv.gj_uv_old 
       ,concat(round(muv.gj_uv /  uv.uv * 100, 2), '%') gj_uv_rate
       ,q_ord.order_uv  
       ,q_ord.order_no    
       ,concat(round(q_ord.order_no/  ouv.order_no * 100, 2), '%') order_no_rate
       ,q_ord.room_night 
       ,concat(round(q_ord.room_night /  ouv.room_night * 100, 2), '%') room_night_rate
       ,concat(round(q_ord.order_no/ muv.gj_uv * 100, 2), '%') CR
       ,round(q_ord.GMV  /  q_ord.room_night ) ADR

       ,order7.order_uv7 
       ,order7.order_no7 
       ,concat(round(order7.order_no7 /  ouv.order_no* 100, 2), '%') order_no7_rate
       ,order7.room_night7 
       ,concat(round(order7.room_night7 /  ouv.room_night * 100, 2), '%') room_night7_rate
       ,concat(round(order7.order_no7 / muv.gj_uv * 100, 2), '%') CR7
       ,round(order7.GMV7  /  order7.room_night7 ) ADR7

       ,muv.gj_uv_z 
       ,muv.gj_uv_new_z 
       ,muv.gj_uv_old_z 
       ,q_ord.order_uv_z 
       ,q_ord.order_no_z  
       ,q_ord.room_night_z    
       ,concat(round(q_ord.order_no_z/ muv.gj_uv_z * 100, 2), '%') CR_z
       ,round(q_ord.gmv_z  /  q_ord.room_night_z ) ADR_z
       ,order7.order_uv7_z 
       ,order7.order_no7_z
       ,order7.room_night7_z 
       ,concat(round(order7.order_no7_z/ muv.gj_uv_z * 100, 2), '%') CR7_z
       ,round(order7.gmv7_z  /  order7.room_night7_z ) ADR7_z
from (--- 48h流量数据
    select t1.dt,user_type_new,category,platform
          ,count(t1.user_name) gj_uv
          ,count(case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
          ,count(case when is_ihotel = 'Y' then t1.user_name end) gj_uv_z
          ,count(case when is_ihotel = 'Y' and is_new_active = '新激活' then t1.user_name end) gj_uv_new_z
          ,count(case when is_ihotel = 'Y' and is_new_active = '老激活' then t1.user_name end) gj_uv_old_z
    from  (
        select dt,user_name,user_type_new,category,platform
               ,max(is_ihotel) is_ihotel
               ,max(is_new_active) is_new_active
        from  market_uv_1
        group by 1,2,3,4,5
    ) t1
    group by 1,2,3,4
)muv 
left join (--- 48h订单数据
    select order_date,user_type_new,category,platform
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no)  order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
          ,count(distinct case when is_ihotel = 'Y' then t1.user_name end) order_uv_z
          ,count(distinct case when is_ihotel = 'Y' then t1.order_no end) order_no_z
          ,sum(case when is_ihotel = 'Y' then t1.room_night end) room_night_z
          ,sum(case when is_ihotel = 'Y' then t1.init_gmv end) gmv_z
    from q_order_1 t1 
    left join (
        select dt,user_name,category,platform
               ,max(is_ihotel) is_ihotel
        from  market_uv_1
        group by 1,2,3,4
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null
    group by 1,2,3,4
)q_ord  on  muv.dt =q_ord.order_date  and muv.user_type_new=q_ord.user_type_new and muv.category=q_ord.category and muv.platform=q_ord.platform
left join (--- 7天订单数据
    select order_date,user_type_new,category,platform
          ,count(distinct t1.user_name) order_uv7
          ,count(distinct t1.order_no)  order_no7
          ,sum(t1.room_night) room_night7
          ,sum(t1.init_gmv) GMV7
          ,count(distinct case when is_ihotel = 'Y' then t1.user_name end) order_uv7_z
          ,count(distinct case when is_ihotel = 'Y' then t1.order_no end) order_no7_z
          ,sum(case when is_ihotel = 'Y' then t1.room_night end) room_night7_z
          ,sum(case when is_ihotel = 'Y' then t1.init_gmv end) gmv7_z
    from q_order_1 t1 
    left join (
        select dt,user_name,category,platform
               ,max(is_ihotel) is_ihotel
        from  market_uv_7
        group by 1,2,3,4
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null
    group by 1,2,3,4
)order7  on muv.dt =order7.order_date  and muv.user_type_new=order7.user_type_new and muv.category=order7.category and muv.platform=order7.platform
left join (--- 国酒订单数据 算占比
    select order_date,user_type_new
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
    from q_order_1 t1 
    group by 1,2
)ouv on  ouv.order_date =muv.dt and muv.user_type_new=ouv.user_type_new
left join (--- 国酒流量 算占比
    select dt,user_type_new
          ,count(distinct user_name) uv
    from ihotel_act_uv
    group by 1,2
) uv on uv.dt =muv.dt and uv.user_type_new=ouv.user_type_new
order by dt desc
;



---- 分维度
select  muv.dt 
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as wkd
       ,muv.user_type
       ,muv.gj_uv 
       ,muv.gj_uv_new 
       ,muv.gj_uv_old 
       ,concat(round(muv.gj_uv /  uv.uv * 100, 2), '%') gj_uv_rate
       ,q_ord.order_uv  
       ,q_ord.order_no    
       ,concat(round(q_ord.order_no/  ouv.order_no * 100, 2), '%') order_no_rate
       ,q_ord.room_night 
       ,concat(round(q_ord.room_night /  ouv.room_night * 100, 2), '%') room_night_rate
       ,concat(round(q_ord.order_no/ muv.gj_uv * 100, 2), '%') CR
       ,round(q_ord.GMV  /  q_ord.room_night ) ADR

       ,order7.order_uv7 
       ,order7.order_no7 
       ,concat(round(order7.order_no7 /  ouv.order_no* 100, 2), '%') order_no7_rate
       ,order7.room_night7 
       ,concat(round(order7.room_night7 /  ouv.room_night * 100, 2), '%') room_night7_rate
       ,concat(round(order7.order_no7 / muv.gj_uv * 100, 2), '%') CR7
       ,round(order7.GMV7  /  order7.room_night7 ) ADR7

       ,muv.gj_uv_z 
       ,muv.gj_uv_new_z 
       ,muv.gj_uv_old_z 
       ,q_ord.order_uv_z 
       ,q_ord.order_no_z  
       ,q_ord.room_night_z    
       ,concat(round(q_ord.order_no_z/ muv.gj_uv_z * 100, 2), '%') CR_z
       ,round(q_ord.gmv_z  /  q_ord.room_night_z ) ADR_z
       ,order7.order_uv7_z 
       ,order7.order_no7_z
       ,order7.room_night7_z 
       ,concat(round(order7.order_no7_z/ muv.gj_uv_z * 100, 2), '%') CR7_z
       ,round(order7.gmv7_z  /  order7.room_night7_z ) ADR7_z
from (--- 48h流量数据
    select t1.dt,user_type
          ,count(t1.user_name) gj_uv
          ,count(case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
          ,count(case when is_ihotel = 'Y' then t1.user_name end) gj_uv_z
          ,count(case when is_ihotel = 'Y' and is_new_active = '新激活' then t1.user_name end) gj_uv_new_z
          ,count(case when is_ihotel = 'Y' and is_new_active = '老激活' then t1.user_name end) gj_uv_old_z
    from  (
        select dt,user_name,user_type
               ,max(is_ihotel) is_ihotel
               ,max(is_new_active) is_new_active
        from  market_uv_1
        group by 1,2,3
    ) t1
    group by 1,2
)muv 
left join (--- 48h订单数据
    select order_date,user_type
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no)  order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
          ,count(distinct case when is_ihotel = 'Y' then t1.user_name end) order_uv_z
          ,count(distinct case when is_ihotel = 'Y' then t1.order_no end) order_no_z
          ,sum(case when is_ihotel = 'Y' then t1.room_night end) room_night_z
          ,sum(case when is_ihotel = 'Y' then t1.init_gmv end) gmv_z
    from q_order_1 t1 
    left join (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
        from  market_uv_1
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null
    group by 1,2
)q_ord  on  muv.dt =q_ord.order_date  and muv.user_type=q_ord.user_type
left join (--- 7天订单数据
    select order_date,user_type
          ,count(distinct t1.user_name) order_uv7
          ,count(distinct t1.order_no)  order_no7
          ,sum(t1.room_night) room_night7
          ,sum(t1.init_gmv) GMV7
          ,count(distinct case when is_ihotel = 'Y' then t1.user_name end) order_uv7_z
          ,count(distinct case when is_ihotel = 'Y' then t1.order_no end) order_no7_z
          ,sum(case when is_ihotel = 'Y' then t1.room_night end) room_night7_z
          ,sum(case when is_ihotel = 'Y' then t1.init_gmv end) gmv7_z
    from q_order_1 t1 
    left join (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
        from  market_uv_7
        group by 1,2
    ) t2  on t1.order_date=t2.dt and t1.user_name=t2.user_name
    where t2.user_name is not null
    group by 1,2
)order7  on muv.dt =order7.order_date  and muv.user_type=order7.user_type
left join (--- 国酒订单数据 算占比
    select order_date,user_type
          ,count(distinct t1.user_name) order_uv
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,sum(t1.init_gmv) GMV
    from q_order_1 t1 
    group by 1,2
)ouv on  ouv.order_date =muv.dt and muv.user_type=ouv.user_type
left join (--- 国酒流量 算占比
    select dt,user_type
          ,count(distinct user_name) uv
    from ihotel_act_uv
    group by 1,2
) uv on uv.dt =muv.dt and uv.user_type=ouv.user_type
order by dt desc
;