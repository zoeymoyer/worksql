
--- 拉新
--- sql1  业务新客，窄口径激活口径和点击口径
with iticket_uv as (  
    select to_date(create_time)    as dt,create_time
            ,o_qunarusername user_name
            ,biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
)
-- ,platform_new as (--- 判定平台新
--     select  dt,
--             user_pk,
--             user_id
--     from pub.dwd_flow_accapp_potential_user_di
--     where dt >= date_sub(current_date, 30)
--         and dict_type = 'pncl_wl_username'
--     group by 1,2,3
-- )
,market_active as (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
    select  t.dt,
            t.uid,
            t.username,
            t.platform,
            t.category,
            case when ad_name like '%国际机票%' then 'Y' else 'N' end is_ihotel
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    where t.dt >= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
    group by 1,2,3,4,5,6
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_name
           ,c.uid,c.platform,c.is_ihotel,c.category
           ,a.create_time
           ,'ALL' user_type
    from iticket_uv a
    left join market_active c on a.user_name=c.username and a.dt=c.dt
    -- left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk 
)
,ihotel_act_uv as (-- 最终国酒活跃明细表-分维度
    select a.dt,user_name,create_time,user_type,uid,platform,is_ihotel,category
    from ihotel_uv a
)
,market_click1 as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 40) and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 40) and date_sub(current_date, 1)
    group by 1,2,3
)
,ad_all as(
    select   dt,site_set,biz,ad_name,ad_type,source
    from pub.ods_market_channel_two_tb_addaily_report_data_di  -- 广告流量转化指标 主要判断宽窄口径
    where dt >= date_sub(current_date, 40)
    and ad_name like '%国际机票%'  -- 窄口径
    group by 1,2,3,4,5,6
)
,market_click as (
    select t1.dt,t1.ad_name,t1.uid,t1.click_time,case when t2.dt is not null then 'Y' else 'N' end is_ihotel
    from market_click1 t1 
    left join ad_all t2 on t1.dt=t2.dt and REGEXP_REPLACE(t1.ad_name, '[0-9]*$', '') = t2.source
)
-- 将活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (---- 国酒活跃48h有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,m.is_ihotel  is_ihotel_ac
            ,i.is_ihotel  is_ihotel_ad
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.create_time) - 172800 and i.click_time <= m.create_time 
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
            ,m.is_ihotel  is_ihotel_ac
            ,i.is_ihotel  is_ihotel_ad
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
    where logdate >= date_sub(current_date, 40)
    and uid is not null 
    and uid not in ('null','NULL','',' ','02:00:00:00:00:00','','0','1111','000000000000000','baidu','organic','0000000000000000000000000000000000000000')
    and pid in ('11010','10010','11030') 
    and ascii(split(channel_key,'-')[0]) between 32  and 126
    and isnormal = 'y'
    group by 1,2
)
,market_uv_1 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,category,is_ihotel_ac,is_ihotel_ad
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,category,is_ihotel_ac,is_ihotel_ad
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7,8
)

---- 新客
select  muv.dt 
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as wkd
       ,muv.gj_uv `引流UV(48H)`
       ,muv.gj_uv_new  `引流UV_新激活(48H)`
       ,muv.gj_uv_old  `引流UV_老激活(48H)`

       ,muv7.gj_uv `引流UV(T7)`
       ,muv7.gj_uv_new  `引流UV_新激活(T7)`
       ,muv7.gj_uv_old  `引流UV_老激活(T7)`

        ,muv.ihotel_uv_ac `引流UV_窄口径_激活归因(48H)`
        ,muv.ihotel_uv_new_ac `引流UV_新激活_窄口径_激活归因(48H)`
        ,muv.ihotel_uv_old_ac `引流UV_老激活_窄口径_激活归因(48H)`
        ,muv.ihotel_uv_ad `引流UV_窄口径_点击归因(48H)`
        ,muv.ihotel_uv_new_ad `引流UV_新激活_窄口径_点击归因(48H)`
        ,muv.ihotel_uv_old_ad `引流UV_老激活_窄口径_点击归因(48H)`
        ,muv7.ihotel_uv_ac `引流UV_窄口径_激活归因(T7)`
        ,muv7.ihotel_uv_new_ac `引流UV_新激活_窄口径_激活归因(T7)`
        ,muv7.ihotel_uv_old_ac `引流UV_老激活_窄口径_激活归因(T7)`
        ,muv7.ihotel_uv_ad `引流UV_窄口径_点击归因(T7)`
        ,muv7.ihotel_uv_new_ad `引流UV_新激活_窄口径_点击归因(T7)`
        ,muv7.ihotel_uv_old_ad `引流UV_老激活_窄口径_点击归因(T7)`

from (--- 48h流量数据
    select t1.dt
          ,count(distinct t1.user_name) gj_uv
          ,count(distinct case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(distinct case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
          ,count(distinct case when is_ihotel_ac = 'Y' then t1.user_name end) ihotel_uv_ac
          ,count(distinct case when is_ihotel_ac = 'Y' and is_new_active = '新激活' then t1.user_name end) ihotel_uv_new_ac
          ,count(distinct case when is_ihotel_ac = 'Y' and is_new_active = '老激活' then t1.user_name end) ihotel_uv_old_ac
          ,count(distinct case when is_ihotel_ad = 'Y' then t1.user_name end) ihotel_uv_ad
          ,count(distinct case when is_ihotel_ad = 'Y' and is_new_active = '新激活' then t1.user_name end) ihotel_uv_new_ad
          ,count(distinct case when is_ihotel_ad = 'Y' and is_new_active = '老激活' then t1.user_name end) ihotel_uv_old_ad
    from  (
        select dt
               ,user_name
               ,is_ihotel_ac
               ,is_ihotel_ad
               ,is_new_active
        from  market_uv_1
        group by 1,2,3,4,5
    ) t1
    group by 1
)muv 
left join (--- 7天订单数据
    select t1.dt
          ,count(distinct t1.user_name) gj_uv
          ,count(distinct case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(distinct case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
          ,count(distinct case when is_ihotel_ac = 'Y' then t1.user_name end) ihotel_uv_ac
          ,count(distinct case when is_ihotel_ac = 'Y' and is_new_active = '新激活' then t1.user_name end) ihotel_uv_new_ac
          ,count(distinct case when is_ihotel_ac = 'Y' and is_new_active = '老激活' then t1.user_name end) ihotel_uv_old_ac
          ,count(distinct case when is_ihotel_ad = 'Y' then t1.user_name end) ihotel_uv_ad
          ,count(distinct case when is_ihotel_ad = 'Y' and is_new_active = '新激活' then t1.user_name end) ihotel_uv_new_ad
          ,count(distinct case when is_ihotel_ad = 'Y' and is_new_active = '老激活' then t1.user_name end) ihotel_uv_old_ad
    from  (
        select dt
               ,user_name
               ,is_ihotel_ac
               ,is_ihotel_ad
               ,is_new_active
        from  market_uv_7
        group by 1,2,3,4,5
    ) t1
    group by 1
)muv7  on muv.dt =muv7.dt  
order by dt desc
;


--- sql2  业务新客，窄口径点击口径  -- 废弃0407
with iticket_uv as (  
    select to_date(create_time)    as dt,create_time
            ,o_qunarusername user_name
            ,biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
)
,platform_new as (--- 判定平台新
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 30)
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,market_active as (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
    select  t.dt,
            t.uid,
            t.username,
            t.platform,
            t.category,
            case when ad_name like '%国际机票%' then 'Y' else 'N' end is_ihotel
    from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
    where t.dt >= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1)
        and t.category in ('信息流', '达人')
    group by 1,2,3,4,5,6
)
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_name
           ,c.uid,c.platform,c.is_ihotel,c.category
           ,a.create_time
           ,case when b.user_pk is not null then '新客' else '老客' end as user_type
    from iticket_uv a
    left join market_active c on a.user_name=c.username and a.dt=c.dt
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk 
)
,ihotel_act_uv as (-- 最终国酒活跃明细表-分维度
    select a.dt,user_name,create_time,user_type,uid,platform,is_ihotel,category
    from ihotel_uv a
)
,market_click1 as (  ---广告点击渠道 --新流量表分IOS、安卓
    select  date(click_time) as dt,
          ad_name,
          uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_ios_di
    where dt between date_sub(current_date, 40)and date_sub(current_date, 1)
        and id is not null
    group by 1,2,3
    union all
    select date(click_time) as dt,
         ad_name,
         uid,max(click_time)click_time
    from pub.dwd_flow_channel_care_click_data_adr_di
    where dt between date_sub(current_date, 40) and date_sub(current_date, 1)
    group by 1,2,3
)
,ad_all as(
    select   dt,site_set,biz,ad_name,ad_type,source
    from pub.ods_market_channel_two_tb_addaily_report_data_di  -- 广告流量转化指标 主要判断宽窄口径
    where dt >='2025-08-01'
    and ad_name like '%国际机票%'  -- 窄口径
    group by 1,2,3,4,5,6
)
,market_click as (
    select t1.dt,t1.ad_name,t1.uid,t1.click_time,case when t2.dt is not null then 'Y' else 'N' end is_ihotel
    from market_click1 t1 
    left join ad_all t2 on t1.dt=t2.dt and REGEXP_REPLACE(t1.ad_name, '[0-9]*$', '') = t2.source
)
-- 将活跃的uid渠道来源定位到广告点击渠道上48h
,market_uv as (---- 国酒活跃48h有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,i.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  unix_timestamp(i.click_time) >= unix_timestamp(m.create_time) - 172800 and i.click_time <= m.create_time 
        and i.uid is not null
    group by 1,2,3,4,5,6,7
)
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,market_uv_7d as (---- 国酒活跃7天有点击广告行为
    select  m.dt
            ,m.uid
            ,m.user_name
            ,m.platform
            ,m.user_type
            ,i.is_ihotel
            ,category
    from ihotel_act_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2,3,4,5,6,7
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
    select a.dt,a.user_name,platform,user_type,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7
)
,market_uv_7 as (--- 市场活跃分平台新业务新，剔除空username  
    select a.dt,a.user_name,platform,user_type,is_ihotel,category
           ,case when c.uid is not null then '新激活' else '老激活' end is_new_active
    from  market_uv_7d a
    left join new_active c on a.dt=c.logdate and lower(a.uid)=lower(c.uid)
    where  a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6,7
)

---- 新客
select  muv.dt 
       ,pmod(datediff(muv.dt, '2018-06-25'), 7)+1  as wkd
       ,muv.gj_uv `引流UV(48H)`
       ,muv.gj_uv_new  `引流UV_新激活(48H)`
       ,muv.gj_uv_old  `引流UV_老激活(48H)`

       ,order7.gj_uv `引流UV(T7)`
       ,order7.gj_uv_new  `引流UV_新激活(T7)`
       ,order7.gj_uv_old  `引流UV_老激活(T7)`

        ,muv.ihotel_uv `引流UV_窄口径(48H)`
        ,muv.ihotel_uv_new `引流UV_窄口径_新激活(48H)`
        ,muv.ihotel_uv_old `引流UV_窄口径_老激活(48H)`
        ,order7.ihotel_uv `引流UV_窄口径(T7)`
        ,order7.ihotel_uv_new `引流UV_窄口径_新激活(T7)`
        ,order7.ihotel_uv_old `引流UV_窄口径_老激活(T7)`

from (--- 48h流量数据
    select t1.dt
          ,count(t1.user_name) gj_uv
          ,count(case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
          ,count(distinct case when is_ihotel = 'Y' then t1.user_name end) ihotel_uv
          ,count(distinct case when is_ihotel = 'Y' and is_new_active = '新激活' then t1.user_name end) ihotel_uv_new
          ,count(distinct case when is_ihotel = 'Y' and is_new_active = '老激活' then t1.user_name end) ihotel_uv_old
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
left join (--- 7天订单数据
    select t1.dt
          ,count(t1.user_name) gj_uv
          ,count(case when is_new_active = '新激活' then t1.user_name end) gj_uv_new
          ,count(case when is_new_active = '老激活' then t1.user_name end) gj_uv_old
          ,count(distinct case when is_ihotel = 'Y' then t1.user_name end) ihotel_uv
          ,count(distinct case when is_ihotel = 'Y' and is_new_active = '新激活' then t1.user_name end) ihotel_uv_new
          ,count(distinct case when is_ihotel = 'Y' and is_new_active = '老激活' then t1.user_name end) ihotel_uv_old
    from  (
        select dt,user_name
               ,max(is_ihotel) is_ihotel
               ,max(is_new_active) is_new_active
        from  market_uv_7
        where user_type = '新客'
        group by 1,2
    ) t1
    group by 1
)order7  on muv.dt =order7.dt  
order by dt desc
;


--- 拉活
--- 3、信息流拉活流量报表
with iticket_uv as (  
    select to_date(create_time)    as dt
            ,o_qunarusername   user_name
            ,biz_order_no         as flight_order_no
            ,o_mobileid
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
)
,platform_new as (--- 判定平台新
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 30)
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,uv as (
    select a.dt
           ,a.user_name
           ,case when b.user_pk is not null then '新客' else '老客' end as user_type
           ,a.o_mobileid
    from iticket_uv a
    left join platform_new b on a.dt = b.dt and a.user_name=b.user_pk 
)
,xxl_lh_flow as (--- 信息流拉活流量数据
    select dt,client_uid,case when account_id in ('73904397','73904566','73904384') then '窄口径' end is_ihotel
    from pub.dwd_flow_active_xxl_oneh_di
    where  dt >= date_sub(current_date, 30)
    group by 1,2,3
)
,xxl_lh_order as (--- 信息流拉活订单数据
    select dt,order_no,uid,username,income,case when account_id in ('73904397','73904566','73904384') then '窄口径' end is_ihotel
    from pub.dwd_mkt_xxl_touch_start_order_single_label_di
    where  abt = 'valid' and order_type_class = 'hotel-inter'
    and is_mkt_lahuo_kpi = 1
    and dt >= date_sub(current_date, 30)
)
,xxl_lh_f as (
    select t1.dt,t1.is_ihotel,t2.user_name,t2.user_type
    from xxl_lh_flow t1 
    join uv t2 on t1.dt=t2.dt and lower(t1.client_uid) = lower(t2.o_mobileid)
    group by 1,2,3,4
)
,xxl_lh_o as (
    select t1.dt,t1.is_ihotel,t2.user_name,t2.user_type
    from xxl_lh_order t1 
    join uv t2 on t1.order_no=t2.flight_order_no
    group by 1,2,3,4
)

select t1.dt
       ,t1.`拉活uv(流量)`
       ,t1.`拉活uv_窄口径(流量)`
       ,t2.`拉活uv(订单)`
       ,t2.`拉活uv_窄口径(订单)`
from (
    select t1.dt
            ,count(distinct t1.user_name) `拉活uv(流量)`
            ,sum(case when is_ihotel = '窄口径' then room_night end) `拉活uv_窄口径(流量)`
    from xxl_lh_f t1
    where t1.user_type = '新客'
    group by 1    
)t1 left join (
        select t1.dt
                ,count(distinct t1.user_name) `拉活uv(订单)`
                ,sum(case when is_ihotel = '窄口径' then 1 end) `拉活uv_窄口径(订单)`
        from xxl_lh_o t1
        where t1.user_type = '新客'
        group by 1
) t2 on t1.dt=t2.dt 
order by 1 desc
;
