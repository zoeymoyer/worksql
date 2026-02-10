-----1、小红书交叉-宽口径
with user_type as
(
    select user_id ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and terminal_channel_type in ('www','app','touch') and is_valid='1'
    and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,init_uv as
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
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,red as
(
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub(current_date, 40) and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
)

,order_a as
(
    select order_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        terminal_channel_type,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,red_uv as (
    select distinct uv.dt
           ,uv.user_id
           ,uv.user_type
           ,uv.mdd
    from init_uv uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
)
,init_uv_all as (
    select dt,count(distinct user_id) all_uv
    from init_uv
    group by 1
)
,order_all as (
    select order_date
        ,count(distinct order_no) order_all
        ,sum(room_night) room_night_all
        ,sum(final_commission_after) yj_all
        ,sum(init_gmv) gmv_all
    from order_a
    group by 1
)


select  a.dt  `日期`,
        date_format(a.dt,'u')`星期`,
        `引流UV`,
        concat(round(`引流UV` / all_uv * 100, 2), '%') as `UV占比`,
        `生单用户量`,
        `订单量`,
        concat(round(`订单量` / order_all * 100, 2), '%') as `订单占比`,
        `间夜量`,
        concat(round(`间夜量` / room_night_all * 100, 2), '%') as `间夜占比`,
        concat(round(`订单量` / `引流UV` * 100, 2), '%') as `CR`,
        round(`GMV` / `间夜量`, 0) as `ADR`
        ,`佣金`
        ,`GMV`
        ,concat(round(`佣金` / yj_all * 100, 2), '%') as `佣金占比`
        ,concat(round(`GMV` / gmv_all * 100, 2), '%') as `GMV占比`
from (
    select
        uv.dt,
        count(distinct uv.user_id) as `引流UV`,
        count(distinct ord.user_id) as `生单用户量`,
        count(distinct ord.order_no) as `订单量`,
        sum(ord.room_night) as `间夜量`,
        sum(ord.init_gmv) as `GMV`,
        sum(final_commission_after)  as `佣金`
    from (select distinct dt, user_id from red_uv) uv
    left join order_a ord on uv.user_id = ord.user_id
    and uv.dt = ord.order_date
    group by 1
)a 
left join init_uv_all b on a.dt = b.dt
left join order_all c on a.dt = c.order_date
order by 1 desc
;

--- 2、机酒交叉
-- T-15交叉用户整体流量和转化
with user_type as(
    select user_id ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and terminal_channel_type in ('www','app','touch') and is_valid='1'
    and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,uv as(
    select distinct dt dates
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,user_jc as( --机酒交叉
    select distinct dates
            ,uv.user_name
            ,'机酒交叉' as `渠道` 
    from uv 
    left join (
            select to_date(create_time) as create_date 
                    ,o_qunarusername 
                    ,biz_order_no as flight_order_no
            from f_fuwu.dw_fact_inter_order_wide 
            where dt>=date_sub(current_date, 60)                     -- 随UV主表时间调整 
            and substr(create_time,1,10)>=date_sub(current_date, 60) -- 随UV主表时间调整
            and ticket_time is not null 
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao' -- 去分销
            and (s_arrcountryname !='中国' or s_depcountryname !='中国')
    )flight 
    on uv.user_name = flight.o_qunarusername
    where flight.create_date >= date_sub(uv.dates, 15)    -- 不动
    and flight.create_date <= uv.dates
    and flight_order_no is not null
)     
,uv_1 as ----多维度活跃用户汇总
(
    select 
        distinct
         a.dates
        ,user_type 
        ,mdd
        ,nvl(d.`渠道`,'其他') as `渠道` 
        ,a.user_id
    from uv a 
    left join user_jc d on a.user_name=d.user_name and a.dates=d.dates
)
,order as ----订单表 包含取消
(
    select 
        order_date 
        ,nvl( u.`渠道` ,'其他') `流量来源`
        ,count(distinct order_no) as `订单量`
        ,count(distinct case when (coupon_substract is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then null when nvl(coupon_substract,0)>0 then order_no end) as `Q_用券订单量`
        ,count(distinct case when (coupon_substract is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then null when nvl(coupon_substract,0)>0 and order_date = b.min_order_date then order_no end ) as `新客用券订单量`
        ,count(distinct case when order_date = b.min_order_date then order_no else null end) as `新客订单量`
        ,count(distinct case when order_date = b.min_order_date then a.user_id else null end) as `新客订单UV`
        ,count(distinct a.user_id) as `订单UV`
        ,sum(room_night) as `间夜量`
        ,sum(case when order_date = b.min_order_date then room_night else 0 end) as `新客间夜量`
    from mdw_order_v3_international a 
    left join user_jc u on  a.user_name=u.user_name and a.order_date =u.dates
    left join user_type b on a.user_id = b.user_id 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type='app'
        -- and terminal_channel_type in ('www','app','touch')
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_date>=date_sub(current_date,14)  and order_date<=date_sub(current_date,1)
    group by 1,2
)



select `日期`,`渠道`,`流量占比`,UV,`CR`,`用券订单占比`,`订单量`,`订单UV`,`U2O`,`间夜量`,`间夜占比`,`新客UV`,`新客流量占比`,`新客CR`,`新客用券订单占比`,`新客订单量`,`新客订单UV`,`新客U2O`,`新客间夜量`,`新客间夜占比` 
from(
    select
    u.dates `日期`
    ,u.`流量来源` `渠道`
    ,concat(round(nvl(u.`UV`/b.`总UV`,0)*100,1),'%') as `流量占比` 
    ,nvl(u.`UV`,0) as UV 
    ,concat(round(nvl(o.`订单量`/u.`UV`,0)*100,1),'%') as `CR` 
    ,concat(round(nvl(o.`Q_用券订单量`,0)/nvl(o.`订单量`,0)*100,1),'%') as `用券订单占比` 
    ,nvl(o.`订单量`,0) as `订单量`
    ,nvl(o.`订单UV`,0) as `订单UV` 
    ,concat(round(nvl(o.`订单UV`,0)/nvl(u.`UV`,0)*100,1),'%') as `U2O` 
    ,nvl(o.`间夜量`,0) as `间夜量`
    ,concat(round(nvl(o.`间夜量`/c.`间夜量_all`,0)*100,1),'%') as `间夜占比` 
    ,nvl(u.`新客UV`,0) as `新客UV`
    ,concat(round(nvl(u.`新客UV`/b.`总新客UV`,0)*100,1),'%') as `新客流量占比` 
    ,concat(round(nvl(o.`新客订单量`/u.`新客UV`,0)*100,1),'%') as `新客CR` 
    ,concat(round(nvl(o.`新客用券订单量`,0)/nvl(o.`新客订单量`,0)*100,1),'%') as `新客用券订单占比` 
    ,nvl(o.`新客订单量`,0) as `新客订单量`
    ,nvl(o.`新客订单UV`,0) as `新客订单UV`
    ,concat(round(nvl(o.`新客订单UV`,0)/nvl(u.`新客UV`,0)*100,1),'%') as `新客U2O` 
    ,nvl(o.`新客间夜量`,0) as `新客间夜量`
    ,concat(round(nvl(o.`新客间夜量`/c.`间夜量_new_all`,0)*100,1),'%') as `新客间夜占比` 
    from (
        select dates
            ,`流量来源` 
            ,sum(`UV`) `UV`
            ,sum(`新客UV`) `新客UV`
        from(
            select
                dates
                ,mdd
                ,`渠道` as `流量来源`
                ,count(distinct user_id) `UV`
                ,count(distinct case when user_type='新客' then user_id else null end ) `新客UV`
            from uv_1 u
            group by 1,2,3
        )a
        group by 1,2
    )u
    left join order o on  u.dates=o.order_date  and u.`流量来源`=o. `流量来源`
    left join(  
        select dates
            ,sum(`总UV`) `总UV`  
            ,sum(`总新客UV`) `总新客UV`
        from(
            select dates
            ,mdd
            ,count(distinct user_id) `总UV`
            ,count(distinct case when user_type='新客' then user_id else null end ) `总新客UV`
            from uv_1 u
            group by 1,2
        )a
        group by 1
    )b on  u.dates=b.dates
    left join (select order_date,sum(`间夜量`) as `间夜量_all`,sum(`新客间夜量`) as `间夜量_new_all` from order group by 1) c on u.dates=c.order_date 
)a
where a.`渠道`<>'其他'
order by  `渠道`,`日期` desc
;

--- 3、短视频交叉
with user_type as(
    select user_id ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and terminal_channel_type in ('www','app','touch') and is_valid='1'
    and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,init_uv as(
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
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,order_a as(
    select order_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        terminal_channel_type,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,video as   -- 换成短视频数据
(
    select distinct 
           t1.dt,split(t1.query,'_')[0] as query
           ,user_name,potential_new_flag,page
    from 
    (   --- 小红书引流表
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1 
        WHERE dt between date_sub(current_date, 40) and date_sub(current_date, 1)
             and nvl(t1.user_name ,'')<>'' 
             and t1.user_name is not null 
             and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1   --- 短视频引流码维表
        left join (
            select query,page,url 
            from pp_pub.dim_video_query_url_cid_mapping_nd  --- 短视频-query货品cid映射表
            where platform in ('douyin','vedio')
            ) t2 
        on t1.query = t2.query
        where dt >= date_sub(current_date, 40) and dt <= date_sub(current_date, 1) 
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
)

,video_uv as (
    select distinct uv.dt
           ,uv.user_id
           ,uv.user_type
           ,uv.mdd
    from init_uv uv
    left join video r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
)
,init_uv_all as (
    select dt,count(distinct user_id) all_uv
    from init_uv
    group by 1
)
,order_all as (
    select order_date
        ,count(distinct order_no) order_all
        ,sum(room_night) room_night_all
        ,sum(final_commission_after) yj_all
        ,sum(init_gmv) gmv_all
    from order_a
    group by 1
)



select  a.dt  `日期`,
        date_format(a.dt,'u')`星期`,
        `引流UV`,
        concat(round(`引流UV` / all_uv * 100, 2), '%') as `UV占比`,
        `生单用户量`,
        `订单量`,
        concat(round(`订单量` / order_all * 100, 2), '%') as `订单占比`,
        `间夜量`,
        concat(round(`间夜量` / room_night_all * 100, 2), '%') as `间夜占比`,
        concat(round(`订单量` / `引流UV` * 100, 2), '%') as `CR`,
        round(`GMV` / `间夜量`, 0) as `ADR`
        ,`佣金`
        ,`GMV`
        ,concat(round(`佣金` / yj_all * 100, 2), '%') as `佣金占比`
        ,concat(round(`GMV` / gmv_all * 100, 2), '%') as `GMV占比`
from (
    select
        uv.dt,
        count(distinct uv.user_id) as `引流UV`,
        count(distinct ord.user_id) as `生单用户量`,
        count(distinct ord.order_no) as `订单量`,
        sum(ord.room_night) as `间夜量`,
        sum(ord.init_gmv) as `GMV`,
        sum(final_commission_after)  as `佣金`
    from (select distinct dt, user_id from video_uv) uv
    left join order_a ord on uv.user_id = ord.user_id
    and uv.dt = ord.order_date
    group by 1
)a 
left join init_uv_all b on a.dt = b.dt
left join order_all c on a.dt = c.order_date
order by 1 desc
;


--- 4、活动交叉
with user_type as(
    select user_id ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and terminal_channel_type in ('www','app','touch') and is_valid='1'
    and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,init_uv as(
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
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,order_a as(
    select order_date
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when a.order_date > b.min_order_date then '老客' else '新客' end as user_type,
        a.user_name,
        terminal_channel_type,
        order_no,
        room_night,init_gmv,
        a.user_id
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,active as (  --- 活动
    select distinct substr(log_time,1,10) as log_date,user_name
    from hotel.dwd_flow_qav_htl_qmark_di a
    inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
    on a.page_cid = t1.code and t1.type = 'page'
    where dt>=date_sub(current_date,30) 
        and dt<=date_sub(current_date,1)
        and substr(log_time,1,10)>=date_sub(current_date,30) 
        and substr(log_time,1,10)<=date_sub(current_date,1)
        and page_url like '%/shark/active%' 
        and user_name not like '0000%'
    union
    select distinct dt,user_name 
    from marketdatagroup.dwd_market_activity_dt t 
    inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
    on t.activity_id = t1.code and t1.type = 'public'
    where dt>=date_sub(current_date,30) and dt<=date_sub(current_date,1)
    union 
    select distinct dt,username from flight.dwd_flow_inter_activity_all_di t 
    inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
    on t.page = t1.code and t1.type = 'flight'
    where dt>=date_sub(current_date,30) and dt<=date_sub(current_date,1)
        and username not like'0000%'
)

,data_t_7 as (
    select a.dt as log_date 
        ,count(distinct a.user_name) as `大盘贡献UV(t-7)`
        ,count(distinct orders.order_no) as `订单量`
        ,sum(orders.room_night) as `间夜量`
    from  (select dt,user_name,user_id from init_uv group by 1,2,3) a 
    left join active uv
    on a.user_name=uv.user_name 
    left join order_a orders on a.user_id=orders.user_id and a.dt=orders.order_date
    where datediff(a.dt,uv.log_date) between 0 and 7 and uv.user_name is not null
    group by 1
)
,init_uv_all as (
    select dt,count(distinct user_id) all_uv
    from init_uv
    group by 1
)
,order_all as (
    select order_date
        ,count(distinct order_no) order_all
        ,sum(room_night) room_night_all
        ,sum(final_commission_after) yj_all
        ,sum(init_gmv) gmv_all
    from order_a
    group by 1
)

select 
      ord.log_date as `日期`
      ,nvl(ord.`活动UV`,0) as `活动UV` 
      ,nvl(`大盘贡献UV`,0) as `大盘贡献UV`
      ,concat(round(nvl(`大盘贡献UV`/ord.`活动UV`*100,0),2),'%') `活动uv渗透率`
      ,concat(round(nvl(`大盘贡献UV`/`all_uv`*100,0),2),'%') `大盘流量占比`
      ,c.`大盘贡献UV(t-7)`
      ,concat(round(nvl(c.`大盘贡献UV(t-7)`/`all_uv`*100,0),2),'%') as `大盘流量占比(t-7)`
      ,nvl(c.`订单量`,0) as `订单量(t-7)`
      ,concat(round(nvl(c.`订单量`/c.`大盘贡献UV(t-7)`*100,0),2),'%') as `CR(t-7)`
      ,nvl(c.`间夜量`,0) as `间夜量(t-7)` 
      ,concat(round(nvl(c.`间夜量`/e.room_night_all*100,0),2),'%') as `大盘间夜占比(t-7)`
from(
        select log_date 
            ,count(distinct uv.user_name) as `活动UV`
            ,count(distinct case when a.user_name is not null then uv.user_name else null end ) as `大盘贡献UV`
        from active uv
        left join init_uv a on uv.user_name=a.user_name and uv.log_date =a.dt
        group by 1
)ord
left join init_uv_all  duv on ord.log_date = duv.dt
left join data_t_7 c on ord.log_date = c.log_date
left join order_all e on ord.log_date=e.order_date
order by `日期` desc
;