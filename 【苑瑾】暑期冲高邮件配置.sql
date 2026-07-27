
---- 1、邮件报表
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as(---D页离店日期在暑期期间
    select  dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
        and checkout_date between '2026-07-06' and '2026-08-16'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
)
,user_jc as(--机酒交叉
    select distinct dt
            , mdd
            , uv.user_name
            , '机酒交叉'      as channel
            , 0              as user_number
    from uv uv
    left join(--- 需要修改时间
    select to_date(create_time)    as create_date
            , o_qunarusername
            , biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 40) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) flight
    on uv.user_name = flight.o_qunarusername
    where flight.create_date >= date_sub(uv.dt, 15)
    and flight.create_date <= uv.dt
    and flight_order_no is not null
)
,user_xhs as(--小红书 宽口径
    select distinct uv.dt
                   , mdd
                   , uv.user_name
                   , '小红书' as channel
                   , 1  as user_number
    from uv uv
    left join(--- 需要修改时间
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)
,user_nr as(--- 内容交叉
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            ,t1.user_name
            ,t1.mdd
            ,'内容交叉' as  channel
            ,2         as  user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = date_sub(current_date, 1) and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join ( --- 需要修改时间
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= from_unixtime(unix_timestamp() - 86400 * 16, 'yyyyMMdd')
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv uv t1 on d.user_id = t1.user_name  and d.dt = replace(t1.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as( --暑期活动
    select distinct uv.dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'营销活动' channel
                   ,3 as     user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as dt
                        ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
            and username not like '0000%'
        ) d on d.user_name = uv.user_name
    where d.dt >= date_sub(uv.dt, 7)
       and d.dt <= uv.dt
       and d.user_name is not null
)
,user_gnjd as( ----国内酒店
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'国内交叉' as channel
                   ,4          as user_number
    from uv uv 
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.dt, 365)
       and g.order_date <= uv.dt
       and g.user_id is not null
)
,user_channel  as(---流量来源渠道整理 
    select distinct dt
            , mdd
            , user_name
            , channel
    from (
        select dt,
                mdd,
                user_name,
                channel,
                row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt, mdd, user_name, channel, user_number
            from user_jc
            union all
            select dt, mdd, user_name, channel, user_number
            from user_xhs
            union all
            select dt, mdd, user_name, channel, user_number
            from user_nr
            union all
            select dt, mdd, user_name, channel, user_number
            from user_hd
            union all
            select dt, mdd, user_name, channel, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as (----多维度活跃用户汇总
    select distinct a.dt     as dates
            ,a.user_type
            ,a.mdd
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
            ,coupon_substract_summary
            ,follow_price_amount,extendinfomap,cashbackmap
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date,1)
        and checkout_date between '2026-07-06' and '2026-08-16'   --- 离店日期在暑期
)
,q_uv_info as(---- 分渠道流量汇总
    select dates
            ,channel
            ,count(user_id)   uv
            ,count(case when user_type = '新客' then  user_id end) new_uv
    from uv_1
    group by 1,2
)
,order_info as (---- 分渠道订单汇总
    select t1.order_date
          ,coalesce(t2.channel,'null') as  channel
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(case when user_type = '新客' then  room_night end) `新客间夜量`
          ,count(distinct case when user_type = '新客' then  order_no end) `新客订单量`
          ,count(distinct case when user_type = '新客' then  t1.user_id end) `新客下单用户量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                            and user_type = '新客'
                        then order_no else null end)             as `Q_新客用券订单量`
    from q_order t1
    left join (select  dates,user_id,channel from uv_1 group by 1,2,3) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by 1,2
)


select t1.dates `日期`
        ,t1.channel `渠道`,`流量占比`,UV
        ,concat(round(nvl(`订单量` / UV,0) * 100,2),'%') as `CR`
        ,concat(round(nvl(`Q_用券订单量`/ `订单量` ,0) * 100,2),'%') as `用券订单占比`
        ,`订单量`,`间夜量`
        ,`新客UV`,`新客流量占比`
        ,concat(round(nvl(`新客订单量` / `新客UV`,0) * 100,2),'%') as `新客CR`
        ,concat(round(nvl(`Q_新客用券订单量`  / `新客订单量`,0) * 100,2),'%') as `新客用券订单占比`
        ,`新客订单量`
        ,`新客间夜量`
from (
    select dates 
            ,channel
            ,uv
            ,new_uv `新客UV`
            ,concat(round(uv / sum(uv) over(partition by dates) * 100,2), '%') `流量占比`
            ,concat(round(new_uv / sum(new_uv) over(partition by dates) * 100,2), '%') `新客流量占比`
    from q_uv_info
)t1 left join (
    select order_date
            ,channel
            ,`Q_用券订单量` 
            ,`订单量` 
            ,`间夜量` 
            ,`新客间夜量` 
            ,`新客订单量` 
            ,`Q_新客用券订单量` 
    from order_info
)t2 on t1.dates=t2.order_date and t1.channel=t2.channel
order by  `渠道`,`日期` desc
;



---- 2、邮件报表：分目的地
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as(---D页离店日期在暑期期间
    select  dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
        and checkout_date between '2026-07-06' and '2026-08-16'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
)
,user_jc as(--机酒交叉
    select distinct dt
            , mdd
            , uv.user_name
            , '机酒交叉'      as channel
            , 0              as user_number
    from uv uv
    left join(--- 需要修改时间
    select to_date(create_time)    as create_date
            , o_qunarusername
            , biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 40) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) flight
    on uv.user_name = flight.o_qunarusername
    where flight.create_date >= date_sub(uv.dt, 15)
    and flight.create_date <= uv.dt
    and flight_order_no is not null
)
,user_xhs as(--小红书 宽口径
    select distinct uv.dt
                   , mdd
                   , uv.user_name
                   , '小红书' as channel
                   , 1  as user_number
    from uv uv
    left join(--- 需要修改时间
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)
,user_nr as(--- 内容交叉
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            ,uv.user_name
            ,uv.mdd
            ,'内容交叉' as  channel
            ,2         as  user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = date_sub(current_date, 1) and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join ( --- 需要修改时间
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= from_unixtime(unix_timestamp() - 86400 * 16, 'yyyyMMdd')
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv uv on d.user_id = uv.user_name  and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as( --暑期活动
    select distinct uv.dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'营销活动' channel
                   ,3 as     user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as dt
                        ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
            and username not like '0000%'
        ) d on d.user_name = uv.user_name
    where d.dt >= date_sub(uv.dt, 7)
       and d.dt <= uv.dt
       and d.user_name is not null
)
,user_gnjd as( ----国内酒店
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'国内交叉' as channel
                   ,4          as user_number
    from uv uv 
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.dt, 365)
       and g.order_date <= uv.dt
       and g.user_id is not null
)
,user_channel  as(---流量来源渠道整理 
    select distinct dt
            , mdd
            , user_name
            , channel
    from (
        select dt,
                mdd,
                user_name,
                channel,
                row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt, mdd, user_name, channel, user_number
            from user_jc
            union all
            select dt, mdd, user_name, channel, user_number
            from user_xhs
            union all
            select dt, mdd, user_name, channel, user_number
            from user_nr
            union all
            select dt, mdd, user_name, channel, user_number
            from user_hd
            union all
            select dt, mdd, user_name, channel, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as (----多维度活跃用户汇总
    select distinct a.dt     as dates
            ,a.user_type
            ,a.mdd
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
            ,coupon_substract_summary
            ,follow_price_amount,extendinfomap,cashbackmap
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date,1)
        and checkout_date between '2026-07-06' and '2026-08-16'   --- 离店日期在暑期
)
,q_uv_info as(---- 分渠道流量汇总
    select dates
            ,mdd
            ,channel
            ,count(user_id)   uv
            ,count(case when user_type = '新客' then  user_id end) new_uv
    from uv_1
    group by 1,2,3
)
,order_info as (---- 分渠道订单汇总
    select t1.order_date
          ,t1.mdd
          ,coalesce(t2.channel,'null') as  channel
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(case when user_type = '新客' then  room_night end) `新客间夜量`
          ,count(distinct case when user_type = '新客' then  order_no end) `新客订单量`
          ,count(distinct case when user_type = '新客' then  t1.user_id end) `新客下单用户量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                            and user_type = '新客'
                        then order_no else null end)             as `Q_新客用券订单量`
    from q_order t1
    left join (select  dates,user_id,channel from uv_1 group by 1,2,3) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by 1,2,3
)


select t1.dates `日期`
        ,t1.channel `渠道`,t1.mdd `目的地`,`流量占比`,UV
        ,concat(round(nvl(`订单量` / UV,0) * 100,2),'%') as `CR`
        ,concat(round(nvl(`Q_用券订单量`/ `订单量` ,0) * 100,2),'%') as `用券订单占比`
        ,`订单量`,`间夜量`
        ,`新客UV`,`新客流量占比`
        ,concat(round(nvl(`新客订单量` / `新客UV`,0) * 100,2),'%') as `新客CR`
        ,concat(round(nvl(`Q_新客用券订单量`  / `新客订单量`,0) * 100,2),'%') as `新客用券订单占比`
        ,`新客订单量`
        ,`新客间夜量`
from (
    select dates 
            ,channel,mdd
            ,uv
            ,new_uv `新客UV`
            ,concat(round(uv / sum(uv) over(partition by dates) * 100,2), '%') `流量占比`
            ,concat(round(new_uv / sum(new_uv) over(partition by dates) * 100,2), '%') `新客流量占比`
    from q_uv_info
)t1 left join (
    select order_date
            ,channel,mdd
            ,`Q_用券订单量` 
            ,`订单量` 
            ,`间夜量` 
            ,`新客间夜量` 
            ,`新客订单量` 
            ,`Q_新客用券订单量` 
    from order_info
)t2 on t1.dates=t2.order_date and t1.channel=t2.channel and t1.mdd=t2.mdd
order by  `渠道`,`日期` desc
;


---- 3、邮件报表：分离店周期
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as(---D页离店日期在暑期期间
    select  dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,case 
                when checkout_date between '2026-07-06' and '2026-07-12' then '0706-0712'
                when checkout_date between '2026-07-13' and '2026-07-19' then '0713-0719'
                when checkout_date between '2026-07-20' and '2026-07-26' then '0720-0726'
                when checkout_date between '2026-07-27' and '2026-08-02' then '0727-0802'
                when checkout_date between '2026-08-03' and '2026-08-09' then '0803-0809'
                when checkout_date between '2026-08-10' and '2026-08-16' then '0810-0816'
                else '其他' end as checkout_wk
            ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
        and checkout_date between '2026-07-06' and '2026-08-16'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6
)
,user_jc as(--机酒交叉
    select distinct dt
            , mdd
            , uv.user_name
            , '机酒交叉'      as channel
            , 0              as user_number
    from uv uv
    left join(--- 需要修改时间
    select to_date(create_time)    as create_date
            , o_qunarusername
            , biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 40) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) flight
    on uv.user_name = flight.o_qunarusername
    where flight.create_date >= date_sub(uv.dt, 15)
    and flight.create_date <= uv.dt
    and flight_order_no is not null
)
,user_xhs as(--小红书 宽口径
    select distinct uv.dt
                   , mdd
                   , uv.user_name
                   , '小红书' as channel
                   , 1  as user_number
    from uv uv
    left join(--- 需要修改时间
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)
,user_nr as(--- 内容交叉
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            ,uv.user_name
            ,uv.mdd
            ,'内容交叉' as  channel
            ,2         as  user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = date_sub(current_date, 1) and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join ( --- 需要修改时间
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= from_unixtime(unix_timestamp() - 86400 * 16, 'yyyyMMdd')
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv uv on d.user_id = uv.user_name  and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as( --暑期活动
    select distinct uv.dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'营销活动' channel
                   ,3 as     user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as dt
                        ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
            and username not like '0000%'
        ) d on d.user_name = uv.user_name
    where d.dt >= date_sub(uv.dt, 7)
       and d.dt <= uv.dt
       and d.user_name is not null
)
,user_gnjd as( ----国内酒店
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'国内交叉' as channel
                   ,4          as user_number
    from uv uv
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.dt, 365)
       and g.order_date <= uv.dt
       and g.user_id is not null
)
,user_channel  as(---流量来源渠道整理 
    select distinct dt
            , mdd
            , user_name
            , channel
    from (
        select dt,
                mdd,
                user_name,
                channel,
                row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt, mdd, user_name, channel, user_number
            from user_jc
            union all
            select dt, mdd, user_name, channel, user_number
            from user_xhs
            union all
            select dt, mdd, user_name, channel, user_number
            from user_nr
            union all
            select dt, mdd, user_name, channel, user_number
            from user_hd
            union all
            select dt, mdd, user_name, channel, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as (----多维度活跃用户汇总
    select distinct a.dt     as dates
            ,a.user_type
            ,a.mdd,a.checkout_wk
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
            ,coupon_substract_summary
            ,follow_price_amount,extendinfomap,cashbackmap
            ,case 
                when checkout_date between '2026-07-06' and '2026-07-12' then '0706-0712'
                when checkout_date between '2026-07-13' and '2026-07-19' then '0713-0719'
                when checkout_date between '2026-07-20' and '2026-07-26' then '0720-0726'
                when checkout_date between '2026-07-27' and '2026-08-02' then '0727-0802'
                when checkout_date between '2026-08-03' and '2026-08-09' then '0803-0809'
                when checkout_date between '2026-08-10' and '2026-08-16' then '0810-0816'
                else '其他' end as checkout_wk
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date,1)
        and checkout_date between '2026-07-06' and '2026-08-16'   --- 离店日期在暑期
)
,q_uv_info as(---- 分渠道流量汇总
    select dates
            ,channel,checkout_wk
            ,count(user_id)   uv
            ,count(case when user_type = '新客' then  user_id end) new_uv
    from uv_1
    group by 1,2,3
)
,order_info as (---- 分渠道订单汇总
    select t1.order_date
          ,coalesce(t2.channel,'null') as  channel
          ,t1.checkout_wk
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(case when user_type = '新客' then  room_night end) `新客间夜量`
          ,count(distinct case when user_type = '新客' then  order_no end) `新客订单量`
          ,count(distinct case when user_type = '新客' then  t1.user_id end) `新客下单用户量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                            and user_type = '新客'
                        then order_no else null end)             as `Q_新客用券订单量`
    from q_order t1
    left join (select  dates,user_id,channel from uv_1 group by 1,2,3) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by 1,2,3
)


select t1.dates `日期`
        ,t1.channel `渠道`,t1.checkout_wk `离店周期`,`流量占比`,UV
        ,concat(round(nvl(`订单量` / UV,0) * 100,2),'%') as `CR`
        ,concat(round(nvl(`Q_用券订单量`/ `订单量` ,0) * 100,2),'%') as `用券订单占比`
        ,`订单量`,`间夜量`
        ,`新客UV`,`新客流量占比`
        ,concat(round(nvl(`新客订单量` / `新客UV`,0) * 100,2),'%') as `新客CR`
        ,concat(round(nvl(`Q_新客用券订单量`  / `新客订单量`,0) * 100,2),'%') as `新客用券订单占比`
        ,`新客订单量`
        ,`新客间夜量`
from (
    select dates 
            ,channel,checkout_wk
            ,uv
            ,new_uv `新客UV`
            ,concat(round(uv / sum(uv) over(partition by dates) * 100,2), '%') `流量占比`
            ,concat(round(new_uv / sum(new_uv) over(partition by dates) * 100,2), '%') `新客流量占比`
    from q_uv_info
)t1 left join (
    select order_date
            ,channel,checkout_wk
            ,`Q_用券订单量` 
            ,`订单量` 
            ,`间夜量` 
            ,`新客间夜量` 
            ,`新客订单量` 
            ,`Q_新客用券订单量` 
    from order_info
)t2 on t1.dates=t2.order_date and t1.channel=t2.channel and t1.checkout_wk=t2.checkout_wk
order by  `渠道`,`日期` desc
;



---- 4、邮件报表：分离店周期分目的地
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as(---D页离店日期在暑期期间
    select  dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,case 
                when checkout_date between '2026-07-06' and '2026-07-12' then '0706-0712'
                when checkout_date between '2026-07-13' and '2026-07-19' then '0713-0719'
                when checkout_date between '2026-07-20' and '2026-07-26' then '0720-0726'
                when checkout_date between '2026-07-27' and '2026-08-02' then '0727-0802'
                when checkout_date between '2026-08-03' and '2026-08-09' then '0803-0809'
                when checkout_date between '2026-08-10' and '2026-08-16' then '0810-0816'
                else '其他' end as checkout_wk
            ,a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
        and checkout_date between '2026-07-06' and '2026-08-16'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6
)
,user_jc as(--机酒交叉
    select distinct dt
            , mdd
            , uv.user_name
            , '机酒交叉'      as channel
            , 0              as user_number
    from uv uv
    left join(--- 需要修改时间
    select to_date(create_time)    as create_date
            , o_qunarusername
            , biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 40) and dt <= date_sub(current_date, 1)
        --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
        and ticket_time is not null      -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao'        -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) flight
    on uv.user_name = flight.o_qunarusername
    where flight.create_date >= date_sub(uv.dt, 15)
    and flight.create_date <= uv.dt
    and flight_order_no is not null
)
,user_xhs as(--小红书 宽口径
    select distinct uv.dt
                   , mdd
                   , uv.user_name
                   , '小红书' as channel
                   , 1  as user_number
    from uv uv
    left join(--- 需要修改时间
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)
,user_nr as(--- 内容交叉
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            ,uv.user_name
            ,uv.mdd
            ,'内容交叉' as  channel
            ,2         as  user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = date_sub(current_date, 1) and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join ( --- 需要修改时间
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= from_unixtime(unix_timestamp() - 86400 * 16, 'yyyyMMdd')
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv uv on d.user_id = uv.user_name  and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as( --暑期活动
    select distinct uv.dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'营销活动' channel
                   ,3 as     user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as dt
                        ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
            and username not like '0000%'
        ) d on d.user_name = uv.user_name
    where d.dt >= date_sub(uv.dt, 7)
       and d.dt <= uv.dt
       and d.user_name is not null
)
,user_gnjd as( ----国内酒店
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'国内交叉' as channel
                   ,4          as user_number
    from uv uv 
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.dt, 365)
       and g.order_date <= uv.dt
       and g.user_id is not null
)
,user_channel  as(---流量来源渠道整理 
    select distinct dt
            , mdd
            , user_name
            , channel
    from (
        select dt,
                mdd,
                user_name,
                channel,
                row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt, mdd, user_name, channel, user_number
            from user_jc
            union all
            select dt, mdd, user_name, channel, user_number
            from user_xhs
            union all
            select dt, mdd, user_name, channel, user_number
            from user_nr
            union all
            select dt, mdd, user_name, channel, user_number
            from user_hd
            union all
            select dt, mdd, user_name, channel, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as (----多维度活跃用户汇总
    select distinct a.dt     as dates
            ,a.user_type
            ,a.mdd,a.checkout_wk
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
            ,coupon_substract_summary
            ,follow_price_amount,extendinfomap,cashbackmap
            ,case 
                when checkout_date between '2026-07-06' and '2026-07-12' then '0706-0712'
                when checkout_date between '2026-07-13' and '2026-07-19' then '0713-0719'
                when checkout_date between '2026-07-20' and '2026-07-26' then '0720-0726'
                when checkout_date between '2026-07-27' and '2026-08-02' then '0727-0802'
                when checkout_date between '2026-08-03' and '2026-08-09' then '0803-0809'
                when checkout_date between '2026-08-10' and '2026-08-16' then '0810-0816'
                else '其他' end as checkout_wk
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date,1)
        and checkout_date between '2026-07-06' and '2026-08-16'   --- 离店日期在暑期
)
,q_uv_info as(---- 分渠道流量汇总
    select dates
            ,channel,checkout_wk,mdd
            ,count(user_id)   uv
            ,count(case when user_type = '新客' then  user_id end) new_uv
    from uv_1
    group by 1,2,3,4
)
,order_info as (---- 分渠道订单汇总
    select t1.order_date
          ,coalesce(t2.channel,'null') as  channel
          ,t1.checkout_wk,t1.mdd
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(case when user_type = '新客' then  room_night end) `新客间夜量`
          ,count(distinct case when user_type = '新客' then  order_no end) `新客订单量`
          ,count(distinct case when user_type = '新客' then  t1.user_id end) `新客下单用户量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                            and user_type = '新客'
                        then order_no else null end)             as `Q_新客用券订单量`
    from q_order t1
    left join (select  dates,user_id,channel from uv_1 group by 1,2,3) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by 1,2,3,4
)


select t1.dates `日期`
        ,t1.channel `渠道`,t1.checkout_wk `离店周期`,t1.mdd `目的地`,`流量占比`,UV
        ,concat(round(nvl(`订单量` / UV,0) * 100,2),'%') as `CR`
        ,concat(round(nvl(`Q_用券订单量`/ `订单量` ,0) * 100,2),'%') as `用券订单占比`
        ,`订单量`,`间夜量`
        ,`新客UV`,`新客流量占比`
        ,concat(round(nvl(`新客订单量` / `新客UV`,0) * 100,2),'%') as `新客CR`
        ,concat(round(nvl(`Q_新客用券订单量`  / `新客订单量`,0) * 100,2),'%') as `新客用券订单占比`
        ,`新客订单量`
        ,`新客间夜量`
from (
    select dates 
            ,channel,checkout_wk,mdd
            ,uv
            ,new_uv `新客UV`
            ,concat(round(uv / sum(uv) over(partition by dates) * 100,2), '%') `流量占比`
            ,concat(round(new_uv / sum(new_uv) over(partition by dates) * 100,2), '%') `新客流量占比`
    from q_uv_info
)t1 left join (
    select order_date
            ,channel,checkout_wk,mdd
            ,`Q_用券订单量` 
            ,`订单量` 
            ,`间夜量` 
            ,`新客间夜量` 
            ,`新客订单量` 
            ,`Q_新客用券订单量` 
    from order_info
)t2 on t1.dates=t2.order_date and t1.channel=t2.channel and t1.checkout_wk=t2.checkout_wk and t1.mdd=t2.mdd
order by  `渠道`,`日期` desc
;