---- 1、渠道归一流量数据
with user_type as
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
     where dt >= '2025-06-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,user_jc as --机酒交叉
(
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
    where dt >= '2025-05-01' and dt <= date_sub(current_date, 1)
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
,user_xhs as --小红书 宽口径
(
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
        where dt >= '2025-05-01' and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)
,user_nr as   --- 内容交叉
(
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            , uv.user_name
            , uv.mdd
            , '内容交叉' as  channel
            , 2         as  user_number
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
            where dt >= '20250601'
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name  and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as --暑期活动
(
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
        where dt >= '2025-05-01'
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= '2025-05-01'
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= '2025-05-01'
            and dt <= date_sub(current_date, 1)
            and username not like '0000%'
        ) d on d.user_name = uv.user_name
    where d.dt >= date_sub(uv.dt, 7)
       and d.dt <= uv.dt
       and d.user_name is not null
)
,user_gnjd as ----国内酒店
(
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'国内交叉' as channel
                   ,4          as user_number
    from uv 
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
,user_channel  as ---流量来源渠道整理 
(
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
,uv_1 as ----多维度活跃用户汇总
(
    select distinct a.dt     as dates
            ,a.user_type
            ,a.mdd
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
)
,q_uv_info as
(   ---- 流量汇总
    select dates
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,if(grouping(channel)=1,'ALL', channel) as  channel
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(user_id)   uv
    from uv_1
    group by dates,cube(user_type, mdd, channel)
) 

select t1.dates,t1.mdd,t1.channel,t1.user_type ,t1.uv,t1.uv/t2.uv uv_rate
from q_uv_info t1 
left join 
(
    select dates,uv
    from q_uv_info
    where mdd='ALL' and channel='ALL' and user_type='ALL'
)t2 on t1.dates=t2.dates
order by 1,2,3,4
;



--- 2、机酒交叉和小红书渠道重叠部分--往后推逻辑
with user_type as
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
     where dt >= '2025-06-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,user_jc as --机酒交叉
(
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
    where dt >= '2025-05-01' and dt <= date_sub(current_date, 1)
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
,user_xhs as --小红书 宽口径
(
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
        where dt >= '2025-05-01' and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)


select t1.dt,DAU,jc_DAU,xhs_DAU,jc_xhs_DAU
       ,jc_DAU / DAU jc_rate
       ,xhs_DAU / DAU xhs_rate
       ,jc_xhs_DAU / jc_DAU jc_xhs_rate
from (
    select dt ,count(user_id) DAU
    from uv
    group by 1
)t1 left join ( --- 机酒DAU
    select dt ,count(user_name) jc_DAU
    from user_jc
    group by 1
)t2 on t1.dt=t2.dt
left join (  --- 小红书DAU
    select dt ,count(user_name) xhs_DAU
    from user_xhs
    group by 1
)t3 on t1.dt=t3.dt
left join (--- 机酒小红书交叉DAU
    select a.dt ,count(a.user_name) jc_xhs_DAU
    from user_jc a
    join user_xhs b on a.dt=b.dt and a.user_name=b.user_name
    group by 1
)t4 on t1.dt=t4.dt
order by 1
;




---- 3、机票订单和小红书渠道交叉往前推逻辑
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
,uv as ----分日去重活跃用户
(
    select  dt 
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
            --            when e.area in ('欧洲','亚太','美洲') then e.area
            --            else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,max(dt) max_action_time
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-01-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
    
)
,user_xhs as --小红书 宽口径 当天及往后推7天内
(
    select distinct uv.dt
                   , user_type
                   , uv.user_name
                   , '小红书' as channel
                   , 1  as user_number
    from uv uv
    left join(--- 需要修改时间
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '2024-12-01' and dt <= date_sub(current_date, 1)
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
)
,flight as (
    select t1.dt,t1.user_name,t1.min_pay_time
          ,max(case when t2.min_order_date <= t1.min_pay_time and t2.min_order_date <= t1.dt then '国际酒店老客' else '国际酒店新客' end) as is_new
    from (
        select to_date(create_time) as dt
            -- ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
            --         when s_arrcityname in ('中国香港','中国澳门') then s_arrcityname
            --         when e.area in ('欧洲','亚太','美洲') then e.area
            --         else '其他' end as s_arrcountryname
             ,o.o_qunarusername as user_name
             ,min(substr(create_time,1,10)) as min_pay_time
        from f_fuwu.dw_fact_inter_order_wide o
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on o.s_arrcountryname = e.country_name
        where dt>='2023-01-01' and dt <= date_sub(current_date, 1)
        and substr(create_time,1, 10) >= '2025-01-01'
        and substr(create_time,1, 10)<= '2025-09-30'   --当天及往前推15天内的机票用户T-14~T
        and ticket_time is not null and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao' -- 去分销
        and (s_arrcountryname !='中国' or s_depcountryname !='中国')
        group by 1,2
    )t1 
    left join user_type t2 on t1.user_name=t2.user_name
    group by 1,2,3
)

,user_jc as (  --- 机酒交叉 当天及往前推15天内的机票用户T-14~T
    select distinct a.dt,a.is_new,a.user_name
    from flight a
    left join uv b
    on a.user_name = b.user_name 
    where b.dt between a.dt and date_add(a.dt, 15)
    and max_action_time >= min_pay_time
    and b.user_name is not null
)


select t1.dt
        ,t1.user_type
        ,flight_uv
        ,flight_act_uv
        ,flight_act_uv / flight_uv jcrate
        ,flight_red_uv
        ,flight_red_uv / flight_act_uv xhsjc_rate
from (
    select dt 
        ,if(grouping(is_new)=1,'ALL', is_new)  user_type
        ,count(distinct user_name) flight_uv
    from flight
    group by dt,cube(is_new)
) t1 left join (
    select dt
        ,if(grouping(is_new)=1,'ALL', is_new)  user_type
        ,count(distinct user_name) flight_act_uv
    from user_jc
    group by dt,cube(is_new)
)t2 on t1.dt=t2.dt and t1.user_type=t2.user_type
left join (--- 机酒交叉和宽口径小红书重叠部分
    select a.dt
        ,if(grouping(is_new)=1,'ALL', is_new)  user_type
        ,count(distinct a.user_name) flight_red_uv
    from user_jc a
    join user_xhs b on a.dt=b.dt and a.user_name = b.user_name
    group by a.dt,cube(is_new)  
)t3 on t1.dt=t3.dt and t1.user_type=t3.user_type
order by 1
;


