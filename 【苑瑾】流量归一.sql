--- 归一逻辑：小红书、短视频、市场投放、机酒交叉、内容交叉、营销活动、国内交叉、自然流量
with user_type as
(
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
,uv as ----分日去重活跃用户
(
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
,user_xhs as (--小红书 宽口径
    select  uv.dt
            ,uv.user_name
            ,'小红书' as channel
            ,0  as user_number
    from uv
    left join(--- 需要修改时间
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(dt, 7)
       and red.flow_dt <= uv.dt
       and red.user_name is not null
    group by 1,2
)
,video as (  -- 换成短视频数据
    select  t1.dt,user_name
    from (
        SELECT  query
            ,user_name
            ,dt
            ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt >= date_sub(current_date,30) and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
        ) t2 
        on t1.query_ori = t2.query
        where dt >=date_sub(current_date,30)
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    group by 1,2
)
,user_vedio as (--短视频 宽口径
    select  uv.dt
            ,uv.user_name
            ,'短视频' as channel
            ,1  as user_number
    from uv
    left join video r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    group by 1,2
) 
,ihotel_uv as (--- 国酒活跃交叉市场信息流达人投放类型用户 获取对应的uid
    select a.dt
           ,a.user_name
           ,c.uid
    from uv a
    left join (--市场设备活跃信息 筛选信息流和达人且取对应的平台类型
        select  t.dt,
                t.uid,
                t.username,
                t.category
        from hotel.dwd_feedstream_flow_accapp_di t   -- 通过信息流投放激活的日数据
        where t.dt >= date_sub(current_date, 15) and t.dt <= date_sub(current_date, 1)
            and t.category in ('信息流', '达人')
        group by 1,2,3,4
    ) c on a.user_name=c.username and a.dt=c.dt
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
-- 将活跃的uid渠道来源定位到广告点击渠道上7天
,user_market as (---- 市场投放  宽口径
    select  m.dt
            ,m.user_name
            ,'市场投放' as channel
            ,2  as user_number
    from ihotel_uv m
    left join market_click i on m.uid = i.uid
    where  i.dt >= date_sub(m.dt, 7) and i.dt <= m.dt 
        and i.uid is not null
    group by 1,2
)
,user_jc as (--机酒交叉
    select  dt
            ,uv.user_name
            ,'机酒交叉'      as channel
            ,3              as user_number
    from uv uv
    left join(--- 需要修改时间
    select to_date(create_time)    as create_date
            , o_qunarusername
            , biz_order_no         as flight_order_no
    from f_fuwu.dw_fact_inter_order_wide
    where dt >= date_sub(current_date, 30) and dt <= date_sub(current_date, 1)
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
    group by 1,2
)
,user_nr as   (--- 内容交叉
    select  concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            , uv.user_name
            , '内容交叉' as  channel
            , 4         as  user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') and status = 0
        ) a
    join (   ---  限制海外城市
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
            where dt >= from_unixtime(unix_timestamp() - 86400 * 30, 'yyyyMMdd')
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name  and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
    group by 1,2
)
,user_hd as (--营销活动
    select  uv.dt
            ,uv.user_name
            ,'营销活动' channel
            ,5 as     user_number
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
    group by 1,2
)
,user_gnjd as (----国内交叉
    select  dt
            ,uv.user_name
            ,'国内交叉' as channel
            ,6         as user_number
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
    group by 1,2
)
,user_channel  as ---流量来源渠道整理 
(
    select  dt
            ,user_name
            ,channel
    from (
        select dt,
                user_name,
                channel,
                row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt, user_name, channel, user_number
            from user_jc
            union all
            select dt, user_name, channel, user_number
            from user_xhs
            union all
            select dt, user_name, channel, user_number
            from user_nr
            union all
            select dt, user_name, channel, user_number
            from user_hd
            union all
            select dt, user_name, channel, user_number
            from user_gnjd
            union all
            select dt, user_name, channel, user_number
            from user_vedio
            union all
            select dt, user_name, channel, user_number
            from user_market
        ) t
    ) tt
    where user_level = 1
    group by 1,2,3
)
,uv_1 as ----多维度活跃用户汇总
(
    select  a.dt 
            ,a.user_type
            ,a.mdd
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_name
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
    group by 1,2,3,4,5
)

    select dt
            ,if(grouping(channel)=1,'ALL', channel) as  channel
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(user_id)   uv
    from uv_1
    group by dt,cube(user_type, channel)








