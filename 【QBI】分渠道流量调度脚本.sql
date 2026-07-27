
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

with user_type as ( --- 用于判定Q新老客 (快照表取 T-1 即可)
    select user_id
          ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ( ----Q流量 (拉长至 14 天)
    select  dt 
           ,case when province_name in ('澳门','香港') then province_name  
                 when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name
                 when e.area in ('欧洲','亚太','美洲') then e.area
                 else '其他' end as mdd
           ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
           ,a.user_id
           ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '${zdt.addDay(-14).format("yyyy-MM-dd")}'
        and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,user_jc as ( --机酒交叉 (基于 14 天，再前置追溯 15 天，共 29 天)
    select distinct dt
                   ,mdd
                   ,uv.user_name
                   ,'机酒交叉' as channel
                   ,0 as user_number
    from uv uv
    left join (
        select to_date(create_time) as create_date
              ,o_qunarusername
              ,biz_order_no as flight_order_no
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '${zdt.addDay(-30).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
            -- and substr(create_time, 1, 10) >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) flight on uv.user_name = flight.o_qunarusername
    where flight.create_date >= date_sub(uv.dt, 15)
        and flight.create_date <= uv.dt
        and flight_order_no is not null
)
,user_xhs as ( --小红书 (基于 14 天，再前置追溯 7 天，共 21 天)
    select distinct uv.dt
                   ,mdd
                   ,uv.user_name
                   ,'小红书' as channel
                   ,1 as user_number
    from uv uv
    left join (
        select distinct flow_dt, user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '${zdt.addDay(-22).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
            and query_platform = 'redbook'
    ) red on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(uv.dt, 7)
        and red.flow_dt <= uv.dt
        and red.user_name is not null
)
,user_nr as ( --- 内容交叉 (对齐 14 天)
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
          ,uv.user_name
          ,uv.mdd
          ,'内容交叉' as channel
          ,2 as user_number
    from (--酒店帖  取最大分区 格式yyyymmdd
        select distinct dt, global_key, poi_id, poi_type, city_name
        from c_desert_feed.dw_feedstream_qulang_detail_info
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
          and status = 0
    ) a
    join (-- 取最大分区 格式yyyy-mm-dd
        select city_type,city_name
        from c_desert_feed.dim_content_city_derived_type_da
        where dt = date_sub(current_date, 1) and city_type = 2
    ) w on a.city_name = w.city_name
    join (-- 取最大分区 格式yyyymmdd
        select distinct dt, global_key, tag_id
        from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
          and tag_id in ('857', '860') and status = 0
    ) c on a.global_key = c.global_key and a.dt = c.dt
    left join (-- 取最大分区 格式yyyymmdd
        select distinct dt, global_key
        from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
          and goods_type = 7
    ) e on a.global_key = e.global_key and a.dt = e.dt
    left join (-- 曝光表取对应周期 14天 格式yyyymmdd
        select dt,user_id,global_key,request_id,is_clicked
        from c_desert_feed.dw_feedstream_erping_list_show
        where dt >= replace('${zdt.addDay(-14).format("yyyy-MM-dd")}', '-', '')
          and dt <= replace('${zdt.addDay(-1).format("yyyy-MM-dd")}', '-', '')
    ) d on a.global_key = d.global_key and a.dt = d.dt
    left join uv on d.user_id = uv.user_name and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null and is_clicked = 1
)
,user_hd as ( --暑期活动 (基于 14 天，再前置追溯 7 天，共 21 天)
    select distinct uv.dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'营销活动' channel
                   ,3 as user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as dt, user_name
        from hotel.dwd_flow_qav_htl_qmark_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page_cid = t1.code and t1.type = 'page'
        where dt >= '${zdt.addDay(-22).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
            and page_url like '%/shark/active%' and user_name not like '0000%'
        union
        select distinct dt, user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= '${zdt.addDay(-22).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
        union
        select distinct dt, username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= '${zdt.addDay(-22).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
            and username not like '0000%'
    ) d on d.user_name = uv.user_name
    where d.dt >= date_sub(uv.dt, 7) and d.dt <= uv.dt and d.user_name is not null
)
,user_gnjd as ( ----国内酒店 (维表取 T-1)
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,'国内交叉' as channel
                   ,4 as user_number
    from uv 
    left join (
        select distinct user_id, order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
          and order_date >= '2022-11-01'
    ) g on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.dt, 365) and g.order_date <= uv.dt and g.user_id is not null
)
,user_channel as ( ---流量来源渠道整理 
    select distinct dt
                   ,mdd
                   ,user_name
                   ,channel
    from (
        select dt, mdd, user_name, channel,
               row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt, mdd, user_name, channel, user_number from user_jc
            union all select dt, mdd, user_name, channel, user_number from user_xhs
            union all select dt, mdd, user_name, channel, user_number from user_nr
            union all select dt, mdd, user_name, channel, user_number from user_hd
            union all select dt, mdd, user_name, channel, user_number from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,platform_new as (--- 判定平台新 (动态对齐 14 天)
    select dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '${zdt.addDay(-14).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv_1 as ( ----多维度活跃用户汇总
    select  a.dt as dates
          ,case when (a.user_type = '新客' and c.user_pk is not null) then '平台新业务新'
                when a.user_type = '新客' then '平台老业务新'
                else '老客' end as user_type
          ,a.mdd
          ,coalesce(d.channel, '自然流量') as channel
          ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
    left join platform_new c on a.user_name = c.user_pk and a.dt = c.dt
    group by 1,2,3,4,5
)
,q_order as (----订单明细表包含取消 
    select order_date
          ,case when province_name in ('澳门','香港') then province_name  
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  
                when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
          ,case when (order_date = b.min_order_date and c.user_pk is not null) then '平台新业务新'
                when order_date = b.min_order_date then '平台老业务新'  else '老客' end as user_type
          ,a.user_id,init_gmv,order_no,room_night
          ,batch_series,hotel_grade,coupon_id,init_commission_after,ext_plat_certificate,coupon_info
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
          ,case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
          ,case when coupon_id is not null and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%' and batch_series not like '%23extra_ZK_ce6f99%' 
                then 'Y' else 'N' end is_user_conpon
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join platform_new c on a.user_name = c.user_pk and a.order_date = c.dt
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '${zdt.addDay(-14).format("yyyy-MM-dd")}' and order_date <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
        and order_no <> '103576132435'
)
,uv_2 as ( ----订单辅助列 (去重避免发散)
    select dates, channel, user_id from uv_1 group by 1,2,3
)

--- 1、使用 CUBE 结合 UNION ALL 动态生成含「新客」的多维组合
,q_uv_info as (
    select dates
        , if(grouping(mdd)=1,'ALL',mdd) as mdd
        , if(grouping(channel)=1,'ALL',channel) as chann
        , if(grouping(user_type)=1,'ALL',user_type) as user_type1  --- 平台新业务新、平台老业务新、老客
        , count(1) as uv
    from uv_1
    group by dates, cube(mdd, channel, user_type)
    
    UNION ALL
    
    select dates
        , if(grouping(mdd)=1,'ALL',mdd) as mdd
        , if(grouping(channel)=1,'ALL',channel) as chann
        , '新客' as user_type1
        , count(1) as uv
    from uv_1
    where user_type in ('平台新业务新', '平台老业务新')
    group by dates, cube(mdd, channel)
) 

--- 2：同样使用 CUBE + UNION ALL 
,order_info as ( 
    select t1.order_date
          ,if(grouping(t1.mdd)=1,'ALL',t1.mdd) as mdd
          ,if(grouping(t1.user_type)=1,'ALL',t1.user_type) as user_type1
          ,if(grouping(coalesce(t2.channel,'null'))=1,'ALL',coalesce(t2.channel,'null')) as channel
          ,sum(room_night) as room_night
          ,count(distinct order_no) as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end) as q_order_no
          ,count(distinct t1.user_id) as order_uv
          ,sum(init_gmv) as init_gmv
          ,sum(coupon_substract_summary) as qb_amt
          ,sum(final_commission_after) as yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date, cube(t1.mdd, t1.user_type, coalesce(t2.channel,'null'))
    
    UNION ALL
    
    select t1.order_date
          ,if(grouping(t1.mdd)=1,'ALL',t1.mdd) as mdd
          ,'新客' as user_type1
          ,if(grouping(coalesce(t2.channel,'null'))=1,'ALL',coalesce(t2.channel,'null')) as channel
          ,sum(room_night) as room_night
          ,count(distinct order_no) as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end) as q_order_no
          ,count(distinct t1.user_id) as order_uv
          ,sum(init_gmv) as init_gmv
          ,sum(coupon_substract_summary) as qb_amt
          ,sum(final_commission_after) as yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    where t1.user_type in ('平台新业务新', '平台老业务新')
    group by t1.order_date, cube(t1.mdd, coalesce(t2.channel,'null'))
)

insert overwrite table ads_flow_gj_ug_qbi_byday_di partition(dt)

select t1.mdd
      ,t1.user_type1 
      ,t1.chann
      ,coalesce(t1.UV, 0) as UV
      ,coalesce(t2.room_night, 0) as room_night
      ,coalesce(t2.order_no, 0) as order_no
      ,coalesce(t2.order_uv, 0) as order_uv
      ,coalesce(t1.UV / nullif(t3.UV, 0), 0) as uv_rate
      ,coalesce(t2.q_order_no, 0) / nullif(t2.order_no, 0) as q_conpon_order_rate
      ,coalesce(t2.init_gmv, 0) as init_gmv
      ,coalesce(t2.qb_amt, 0) as qb_amt
      ,coalesce(t2.yj, 0) as yj
      
      ,t1.dates as dt
from q_uv_info t1 
left join order_info t2 on t1.dates=t2.order_date and t1.mdd=t2.mdd and t1.user_type1=t2.user_type1 and t1.chann=t2.channel
left join (
    select dates, mdd, UV
    from q_uv_info 
    where user_type1 = 'ALL' and chann = 'ALL'
) t3 on t1.dates=t3.dates and t1.mdd=t3.mdd 
;