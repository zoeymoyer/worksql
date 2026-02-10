--spark-sql -e "
with user_type -----新老客
as (
        select user_id
                , min(order_date) as min_order_date
        from mdw_order_v3_international   --- 海外订单表
        where dt = '${zdt.addDay(-1).format("yyyyMMdd")}' 
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
     where dt = '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
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
     left join(
        select to_date(create_time)    as create_date
                , o_qunarusername
                , biz_order_no         as flight_order_no
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '${zdt.addDay(-20).format("yyyy-MM-dd")}'
            and substr(create_time, 1, 10) >= '${zdt.addDay(-20).format("yyyy-MM-dd")}'  -- 生单时间
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
    left join(
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '${zdt.addDay(-20).format("yyyy-MM-dd")}'
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
            where dt = '${zdt.addDay(-1).format("yyyyMMdd")}'  and status = 0
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
            where dt = '${zdt.addDay(-1).format("yyyyMMdd")}' 
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = '${zdt.addDay(-1).format("yyyyMMdd")}'  and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join (
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= '20250601'
                  and dt <= '${zdt.addDay(-1).format("yyyyMMdd")}' 
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name 
    and concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) = uv.dt
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
        --inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        --on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub('${zdt.addDay(-1).format("yyyy-MM-dd")}' , 10)
            and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'  --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
            --- new
            and page_cid in ('17804','18290','18890','18989','19895','20391','24144','20865','18910','18937','18938','18986','19291','23400','21920','24467','24212','25007','25263','25507','24842','25981','25878','25080','25613','25910','25953','26599','12135','27237','26998','27101','26092','25177','27520','27521','11426','27610','28028','27576','27933','28878','28804','29148','28960','29536','29274','29902','29643','29774','29902','29960','29965','28405','30546','30756','30661','30568','30784','30651','30540','30524','30997','30994','31007','30926','31353','31425','31631','31551','31744','32508','32502','32589','32689','32973','32875','32980','32981','33932','34391','34221','34220','34217','34440','35022','35333','35899','34788','36049','35963','36320','36208','35421','36400','36701')
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        --inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        --on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub('${zdt.addDay(-1).format("yyyy-MM-dd")}' , 10)
            and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
            --- new
            and activity_id in ('24springFestivalGift','summerPromotion202407','24_wuyi','24sy','25newyearPhase1','25wy_','25_20anniversary','25shucu','25shiyi')
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        --inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        --on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub('${zdt.addDay(-1).format("yyyy-MM-dd")}' , 10)
            and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'  
            and username not like '0000%'
            ---new
            and t.page in ('normal_active_31425','normal_active_32973','normal_active_32875','normal_active_33070','normal_active_33791','normal_active_33699','normal_active_33594','normal_active_33898','normal_active_34349','normal_active_35175','normal_active_34482','normal_active_35215','normal_active_35204','normal_active_36473','normal_active_36887')
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
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt = '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv_1 as ----多维度活跃用户汇总
(
    select distinct a.dt                                                    as dates
            -- ,a.user_type
            ,case
                when (a.user_type = '新客' and c.user_pk is not null) then '平台新业务新'
                when a.user_type = '新客' then '平台老业务新'
                else '老客'
                end  user_type
            ,a.mdd
            ,COALESCE(d.channel, '自然流量')                                        as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
    left join platform_new c on a.user_name = c.user_pk  and a.dt = c.dt
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,case when (order_date = b.min_order_date and c.user_pk is not null) then '平台新业务新'
                  when order_date = b.min_order_date then '平台老业务新'  else '老客' end as user_type
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,ext_plat_certificate
            ,coupon_info
          
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else nvl(coupon_substract_summary,0) end as coupon_substract_summary
            
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon
           
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join platform_new c on a.user_name = c.user_pk  and a.order_date = c.dt
    where dt = '${zdt.addDay(-1).format("yyyyMMdd")}' 
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date = '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
        and order_no <> '103576132435'
)

,uv_2 as ----订单辅助列
(
    select   dates
            ,channel
            ,user_id
    from uv_1 group by 1,2,3
)
,q_uv_info as
(   ---- 流量汇总
    select dates    ---全部
          ,'ALL' mdd
          ,'ALL' chann
          ,'ALL' user_type1
          ,count(1) uv
    from uv_1
    group by 1
    union all
    select dates   ---分目的地
        ,mdd
        ,'ALL' chann
        ,'ALL' user_type1
        ,count(1) uv
    from uv_1
    group by 1,2
    union all
    select dates  ---分渠道
        ,'ALL'  mdd
        ,channel chann
        ,'ALL' user_type1
        ,count(1) uv
    from uv_1
    group by 1,3
    union all
    select dates  ---分新老
        ,'ALL'  mdd
        ,'ALL' chann
        ,user_type as user_type1
        ,count(1) uv
    from uv_1
    group by 1,4
    union all
    select dates   --- 分目的地渠道
        ,mdd
        ,channel chann
        ,'ALL' user_type1
        ,count(1) uv
    from uv_1
    group by 1,2,3
    union all
    select dates   --- 分目的地新老
        ,mdd
        ,'ALL' chann
        ,user_type as user_type1
        ,count(1) uv
    from uv_1
    group by 1,2,4
    union all
    select dates   --- 分渠道新老
        ,'ALL' mdd
        ,channel chann
        ,user_type as user_type1
        ,count(1) uv
    from uv_1
    group by 1,3,4
    union all
    select dates   --- 分目的地渠道新老
        ,mdd
        ,channel chann
        ,user_type as user_type1
        ,count(1) uv
    from uv_1
    group by 1,2,3,4
) 


,order_info as ---- 订单汇总
(
    select t1.order_date  ---全部
          ,'ALL' mdd
          ,'ALL'  user_type1
          ,'ALL'   channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date
    union all
    select t1.order_date ---分目的地
          ,mdd
          ,'ALL' user_type1
          ,'ALL'   channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,mdd

    union all
    select t1.order_date ---分新老
          ,'ALL'  mdd
          ,user_type as user_type1
          ,'ALL'   channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,user_type

    union all
    select t1.order_date ---分渠道
          ,'ALL'  mdd
          ,'ALL'   user_type1
          ,coalesce(t2.channel,'null') channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,coalesce(t2.channel,'null')

    union all
    select t1.order_date ---分目的地渠道
          , mdd
          ,'ALL'   user_type1
          ,coalesce(t2.channel,'null') channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,mdd,coalesce(t2.channel,'null')

    union all
    select t1.order_date ---分新老渠道
          ,'ALL'   mdd
          ,user_type as user_type1
          ,coalesce(t2.channel,'null') channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,user_type,coalesce(t2.channel,'null')

    union all
    select t1.order_date ---分目的地新老
          ,mdd
          ,user_type as user_type1
          ,'ALL'  channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,mdd,user_type

    union all
    select t1.order_date ---分目的地新老渠道
          ,mdd
          ,user_type as user_type1
          ,coalesce(t2.channel,'null') channel
          ,sum(room_night)   as room_night
          ,count(distinct order_no)   as order_no
          ,count(distinct case when is_user_conpon='Y' then order_no else null end)  as q_order_no
          ,count(distinct t1.user_id)      as order_uv
          ,sum(init_gmv)   as  init_gmv
          ,sum(coupon_substract_summary) qb_amt
          ,sum(final_commission_after) yj
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,mdd,user_type,coalesce(t2.channel,'null')
)

insert overwrite table ihotel_default.ads_flow_gj_ug_qbi_byday_di partition(dt='${zdt.addDay(-1).format("yyyy-MM-dd")}' )

select t1.mdd
        ,t1.user_type1 
        ,t1.chann
        ,nvl(t1.UV, 0)   as UV
        ,COALESCE(t2.room_night, 0)     as room_night
        ,COALESCE(t2.order_no, 0)      as order_no
        ,COALESCE(t2.order_uv, 0)      as order_uv
        ,COALESCE(t1.UV / t3.UV, 0)   as uv_rate
        ,COALESCE(t2.q_order_no, 0) / nvl(t2.order_no, 0) as q_conpon_order_rate
        ,COALESCE(t2.init_gmv, 0)      as init_gmv
        ,COALESCE(t2.qb_amt, 0)      as qb_amt
        ,COALESCE(t2.yj, 0)      as yj
from q_uv_info t1 
left join order_info t2 on t1.dates=t2.order_date and t1.mdd=t2.mdd 
        and t1.user_type1=t2.user_type1 and t1.chann=t2.channel
left join (  --- 计算流量占比
    select dates,mdd,UV
    from q_uv_info 
    where user_type1 = 'ALL' and chann = 'ALL'
) t3 on t1.dates=t3.dates and t1.mdd=t3.mdd 

--"
