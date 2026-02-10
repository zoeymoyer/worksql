with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
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
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)

,user_gnjd as ----国内酒店
(
    select distinct dt
                   ,uv.mdd
                   ,uv.user_name
                   ,uv.user_id
                   ,uv.user_type
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

,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15)
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv_info as (--- 多维度流量
   select  t1.dt,t1.mdd,t1.user_id,t1.user_name,t1.user_type
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from user_gnjd t1 
    left join platform_new t2 on t1.dt=t2.dt and t1.user_name=t2.user_pk 
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度 全端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_use_conpon   --- 是否用券
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary  --- 券补金额
            ,case when (coupon_substract_summary is null or batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0 else coalesce(coupon_substract_summary,0) end qbje

            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,array_contains(supplier_promotion_code, '2913')  jf   --- 是否使用积分 值取true
            ,coalesce(get_json_object(promotion_score_info,'$.deductionPointsInfoV2.exchangeAmount'), 0) jfje  --- 积分金额
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info as (--- 多维度订单
   select t1.order_date,t1.mdd,t1.user_id,t1.user_type,init_gmv,order_no,room_night
           ,is_use_conpon,coupon_substract_summary,final_commission_after,jf,jfje,batch_series,coupon_id
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1 
    from q_order t1 
    left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk
    left join uv_info t3 on t1.user_name=t3.user_name and t1.order_date=t3.dt
    where t3.user_name is not null
)
,order_conpon_info as (--- 不同分类券订单
    select order_date,order_no,user_id,room_night,init_gmv,coupon_substract_summary,mdd,user_type,user_type1,is_use_conpon
            ,case when sponsor = '国际酒店' then 'BU券' 
                  when sponsor like '市场%' then '市场券' 
                  when sponsor is not null then '非BU券' end as sponsor
    from order_info  a
    left join temp.temp_xianjing_ye_temp_xianjing_ye_international_hotel_coupon_list_test3 b
    on a.batch_series = b.batch_series
    where coupon_id not like '%,%'
    and a.batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83')
    and a.batch_series not like '%23base_ZK_728810%' and a.batch_series not like '%23extra_ZK_ce6f99%'
    union all
    select order_date,order_no,user_id,room_night,init_gmv,coupon_substract_summary,mdd,user_type,user_type1,is_use_conpon
            ,case when sponsor = '国际酒店' then 'BU券' 
                  when sponsor like '市场%' then '市场券' 
                  when sponsor is not null then '非BU券' end as sponsor
    from order_info  a
    join temp.temp_xianjing_ye_temp_xianjing_ye_international_hotel_coupon_list_test3 b
    on  split(a.batch_series,',')[0]= b.batch_series
    where coupon_id like '%,%'
    and split(a.batch_series,',')[0] not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83')
    and split(a.batch_series,',')[0] not like '%23base_ZK_728810%' and split(a.batch_series,',')[0] not like '%23extra_ZK_ce6f99%'
    union all
    select order_date,order_no,user_id,room_night,init_gmv,coupon_substract_summary,mdd,user_type,user_type1,is_use_conpon
            ,case when sponsor = '国际酒店' then 'BU券' 
                  when sponsor like '市场%' then '市场券' 
                  when sponsor is not null then '非BU券' end as sponsor
    from order_info  a
    join temp.temp_xianjing_ye_temp_xianjing_ye_international_hotel_coupon_list_test3 b
    on  split(a.batch_series,',')[1]= b.batch_series
    where coupon_id like '%,%'
    and split(a.batch_series,',')[1] not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83')
    and split(a.batch_series,',')[1] not like '%23base_ZK_728810%' and split(a.batch_series,',')[1] not like '%23extra_ZK_ce6f99%'
)

select * from order_conpon_info where user_type1 = '平台新业务新' and sponsor = 'BU券' and  is_use_conpon = 'Y' 
;



with user_type -----新老客
as (
        select user_id
                , min(order_date) as min_order_date
        from mdw_order_v3_international   --- 海外订单表
        where dt = '20240101' 
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
     where dt = '2024-01-01' 
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
            where dt = '20240101'  and status = 0
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
            where dt = '20240101' 
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = '20240101'  and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join (
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt = '20240101' 
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
        where dt >= date_sub('2024-01-01' , 10)
            and dt <= '2024-01-01'  --日期
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
        where dt >= date_sub('2024-01-01' , 10)
            and dt <= '2024-01-01' 
            --- new
            and activity_id in ('24springFestivalGift','summerPromotion202407','24_wuyi','24sy','25newyearPhase1','25wy_','25_20anniversary','25shucu','25shiyi')
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        --inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        --on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub('2024-01-01' , 10)
            and dt <= '2024-01-01'  
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
    where dt = '2024-01-01' 
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
    where a.dt = '20240101' 
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date = '2024-01-01' 
        and order_no <> '103576132435'
)
,uv_2 as ----订单辅助列
(
    select   dates
            ,channel
            ,user_id
    from uv_1 group by 1,2,3
)

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