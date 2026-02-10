with user_type as  -- 用户首单日，不含取消
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
            ,case when province_name in ('澳门','香港') then province_name  
                  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                  when e.area in ('欧洲','亚太','美洲') then e.area
                  else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= date_sub(current_date, 7)
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
     left join(
        select to_date(create_time)    as create_date
                , o_qunarusername
                , biz_order_no         as flight_order_no
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2024-01-01'
            and substr(create_time, 1, 10) >= '2024-01-01'  -- 生单时间
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
        where dt >= '2025-01-01'
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
    left join (
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= '20250601'
                  and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
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
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 70)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 70)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 70)
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
            ,COALESCE(d.channel, '自然流量')    as channel
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
    group by dates,cube(user_type , mdd, channel)
) 
,uv_2 as ----订单辅助列
(
    select   dates
            ,channel
            ,user_id
    from uv_1 group by 1,2,3
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after_new+COALESCE(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when nvl(four_a, third_a) is not null and dt <= "20221124" then round(nvl(((nvl(second_a, first_a) - nvl(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - nvl(four_a, third_a)) * room_night)),2)
                   when nvl(four_a, third_a) is not null and order_date <= "2024-03-29" then (nvl(four_a_reduce, third_a_reduce)*room_night)
                   else nvl(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,nvl(cashbackmap['framework_amount'],0) as qb  ---券补
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else nvl(coupon_substract_summary,0) end as coupon_substract_summary  -- 券补贴金额
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,7) and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
)

,order_info as ( --- q app 订单汇总
    select t1.order_date 
         ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,if(grouping(coalesce(t2.channel,'null'))=1,'ALL',coalesce(t2.channel,'null')) as  channel
         ,sum(final_commission_after) as `Q_佣金`
         ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as `Q_佣金（C视角）`
         ,sum(init_gmv) as `Q_GMV`
         ,sum(coupon_substract_summary) as `Q_券额`
         ,count(distinct order_no) as `Q_订单量`
         ,count(distinct t1.user_id) as `Q_下单用户`
         ,sum(room_night) as `Q_间夜量`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from q_order t1
    left join uv_2 t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,cube(t1.mdd,t1.user_type,coalesce(t2.channel,'null'))
)


select t1.dates   `日期`
        ,t1.mdd
        ,t1.user_type  `新老客`
        ,t1.channel  `渠道`
        ,nvl(t1.uv, 0)                 as uv
        ,COALESCE(t2.`Q_间夜量`, 0)     as `Q_间夜量`
        ,COALESCE(t2.`Q_订单量`, 0)     as `Q_订单量`
        ,COALESCE(t2.`Q_下单用户`, 0)   as `Q_下单用户`
        ,COALESCE(t2.`Q_GMV`, 0)       as `Q_GMV`
        ,COALESCE(t2.`Q_佣金`, 0)       as `Q_佣金`
        ,COALESCE(t2.`Q_券额`, 0)       as `Q_券额`
        ,COALESCE(t2.`Q_高星间夜量`, 0)  as `Q_高星间夜量`
        ,COALESCE(t2.`Q_中星间夜量`, 0)  as `Q_中星间夜量`
        ,COALESCE(t2.`Q_低星间夜量`, 0)  as `Q_低星间夜量`
        ,COALESCE(t1.uv / t3.uv, 0)    as `Q流量占比`
        ,COALESCE(t2.`Q_订单量` / t1.uv, 0)  as `Q_CR`
        ,COALESCE(t2.`Q_间夜量`, 0) / COALESCE(t2.`Q_订单量`, 0)  as `Q_单间夜`
        ,COALESCE(t2.`Q_佣金`, 0) / COALESCE(t2.`Q_GMV`, 0)      as `Q_收益率`
        ,COALESCE(t2.`Q_券额`, 0) / COALESCE(t2.`Q_GMV`, 0)      as `Q_券补贴率`
        ,COALESCE(t2.`Q_GMV`, 0) / COALESCE(t2.`Q_间夜量`, 0)     as `Q_ADR`
        ,concat(round(COALESCE(t2.`Q_用券订单量`, 0) / nvl(t2.`Q_订单量`, 0) * 100, 1), '%') as `Q用券订单占比`
from q_uv_info t1 
left join order_info t2 on t1.dates=t2.order_date and t1.mdd=t2.mdd 
and t1.user_type=t2.user_type and t1.channel=t2.channel
left join (  --- 计算流量占比
    select dates,mdd,uv
    from q_uv_info 
    where user_type = 'ALL' and channel = 'ALL'
) t3 on t1.dates=t3.dates and t1.mdd=t3.mdd 
order by t1.channel,t1.mdd, t1.dates,COALESCE(t1.uv, 0) desc
;
