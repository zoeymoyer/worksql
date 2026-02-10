---- 流量渠道X目的地X新老
with user_type -----新老客
as (
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
,uv as ---D页离店日期在国庆期间
(
    select distinct dt as `日期`
        , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        , case when dt > b.min_order_date then '老客' else '新客' end as user_type
        , a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-08-20'
        and checkout_date between '2025-10-01' and '2025-10-08'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
)
,user_jc as --机酒交叉
(
    select distinct `日期`
                   , mdd
                   , uv.user_name
                   , '机酒交叉'      as `渠道`
                   , 0              as user_number
     from uv uv
     left join(
        select to_date(create_time)    as create_date
                , o_qunarusername
                , biz_order_no         as flight_order_no
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2025-01-01'
            and substr(create_time, 1, 10) >= '2025-01-01'  -- 生单时间
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
        ) flight
     on uv.user_name = flight.o_qunarusername
     where flight.create_date >= date_sub(uv.`日期`, 15)
        and flight.create_date <= uv.`日期`
        and flight_order_no is not null
)
,user_xhs as --小红书 宽口径
(
    select distinct uv.`日期`
                   , mdd
                   , uv.user_name
                   , '小红书' as `渠道`
                   , 1  as user_number
    from uv uv
    left join(
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '2022-12-01'
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(`日期`, 7)
       and red.flow_dt <= uv.`日期`
       and red.user_name is not null
)
,user_nr as   --- 内容交叉
(
    select distinct concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) `日期`
            , uv.user_name
            , uv.mdd
            , '内容交叉' as  `渠道`
            , 2         as   user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = '%(DATE)s' and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = '%(FORMAT_DATE)s' and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = '%(DATE)s'
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = '%(DATE)s' and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join (
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= '20250501'
                  and dt <= '%(DATE)s'
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name 
    and concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) = uv.`日期`
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as --暑期活动
(
    select distinct uv.`日期`
                   , uv.mdd
                   , uv.user_name
                   , '营销活动' `渠道`
                   , 3 as       user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as `日期`
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
    where d.`日期` >= date_sub(uv.`日期`, 7)
       and d.`日期` <= uv.`日期`
       and d.user_name is not null
)
,user_gnjd as ----国内酒店
(
    select distinct `日期`
                   , uv.mdd
                   , uv.user_name
                   , '国内交叉' as `渠道`
                   , 4          as user_number
    from uv uv
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.`日期`, 365)
       and g.order_date <= uv.`日期`
       and g.user_id is not null
)
,user_channel  as ---流量来源渠道整理 
(
    select distinct `日期`
            , mdd
            , user_name
            , `渠道`
    from (
        select `日期`,
                mdd,
                user_name,
                `渠道`,
                row_number() over (partition by `日期`,user_name order by user_number) as user_level
        from (
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_jc
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_xhs
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_nr
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_hd
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as ----多维度活跃用户汇总
(
    select distinct a.`日期`                                                    as dates
            ,a.user_type
            ,a.mdd
            ,nvl(d.`渠道`, '自然流量')                                        as `渠道`
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.`日期` = d.`日期`
)
,q_uv_info as
(   ---- 流量汇总
    select dates
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,if(grouping(`渠道`)=1,'ALL', `渠道`) as  `渠道`
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(user_id)   `UV`
    from uv_1
    group by dates,cube(user_type , mdd, `渠道`)
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
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-08-20' and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
        and checkout_date between '2025-10-01' and '2025-10-08'   --- 离店日期在国庆

)
,order_info as ---- 订单汇总
(
    select t1.order_date
          ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
          ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
          ,if(grouping(coalesce(t2.`渠道`,'null'))=1,'ALL',coalesce(t2.`渠道`,'null')) as  `渠道`
          ,sum(room_night)   as `间夜量`
          ,count(distinct order_no)   as `订单量`
          ,count(distinct case when coupon_id is not null 
                            and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                            and batch_series not like '%23base_ZK_728810%'
                            and batch_series not like '%23extra_ZK_ce6f99%' 
                        then order_no else null end)             as `Q_用券订单量`
          ,count(t1.user_id)             as `下单用户量`
          ,sum(init_gmv)     as `GMV`
          ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量`
          ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量`
          ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量`
    from q_order t1
    left join (select distinct dates,user_id,`渠道` from uv_1) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    group by t1.order_date,cube(t1.mdd,t1.user_type,coalesce(t2.`渠道`,'null'))
)


,result as (
select t1.dates   `日期`
        ,t1.mdd
        ,t1.user_type  `新老客`
        ,t1.`渠道`
        ,nvl(t1.`UV`, 0)   as UV
        ,COALESCE(t2.`间夜量`, 0)     as `间夜量`
        ,COALESCE(t2.`订单量`, 0)      as `订单量`
        ,COALESCE(t2.`下单用户量`, 0)      as `下单用户量`
        ,concat(round(COALESCE(t1.`UV` / t3.`UV`, 0) * 100, 1), '%')   as `流量占比`
        ,concat(round(COALESCE(t2.`订单量` / t1.`UV`, 0) * 100, 1), '%')  as `CR`
        ,concat(round(COALESCE(t2.`Q_用券订单量`, 0) / nvl(t2.`订单量`, 0) * 100, 1), '%') as `用券订单占比`
        ,COALESCE(t2.`GMV`, 0)      as `GMV`
        ,COALESCE(t2.`Q_高星间夜量`, 0)      as `Q_高星间夜量`
        ,COALESCE(t2.`Q_中星间夜量`, 0)      as `Q_中星间夜量`
        ,COALESCE(t2.`Q_低星间夜量`, 0)      as `Q_低星间夜量`
from q_uv_info t1 
left join order_info t2 on t1.dates=t2.order_date and t1.mdd=t2.mdd 
        and t1.user_type=t2.user_type and t1.`渠道`=t2.`渠道`
left join (  --- 计算流量占比
    select dates,mdd,user_type,`渠道`,`UV`
    from q_uv_info 
    where user_type = 'ALL' and `渠道` = 'ALL'
) t3 on t1.dates=t3.dates and t1.mdd=t3.mdd 
)


select t1.`日期`,t1.`渠道`,`流量占比`,UV,`CR`,`用券订单占比`,`订单量`,`间夜量`
        ,`新客UV`,`新客流量占比`,`新客CR`,`新客用券订单占比`,`新客订单量`,`新客间夜量`
from 
(
    select `日期`,`渠道`,UV,`CR`,`用券订单占比`,`订单量`,`间夜量`,`流量占比`
    from result
    where  mdd = 'ALL'
    and `新老客` = 'ALL'
    and `渠道` != 'ALL'
)t1 left join (
    select `日期`,`渠道`
            ,UV as `新客uv`
            ,concat(round(UV / sum(UV) over(partition by `日期`) * 100,2), '%') `新客流量占比`
            ,`CR` as `新客CR`
            ,`用券订单占比` as `新客用券订单占比`
            ,`订单量` as `新客订单量`
            ,`间夜量` as  `新客间夜量`
    from result
    where  mdd = 'ALL'
    and `新老客` = '新客'
    and `渠道` != 'ALL'
)t2 on t1.`日期`=t2.`日期` and t1.`渠道`=t2.`渠道`
;

----------

----------
---- 流量渠道X目的地X新老
with user_type -----新老客
as (
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
,uv as ---D页离店日期在国庆期间
(
    select distinct dt as `日期`
        , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        , case when dt > b.min_order_date then '老客' else '新客' end as user_type
        , a.user_id,user_name
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-08-20'
        and checkout_date between '2025-10-01' and '2025-10-08'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
)
,user_jc as --机酒交叉
(
    select distinct `日期`
                   , mdd
                   , uv.user_name
                   , '机酒交叉'      as `渠道`
                   , 0              as user_number
     from uv uv
     left join(
        select to_date(create_time)    as create_date
                , o_qunarusername
                , biz_order_no         as flight_order_no
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2025-01-01'
            and substr(create_time, 1, 10) >= '2025-01-01'  -- 生单时间
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
        ) flight
     on uv.user_name = flight.o_qunarusername
     where flight.create_date >= date_sub(uv.`日期`, 15)
        and flight.create_date <= uv.`日期`
        and flight_order_no is not null
)
,user_xhs as --小红书 宽口径
(
    select distinct uv.`日期`
                   , mdd
                   , uv.user_name
                   , '小红书' as `渠道`
                   , 1  as user_number
    from uv uv
    left join(
        select distinct flow_dt,
                user_name
        from pp_pub.dwd_redbook_global_flow_detail_di
        where dt >= '2022-12-01'
         --   and business_type = 'hotel-inter'
            and query_platform = 'redbook') red
    on uv.user_name = red.user_name
    where red.flow_dt >= date_sub(`日期`, 7)
       and red.flow_dt <= uv.`日期`
       and red.user_name is not null
)
,user_nr as   --- 内容交叉
(
    select distinct concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) `日期`
            , uv.user_name
            , uv.mdd
            , '内容交叉' as  `渠道`
            , 2         as   user_number
    from (--酒店帖
            select distinct global_key
                         , poi_id
                         , poi_type
                         , city_name
            from c_desert_feed.dw_feedstream_qulang_detail_info
            where dt = '%(DATE)s' and status = 0
        ) a
    join (
            select city_type,city_name
            from c_desert_feed.dim_content_city_derived_type_da
            where dt = '%(FORMAT_DATE)s' and city_type = 2
        ) w on a.city_name = w.city_name
    --AB级
    join (
            select distinct global_key, tag_id
            from c_desert_feed.ods_feedstream_qulang_footprint_detail_level_tags
            where dt = '%(DATE)s'
                and tag_id in ('857', '860')
                and status = 0
        ) c on a.global_key = c.global_key
    left join (
            select distinct global_key
            from c_desert_feed.ods_feedstream_qulang_content_goods_relate_info
            where dt = '%(DATE)s' and goods_type = 7
        ) e on a.global_key = e.global_key
    --曝光表
    left join (
            select dt,user_id,global_key,request_id,is_clicked
            from c_desert_feed.dw_feedstream_erping_list_show
            where dt >= '20250501'
                  and dt <= '%(DATE)s'
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name 
    and concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) = uv.`日期`
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as --暑期活动
(
    select distinct uv.`日期`
                   , uv.mdd
                   , uv.user_name
                   , '营销活动' `渠道`
                   , 3 as       user_number
    from uv uv
    left join (
        select distinct substr(log_time, 1, 10) as `日期`
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
    where d.`日期` >= date_sub(uv.`日期`, 7)
       and d.`日期` <= uv.`日期`
       and d.user_name is not null
)
,user_gnjd as ----国内酒店
(
    select distinct `日期`
                   , uv.mdd
                   , uv.user_name
                   , '国内交叉' as `渠道`
                   , 4          as user_number
    from uv uv
    left join (
        select distinct user_id,
                 order_date
        from hotel.ads_ord_user_da_2inl
        where dt = date_sub(current_date, 1)
        and order_date >= '2022-11-01'
        ) g  on uv.user_id = g.user_id
    where g.order_date >= date_sub(uv.`日期`, 365)
       and g.order_date <= uv.`日期`
       and g.user_id is not null
)
,user_channel  as ---流量来源渠道整理 
(
    select distinct `日期`
            , mdd
            , user_name
            , `渠道`
    from (
        select `日期`,
                mdd,
                user_name,
                `渠道`,
                row_number() over (partition by `日期`,user_name order by user_number) as user_level
        from (
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_jc
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_xhs
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_nr
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_hd
            union all
            select `日期`, mdd, user_name, `渠道`, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as ----多维度活跃用户汇总
(
    select distinct a.`日期`                                                    as dates
            ,a.user_type
            ,a.mdd
            ,nvl(d.`渠道`, '自然流量')                                        as `渠道`
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.`日期` = d.`日期`
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
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-08-20' and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
        and checkout_date between '2025-10-01' and '2025-10-08'   --- 离店日期在国庆

)

,q_uv_info as
(   ---- 流量汇总
    select dates
            ,`渠道`
            ,count(user_id)   `UV`
            ,count(case when user_type = '新客' then  user_id end) `新客UV`
    from uv_1
    where mdd = '日本'
    group by 1,2
) 


,order_info as ---- 订单汇总
(
    select t1.order_date
          ,coalesce(t2.`渠道`,'null') as  `渠道`
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
    left join (select distinct dates,user_id,`渠道` from uv_1) t2 on t1.user_id=t2.user_id and t1.order_date=t2.dates
    where mdd = '日本'
    group by 1,2
)


select t1.`日期`,t1.`渠道`,`流量占比`,UV
        ,concat(round(nvl(`订单量` / UV,0) * 100,2),'%') as `CR`
        ,concat(round(nvl(`Q_用券订单量`/ `订单量` ,0) * 100,2),'%') as `用券订单占比`
        ,`订单量`,`间夜量`
        ,`新客UV`,`新客流量占比`
        ,concat(round(nvl(`新客订单量` / `新客UV`,0) * 100,2),'%') as `新客CR`
        ,concat(round(nvl(`Q_新客用券订单量`  / `新客订单量`,0) * 100,2),'%') as `新客用券订单占比`
        ,`新客订单量`
        ,`新客间夜量`
from 
(
    select dates as `日期`
            ,`渠道`
            ,UV
            ,`新客UV`
            ,concat(round(UV / sum(UV) over(partition by dates) * 100,2), '%') `流量占比`
            ,concat(round(`新客UV` / sum(`新客UV`) over(partition by dates) * 100,2), '%') `新客流量占比`
    from q_uv_info
)t1 left join (
    select order_date as `日期`
            ,`渠道`
            ,`Q_用券订单量` 
            ,`订单量` 
            ,`间夜量` 
            ,`新客间夜量` 
            ,`新客订单量` 
            ,`Q_新客用券订单量` 
    from order_info
)t2 on t1.`日期`=t2.`日期` and t1.`渠道`=t2.`渠道`
order by  t1.`渠道`,t1.`日期` desc
;

