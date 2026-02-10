---- 流量渠道X目的地
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
,uv as ----分日去重活跃用户
(
    select distinct dt as `日期`
            ,case when province_name in ('澳门','香港','台湾') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,user_id
            ,user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     where dt >= date_sub(current_date, 125)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and user_name is not null and user_name not in ('null', 'NULL', '', ' ')
       and user_id is not null and user_id not in ('null', 'NULL', '', ' ')
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
        where dt >= '2024-01-01'
            and substr(create_time, 1, 10) >= '2024-01-01'
            and ticket_time is not null
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
                   , 1        as user_number
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
        where dt >= date_sub(current_date, 125)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 125)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 125)
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
    select distinct a.dt                                                    as dates
            ,case when a.dt > b.min_order_date then '老客' else '新客' end   as user_type
            ,d.mdd
            ,nvl(d.`渠道`, '自然流量')                                        as `流量来源`
            ,a.user_id
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join user_type b on a.user_id = b.user_id
    left join user_channel d on a.user_name = d.user_name and a.dt = d.`日期`
    where dt >= date_sub(current_date, 125)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,uv_2 as ----辅助订单表用户划分维度
(
    select distinct dates
            , mdd
            , user_id
            , `流量来源`
     from uv_1
)
,order_info as ----订单表 包含取消
(
    select order_date
          , nvl(u.`流量来源`, 'null')                                                              `流量来源`
          , u.mdd
          , case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type
          , count(distinct order_no)                                                            as `订单量`
          , count(distinct case
                            when coupon_id is not null and batch_series not in
                                                            ('MacaoDisco_ZK_5e27de', '2night_ZK_952825',
                                                            '3night_ZK_ad8c83') and
                                batch_series not like '%23base_ZK_728810%' and
                                batch_series not like '%23extra_ZK_ce6f99%' then order_no
                            else null end)                                                    as `Q_用券订单量`
--           , count(distinct case
--                                when coupon_id is not null and batch_series not in
--                                                               ('MacaoDisco_ZK_5e27de', '2night_ZK_952825',
--                                                                '3night_ZK_ad8c83') and
--                                     batch_series not like '%23base_ZK_728810%' and
--                                     batch_series not like '%23extra_ZK_ce6f99%' then order_no else null end) as `新客用券订单量`
--           , count(distinct  order_no) as `订单量`
          , count(distinct a.user_id)                                                           as `下单用户量`
          , sum(room_night)                                                                     as `间夜量`
    from mdw_order_v3_international a
    left join uv_2 u on a.user_id = u.user_id and a.order_date = u.dates
    left join user_type b on a.user_id = b.user_id
    where a.dt = '%(DATE)s'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and terminal_channel_type = 'app'
       -- and terminal_channel_type in ('www','app','touch')
       and is_valid = '1'
       and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
       and (first_rejected_time is null or date(first_rejected_time) > order_date)
       and (refund_time is null or date(refund_time) > order_date)
       and order_date >= date_sub(current_date, 125)
       and order_date <= date_sub(current_date, 1)
     group by 1, 2, 3,4 
)


select `日期`,
       `渠道`,
       mdd,
       `新老客`,
       `流量占比`,
       UV,
       `CR`,
       `用券订单占比`,
       `订单量`,
       `间夜量`
from (
    select u.dates           `日期`
           , u.user_type     `新老客`
           , u.`流量来源`     `渠道`
           , u.mdd
           , concat(round(nvl(u.`UV` / b.uv, 0) * 100, 1), '%')                            as `流量占比`
           , nvl(u.`UV`, 0)                                                                as UV
           , concat(round(nvl(o.`订单量` / u.`UV`, 0) * 100, 1), '%')                         as `CR`
           , concat(round(nvl(o.`Q_用券订单量`, 0) / nvl(o.`订单量`, 0) * 100, 1), '%')       as `用券订单占比`
           , nvl(o.`订单量`, 0)                                                               as `订单量`
           , nvl(o.`间夜量`, 0)                                                               as `间夜量`
    from (
        select dates, `流量来源`,mdd ,user_type, sum(`UV`) `UV`
        from (
            select dates
                ,mdd
                ,`流量来源`
                ,user_type
                ,count(distinct user_id)   `UV`
--              ,count(distinct case when user_type = '新客' then user_id else null end) `新客UV`
            from uv_1 u
            group by 1, 2, 3, 4
        ) a
         group by 1, 2,3,4 
    ) u
    left join order_info o on u.dates = o.order_date and u.`流量来源` = o.`流量来源` and u.user_type = o.user_type
    and u.mdd  = o.mdd
    left join (
        select dates
                ,mdd,user_type
                ,count(distinct user_id)        uv
        from uv_1 u
        group by 1, 2,3
    ) b on u.dates = b.dates and u.user_type = b.user_type and u.mdd = b.mdd
) a
order by `渠道`,mdd, `日期` desc;