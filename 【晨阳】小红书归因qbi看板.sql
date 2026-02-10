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
,uv as ----分日去重活跃用户
(
    select distinct dt as `日期`
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-07-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
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
        where dt >= '2024-12-01'
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
            where dt >= '20250701'
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
        where dt >= date_sub(current_date, 170)
            and dt <= date_sub(current_date, 1) --日期
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                        ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 170)
            and dt <= date_sub(current_date, 1)
        union
        select distinct dt
                        , username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 170)
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
,uv_1 as ----多维度活跃用户汇总明细
(
    select distinct a.`日期`                                                    as dates
            ,a.user_type
            ,a.mdd
            ,nvl(d.`渠道`, '自然流量')                                        as `渠道`
            -- ,a.user_id
            ,a.user_name
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.`日期` = d.`日期`
)

,q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,order_no,room_night,user_name
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
        and order_date >= '2025-07-01' and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
)

,read_uv as (  --- 每日小红书引流用户-首次归因业务
  select flow_dt
          ,user_name
          ,business_name
  from (
      select  flow_dt
              ,user_name
              ,business_name
              ,log_time
              ,row_number() over(partition by flow_dt,user_name order by log_time) rn
      from pp_pub.dwd_redbook_global_flow_detail_di
      where dt >= date_sub('2025-07-01', 8)
        --   and business_type = 'hotel-inter'
          and query_platform = 'redbook'
  ) t where rn = 1
)
,xhs_channel as ( --- 每日小红书引流用户-近7天引流中首次归因业务
  select flow_dt,user_name
          ,case when business_name = '酒店' then '酒店'
                when business_name = '机票' then '机票'
                when business_name = '国际酒店' then '国际酒店'
                when business_name = '国际机票' then '国际机票'
                when business_name = '门票' then '门票'
                else '其他' end business_name
          ,row_number() over(partition by user_name order by flow_dt desc) rn
  from (
      select 
          t1.flow_dt
          ,t1.user_name
          ,t1.business_name
          ,t2.flow_dt as dt
          ,row_number() over(partition by t1.flow_dt, t1.user_name order by t2.flow_dt) rn 
      from read_uv t1 
      left join read_uv t2 on t1.user_name = t2.user_name
      and  t2.flow_dt >= date_sub(t1.flow_dt, 7)
      and  t2.flow_dt <= t1.flow_dt
  ) t where rn = 1
)



select t1.`日期`
      ,t1.business_name
      ,uv `引流uv`
      ,concat(round(uv / sum(uv) over(partition by t1.`日期`) * 100,2), '%') `引流uv占比`
      ,`新客订单uv`
      ,concat(round(`新客订单uv` / sum(`新客订单uv`) over(partition by t1.`日期`) * 100,2),'%')  `新客订单uv占比`
      ,`新客订单量`
      ,concat(round(`新客订单量` / sum(`新客订单量`) over(partition by t1.`日期`) * 100,2),'%')  `新客订单量占比`
      ,`订单uv`
      ,concat(round(`订单uv` / sum(`订单uv`) over(partition by t1.`日期`) * 100,2),'%')  `订单uv占比`
      ,`订单量`
      ,concat(round(`订单量` / sum(`订单量`) over(partition by t1.`日期`) * 100,2),'%')  `订单量占比`
from (
    select t1.dates `日期`
        ,business_name
        ,count(t1.user_name) uv
    from (
        select  dates,user_name from uv_1 t1 
        where `渠道` = '小红书'
    )t1 
    left join (select user_name,business_name from xhs_channel where rn =1) t2 on t1.user_name = t2.user_name
    group by 1,2
) t1 
left join (
    select  order_date,business_name
            ,count(distinct t1.user_name)  `订单uv`
            ,count(t1.order_no) `订单量`
            ,count(distinct case when user_type = '新客' then t1.user_name end) `新客订单uv`
            ,count(case when user_type = '新客' then t1.order_no end) `新客订单量`
    from q_order t1 
    left join (select distinct dates,user_name,`渠道` from uv_1 where `渠道` = '小红书') t2 
    on t1.user_name=t2.user_name and t1.order_date=t2.dates
    left join (select user_name,business_name from xhs_channel where rn =1) t3 on t1.user_name = t3.user_name
    where t2.user_name is not null
    group by 1,2
) t2 on t1.`日期`=t2.order_date and t1.business_name=t2.business_name
order by t1.`日期` desc
;




