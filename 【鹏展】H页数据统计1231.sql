with user_type as (-----新老客
    select user_name,user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,user_profile as (
    select  user_id,
            user_name,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month,level_desc
    from pub.dim_user_profile_nd
)
,q_order as (----订单明细表包含取消  分目的地、新老维度 
    select checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        -- and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,s_uv as (--- L页流量
    select  dt 
            ,a.user_name
            ,count(1) search_pv
    from ihotel_default.dw_user_app_log_search_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2025-12-01'
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2
)
,h_uv as ( --- H页流量
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,a.user_name
        ,case when CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) > b.min_order_date then '老客' else '新客' end as user_type
        ,count(distinct get_json_object(value,'$.ext.cityUrl')) city_num
    from default.dw_qav_ihotel_track_info_di a
    left join user_type b on a.user_name = b.user_name 
    where dt >= '20251201' and dt <= '%(DATE)s'
        and key in ('ihotel/home/preload/monitor/homePreFetch'      --- H页曝光
        )
    group by 1,2,3  
)
,h_track_info as (
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
            ,user_name
            ,key
            ,value
    from default.dw_qav_hotel_track_info_di
    where dt >= '20251201' and dt <= '%(DATE)s'
        and key in ('hotel/home/searchCard/click/tabClick'        --- 搜索框海外 tab 点击
                ,'hotel/home/searchCard/click/searchTimeClick'    --- 入离日期点击
                ,'hotel/home/searchCard/click/cityClick'          --- 城市选择点击
                ,'hotel/home/searchCard/click/quickFilter'        --- 关键词点击
                ,'hotel/home/searchCard/click/searchClick'        --- 开始搜索点击
                ,'hotel/home/bottomEntrance/click/entrance'       --- 金刚位点击
                ,'hotel/global/home/recommendHotelInter/click'    --- H 页-精选频道 点击   hoteName index
                ,'hotel/global/home/recommendCityInter/click'     --- H 页-热门目的地 点击
                ,'hotel/global/home/recommendCityInter/show'      --- H 页-热门目的地 曝光
                ,'hotel/global/travelList/click'                  --- H 页-达人精选 点击
        )
)


select t1.dt
       ,h_uv_all `H页UV`
       ,l_uv_all `L页UV`
       ,h2l

       ,`搜索框海外tab点击UV`
       ,`入离日期点击UV`
       ,`城市选择点击UV`
       ,`关键词点击UV`
       ,`开始搜索点击UV`
       ,`精搜UV`
       ,`空搜UV`
       ,`金刚位点击UV`
       ,`金刚位优惠中心点击UV`
       ,`金刚位会员中心点击UV`
       ,`金刚位收藏/足迹点击UV`
       ,`金刚位酒店订单点击UV`
       ,`精选频道点击UV`
       ,`低价严选点击UV`
       ,`榜单点击UV`
       ,`华人优选点击UV`
       ,`签到点击UV`
       ,`酒店左点击UV`
       ,`酒店右点击UV`
       ,`热门目的地曝光UV`
       ,`热门目的地点击UV`
       ,`目的地1点击UV`
       ,`目的地2点击UV`
       ,`目的地3点击UV`
       ,`目的地4点击UV`
       ,`目的地5点击UV`
       ,`热门目的地曝光UV` / h_uv_all `H页二屏曝光率`
       ,`达人精选点击UV`

       ,h_new_uv_all  `H页新客uv`
       ,h_new_uv_rate `H页新客占比`
       ,`大众会员UV`
       ,`白银会员UV`
       ,`黄金会员UV`
       ,`铂金会员UV`
       ,`钻石会员UV`
       ,`有订单用户数`
       ,`有订单用户数` / h_uv_all `H页有订单用户占比`
       ,city_num / h_uv_all `用户平均切换目的地次数`
from (
    select t1.dt    
        ,count(distinct t1.user_name) h_uv_all   ---H页UV
        ,count(distinct t2.user_name) l_uv_all   ---L页UV
        ,count(distinct case when user_type='新客' then t1.user_name end) h_new_uv_all  ---H页新客UV
        ,count(distinct case when user_type='新客' then t1.user_name end) / count(distinct t1.user_name) h_new_uv_rate  ---H页新客UV占比
        ,count(distinct t2.user_name) / count(distinct t1.user_name) h2l  --- h2l
        ,sum(city_num) city_num  ---切换目的地次数
        ,count(distinct case when level_desc='大众' then t1.user_name end) `大众会员UV`
        ,count(distinct case when level_desc='白银' then t1.user_name end) `白银会员UV`  
        ,count(distinct case when level_desc='黄金' then t1.user_name end) `黄金会员UV` 
        ,count(distinct case when level_desc='铂金' then t1.user_name end) `铂金会员UV`  
        ,count(distinct case when level_desc='钻石' then t1.user_name end) `钻石会员UV`    
    from h_uv t1
    left join s_uv t2 on t1.user_name=t2.user_name and t1.dt=t2.dt
    left join user_profile t3 on t1.user_name=t3.user_name
    group by 1
)t1
left join (--- 头部按钮点击
    select dt
          ,count(distinct case when key = 'hotel/home/searchCard/click/tabClick' then user_name end) `搜索框海外tab点击UV`
          ,count(distinct case when key = 'hotel/home/searchCard/click/searchTimeClick' then user_name end) `入离日期点击UV`
          ,count(distinct case when key = 'hotel/home/searchCard/click/cityClick' then user_name end) `城市选择点击UV`
          ,count(distinct case when key = 'hotel/home/searchCard/click/quickFilter' then user_name end) `关键词点击UV`
          ,count(distinct case when key = 'hotel/home/searchCard/click/searchClick' then user_name end) `开始搜索点击UV`

          ,count(distinct case when key = 'hotel/home/searchCard/click/searchClick' 
                       and get_json_object(value,'$.ext.keyword') is not null 
                       and get_json_object(value,'$.ext.keyword') != 'null'
                       then user_name end) `精搜UV`
          ,count(distinct case when key = 'hotel/home/searchCard/click/searchClick' 
                       and (get_json_object(value,'$.ext.keyword') is  null 
                       or get_json_object(value,'$.ext.keyword') = 'null')
                       then user_name end) `空搜UV`

    from h_track_info
    where get_json_object(value,'$.ext.searchType') = 'Overseas'
    group by 1
) t2  on t1.dt=t2.dt
left join (---- 金刚位点击
    select dt
          ,count(distinct case when key = 'hotel/home/bottomEntrance/click/entrance' then user_name end) `金刚位点击UV`
          ,count(distinct case when key = 'hotel/home/bottomEntrance/click/entrance' and get_json_object(value,'$.ext.bottomDesc')='优惠中心' then user_name end) `金刚位优惠中心点击UV`
          ,count(distinct case when key = 'hotel/home/bottomEntrance/click/entrance' and get_json_object(value,'$.ext.bottomDesc')='收藏/足迹' then user_name end) `金刚位会员中心点击UV`
          ,count(distinct case when key = 'hotel/home/bottomEntrance/click/entrance' and get_json_object(value,'$.ext.bottomDesc')='收藏/足迹' then user_name end) `金刚位收藏/足迹点击UV`
          ,count(distinct case when key = 'hotel/home/bottomEntrance/click/entrance' and get_json_object(value,'$.ext.bottomDesc')='酒店订单' then user_name end) `金刚位酒店订单点击UV`
    from h_track_info
    where get_json_object(value,'$.ext.isForeignCity') = 'true'
    group by 1
) t3 on t1.dt=t3.dt
left join (---  精选频道点击
    select dt
          ,count(distinct  user_name) `精选频道点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '0' and get_json_object(value,'$.hoteName') is null then user_name end) `低价严选点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '0' and get_json_object(value,'$.hoteName') = '榜单' then user_name end) `榜单点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '1' and get_json_object(value,'$.hoteName') is null then user_name end) `华人优选点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '2' and get_json_object(value,'$.hoteName') is null then user_name end) `签到点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '1' and get_json_object(value,'$.hoteName') is not null then user_name end) `酒店左点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '2' and get_json_object(value,'$.hoteName') is not null then user_name end) `酒店右点击UV`

    from h_track_info
    where key = 'hotel/global/home/recommendHotelInter/click'
    group by 1
) t4 on t1.dt=t4.dt
left join (---  热门目的地点击
    select dt
          ,count(distinct  user_name) `热门目的地点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '0'  then user_name end) `目的地1点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '1'  then user_name end) `目的地2点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '2'  then user_name end) `目的地3点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '3'  then user_name end) `目的地4点击UV`
          ,count(distinct case when get_json_object(value,'$.index') = '4'  then user_name end) `目的地5点击UV`

    from h_track_info
    where key = 'hotel/global/home/recommendCityInter/click'
    group by 1
) t5 on t1.dt=t5.dt
left join (---  热门目的地点击
    select dt
          ,count(distinct  user_name) `热门目的地曝光UV`

    from h_track_info
    where key = 'hotel/global/home/recommendCityInter/show'
    group by 1
) t11 on t1.dt=t11.dt
left join (---  达人精选点击
    select dt
          ,count(distinct  user_name) `达人精选点击UV`
    from h_track_info
    where key = 'hotel/global/travelList/click'
    group by 1
) t6 on t1.dt=t6.dt
left join (---  访问H页是否有订单
    select dt
          ,count(distinct  t2.user_name) `有订单用户数`
    from h_uv t1
    left join q_order t2 on t1.user_name=t2.user_name and t2.checkout_date >= t1.dt
    group by 1
) t7 on t1.dt=t7.dt
order by 1
;




---- L页面来源
select  dt 
        ,get_json_object(action_entrance_map,'fromforlog') fromforlog
        ,count(distinct a.user_name) search_pv
from ihotel_default.dw_user_app_log_search_di_v1 a
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
where dt >= '2025-12-01'
and business_type = 'hotel'
and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
group by 1,2
;


--- 分渠道H页数据
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
,user_profile as (
    select  user_id,
            user_name,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month,level_desc
    from pub.dim_user_profile_nd
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,c.user_id
            ,a.user_name
     from ihotel_default.dw_user_app_log_home_visit_hi_v1 a
     left join user_profile c on a.user_name=c.user_name
     left join user_type b on a.user_name = b.user_name 
     where dt >= date_sub(current_date, 15)
       and dt <= date_sub(current_date, 1)
)
,user_jc as --机酒交叉
(
    select distinct dt
                   , uv.user_name
                   , '机酒交叉'      as channel
                   , 0              as user_number
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
)
,user_xhs as --小红书 宽口径
(
    select distinct uv.dt
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
,user_nr as   --- 内容交叉
(
    select distinct concat(substr(d.dt, 1, 4), '-', substr(d.dt, 5, 2), '-', substr(d.dt, 7, 2)) dt
            , uv.user_name
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
            where dt >= from_unixtime(unix_timestamp() - 86400 * 30, 'yyyyMMdd')
                and dt <= from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        ) d on a.global_key = d.global_key
    left join uv on d.user_id = uv.user_name  and d.dt = replace(uv.dt,'-','')
    where e.global_key is not null
          and is_clicked = 1
)
,user_hd as --暑期活动
(
    select distinct uv.dt
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
,user_gnjd as ----国内酒店
(
    select distinct dt
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
            , user_name
            , channel
    from (
        select dt,
                user_name,
                channel,
                row_number() over (partition by dt,user_name order by user_number) as user_level
        from (
            select dt,  user_name, channel, user_number
            from user_jc
            union all
            select dt,  user_name, channel, user_number
            from user_xhs
            union all
            select dt, user_name, channel, user_number
            from user_nr
            union all
            select dt,  user_name, channel, user_number
            from user_hd
            union all
            select dt,  user_name, channel, user_number
            from user_gnjd
        ) t
    ) tt
    where user_level = 1
)
,uv_1 as ----多维度活跃用户汇总
(
    select distinct a.dt     as dates
            ,a.user_type
            ,coalesce(d.channel, '自然流量')    as channel
            ,a.user_id
    from uv a
    left join user_channel d on a.user_name = d.user_name and a.dt = d.dt
)
,uv_info as (
    select dates dt
            ,if(grouping(channel)=1,'ALL', channel) as  channel
            ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
            ,count(user_id)   uv
    from uv_1
    group by dates,cube(user_type , channel)
 )
 
 select t1.dt,t1.channel,t1.user_type,t1.uv,t1.uv / t2.uv `占比`
 from uv_info t1
 left join (
    select dt,uv,user_type 
    from  uv_info 
    where channel='ALL' 
) t2 on t1.dt=t2.dt  and t1.user_type=t2.user_type
 order by 1
 ;

 --- 访问H页用户历史待支付订单情况
 with q_order as (----订单明细表包含取消  分目的地、新老维度 
    select checkout_date,order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        --and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        -- and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,h_uv as ( --- H页流量
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,a.user_name
    from default.dw_qav_ihotel_track_info_di a
    where dt >= '20251201' and dt <= '%(DATE)s'
        and key in ('ihotel/home/preload/monitor/homePreFetch'      --- H页曝光
        )
    group by 1,2,3  
)


--- 复购行为
with q_order as (----订单明细表包含取消  分目的地、新老维度 
    select order_date
            ,a.user_name
            ,hotel_seq
            ,order_no

    from mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        --and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'

)
,h_uv as ( --- H页流量
    select CONCAT(SUBSTR(dt, 1, 4),'-',SUBSTR(dt, 5, 2),'-',SUBSTR(dt, 7, 2)) AS dt
        ,a.user_name
    from default.dw_qav_ihotel_track_info_di a
    where dt >= '20251201' and dt <= '%(DATE)s'
        and key in ('ihotel/home/preload/monitor/homePreFetch'      --- H页曝光
        )
    group by 1,2
)

select t1.order_date
        ,o_uv  `生单UV`
        ,o_fg_uv `复购生单UV`
        ,o_fg_h_uv `复购生单访问H页UV`
from (
    select order_date,count(distinct user_name) o_uv
    from q_order 
    where order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
    group by 1
) t1 
left join ( --- 同酒店复购用户
    select order_date
          ,count(distinct t1.user_name) o_fg_uv
          ,count(distinct t2.user_name) o_fg_h_uv
    from (
        select order_date
                ,user_name
                ,hotel_seq
                ,count(distinct order_no) o_cnt
        from q_order 
        group by 1,2,3
    ) t1
    left join h_uv t2 on t1.user_name=t2.user_name and t1.order_date=t2.dt
    where o_cnt >= 2
    group by 1
)t2 on t1.order_date=t2.order_date
order by  1 
;
