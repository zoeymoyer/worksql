with h_uv as (--- H页流量
    -- select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) dt
    --       ,user_name
    -- from default.dw_qav_ihotel_track_info_di
    -- where dt >= '%(DATE_14)s' and dt <= '%(DATE)s'
    -- and key = 'ihotel/home/preload/monitor/homePreFetch'
    -- group by 1,2

    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) dt
          ,user_name
    from default.dw_qav_ihotel_track_info_di
    where dt >= '%(DATE_30)s' and dt <= '%(DATE)s'
    and key = 'ihotel/home/preload/monitor/homePreFetch'
    group by 1,2
)
,reserved as (--- 降价提醒用户
    select date(t.update_time) as dt
          ,t.user_name
    from ihotel_default.ods_hotel_intl_greway_hotel_room_price_reduction_tips_hi t
    where t.dt>=date_sub(current_date, 40) and t.dt<=date_sub(current_date, 1)
    group by 1,2
)
,d_uv as (--- D页流量用户
    select dt
        ,user_name
        ,hotel_seq
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    where dt >= date_sub(current_date, 40) and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3
)
,collect_uv as (--- 点击收藏用户 
    select concat(substr(dt, 1, 4),'-',substr(dt, 5, 2),'-',substr(dt, 7, 2)) AS dt,
         user_name
    from default.dw_qav_hotel_track_info_di  -- 旧埋点表
    --- ihotel_default.dw_qav_hotel_track_info_di 旧埋点表
    --- default.dw_qav_ihotel_track_info_di 新埋点表
    where  dt between '20260331' AND '20260505'
      and key = 'ihotel/GDetail/header/click/collect'
    group by 1,2
)
,q_order_app as (----订单明细表 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,init_gmv,order_no,room_night,user_name
            ,init_commission_after
            ,case when batch_series in ('se50ljcpl_MJ_d7bcd9','ser50lj_MJ_fb6e56','zmlztoast_ZK_c60a0f','xjthhwtoas_ZK_81e53a','YXwylianzh_ZK_663947','dasou95zhe_ZK_d7962f') then 'Y' else 'N' end as is_zc
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

-- =========================================================================
-- 1. 分别构建近7天用户标签 (隔离 Range Join 以提升运行效率)
-- =========================================================================
,user_r_flag as (
    select h.dt, h.user_name
    from h_uv h
    join reserved r on h.user_name = r.user_name 
    where r.dt >= date_sub(h.dt, 7) and r.dt <= h.dt
    group by 1,2
)
,user_c_flag as (
    select h.dt, h.user_name
    from h_uv h
    join collect_uv c on h.user_name = c.user_name 
    where c.dt >= date_sub(h.dt, 7) and c.dt <= h.dt
    group by 1,2
)
,user_d_flag as (
    select h.dt, h.user_name
    from h_uv h
    join d_uv d on h.user_name = d.user_name 
    where d.dt >= date_sub(h.dt, 7) and d.dt <= h.dt
    group by 1,2
    having count(distinct d.hotel_seq) >= 2
)
-- =========================================================================
-- 2. 汇总H页用户宽表
-- =========================================================================
, h_uv_flags as (
    select h.dt
         ,h.user_name
         ,case when r.user_name is not null then 1 else 0 end as is_reserved
         ,case when c.user_name is not null then 1 else 0 end as is_collect
         ,case when d.user_name is not null then 1 else 0 end as is_d_page
         ,case when r.user_name is not null or c.user_name is not null or d.user_name is not null then 1 else 0 end as is_any_3
    from h_uv h
    left join user_r_flag r on h.dt = r.dt and h.user_name = r.user_name
    left join user_c_flag c on h.dt = c.dt and h.user_name = c.user_name
    left join user_d_flag d on h.dt = d.dt and h.user_name = d.user_name
)
-- =========================================================================
-- 3. 统计当日订单 (按用户去重计算订单量)
-- =========================================================================
, order_agg as (
    select order_date
         , user_name
         , count(distinct order_no) as orders
         , count(distinct case when is_zc = 'Y' then order_no end) as zc_orders
    from q_order_app
    group by 1,2
)
-- =========================================================================
-- 4. 融合流量与订单宽表
-- =========================================================================
, final_user_daily as (
    select f.dt
         , f.user_name
         , f.is_reserved
         , f.is_collect
         , f.is_d_page
         , f.is_any_3
         , nvl(o.orders, 0) as user_orders
         , nvl(o.zc_orders, 0) as zc_orders
    from h_uv_flags f
    left join order_agg o on f.dt = o.order_date and f.user_name = o.user_name
)
-- =========================================================================
-- 5. 最终指标计算输出
-- =========================================================================
select 
      dt as `日期`
    , count(distinct user_name) as `H页UV`
    
    , sum(is_reserved) as `订阅降价提醒UV`
    , concat(round(sum(is_reserved) / count(distinct user_name) * 100, 2), '%') as `订阅降价提醒UV占比`
    
    , sum(is_collect) as `点击收藏UV`
    , concat(round(sum(is_collect) / count(distinct user_name) * 100, 2), '%') as `点击收藏UV占比`
    
    , sum(is_d_page) as `浏览D页UV`
    , concat(round(sum(is_d_page) / count(distinct user_name) * 100, 2), '%') as `浏览D页UV占比`
    
    , sum(is_any_3) as `三类总去重UV`
    , concat(round(sum(is_any_3) / count(distinct user_name) * 100, 2), '%') as `三类总去重UV占比`
    
    , sum(user_orders) as `订单量`
    , sum(case when is_reserved = 1 then user_orders else 0 end) as `订阅降价提醒订单量`
    , sum(case when is_collect = 1 then user_orders else 0 end) as `点击收藏订单量`
    , sum(case when is_d_page = 1 then user_orders else 0 end) as `浏览D页订单量`

    , sum(zc_orders) as `追C订单量`
    , sum(case when is_reserved = 1 then zc_orders else 0 end) as `订阅降价提醒追C订单量`
    , sum(case when is_collect = 1 then zc_orders else 0 end) as `点击收藏追C订单量`
    , sum(case when is_d_page = 1 then zc_orders else 0 end) as `浏览D页追C订单量`
    
    , concat(round(sum(case when is_reserved = 1 then user_orders else 0 end) / nullif(sum(is_reserved), 0) * 100, 2), '%') as `订阅降价提醒CR`
    , concat(round(sum(case when is_collect = 1 then user_orders else 0 end) / nullif(sum(is_collect), 0) * 100, 2), '%') as `点击收藏CR`
    , concat(round(sum(case when is_d_page = 1 then user_orders else 0 end) / nullif(sum(is_d_page), 0) * 100, 2), '%') as `浏览D页CR`
from final_user_daily
group by dt
order by dt desc;






select q.order_date as `预定日期`
    , coalesce(q.belong_city_name,c.city_name) as `城市`
    , `Q订单`
    , `Q间夜`
    , `Qgmv`
    , `Q佣金`
    , `C订单`
    , `C间夜量`
    , `Cgmv`
    , `C佣金`


from
    (select order_date
        , belong_city_name
        , count(distinct order_no) as `Q订单`
        , sum(room_night) as `Q间夜`
        , sum(init_gmv) as `Qgmv`
        , sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else init_commission_after+coalesce(ext_plat_certificate,0) end) as `Q佣金`
    from mdw_order_v3_international a
    where dt = '20260505'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
        and terminal_channel_type = 'app'
        -- and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
        and order_date >= '2026-01-01'
        and a.order_no <> '103576132435'
        and country_name = '马来西亚'
    group by 1,2
    ) q

left join
    (select substr(order_date,1,10) as order_date
        , extend_info['CITY'] as city_name
        , count(distinct order_no) as `C订单`
        , sum(extend_info['room_night'] ) as `C间夜量`
        , sum(room_fee)as `Cgmv`
        , sum(comission) as `C佣金`

    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = '2026-05-05'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(order_date,1,10) >= '2026-01-01'
        and extend_info['COUNTRY'] = '马来西亚'
    group by 1,2
    ) c
on q.order_date = c.order_date
and q.belong_city_name = c.city_name


with uv as (---分日去重活跃用户
    select  dt 
            ,case  
                when city_name = '吉隆坡' then '吉隆坡'
                when city_name = '兰卡威' then '兰卡威'
                when city_name = '新山' then '新山'
                when city_name = '雪邦' then '雪邦'
                when city_name in ('仙本那', '斗湖省') then '仙本那' 
                
                when city_name = '西海岸省' then '亚庇'
                when city_name = '东北县' then '乔治市'
                when city_name = '八打灵县' then '八打灵再也'
                when city_name in ('中央县', '马六甲中央县') then '马六甲'
                else '其他'
            end as city_name
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-04-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and a.country_name = '马来西亚'
    group by 1,2,3
)
,c_uv as
(   --- C 流量 目的地加和
    select dt ,case when cityname in ('吉隆坡', '兰卡威', '新山', '雪邦', '仙本那', '亚庇', '乔治市', '八打灵再也', '马六甲') then cityname
                    else '其他' end as cityname
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
        and  dt>= '2026-04-01' and dt<= date_sub(current_date, 1)
        and a.countryname = '马来西亚'
    group by 1,2
)

select t1.dt
        ,coalesce(t1.city_name, t2.cityname) as city_name
        ,uv as q_uv
        ,c_uv as c_uv
        ,uv / nullif(c_uv, 0) as uv_qc
from (
    select dt
        ,city_name
        ,count(distinct user_name) as uv
    from uv
    group by 1,2
) t1 
left join c_uv t2 on t1.dt = t2.dt and t1.city_name = t2.cityname
order by dt,q_uv desc
;