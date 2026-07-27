
--- 1、亲子酒店星级分布
with hotel_info as (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
        from ihotel_default.dim_hotel_info_intl_v3  a
        where a.dt = '%(DATE)s'
        group by 1,2,3
)
,family_hotel as (-- 亲子酒店 57662家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)

select hotel_grade
    ,count(distinct t1.hotel_seq) hotels
    ,sum(count(distinct t1.hotel_seq)) over() all_hotels
    ,count(distinct t1.hotel_seq) / sum(count(distinct t1.hotel_seq)) over() ratio
from family_hotel t1
left join hotel_info t2 on t1.hotel_seq = t2.hotel_seq
group by 1 order by 2 desc
;

--- 2、亲子榜单星级分布
with hotel_info as (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
        from ihotel_default.dim_hotel_info_intl_v3  a
        where a.dt = '%(DATE)s'
        group by 1,2,3
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)

select hotel_grade
    ,count(distinct t1.hotel_seq) hotels
    ,sum(count(distinct t1.hotel_seq)) over() all_hotels
    ,count(distinct t1.hotel_seq) / sum(count(distinct t1.hotel_seq)) over() ratio
from family_rank_hotel t1
left join hotel_info t2 on t1.hotel_seq = t2.hotel_seq
group by 1
;

--- 4、亲子酒店城市国家分布
with family_hotel as (-- 亲子酒店 57662家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id,hotel_seq
            ,city_name
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-05-26' and order_date <= '2026-06-24'
        and order_no <> '103576132435'
)


select mdd
    ,city_name
    ,sum(room_night) room_nights
    ,sum(case when t2.hotel_seq is not null then room_night else 0 end) family_room_nights
    ,sum(case when t3.hotel_seq is not null then room_night else 0 end) family_rank_room_nights
    ,sum(sum(room_night)) over() all_room_nights
    ,sum(sum(case when t2.hotel_seq is not null then room_night else 0 end) ) over() all_family_room_nights
    ,sum(sum(case when t3.hotel_seq is not null then room_night else 0 end)) over() all_family_rank_room_nights
    ,sum(room_night) / sum(sum(room_night)) over() family_ratio
    ,sum(case when t2.hotel_seq is not null then room_night else 0 end) / sum(sum(case when t2.hotel_seq is not null then room_night else 0 end) ) over() family_rank_ratio  
    ,sum(case when t3.hotel_seq is not null then room_night else 0 end) / sum(sum(case when t3.hotel_seq is not null then room_night else 0 end)) over() family_to_family_rank_ratio
from q_order t1
left join family_hotel t2 on t1.hotel_seq = t2.hotel_seq
left join family_rank_hotel t3 on t1.hotel_seq = t3.hotel_seq
group by 1,2 order by 3 desc
;

--- 3、亲子酒店ADR分布
with family_hotel as (-- 亲子酒店 57662家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id,hotel_seq
            ,case when init_gmv / room_night <= 200 then '0-200'
                  when init_gmv / room_night > 200 and init_gmv / room_night <= 400 then '200-400'
                  when init_gmv / room_night > 400 and init_gmv / room_night <= 600 then '400-600'
                  when init_gmv / room_night > 600 and init_gmv / room_night <= 800 then '600-800'
                  when init_gmv / room_night > 800 and init_gmv / room_night <= 1000 then '800-1000'
                  when init_gmv / room_night > 1000 and init_gmv / room_night <= 1200 then '1000-1200'
                  when init_gmv / room_night > 1200 and init_gmv / room_night <= 1400 then '1200-1400'
                  when init_gmv / room_night > 1400 and init_gmv / room_night <= 1600 then '1400-1600'
                  when init_gmv / room_night > 1600 and init_gmv / room_night <= 1800 then '1600-1800'
                  when init_gmv / room_night > 1800 and init_gmv / room_night <= 2000 then '1800-2000'
                  when init_gmv / room_night > 2000 then '2000+' end as adr_range
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-05-26' and order_date <= '2026-06-24'
        and order_no <> '103576132435'
)


select adr_range
    ,sum(room_night) room_nights
    ,sum(case when t2.hotel_seq is not null then room_night else 0 end) family_room_nights
    ,sum(case when t3.hotel_seq is not null then room_night else 0 end) family_rank_room_nights
    ,sum(sum(room_night)) over() all_room_nights
    ,sum(sum(case when t2.hotel_seq is not null then room_night else 0 end) ) over() all_family_room_nights
    ,sum(sum(case when t3.hotel_seq is not null then room_night else 0 end)) over() all_family_rank_room_nights
    ,sum(room_night) / sum(sum(room_night)) over() family_ratio
    ,sum(case when t2.hotel_seq is not null then room_night else 0 end) / sum(sum(case when t2.hotel_seq is not null then room_night else 0 end) ) over() family_rank_ratio  
    ,sum(case when t3.hotel_seq is not null then room_night else 0 end) / sum(sum(case when t3.hotel_seq is not null then room_night else 0 end)) over() family_to_family_rank_ratio
from q_order t1
left join family_hotel t2 on t1.hotel_seq = t2.hotel_seq
left join family_rank_hotel t3 on t1.hotel_seq = t3.hotel_seq
group by 1 order by 2 desc
;


    
--- 4、亲子酒店国家分布
with hotel_info as (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
            ,country_name --- 国家
            ,city_name --- 城市
        from ihotel_default.dim_hotel_info_intl_v3  a
        where a.dt = '%(DATE)s'
        group by 1,2,3,4,5
)
,family_hotel as (-- 亲子酒店 57662家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,hotel_type as (
    select hotel_seq
        ,'亲子酒店' as hotel_type
    from family_hotel
    union all
    select hotel_seq
        ,'亲子榜单酒店' as hotel_type
    from family_rank_hotel
)

select hotel_type
    ,country_name
    -- ,city_name
    ,count(distinct t1.hotel_seq) as hotels
    ,sum(count(distinct t1.hotel_seq)) over(partition by hotel_type) as all_hotels
    ,count(distinct t1.hotel_seq) / sum(count(distinct t1.hotel_seq)) over(partition by hotel_type) as ratio
from hotel_type t1
left join hotel_info t2 on t1.hotel_seq = t2.hotel_seq
group by 1,2
order by 1,4 desc
;



--- 5、分星级流量QC
with q_c_hotel_mapping as(--- 用于映射q和c的酒店
    select hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
    group by 1,2
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,family_hotel as (-- 亲子酒店 57521家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,hotel_info as (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
        from ihotel_default.dim_hotel_info_intl_v3  a
        where a.dt = '%(DATE)s'
        group by 1,2,3
)
,c_uv as(--- C流量
    select dt 
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,m.hotel_seq
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    left join q_c_hotel_mapping m on a.masterhotelid = m.partner_hotel_id
    where device_chl='app'
    and  dt>= '2026-05-26' and dt<= '2026-06-24'
    group by 1,2,3,4
)
,uv as (-- Q流量
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
            ,hotel_seq
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-05-26'
       and dt <= '2026-06-24'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,hotel_scope as (
    select hotel_seq
        ,'整体' as hotel_type
    from hotel_info
    union all
    select hotel_seq
        ,'亲子酒店' as hotel_type
    from family_hotel
    union all
    select hotel_seq
        ,'亲子榜单酒店' as hotel_type
    from family_rank_hotel
)
,q_order_90d as (-- Q酒店近90天平均ADR
    select a.hotel_seq
        ,sum(init_gmv) / sum(room_night) as avg_adr_90d
    from default.mdw_order_v3_international a
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub('2026-05-26', 90)
        and order_date < '2026-05-26'
        and order_no <> '103576132435'
    group by 1
)
,c_order_90d as (-- C酒店近90天平均ADR
    select m.hotel_seq
        ,sum(room_fee) / sum(extend_info['room_night']) as avg_adr_90d
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join q_c_hotel_mapping m on o.hotel_seq = m.partner_hotel_id
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(order_date,1,10) >= date_sub('2026-05-26', 90)
        and substr(order_date,1,10) < '2026-05-26'
    group by 1
)
,q_hotel_adr_range as (
    select hotel_seq
        ,case when avg_adr_90d <= 200 then '0-200'
              when avg_adr_90d > 200 and avg_adr_90d <= 400 then '200-400'
              when avg_adr_90d > 400 and avg_adr_90d <= 600 then '400-600'
              when avg_adr_90d > 600 and avg_adr_90d <= 800 then '600-800'
              when avg_adr_90d > 800 and avg_adr_90d <= 1000 then '800-1000'
              when avg_adr_90d > 1000 and avg_adr_90d <= 1200 then '1000-1200'
              when avg_adr_90d > 1200 and avg_adr_90d <= 1400 then '1200-1400'
              when avg_adr_90d > 1400 and avg_adr_90d <= 1600 then '1400-1600'
              when avg_adr_90d > 1600 and avg_adr_90d <= 1800 then '1600-1800'
              when avg_adr_90d > 1800 and avg_adr_90d <= 2000 then '1800-2000'
              when avg_adr_90d > 2000 then '2000+'
              else '无近90天订单' end as adr_range
    from q_order_90d
)
,c_hotel_adr_range as (
    select hotel_seq
        ,case when avg_adr_90d <= 200 then '0-200'
              when avg_adr_90d > 200 and avg_adr_90d <= 400 then '200-400'
              when avg_adr_90d > 400 and avg_adr_90d <= 600 then '400-600'
              when avg_adr_90d > 600 and avg_adr_90d <= 800 then '600-800'
              when avg_adr_90d > 800 and avg_adr_90d <= 1000 then '800-1000'
              when avg_adr_90d > 1000 and avg_adr_90d <= 1200 then '1000-1200'
              when avg_adr_90d > 1200 and avg_adr_90d <= 1400 then '1200-1400'
              when avg_adr_90d > 1400 and avg_adr_90d <= 1600 then '1400-1600'
              when avg_adr_90d > 1600 and avg_adr_90d <= 1800 then '1600-1800'
              when avg_adr_90d > 1800 and avg_adr_90d <= 2000 then '1800-2000'
              when avg_adr_90d > 2000 then '2000+'
              else '无近90天订单' end as adr_range
    from c_order_90d
)
,q_star_uv as (
    select hotel_type
        ,dim_value
        ,avg(q_dau) as q_dau
    from (
        select t1.dt
            ,t2.hotel_type
            ,coalesce(t3.hotel_grade, '未知') as dim_value
            ,count(distinct t1.user_id) as q_dau
        from uv t1
        join hotel_scope t2
            on t1.hotel_seq = t2.hotel_seq
        left join hotel_info t3
            on t1.hotel_seq = t3.hotel_seq
        group by 1,2,3
    ) a
    group by 1,2
)
,c_star_uv as (
    select hotel_type
        ,dim_value
        ,avg(c_dau) as c_dau
    from (
        select t1.dt
            ,t2.hotel_type
            ,coalesce(t3.hotel_grade, '未知') as dim_value
            ,count(distinct t1.uid) as c_dau
        from c_uv t1
        join hotel_scope t2
            on t1.hotel_seq = t2.hotel_seq
        left join hotel_info t3
            on t1.hotel_seq = t3.hotel_seq
        group by 1,2,3
    ) a
    group by 1,2
)
,q_adr_uv as (
    select hotel_type
        ,dim_value
        ,avg(q_dau) as q_dau
    from (
        select t1.dt
            ,t2.hotel_type
            ,coalesce(t3.adr_range, '无近90天订单') as dim_value
            ,count(distinct t1.user_id) as q_dau
        from uv t1
        join hotel_scope t2
            on t1.hotel_seq = t2.hotel_seq
        left join q_hotel_adr_range t3
            on t1.hotel_seq = t3.hotel_seq
        group by 1,2,3
    ) a
    group by 1,2
)
,c_adr_uv as (
    select hotel_type
        ,dim_value
        ,avg(c_dau) as c_dau
    from (
        select t1.dt
            ,t2.hotel_type
            ,coalesce(t3.adr_range, '无近90天订单') as dim_value
            ,count(distinct t1.uid) as c_dau
        from c_uv t1
        join hotel_scope t2
            on t1.hotel_seq = t2.hotel_seq
        left join c_hotel_adr_range t3
            on t1.hotel_seq = t3.hotel_seq
        group by 1,2,3
    ) a
    group by 1,2
)
select '星级分布' as dim_type
    ,coalesce(t1.hotel_type, t2.hotel_type) as hotel_type
    ,coalesce(t1.dim_value, t2.dim_value) as dim_value
    ,coalesce(t1.q_dau, 0) as q_dau
    ,coalesce(t2.c_dau, 0) as c_dau
    ,coalesce(t1.q_dau, 0) / coalesce(t2.c_dau, 0) as traffic_qc
from q_star_uv t1
full outer join c_star_uv t2
    on t1.hotel_type = t2.hotel_type
    and t1.dim_value = t2.dim_value

union all

select '90天平均ADR价格带分布' as dim_type
    ,coalesce(t1.hotel_type, t2.hotel_type) as hotel_type
    ,coalesce(t1.dim_value, t2.dim_value) as dim_value
    ,coalesce(t1.q_dau, 0) as q_dau
    ,coalesce(t2.c_dau, 0) as c_dau
    ,coalesce(t1.q_dau, 0) / coalesce(t2.c_dau, 0) as traffic_qc
from q_adr_uv t1
full outer join c_adr_uv t2
    on t1.hotel_type = t2.hotel_type
    and t1.dim_value = t2.dim_value
;



--- 6、亲子酒店分国家QC
with q_c_hotel_mapping as(--- 用于映射q和c的酒店
    select hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
    group by 1,2
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,family_hotel as (-- 亲子酒店 57521家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,hotel_info as (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
            ,country_name --- 国家
            ,city_name --- 城市
        from ihotel_default.dim_hotel_info_intl_v3  a
        where a.dt = '%(DATE)s'
        group by 1,2,3,4,5
)
,c_uv as(--- C流量
    select dt 
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,m.hotel_seq
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    left join q_c_hotel_mapping m on a.masterhotelid = m.partner_hotel_id
    where device_chl='app'
    and  dt>= '2026-05-26' and dt<= '2026-06-24'
    group by 1,2,3,4
)
,uv as (-- Q流量
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
            ,hotel_seq
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-05-26'
       and dt <= '2026-06-24'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,hotel_type as (
    select hotel_seq
        ,'亲子酒店' as hotel_type
    from family_hotel
    union all
    select hotel_seq
        ,'亲子榜单酒店' as hotel_type
    from family_rank_hotel
)
,q_city_uv_daily as (
    select t1.dt
        ,t2.hotel_type
        ,t1.mdd
        ,count(distinct t1.user_id) as q_dau
    from uv t1
    join hotel_type t2
        on t1.hotel_seq = t2.hotel_seq
    left join hotel_info t3
        on t1.hotel_seq = t3.hotel_seq
    group by 1,2,3
)
,c_city_uv_daily as (
    select t1.dt
        ,t2.hotel_type
        ,t1.mdd
        ,count(distinct t1.uid) as c_dau
    from c_uv t1
    join hotel_type t2
        on t1.hotel_seq = t2.hotel_seq
    left join hotel_info t3
        on t1.hotel_seq = t3.hotel_seq
    group by 1,2,3
)
,q_city_uv as (
    select hotel_type
        ,mdd
        ,avg(q_dau) as q_dau
    from q_city_uv_daily
    group by 1,2
)
,c_city_uv as (
    select hotel_type
        ,mdd
        ,avg(c_dau) as c_dau
    from c_city_uv_daily
    group by 1,2
)

    select coalesce(t1.hotel_type, t2.hotel_type) as hotel_type
        ,coalesce(t1.mdd, t2.mdd) as mdd
        ,coalesce(t1.q_dau, 0) as q_dau
        ,coalesce(t2.c_dau, 0) as c_dau
        ,coalesce(t1.q_dau, 0) / coalesce(t2.c_dau, 0) as traffic_qc
    from q_city_uv t1
    full outer join c_city_uv t2
        on t1.hotel_type = t2.hotel_type
        and t1.mdd = t2.mdd
;

--- 7、亲子酒店亲子请求占比
with family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,family_hotel as (-- 亲子酒店 57521家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,uv as (---D页离店日期在国庆期间
    select  dt
        ,a.user_id,user_name,hotel_seq,case when guestinfos['child_num'] is not null and guestinfos['child_num'] > 0 then '亲子请求' else '非亲子请求' end as request_type
        ,count(distinct qtrace_id) pv
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
)
,hotel_type as (
    select hotel_seq
        ,'亲子酒店' as hotel_type
    from family_hotel
    union all
    select hotel_seq
        ,'亲子榜单酒店' as hotel_type
    from family_rank_hotel
)

select hotel_type,t1.dt
    ,count(distinct t1.user_id) as dau
    ,count(distinct case when request_type = '亲子请求' then t1.user_id else null end) as family_dau
    ,count(distinct case when request_type = '亲子请求' then t1.user_id else null end) / count(distinct t1.user_id) as family_ratio
    ,sum(pv) as pv
    ,sum(case when request_type = '亲子请求' then pv else 0 end) as family_pv
    ,sum(case when request_type = '亲子请求' then pv else 0 end) / sum(pv) as family_pv_ratio

from uv t1
join hotel_type t2
    on t1.hotel_seq = t2.hotel_seq
group by 1,2
order by 1,2 desc
;

--- 8、亲子酒店分国家QC
with q_c_hotel_mapping as(--- 用于映射q和c的酒店
    select hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
    group by 1,2
)
,family_rank_hotel as (-- 亲子榜单酒店  519家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-26' and dt <= '2026-06-24' and theme_tab_name='亲子乐园'
    group by 1
)
,family_hotel as (-- 亲子酒店 57521家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-26' and dt <= '2026-06-24'
        and tree_node_name='亲子酒店'
    group by 1
)
,hotel_info as (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
            ,country_name --- 国家
            ,city_name --- 城市
        from ihotel_default.dim_hotel_info_intl_v3  a
        where a.dt = '%(DATE)s'
        group by 1,2,3,4,5
)
,q_channel_uv as (--- Q分渠道明细
    select dt,channel,user_name
    from ihotel_default.dwd_flow_ug_channel_di
    where dt >= '2026-05-26' and dt <= '2026-06-24'
    group by 1,2,3
)
,uv as (-- Q流量
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,channel
            ,hotel_seq
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join q_channel_uv t2 on a.user_name = t2.user_name and a.dt = t2.dt
    where dt >= '2026-05-26'
       and dt <= '2026-06-24'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,hotel_type as (
    select hotel_seq
        ,'整体' as hotel_type
    from hotel_info
    union all
    select hotel_seq
        ,'亲子酒店' as hotel_type
    from family_hotel
    union all
    select hotel_seq
        ,'亲子榜单酒店' as hotel_type
    from family_rank_hotel
)
,q_city_uv_daily as (
    select t1.dt
        ,t2.hotel_type
        ,t1.channel
        ,count(distinct t1.user_id) as q_dau
    from uv t1
    join hotel_type t2
        on t1.hotel_seq = t2.hotel_seq
    group by 1,2,3
)

select hotel_type
    ,channel
    ,avg(q_dau) as q_dau
from q_city_uv_daily
group by 1,2

;