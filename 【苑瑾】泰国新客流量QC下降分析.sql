with user_type as
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
    select  dt 
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
            --            when e.area in ('欧洲','亚太','美洲') then e.area
            --            else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,city_name,province_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-11-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
       and a.country_name = '泰国'
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        -- ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,cityname
        ,case when star in (4,5) then '高星'
            when star in (3) then '中星'
            else '低星' end hotel_grade
        ,check_in
        ,provincename
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-11-01' and dt<= date_sub(current_date, 1)
    and a.countryname = '泰国'
)


select t1.dt
      ,q_uv / c_uv qc_uv
      ,q_hight_uv / c_hight_uv qc_hight_uv
      ,q_mid_uv / c_mid_uv qc_mid_uv
      ,q_low_uv / c_low_uv qc_low_uv
      ,q_uv
      ,c_uv
      ,q_hight_uv
      ,c_hight_uv
      ,q_mid_uv
      ,c_mid_uv
      ,q_low_uv
      ,c_low_uv
from (
    select dt
          ,count(distinct user_name) q_uv
          ,count(distinct case when hotel_grade = '高星' then user_name end) q_hight_uv
          ,count(distinct case when hotel_grade = '中星' then user_name end) q_mid_uv
          ,count(distinct case when hotel_grade = '低星' then user_name end) q_low_uv
    from uv
    where user_type = '新客'
    group by 1
)t1 left join (
    select dt
          ,count(distinct uid) c_uv
          ,count(distinct case when hotel_grade = '高星' then uid end) c_hight_uv
          ,count(distinct case when hotel_grade = '中星' then uid end) c_mid_uv
          ,count(distinct case when hotel_grade = '低星' then uid end) c_low_uv
    from c_uv
    where user_type = '新客'
    group by 1
) t2 on t1.dt=t2.dt
order by 1
;


select t1.dt,city_name
      ,q_uv / c_uv qc_uv
      ,q_hight_uv / c_hight_uv qc_hight_uv
      ,q_mid_uv / c_mid_uv qc_mid_uv
      ,q_low_uv / c_low_uv qc_low_uv
      ,q_uv
      ,c_uv
      ,q_hight_uv
      ,c_hight_uv
      ,q_mid_uv
      ,c_mid_uv
      ,q_low_uv
      ,c_low_uv
from (
    select dt,case when city_name = '邦拉蒙' then '芭堤雅' else  city_name end city_name
          ,count(distinct user_name) q_uv
          ,count(distinct case when hotel_grade = '高星' then user_name end) q_hight_uv
          ,count(distinct case when hotel_grade = '中星' then user_name end) q_mid_uv
          ,count(distinct case when hotel_grade = '低星' then user_name end) q_low_uv
    from uv
    where user_type = '新客'
    group by 1,2
)t1 left join (
    select dt,cityname
          ,count(distinct uid) c_uv
          ,count(distinct case when hotel_grade = '高星' then uid end) c_hight_uv
          ,count(distinct case when hotel_grade = '中星' then uid end) c_mid_uv
          ,count(distinct case when hotel_grade = '低星' then uid end) c_low_uv
    from c_uv
    where user_type = '新客'
    group by 1,2
) t2 on t1.dt=t2.dt and t1.city_name=t2.cityname
order by 1
;


---- 提前订分布
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
    select  dt
        -- , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,checkin_date
        ,case when datediff(checkin_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkin_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkin_date, dt) between 4 and 7 then '提前订4-7天'
              when datediff(checkin_date, dt) between 8 and 14 then '提前订8-14天'
              when datediff(checkin_date, dt) between 15 and 30 then '提前订15-30天'
              when datediff(checkin_date, dt) between 31 and 60 then '提前订31-60天'
              when datediff(checkin_date, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-11-01' 
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and a.country_name = '泰国'
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        -- ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,cityname
        ,check_in
        ,case when datediff(check_in, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_in, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_in, dt) between 4 and 7 then '提前订4-7天'
              when datediff(check_in, dt) between 8 and 14 then '提前订8-14天'
              when datediff(check_in, dt) between 15 and 30 then '提前订15-30天'
              when datediff(check_in, dt) between 31 and 60 then '提前订31-60天'
              when datediff(check_in, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
        ,provincename
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-11-01' and dt<= date_sub(current_date, 1)
    and page_short_domain='dbo'
    and a.countryname = '泰国'
)

select t1.dt,t1.per_type
      ,q_uv / c_uv qc_uv
      ,q_uv
      ,c_uv
from (
    select dt,per_type
          ,count(distinct user_name) q_uv
    from uv
    where user_type = '新客'
    group by 1,2
)t1 left join (
    select dt,per_type
          ,count(distinct uid) c_uv
    from c_uv
    where user_type = '新客'
    group by 1,2
) t2 on t1.dt=t2.dt and t1.per_type=t2.per_type
order by 1
;


---- 渠道情况



---- 提前订分布
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
    select  dt
        -- , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,checkin_date
        ,case when datediff(checkin_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkin_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkin_date, dt) between 4 and 7 then '提前订4-7天'
              when datediff(checkin_date, dt) between 8 and 14 then '提前订8-14天'
              when datediff(checkin_date, dt) between 15 and 30 then '提前订15-30天'
              when datediff(checkin_date, dt) between 31 and 60 then '提前订31-60天'
              when datediff(checkin_date, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-11-01' 
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and a.country_name = '韩国'
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        -- ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,cityname
        ,check_in
        ,case when datediff(check_in, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_in, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_in, dt) between 4 and 7 then '提前订4-7天'
              when datediff(check_in, dt) between 8 and 14 then '提前订8-14天'
              when datediff(check_in, dt) between 15 and 30 then '提前订15-30天'
              when datediff(check_in, dt) between 31 and 60 then '提前订31-60天'
              when datediff(check_in, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
        ,provincename
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-11-01' and dt<= date_sub(current_date, 1)
    and page_short_domain='dbo'
    and a.countryname = '韩国'
)

select t1.dt,t1.per_type
      ,q_uv / c_uv qc_uv
      ,q_uv
      ,c_uv
from (
    select dt,per_type
          ,count(distinct user_name) q_uv
    from uv
    where user_type = '新客'
    group by 1,2
)t1 left join (
    select dt,per_type
          ,count(distinct uid) c_uv
    from c_uv
    where user_type = '新客'
    group by 1,2
) t2 on t1.dt=t2.dt and t1.per_type=t2.per_type
order by 1
;



---- 提前订分布
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
    select  dt
        , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,checkin_date
        ,case when datediff(checkin_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkin_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkin_date, dt) between 4 and 7 then '提前订4-7天'
              when datediff(checkin_date, dt) between 8 and 14 then '提前订8-14天'
              when datediff(checkin_date, dt) between 15 and 30 then '提前订15-30天'
              when datediff(checkin_date, dt) between 31 and 60 then '提前订31-60天'
              when datediff(checkin_date, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,cityname
        ,check_in
        ,case when datediff(check_in, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_in, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_in, dt) between 4 and 7 then '提前订4-7天'
              when datediff(check_in, dt) between 8 and 14 then '提前订8-14天'
              when datediff(check_in, dt) between 15 and 30 then '提前订15-30天'
              when datediff(check_in, dt) between 31 and 60 then '提前订31-60天'
              when datediff(check_in, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
        ,provincename
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
    and page_short_domain='dbo'
)

select t1.dt,t1.mdd,checkin_date
      ,q_uv / c_uv qc_uv
      ,q_uv
      ,c_uv
      ,sum(q_uv) over(partition by t1.dt,t1.mdd) q_uv_all
      ,sum(c_uv) over(partition by t1.dt,t1.mdd) c_uv_all
      ,q_uv / sum(q_uv) over(partition by t1.dt,t1.mdd) q_rate
      ,c_uv / sum(c_uv) over(partition by t1.dt,t1.mdd) c_rate
from (
    select dt,mdd,checkin_date
          ,count(distinct user_name) q_uv
    from uv
    where user_type = '新客'
    group by 1,2,3
)t1 left join (
    select dt,mdd,check_in
          ,count(distinct uid) c_uv
    from c_uv
    where user_type = '新客'
    group by 1,2,3
) t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.checkin_date=t2.check_in
order by 1,2,3
;


---- 提前订分布--离店时间
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
    select  dt
        , case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
        ,a.user_id,user_name
        ,checkout_date
        ,case when datediff(checkout_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkout_date, dt) between 1 and 3 then '提前订1-3天'
              when datediff(checkout_date, dt) between 4 and 7 then '提前订4-7天'
              when datediff(checkout_date, dt) between 8 and 14 then '提前订8-14天'
              when datediff(checkout_date, dt) between 15 and 30 then '提前订15-30天'
              when datediff(checkout_date, dt) between 31 and 60 then '提前订31-60天'
              when datediff(checkout_date, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,cityname
        ,check_out
        ,case when datediff(check_out, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_out, dt) between 1 and 3 then '提前订1-3天'
              when datediff(check_out, dt) between 4 and 7 then '提前订4-7天'
              when datediff(check_out, dt) between 8 and 14 then '提前订8-14天'
              when datediff(check_out, dt) between 15 and 30 then '提前订15-30天'
              when datediff(check_out, dt) between 31 and 60 then '提前订31-60天'
              when datediff(check_out, dt) between 61 and 180 then '提前订61-180天'
              else '其他'  end  per_type
        ,provincename
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
    and page_short_domain='dbo'
)

select t1.dt,t1.mdd,checkout_date
      ,q_uv / c_uv qc_uv
      ,q_uv
      ,c_uv
      ,sum(q_uv) over(partition by t1.dt,t1.mdd) q_uv_all
      ,sum(c_uv) over(partition by t1.dt,t1.mdd) c_uv_all
      ,q_uv / sum(q_uv) over(partition by t1.dt,t1.mdd) q_rate
      ,c_uv / sum(c_uv) over(partition by t1.dt,t1.mdd) c_rate
from (
    select dt,mdd,checkout_date
          ,count(distinct user_name) q_uv
    from uv
    where user_type = '新客'
    and mdd='香港'
    group by 1,2,3
)t1 left join (
    select dt,mdd,check_out
          ,count(distinct uid) c_uv
    from c_uv
    where user_type = '新客'
    and mdd='香港'
    group by 1,2,3
) t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.checkout_date=t2.check_out
where t1.checkout_date >= t1.dt
order by 1,2,3
;
