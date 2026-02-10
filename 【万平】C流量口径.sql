with q_uv_dis as 
(  --- Q流量  去重
    select  dt 
            ,count(distinct a.user_id) q_uv
            ,count(distinct case when a.country_name != '中国' then a.user_id end) q_n_gat_uv
            ,count(distinct case when province_name in ('台湾', '澳门', '香港') then a.user_id end) q_gat_uv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1
)
,q_uv as ( --- Q流量分目的地
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                  when e.area in ('欧洲','亚太','美洲') then e.area
             else '其他' end as mdd
          ,count(distinct a.user_id) q_uv
          ,count(distinct case when a.country_name != '中国' then a.user_id end) q_n_gat_uv
          ,count(distinct case when province_name in ('台湾', '澳门', '香港') then a.user_id end) q_gat_uv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >=  '2025-01-01'
      and dt <= date_sub(current_date, 1)
      and business_type = 'hotel'
      and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
      and (search_pv + detail_pv + booking_pv + order_pv) > 0
      and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
      and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2
)
,c_uv_dis as (  --- c流量去重
    select dt 
        ,count(distinct uid) c_uv
        ,count(distinct case when a.countryname != '中国' then uid end) c_n_gat_uv
        ,count(distinct case when provincename in ('台湾','澳门','香港') then uid end) c_gat_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name
    where device_chl='app'
    and dt >= '2025-01-01'
    and dt <= date_sub(current_date, 1)  
    group by 1
)
,c_uv as (  --- c流量分目的地
    select dt
         ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
         ,count(distinct uid) c_uv
         ,count(distinct case when a.countryname != '中国' then uid end) c_n_gat_uv
         ,count(distinct case when provincename in ('台湾','澳门','香港') then uid end) c_gat_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name
    where device_chl='app'
    and dt >= '2025-01-01'
    and dt <= date_sub(current_date, 1)  
    group by 1,2
)
,q_order as (-- q订单量
    select order_date 
         ,count(distinct order_no) as q_order_no
         ,count(distinct case when a.country_name != '中国' then order_no end) as q_n_gat_order_no
         ,count(distinct case when province_name in ('台湾', '澳门', '香港') then order_no end) as q_gat_order_no
    from default.mdw_order_v3_international a
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
      and terminal_channel_type in ('app')
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
      and (first_rejected_time is null or date(first_rejected_time) > order_date)
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid='1'
      and order_no <> '103576132435'
      and order_date >= '2025-01-01'
      and order_date <= date_sub(current_date, 1)  
    group by 1
)
,c_order as (  --- c订单
    select substr(order_date,1,10) as dt
         ,count(distinct order_no) as c_order_no
         ,count(distinct case when extend_info['COUNTRY'] != '中国' then order_no end) as c_n_gat_order_no
         ,count(distinct case when extend_info['PROVINCE'] in ('台湾', '澳门', '香港') then order_no end) as c_gat_order_no
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1) 
    group by 1
)



select t1.dt
        ,t1.q_uv    as  `QUV-去重`
        ,t1.q_n_gat_uv  as  `QUV-去港澳台去重`
        ,t1.q_gat_uv as  `QUV-港澳台去重`
        ,t2.q_uv_p as  `QUV-加和口径`
        ,t2.q_n_gat_uv_p as  `QUV-去港澳台加和口径`
        ,t2.q_gat_uv_p as  `QUV-港澳台加和口径`
        ,t3.c_uv as  `CUV-去重`
        ,t3.c_n_gat_uv as  `CUV-去港澳台去重`
        ,t3.c_gat_uv as  `CUV-港澳台去重`
        ,t4.c_uv_p as  `CUV-加和口径`
        ,t4.c_n_gat_uv_p as  `CUV-去港澳台加和口径`
        ,t4.c_gat_uv_p as  `CUV-港澳台加和口径`
        ,t5.q_order_no as  `Q订单量`
        ,t5.q_n_gat_order_no as  `Q订单量-去港澳台`
        ,t5.q_gat_order_no as  `Q订单量-港澳台`
        ,t6.c_order_no as  `C订单量`
        ,t6.c_n_gat_order_no as  `C订单量-去港澳台`
        ,t6.c_gat_order_no as  `C订单量-港澳台`

        ,t5.q_order_no / t1.q_uv as  `QCR-去重`
        ,t5.q_n_gat_order_no / t1.q_n_gat_uv as  `QCR-去港澳台去重`
        ,t5.q_gat_order_no / t1.q_gat_uv as  `QCR-港澳台去重`

        ,t6.c_order_no / t3.c_uv as  `CCR-去重`
        ,t6.c_n_gat_order_no / t3.c_n_gat_uv as  `CCR-去港澳台去重`
        ,t6.c_gat_order_no / t3.c_gat_uv as  `CCR-港澳台去重`
from  q_uv_dis t1 
left join (  -- q流量  目的地加和
    select dt
          ,sum(q_uv) q_uv_p
          ,sum(q_n_gat_uv) q_n_gat_uv_p
          ,sum(q_gat_uv) q_gat_uv_p
    from q_uv
    group by 1
) t2 on t1.dt = t2.dt
left join c_uv_dis t3 on t1.dt=t3.dt
left join (  -- c流量  目的地加和
    select dt
          ,sum(c_uv) c_uv_p
          ,sum(c_n_gat_uv) c_n_gat_uv_p
          ,sum(c_gat_uv) c_gat_uv_p
    from c_uv
    group by 1
) t4 on t1.dt = t4.dt
left join q_order t5 on t1.dt=t5.order_date
left join c_order t6 on t1.dt=t6.dt
order by t1.dt
;


---- C流量分目的地
with c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,c_uv as
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-01-01' and dt<= date_sub(current_date,1)
    group by 1,2,3
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)


select t1.dt,t1.mdd,C_DAU,c_orders_app / C_DAU cr
from  (-- C流量
    select dt,mdd
            ,sum(c_uv) as C_DAU
    from c_uv
    group by 1,2
)t1
left join (-- C订单 APP端
    select dt,mdd
            ,sum(comission) as c_yj_app
            ,sum(room_fee) as c_gmv_app
            ,sum(cqe) as c_qe_app
            ,count(distinct order_no) as c_orders_app
            ,count(distinct user_id) as c_order_uv_app
            ,sum(room_night) as c_room_nights_app
            ,sum(case when star in (4,5) then room_night else 0 end ) as c_room_night_high_app
            ,sum(case when star in (3) then room_night else 0 end ) as c_room_night_middle_app
            ,sum(case when star not in (3,4,5) then room_night else 0 end ) as c_room_night_low_app
    from c_order
    group by 1,2
)t5 on t1.dt=t5.dt and t1.mdd=t5.mdd
order by t1.dt 
;