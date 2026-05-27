with uv as ( ----分日去重活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     where dt >= date_sub(current_date, 30)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
     group by 1,2,3,4
)
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
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
,c_uv as (   --- C 流量 目的地加和
    select dt 
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= date_sub(current_date, 30) and dt<= date_sub(current_date, 1)
    group by 1,2
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= date_sub(current_date, 30)
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)


select t1.dt
      ,'ALL' mdd
      ,t3.order_cnt
      ,t3.order_cnt / t4.c_order_cnt  as order_cnt_qc
      ,t1.q_uv / t2.c_uv  as uv_qc
      ,t3.room_night / t4.c_room_night as room_night_qc
from (
    select dt,count(user_id) q_uv
    from uv
    group by 1
) t1 
left join (
    select dt
            ,sum(c_uv) as c_uv
    from c_uv
    group by 1
) t2 on t1.dt = t2.dt
left join (
    select order_date
            ,count(distinct order_no) order_cnt
            ,sum(room_night) room_night
    from q_order_app
    group by 1
) t3 on t1.dt = t3.order_date
left join (
    select dt
            ,count(distinct order_no) c_order_cnt
            ,sum(room_night) c_room_night
    from c_order
    group by 1
) t4 on t1.dt = t4.dt

union all

select t1.dt
      ,t1.mdd
      ,t3.order_cnt
      ,t3.order_cnt / t4.c_order_cnt  as order_cnt_qc
      ,t1.q_uv / t2.c_uv  as uv_qc
      ,t3.room_night / t4.c_room_night as room_night_qc
from (
    select dt,mdd,count(distinct user_id) q_uv
    from uv
    group by 1,2
) t1 
left join (
    select dt,mdd
            ,sum(c_uv) as c_uv
    from c_uv
    group by 1,2
) t2 on t1.dt = t2.dt and t1.mdd = t2.mdd
left join (
    select order_date,mdd
            ,count(distinct order_no) order_cnt
            ,sum(room_night) room_night
    from q_order_app
    group by 1,2
) t3 on t1.dt = t3.order_date and t1.mdd = t3.mdd
left join (
    select dt,mdd
            ,count(distinct order_no) c_order_cnt
            ,sum(room_night) c_room_night
    from c_order
    group by 1,2
) t4 on t1.dt = t4.dt and t1.mdd = t4.mdd
;







