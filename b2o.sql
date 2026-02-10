select a.order_date
     , a.`目的地`
     , sd_UV
     , s_all_UV
     , d_all_UV
     , d_z_UV
     , d_s_UV
     , b_all_UV
     , b_dz_UV
     , b_ds_UV
     , o_UV
     , o_dz_order
     , o_ds_order
     , order_cnt as q_order_cnt
     , room_night
     , c.s_all_UV_c
     , c.d_s_UV_c
     , c.b_ds_UV_c
     , c.o_ds_order_c
     , q_uv
     , c_uv
     , c_order_cnt
     , c_room_night
from (select order_date
           , `目的地`
           , sum(sd_UV)      sd_UV
           , sum(s_all_UV)   s_all_UV
           , sum(d_all_UV)   d_all_UV
           , sum(d_z_UV)     d_z_UV
           , sum(d_s_UV)     d_s_UV
           , sum(b_all_UV)   b_all_UV
           , sum(b_dz_UV)    b_dz_UV
           , sum(b_ds_UV)    b_ds_UV
           , sum(o_UV)       o_UV
           , sum(o_dz_order) o_dz_order
           , sum(o_ds_order) o_ds_order
           , sum(q_uv) as    q_uv
      from (select a.order_date
                 , a.`目的地`
                 , count(distinct case when search_pv > 0 or detail_pv > 0 then a.user_id else null end)           sd_UV
                 , count(distinct case when search_pv > 0 then a.user_id else null end)                            s_all_UV
                 , count(distinct case when detail_pv > 0 then a.user_id else null end)                            d_all_UV
                 , count(distinct case when detail_pv > 0 and search_pv <= 0 then a.user_id else null end)         d_z_UV
                 , count(distinct case when detail_pv > 0 and search_pv > 0 then a.user_id else null end)          d_s_UV
                 , count(distinct case when booking_pv > 0 then a.user_id else null end)                           b_all_UV
                 , count(distinct case
                                      when booking_pv > 0 and detail_pv > 0 and search_pv <= 0 then a.user_id
                                      else null end)                                                               b_dz_UV
                 , count(distinct case
                                      when booking_pv > 0 and detail_pv > 0 and search_pv > 0 then a.user_id
                                      else null end)                                                               b_ds_UV
                 , count(distinct case when order_pv > 0 then a.user_id else null end)                             o_UV
                 , count(distinct case
                                      when b.user_id is not null and detail_pv > 0 and search_pv <= 0 then order_no
                                      else null end)                                                               o_dz_order
                 , count(distinct case
                                      when b.user_id is not null and detail_pv > 0 and search_pv > 0 then order_no
                                      else null end)                                                               o_ds_order
                 , count(distinct
                         case when search_pv + detail_pv + booking_pv + order_pv > 0 then a.user_id else null end) q_uv
            from (select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as order_date
                       , case
                             when province_name in ('澳门', '香港') then province_name
                             when a.country_name in
                                  ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯')
                                 then a.country_name
                             when e.area in ('欧洲', '亚太', '美洲') then e.area
                             else '其他' end                                                    as `目的地`
                       , user_id
                       , sum(search_pv)                                                            search_pv
                       , sum(detail_pv)                                                            detail_pv
                       , sum(booking_pv)                                                           booking_pv
                       , sum(order_pv)                                                             order_pv
                  from default.mdw_user_app_sdbo_di_v3 a
                           left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
                                     on a.country_name = e.country_name
                  where dt >= '20240101'
                    and dt <= from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
                    and business_type = 'hotel'
                    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国')
                    and (search_pv + detail_pv + booking_pv + order_pv) > 0
                  group by 1, 2, 3) a -- 流量表
                     left join
                 (select distinct order_date
                                , case
                                      when province_name in ('澳门', '香港') then province_name
                                      when a.country_name in
                                           ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚',
                                            '俄罗斯') then a.country_name
                                      when e.area in ('欧洲', '亚太', '美洲') then e.area
                                      else '其他' end as `目的地`
                                , user_id
                                , order_no
                  from default.mdw_order_v3_international a
                           left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
                                     on a.country_name = e.country_name
                  where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
                    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国')
                    -- and terminal_channel_type in ('www','app','touch')
                    and terminal_channel_type = 'app'
                    and (first_cancelled_time is null or date (first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date>='2024-01-01' and order_date<=date_sub(current_date,1)) b on a.order_date=b. order_date and a.user_id=b.user_id and a.`目的地`=b.`目的地`   -- 订单表
      group by 1, 2) a --- 页面宽窄加订单信息以目的地进行聚合
group by 1, 2 )a    -- 页面宽窄加订单信息
left join 
(
   select order_date 
   ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
   ,count(order_no) as  order_cnt
   ,sum(room_night) as room_night
   from default.mdw_order_v3_international a
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   where dt =from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
   and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
  -- and terminal_channel_type in ('www','app','touch') 
   and terminal_channel_type='app'
   and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
   and (first_rejected_time is null or date(first_rejected_time) > order_date) 
   and (refund_time is null or date(refund_time) > order_date)
   and is_valid='1'
   and order_date>='2024-01-01' 
   and order_date<=date_sub(current_date,1)
   group by 1,2
) b
on a.order_date=b.order_date and a.`目的地`=b.`目的地`
    left join (
    select dt
    ,`目的地`
    , sum (s_all_UV_c) s_all_UV_c
    , sum (d_s_UV_c) d_s_UV_c
    , sum (b_ds_UV_c) b_ds_UV_c
    , sum (o_ds_order_c) o_ds_order_c
    , sum (c_uv) as c_uv
    from (
    select tt.dt
    ,tt.`目的地`
--宽口径
    , count (distinct case when search_pv >0 then tt.uid else null end) as s_all_UV_c
    , count (distinct case when detail_pv >0 then tt.uid else null end ) d_s_UV_c
    , count (distinct case when booking_pv >0 then tt.uid else null end ) b_ds_UV_c
    , count (distinct case when tt1.ubt_user_id is not null then order_no else null end ) o_ds_order_c
    , count (distinct uid) as c_uv

    --窄口径       ,count(distinct case when search_pv >0 then  tt.uid else null end) as s_all_UV_c
--           ,count(distinct case when detail_pv >0 and search_pv >0 then  tt.uid else null end )d_s_UV_c
--           ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  tt.uid else null end )b_ds_UV_c
--           ,count(distinct case when tt1.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
--           ,count(distinct uid) as c_uv
    from (
    select
    dt
    ,uid
    , case when provincename in ('澳门','香港') then provincename when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
    , count (distinct case when page_short_domain='list' then uid else null end ) search_pv
    , count (distinct case when page_short_domain='dbo' then uid else null end ) detail_pv
    , count (distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end ) booking_pv
    , count (distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end ) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name
    where device_chl='app'
    and dt>= '2024-01-01' and dt<= date_sub(current_date,1)
    group by 1,2,3
    ) tt
    left join (
    select distinct substr(order_date,1,10) `日期`
    , case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
    when c.area in ('欧洲','亚太','美洲') then c.area
    else '其他' end as `目的地`
    ,ubt_user_id
    ,order_no
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt =from_unixtime(unix_timestamp() -86400, 'yyyy-MM-dd')
    and extend_info['IS_IBU'] = '0'
    and extend_info['book_channel'] = 'Ctrip'
    and extend_info['sub_book_channel'] = 'Direct-Ctrip'
    --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
    and terminal_channel_type = 'app'
    and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
    and substr(order_date,1,10)>='2024-01-01' and substr(order_date,1,10)<=date_sub(current_date,1)
    ) tt1 on tt.dt=tt1.`日期` and tt.uid=tt1.ubt_user_id and tt.`目的地`=tt1.`目的地`
    group by 1,2
    ) a group by 1,2
    ) c on a.order_date= c.dt and a.`目的地`= c.`目的地`
    left join
    (
    select substr(order_date,1,10) order_date
    , case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
    when c.area in ('欧洲','亚太','美洲') then c.area
    else '其他' end as `目的地`
    , count (order_no) as c_order_cnt
    , sum (extend_info['room_night']) as c_room_night
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt =from_unixtime(unix_timestamp() -86400, 'yyyy-MM-dd')
    and extend_info['IS_IBU'] = '0'
    and extend_info['book_channel'] = 'Ctrip'
    and extend_info['sub_book_channel'] = 'Direct-Ctrip'
    and terminal_channel_type = 'app'
    and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
    and substr(order_date,1,10)>='2024-01-01'
    and substr(order_date,1,10)<=date_sub(current_date,1)
    group by 1,2
    ) d on a.order_date=d.order_date and a.`目的地`=d.`目的地`