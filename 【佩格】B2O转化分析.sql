
--- T0取消率
with user_type as (-----新老客
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

,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,count(distinct order_no) order_no_q
            ,count(distinct case when (first_cancelled_time is null or date(first_cancelled_time) > order_date)  
                          and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                          and (refund_time is null or date(refund_time) > order_date)
                    then order_no end) no_t0_cancel_order_no_q
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1
)
,c_user_type as
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
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
        --    ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
        --        when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
        --        when c.area in ('欧洲','亚太','美洲') then c.area
        --        else '其他' end as mdd
        --     ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,count(distinct order_no) order_no_c
            ,count(distinct case when (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
                    then order_no end) no_t0_cancel_order_no_c
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and substr(order_date,1,10) >= '2025-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1
)


select t1.order_date
      ,order_no_q
      ,no_t0_cancel_order_no_q
      ,1 - no_t0_cancel_order_no_q / order_no_q  cancel_rate_q
      ,order_no_c
      ,no_t0_cancel_order_no_c
      ,1 - no_t0_cancel_order_no_c / order_no_c cancel_rate_c
from q_app_order t1
left join c_order t2
on t1.order_date=t2.dt
order by 1
;

---------------- dbo分目的地
with user_type as (-----新老客
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
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,c_user_type as
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
        ,uid
        ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
        ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
        ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
        ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-01-01'  and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
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

---- 分目的地
select t1.dt
      ,t1.mdd
      ,s_all_UV
      ,d_s_UV
      ,b_ds_UV
      ,o_ds_order
      ,concat(round(d_s_UV / s_all_UV * 100, 2), '%')  s2d
      ,concat(round(b_ds_UV / d_s_UV * 100, 2), '%')  d2b
      ,concat(round(o_ds_order / b_ds_UV * 100, 2), '%')  b2o

      ,s_all_UV_c
      ,d_s_UV_c
      ,b_ds_UV_c
      ,o_ds_order_c
      ,concat(round(d_s_UV_c / s_all_UV_c * 100, 2), '%') s2d_c
      ,concat(round(b_ds_UV_c / d_s_UV_c * 100, 2), '%') d2b_c
      ,concat(round(o_ds_order_c / b_ds_UV_c * 100, 2), '%') b2o_c
      
      ,concat(round((d_s_UV / s_all_UV) / (d_s_UV_c / s_all_UV_c) * 100, 2), '%')    s2d_qc
      ,concat(round((b_ds_UV / d_s_UV) / (b_ds_UV_c / d_s_UV_c) * 100, 2), '%')    d2b_qc
      ,concat(round((o_ds_order / b_ds_UV) / (o_ds_order_c / b_ds_UV_c) * 100, 2), '%')   b2o_qc
      
      ,concat(round(s_all_UV / s_all_UV_c * 100, 2), '%')  s_uv_qc
      ,concat(round(d_s_UV / d_s_UV_c * 100, 2), '%')      d_uv_qc
      ,concat(round(b_ds_UV / b_ds_UV_c * 100, 2), '%')    b_uv_qc
      ,concat(round(o_ds_order / o_ds_order_c * 100, 2), '%')  o_uv_qc

from(---- Q得DBO转化
    select 
         a.dt
        ,a.mdd
        ,count(distinct case when search_pv >0 then  a.user_id else null end )s_all_UV
        ,count(distinct case when detail_pv >0 then  a.user_id else null end )d_all_UV
        ,count(distinct case when booking_pv >0 then a.user_id else null end )b_all_UV
        ,count(distinct case when order_pv >0 then   a.user_id else null end )o_UV
        ,count(distinct case when search_pv >0 or detail_pv>0 then  a.user_id else null end )sd_UV

        ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order

        ,count(distinct case when detail_pv >0 and search_pv <=0 then  a.user_id else null end )  d_z_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv <=0 then  a.user_id else null end )b_dz_UV
        ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv <=0 then order_no else null end )o_dz_order
        ,count(distinct a.user_id) q_uv
        ,count(distinct b.user_id) order_user_cnt
    from  uv a  -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id   -- 订单表
    group by 1,2
)t1   
left join (---- C得DBO转化
    select t1.dt
        ,t1.mdd
        ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
        ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
        ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
        ,count(distinct uid) as c_uv
        ,count(distinct t2.ubt_user_id) as order_user_cnt_c
    from c_uv t1 
    left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id 
    group by 1,2
)t2 on t1.dt=t2.dt  and t1.mdd=t2.mdd
order by 1 desc
;



---------------- dbo请求单晚和多晚
with user_type as (-----新老客
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
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,c_user_type as
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
        ,uid
        ,case when datediff(a.check_out, a.check_in) >= 2 then '多晚' else '单晚' end is_more_roomnight
        ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
        ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
        ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
        ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-01-01'  and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
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
,d_uv as (
    select  dt 
            ,a.user_id
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,count(1) detail_pv
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2025-01-01'
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2,3
)
,b_uv as (
    select  dt 
            ,a.user_id
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,count(1) booking_pv
    from ihotel_default.dw_user_app_log_booking_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2025-01-01'
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2,3
)
,s_uv as (
    select  dt 
            ,a.user_id
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,count(1) search_pv
    from ihotel_default.dw_user_app_log_search_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2025-01-01'
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2,3
)
        

select t1.dt
      ,t1.is_more_roomnight
      ,d_s_UV
      ,b_ds_UV
      ,o_ds_order
      ,concat(round(b_ds_UV / d_s_UV * 100, 2), '%')  d2b
      ,concat(round(o_ds_order / b_ds_UV * 100, 2), '%')  b2o

      ,s_all_UV_c
      ,d_s_UV_c
      ,b_ds_UV_c
      ,o_ds_order_c
      ,concat(round(d_s_UV_c / s_all_UV_c * 100, 2), '%') s2d_c
      ,concat(round(b_ds_UV_c / d_s_UV_c * 100, 2), '%') d2b_c
      ,concat(round(o_ds_order_c / b_ds_UV_c * 100, 2), '%') b2o_c
      
      ,concat(round((b_ds_UV / d_s_UV) / (b_ds_UV_c / d_s_UV_c) * 100, 2), '%')    d2b_qc
      ,concat(round((o_ds_order / b_ds_UV) / (o_ds_order_c / b_ds_UV_c) * 100, 2), '%')   b2o_qc
      
from(---- Q得DBO转化
    select 
         a.dt
        ,a.is_more_roomnight

        ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0  and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when d.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order
    from  s_uv a   --- s流量
    left join d_uv b on a.dt=b.dt and a.user_id=b.user_id   --- d流量
    left join b_uv c on a.dt=c.dt and a.user_id=c.user_id  -- b流量
    left join q_app_order d on a.dt=d.order_date and a.user_id=d.user_id   -- 订单表
    group by 1,2
)t1   
left join (---- C得DBO转化
    select t1.dt
        ,t1.is_more_roomnight
        ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
        ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
        ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
        ,count(distinct uid) as c_uv
        ,count(distinct t2.ubt_user_id) as order_user_cnt_c
    from c_uv t1 
    left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id 
    group by 1,2
)t2 on t1.dt=t2.dt  and t1.is_more_roomnight=t2.is_more_roomnight
order by 1 desc
;



---------------- 分新老
with user_type as (-----新老客
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
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,c_user_type as
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
        ,uid
        ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
        ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
        ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
        ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-01-01'  and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
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

        

select t1.dt
      ,t1.user_type
      ,b_ds_UV
      ,o_ds_order
      ,concat(round(o_ds_order / b_ds_UV * 100, 2), '%')  b2o

      ,d_s_UV_c
      ,b_ds_UV_c
      ,o_ds_order_c
      ,concat(round(d_s_UV_c / s_all_UV_c * 100, 2), '%') s2d_c
      ,concat(round(b_ds_UV_c / d_s_UV_c * 100, 2), '%') d2b_c
      ,concat(round(o_ds_order_c / b_ds_UV_c * 100, 2), '%') b2o_c
      
      ,concat(round((b_ds_UV / d_s_UV) / (b_ds_UV_c / d_s_UV_c) * 100, 2), '%')    d2b_qc
      ,concat(round((o_ds_order / b_ds_UV) / (o_ds_order_c / b_ds_UV_c) * 100, 2), '%')   b2o_qc

      ,order_no
      ,order_no_c
      ,room_night
      ,room_night_c
      ,init_gmv
      ,init_gmv_c
      ,yj
      ,yj_c
      ,qe
      ,qe_c
      ,yj - yj_c  yj_gap
      ,yj / init_gmv yj_rate
      ,yj_c / init_gmv_c yj_rate_c
      ,qe / init_gmv qe_rate
      ,qe_c / init_gmv_c qe_rate_c
      
from(---- Q得DBO转化
    select 
         a.dt
        ,a.user_type
        ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0  and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when d.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order
    from uv a   
    left join q_app_order d on a.dt=d.order_date and a.user_id=d.user_id   -- 订单表
    group by 1,2
)t1   
left join (---- C得DBO转化
    select t1.dt
        ,t1.user_type
        ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
        ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
        ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
        ,count(distinct uid) as c_uv
        ,count(distinct t2.ubt_user_id) as order_user_cnt_c
    from c_uv t1 
    left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id 
    group by 1,2
)t2 on t1.dt=t2.dt  and t1.user_type=t2.user_type
left join (---- Q的订单
    select 
         a.order_date
        ,d.user_type
        ,count(distinct order_no) order_no
        ,sum(room_night) room_night
        ,sum(init_gmv) init_gmv
        ,sum(final_commission_after) yj
        ,sum(coupon_substract_summary) qe
    from q_app_order a   
    left join (--- b2o条件
        select dt,user_id,user_type
        from uv 
        where  detail_pv >0 and search_pv >0  
        group by 1,2,3
    ) d on a.order_date=d.dt and a.user_id=d.user_id  
    where d.user_id is not null
    group by 1,2
) t3 on t1.dt=t3.order_date  and t1.user_type=t3.user_type
left join (---- C的订单
    select 
         a.dt
        ,d.user_type
        ,count(distinct order_no) order_no_c
        ,sum(room_night) room_night_c
        ,sum(room_fee) init_gmv_c
        ,sum(comission) yj_c
        ,sum(cqe) qe_c
    from c_order a   
    left join (
        select dt,uid ,user_type
        from c_uv 
        where  detail_pv >0 and search_pv >0  
        group by 1,2,3
    ) d on a.dt=d.dt and d.uid=a.ubt_user_id 
    where d.uid is not null
    group by 1,2
) t4 on t1.dt=t4.dt  and t1.user_type=t4.user_type
order by 1 desc
;