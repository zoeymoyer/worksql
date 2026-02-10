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
     where dt >= '2024-01-01'
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
        and order_date >= '2024-01-01'  and order_date <= date_sub(current_date, 1)
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
    and  dt>= '2024-01-01'  and dt<= date_sub(current_date, 1)
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
      and substr(order_date,1,10) >= '2024-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)

---- 整体
select t1.dt
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

from(
    select dt
         ,sum(s_all_UV) s_all_UV
         ,sum(d_all_UV) d_all_UV
         ,sum(b_all_UV) b_all_UV
         ,sum(d_s_UV) d_s_UV
         ,sum(b_ds_UV) b_ds_UV
         ,sum(o_ds_order) o_ds_order
         ,sum(q_uv) q_uv
         ,sum(order_user_cnt) order_user_cnt
    from (---- Q得DBO转化
        select 
            a.dt
            ,a.mdd
            ,a.user_type
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
        left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.mdd=b.mdd   -- 订单表
        group by 1,2,3
    ) a 
    group by 1
)t1   
left join (---- C得DBO转化
    select dt
          ,sum(s_all_UV_c) s_all_UV_c
          ,sum(d_s_UV_c) d_s_UV_c
          ,sum(b_ds_UV_c) b_ds_UV_c
          ,sum(o_ds_order_c) o_ds_order_c
          ,sum(c_uv) c_uv
          ,sum(order_user_cnt_c) order_user_cnt_c
    from (
        select t1.dt
            ,t1.mdd
            ,t1.user_type
            ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
            ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
            ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
            ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
            ,count(distinct uid) as c_uv
            ,count(distinct t2.ubt_user_id) as order_user_cnt_c
        from c_uv t1 
        left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id and t1.mdd=t2.mdd
        group by 1,2,3
    ) t 
    group by 1
)t2 on t1.dt=t2.dt 
order by 1 desc
;

---- 分新老
select t1.dt
      ,t1.user_type
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
        ,a.user_type
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
order by 1 desc
;



---- 分目的地和新老
select t1.dt
      ,t1.mdd
      ,t1.user_type
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
        ,a.user_type
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
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.mdd=b.mdd   -- 订单表
    group by 1,2,3
)t1   
left join (---- C得DBO转化
    select t1.dt
        ,t1.mdd
        ,t1.user_type
        ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
        ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
        ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
        ,count(distinct uid) as c_uv
        ,count(distinct t2.ubt_user_id) as order_user_cnt_c
    from c_uv t1 
    left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id and t1.mdd=t2.mdd
    group by 1,2,3
)t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by 1 desc
;


