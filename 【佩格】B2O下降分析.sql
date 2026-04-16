-- 1、SDBO分目的地分新老
with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----分日去重活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
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
     where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
    from default.mdw_order_v3_international a 
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
        and order_date >= '2026-01-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_user_type as(   --- 用于判定c新老客
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
,c_uv as(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when provincename in ('澳门','香港') then '港澳'  
            when a.countryname in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
            when a.countryname in ('日本','韩国','泰国') then a.countryname 
            else '其他' end as new_mdd
        ,uid
        ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
        ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
        ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
        ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2026-01-01'  and dt<= date_sub(current_date, 1)
    group by 1,2,3,4,5
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)

select t1.dt,t1.mdd,t1.user_type
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

      ,d_all_UV,b_all_UV
from(
    select dt,if(grouping(new_mdd)=1,'ALL',new_mdd) mdd,if(grouping(user_type)=1,'ALL',user_type)user_type
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
            ,a.new_mdd
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
        left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.new_mdd=b.new_mdd   -- 订单表
        group by 1,2,3
    ) a 
    group by 1,cube(new_mdd,user_type)
)t1   
left join (---- C得DBO转化
    select dt,if(grouping(new_mdd)=1,'ALL',new_mdd) mdd,if(grouping(user_type)=1,'ALL',user_type)user_type
          ,sum(s_all_UV_c) s_all_UV_c
          ,sum(d_s_UV_c) d_s_UV_c
          ,sum(b_ds_UV_c) b_ds_UV_c
          ,sum(o_ds_order_c) o_ds_order_c
          ,sum(c_uv) c_uv
          ,sum(order_user_cnt_c) order_user_cnt_c
    from (
        select t1.dt
            ,t1.new_mdd
            ,t1.user_type
            ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
            ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
            ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
            ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
            ,count(distinct uid) as c_uv
            ,count(distinct t2.ubt_user_id) as order_user_cnt_c
        from c_uv t1 
        left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id and t1.new_mdd=t2.new_mdd
        group by 1,2,3
    ) t 
    group by 1,cube(new_mdd,user_type)
)t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by dt 
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when  user_type = '老客' then 3 end asc
;

--- 1.2 SDBO请求多晚单晚
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
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
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
        and order_date >= '2026-02-26'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_user_type as(   --- 用于判定c新老客
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
,c_uv as(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when provincename in ('澳门','香港') then '港澳'  
            when a.countryname in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
            when a.countryname in ('日本','韩国','泰国') then a.countryname 
            else '其他' end as new_mdd
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
    and  dt >= '2026-02-26' and dt <= date_sub(current_date, 1)
    group by 1,2,3,4,5,6
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
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
      and substr(order_date,1,10) >= '2026-02-26' 
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,s_uv as (
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,a.user_id
            ,count(1) search_pv
    from ihotel_default.dw_user_app_log_search_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-02-26' 
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2,3,4,5,6
)
,d_uv as (
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,a.user_id
            ,count(1) detail_pv
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-02-26' 
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2,3,4,5,6
)
,b_uv as (
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,a.user_id
            ,count(1) booking_pv
    from ihotel_default.dw_user_app_log_booking_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-02-26' 
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    group by 1,2,3,4,5,6
)   
select t1.dt
      ,t1.is_more_roomnight
      ,t1.user_type
      ,t1.mdd
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
        ,if(grouping(a.is_more_roomnight)=1,'ALL', a.is_more_roomnight) as  is_more_roomnight
        ,if(grouping(a.user_type)=1,'ALL', a.user_type) as  user_type
        ,if(grouping(a.new_mdd)=1,'ALL', a.new_mdd) as  mdd
        ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
        ,count(distinct case when booking_pv >0 and detail_pv >0  and search_pv >0 then  a.user_id else null end ) b_ds_UV
        ,count(distinct case when d.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order
    from  s_uv a   --- s流量
    left join d_uv b on a.dt=b.dt and a.user_id=b.user_id   --- d流量
    left join b_uv c on a.dt=c.dt and a.user_id=c.user_id  -- b流量
    left join q_app_order d on a.dt=d.order_date and a.user_id=d.user_id   -- 订单表
    group by 1,cube(a.is_more_roomnight,a.user_type,a.new_mdd)
)t1   
left join (---- C得DBO转化
    select t1.dt
        ,if(grouping(t1.is_more_roomnight)=1,'ALL', t1.is_more_roomnight) as  is_more_roomnight
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.new_mdd)=1,'ALL', t1.new_mdd) as  mdd
        ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
        ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
        ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
        ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
        ,count(distinct uid) as c_uv
        ,count(distinct t2.ubt_user_id) as order_user_cnt_c
    from c_uv t1 
    left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id 
    group by 1,cube(t1.is_more_roomnight,t1.user_type,t1.new_mdd)
)t2 on t1.dt=t2.dt  and t1.is_more_roomnight=t2.is_more_roomnight and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by 1 desc
;


--- 2、产品力数据
with qc_price as (
    select order_date
        ,if(grouping(is_more_roomnight)=1,'ALL', is_more_roomnight) as  is_more_roomnight
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            -- ,case   when province_name in ('澳门','香港') then '港澳'  
            --         when a.country_name in ('泰国','日本','韩国') then a.country_name  
            --         else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                    when a.country_name in ('日本','韩国','泰国') then a.country_name 
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
            ,case when datediff(check_out, check_in) >= 2 then '多晚' else '单晚' end is_more_roomnight
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260226' and dt <= replace(date_sub(current_date, 1),'-','')
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            -- and check_out >= '2026-02-01' and check_out <= '2026-02-05'
    )t
    group by order_date,cube(is_more_roomnight,user_type,new_mdd)
)

select order_date,mdd
      ,is_more_roomnight
      ,user_type
      ,`支付价lose率`
      ,`支付价lose深度`
      ,`支付价beat深度`
      ,`支付价beat数`       /   `支付价抓取次数`  `beat率`
      ,`支付价beat0-3%次数`   / `支付价抓取次数`  `支付价beat0-3%率`
      ,`支付价beat3-4%次数`   / `支付价抓取次数`  `支付价beat3-4%率`
      ,`支付价beat4-5%次数`   / `支付价抓取次数`  `支付价beat4-5%率`
      ,`支付价beat5-6%次数`   / `支付价抓取次数`  `支付价beat5-6%率`
      ,`支付价beat6-7%次数`   / `支付价抓取次数`  `支付价beat6-7%率`
      ,`支付价beat7-8%次数`   / `支付价抓取次数`  `支付价beat7-8%率`
      ,`支付价beat8%以上次数` /  `支付价抓取次数`  `支付价beat8%以上率`
from qc_price
order by 1,2,3,4
;

--- 3、佣金率、券补
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----分日去重活跃用户
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-02-26' 
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_uv_info as(   ---- 流量汇总
    select dt
        ,if(grouping(new_mdd)=1 ,'ALL' ,new_mdd) as  mdd
        ,if(grouping(user_type)=1 ,'ALL' ,user_type) as  user_type
        ,count(user_id)   uv
    from uv
    group by dt,cube(user_type, new_mdd)
) 
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
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
    from default.mdw_order_v3_international a 
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
        and order_date >= '2026-02-26'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info_app as ( --- q app 订单汇总
    select t1.order_date 
         ,if(grouping(t1.new_mdd)=1,'ALL', t1.new_mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,sum(final_commission_after) as `Q_佣金_app`
         ,sum(init_gmv) as `Q_GMV_app`
         ,sum(coupon_substract_summary) as `Q_券额_app`
         ,count(distinct order_no) as `Q_订单量_app`
         ,count(distinct t1.user_id) as `Q_下单用户_app`
         ,sum(room_night) as `Q_间夜量_app`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量_app`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量_app`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量_app`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量_app`
    from q_order_app t1
    group by t1.order_date,cube(t1.new_mdd,t1.user_type)
)
/******************************** 预定口径Q分区域分新老结果数据 ********************************/ 
,q_data_info as (
    select t1.dt
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.uv, 0)   as uv
            ,coalesce(t4.`Q_间夜量_app`, 0)  as `Q_间夜量_app`
            ,coalesce(t4.`Q_订单量_app`, 0)  as `Q_订单量_app`
            ,coalesce(t4.`Q_下单用户_app`, 0) as `Q_下单用户_app`
            ,coalesce(t4.`Q_GMV_app`, 0)      as `Q_GMV_app`
            ,coalesce(t4.`Q_佣金_app`, 0)      as `Q_佣金_app`
            ,coalesce(t4.`Q_券额_app`, 0)      as `Q_券额_app`
            ,coalesce(t4.`Q_高星间夜量_app`, 0)      as `Q_高星间夜量_app`
            ,coalesce(t4.`Q_中星间夜量_app`, 0)      as `Q_中星间夜量_app`
            ,coalesce(t4.`Q_低星间夜量_app`, 0)      as `Q_低星间夜量_app`
            ,coalesce(t4.`Q_订单量_app` / t1.uv, 0)  as `Q_CR_app`
            ,coalesce(t4.`Q_间夜量_app`, 0) / coalesce(t4.`Q_订单量_app`, 0) as `Q_单间夜_app`
            ,coalesce(t4.`Q_佣金_app`, 0)  /  coalesce(t4.`Q_GMV_app`, 0)   as `Q_收益率_app`
            ,coalesce(t4.`Q_券额_app`, 0)  /  coalesce(t4.`Q_GMV_app`, 0)   as `Q_券补贴率_app`
            ,coalesce(t4.`Q_GMV_app`, 0)  /  coalesce(t4.`Q_间夜量_app`, 0) as `Q_ADR_app`
            ,concat(round(coalesce(t4.`Q_用券订单量_app`, 0) / coalesce(t4.`Q_订单量_app`, 0) * 100, 1), '%') as `Q_用券订单占比_app`
    from q_uv_info t1 
    left join order_info_app t4 on t1.dt=t4.order_date and t1.mdd=t4.mdd 
    and t1.user_type=t4.user_type 
)
/**************************************** c相关数据 ****************************************/ 
,c_user_type as(   --- 用于判定c新老客
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
,c_uv as(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,case when provincename in ('澳门','香港') then '港澳'  
            when a.countryname in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
            when a.countryname in ('日本','韩国','泰国') then a.countryname 
            else '其他' end as new_mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2026-02-26'  and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            -- ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-02-26' 
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,c_uv_info as(  ---- c流量汇总
    select dt
           ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,sum(c_uv) as c_uv
    from c_uv
    group by dt,cube(user_type, new_mdd)
)
,c_order_info as(  ---- c订单汇总
    select dt
           ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,count(order_no) as `C_订单量`
           ,sum(room_night) as `C_间夜量`
           ,sum(room_fee)as `C_GMV`
           ,sum(comission) as `C_佣金`
           ,sum(cqe) as `C_券额`
           ,count(distinct user_id)  `C_下单用户`
           ,sum(case when star in (4,5) then room_night else 0 end) as `C_高星间夜量`
           ,sum(case when star in (3) then room_night else 0 end) as `C_中星间夜量`
           ,sum(case when star not in (3,4,5) then room_night else 0 end) as `C_低星间夜量`
    from c_order
    group by dt,cube(user_type, new_mdd)
)
/******************************** C分区域分新老结果数据 ********************************/ 
,c_data_info as (
    select t1.dt   
            ,t1.mdd
            ,t1.user_type  
            ,coalesce(t1.c_uv, 0)   as c_uv
            ,coalesce(t2.`C_间夜量`, 0) as `C_间夜量`
            ,coalesce(t2.`C_订单量`, 0)  as `C_订单量`
            ,coalesce(t2.`C_下单用户`, 0)   as `C_下单用户`
            ,coalesce(t2.`C_GMV`, 0)      as `C_GMV`
            ,coalesce(t2.`C_佣金`, 0)      as `C_佣金`
            ,coalesce(t2.`C_券额`, 0)      as `C_券额`
            ,coalesce(t2.`C_高星间夜量`, 0)      as `C_高星间夜量`
            ,coalesce(t2.`C_中星间夜量`, 0)      as `C_中星间夜量`
            ,coalesce(t2.`C_低星间夜量`, 0)      as `C_低星间夜量`
            ,coalesce(t2.`C_订单量` / t1.c_uv, 0)  as `C_CR`
            ,coalesce(t2.`C_间夜量`, 0) / coalesce(t2.`C_订单量`, 0)  as `C_单间夜`
            ,coalesce(t2.`C_佣金`, 0) / coalesce(t2.`C_GMV`, 0)  as `C_收益率`
            ,coalesce(t2.`C_券额`, 0) / coalesce(t2.`C_GMV`, 0)  as `C_券补贴率`
            ,coalesce(t2.`C_GMV`, 0) / coalesce(t2.`C_间夜量`, 0)  as `C_ADR`
    from c_uv_info t1 
    left join c_order_info t2 on t1.dt=t2.dt and t1.mdd=t2.mdd 
    and t1.user_type=t2.user_type
)

/******************************** QC分区域分新老结果数据 ********************************/ 
select t1.dt
        ,t1.mdd
        ,t1.user_type

        ,concat(round(`Q_间夜量_app` / `C_间夜量` *100, 2), '%') as `间夜QC`
        ,concat(round(uv / c_uv *100, 2), '%')  as `流量QC`
        ,concat(round(`Q_CR_app` / `C_CR` *100, 2), '%') as `转化QC`
        ,concat(round(`Q_佣金_app` / `C_佣金` *100, 2), '%') as `收益QC`
        ,concat(round((`Q_收益率_app` - `C_收益率`) *100, 2), '%')  as `收益率QC差`
        ,concat(round((`Q_券补贴率_app` - `C_券补贴率`) *100, 2), '%')  as `券补贴率QC差`
        ,concat(round(`Q_ADR_app` / `C_ADR` *100, 2), '%')   as `ADR_QC`
        ,concat(round(`Q_订单量_app` / `C_订单量` *100, 2), '%')  as `订单量QC`
        ,concat(round(`Q_单间夜_app` / `C_单间夜` *100, 2), '%') as `单间夜QC`

        ,uv
        ,c_uv
        ,`Q_间夜量_app`
        ,`C_间夜量`
        ,`C_佣金`
        ,`Q_佣金_app`

        ,concat(round(`Q_CR_app` * 100, 2), '%')  `Q_CR_app`
        ,concat(round(`C_CR` * 100, 2), '%')      `C_CR`
        ,concat(round(`Q_收益率_app` * 100, 2), '%') `Q_收益率_app` --佣金率
        ,concat(round(`C_收益率` * 100, 2), '%')     `C_收益率`     --佣金率

        ,concat(round(`Q_券补贴率_app` * 100, 2), '%') `Q_券补贴率_app`
        ,concat(round(`C_券补贴率` * 100, 2), '%')     `C_券补贴率`
        ,`Q_订单量_app`
        ,`C_订单量`

        ,`Q_GMV_app`
        ,`C_GMV`

        ,`Q_券额_app`
        ,`C_券额`

        ,`Q_下单用户_app`
        ,`C_下单用户`

        ,`Q_ADR_app`
        ,`C_ADR`

        ,`Q_用券订单占比_app`

        ,`Q_高星间夜量_app`
        ,`C_高星间夜量`

        ,`Q_中星间夜量_app`
        ,`C_中星间夜量`

        ,`Q_低星间夜量_app`
        ,`C_低星间夜量`

        ,`Q_单间夜_app`
        ,`C_单间夜`
from (---- 预定口径Q数据
    select dt, mdd,user_type,uv
           ,`Q_间夜量_app`
           ,`Q_订单量_app`
           ,`Q_下单用户_app`
           ,`Q_GMV_app`
           ,`Q_佣金_app`
           ,`Q_券额_app`
           ,`Q_高星间夜量_app`
           ,`Q_中星间夜量_app`
           ,`Q_低星间夜量_app`
           ,`Q_CR_app`
           ,`Q_单间夜_app`
           ,`Q_收益率_app`
           ,`Q_券补贴率_app`
           ,`Q_ADR_app`
           ,`Q_用券订单占比_app`
    from q_data_info
) t1
left join c_data_info t2   --- 预定口径C数据
on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by t1.dt 
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when  user_type = '老客' then 3 end asc
;

--- 4、T0取消率
with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_app_order as (----订单明细表表包含取消 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,order_no
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date)  
                          and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                          and (refund_time is null or date(refund_time) > order_date)
                    then 'Y' else 'N' end is_t0_cancel
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_date >= '2026-02-26' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_user_type as(   --- 用于判定c新老客
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
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,order_no
            ,case when (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
                    then 'Y' else 'N' end is_t0_cancel_c
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and substr(order_date,1,10) >= '2026-02-26'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)

select t1.order_date,t1.mdd,t1.user_type
      ,order_cnt,t0_cancel_order_cnt,t0_no_cancel_order_cnt
      ,t0_cancel_order_cnt / order_cnt t0_cancel_rate
      ,order_cnt_c
      ,t0_cancel_order_cnt_c
      ,t0_no_cancel_order_cnt_c
      ,t0_cancel_order_cnt_c / order_cnt_c t0_cancel_rate_c
from (
    select order_date,if(grouping(new_mdd)=1,'ALL',new_mdd) mdd,if(grouping(user_type)=1,'ALL',user_type)user_type
          ,count(distinct order_no) order_cnt
          ,count(distinct case when is_t0_cancel = 'N' then order_no end) t0_cancel_order_cnt
          ,count(distinct case when is_t0_cancel = 'Y' then order_no end) t0_no_cancel_order_cnt
    from q_app_order
    group by 1,cube(new_mdd,user_type)
) t1
left join (
    select dt,if(grouping(new_mdd)=1,'ALL',new_mdd) mdd,if(grouping(user_type)=1,'ALL',user_type)user_type
          ,count(distinct order_no) order_cnt_c
          ,count(distinct case when is_t0_cancel_c = 'N' then order_no end) t0_cancel_order_cnt_c
          ,count(distinct case when is_t0_cancel_c = 'Y' then order_no end) t0_no_cancel_order_cnt_c
    from c_order
    group by 1,cube(new_mdd,user_type)
) t2 on t1.order_date=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by 1
;

--- 5、顺畅度数据-分货源新老区域
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,wrapper_mapping as (
    select distinct id 
        ,supplier_wrapper_group as wrapper_id 
    from ihotel_default.ods_qta_supplier
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))=date_sub(current_date, 1) and inter_flag=1
)
,qc_room_mapping as (
    select 
        dt,
        product_id,
        partner_product_id
    from ihotel_default.dwd_supply_qc_product_mapping_di
    where dt >= '2026-02-26' and dt <= date_sub(current_date, 1)
    group by 1,2,3
)
,is_agent_mapping as (
    select 
        concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) d,
        product_id as room,
        grouptype
    from default.ceq_three_sync_pull_ctrip_qunar_adm_cq_fenxiao_detail a
    left join qc_room_mapping b on a.room = b.partner_product_id and concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) = b.dt
    where concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) >= '2026-02-26'
      and concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) <= date_sub(current_date, 1)
    group by 1,2,3
)

select a.datas as `日期`
       ,a.supplier,a.is_more_roomnight,a.user_type,a.mdd,
       `L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
       `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
       `B2O-房态房价一致率`,
       round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas,
            supplier,is_more_roomnight,user_type,mdd,
            round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
            round((1-e/a)*100,2) as `L2D-房态一致率`,
            round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
    from (
        select a.dt as  datas
                ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
                ,if(grouping(is_more_roomnight)=1,'ALL', is_more_roomnight) as  is_more_roomnight
                ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
                ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd,
                count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
                count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b,
                count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e
        from (
            select dt
                    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
                    ,case when province_name in ('澳门','香港') then '港澳'  
                        when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                        when a.country_name in ('日本','韩国','泰国') then a.country_name 
                        else '其他' end as new_mdd
                    ,case when dt > f.min_order_date then '老客' else '新客' end as user_type
                    ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
                    ,log_id
                    ,case 
                        when b.grouptype = 'DC' then 'DC'
                        when split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                        when d.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                        else '其他' end as supplier
                    ,ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice
                    ,ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price
                    ,regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full
                    ,get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up
                    ,get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult
                    ,split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] as supplier_id
                    ,split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[0] as room_id
                    ,action_entrance_map['fromforlog'] as is_list
                    ,checkin_date
                    ,checkout_date
                    ,a.user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
            left join is_agent_mapping b on split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[0] = b.room and a.dt = b.d
            left join wrapper_mapping c on split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] = c.id
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) d on c.wrapper_id = d.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where dt >= '2026-02-26' and dt <= date_sub(current_date, 1)
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                -- and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                -- and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
                and a.user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874','27117935','1263291304','1475831104','1534870051','175004090','223703725','428927726','1005465130','134486580','1534045148','169408570','185495343','185711487','263070154','125896658','140775252','1424343583','1554251482','1070931535','137263924','162660539','273860152','1409683183','1607050360','139741136','196432845')
        ) a
        where match_adult != 'false' or match_adult is null
        group by 1,cube(supplier,new_mdd,user_type,is_more_roomnight)
    ) a
) a
left join(
    select a.booking_date,
            supplier,is_more_roomnight,user_type,mdd,
            round((1-b/c)*100,2) as `D2B-房态一致率`,
            round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
            round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
                ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
                ,if(grouping(is_more_roomnight)=1,'ALL', is_more_roomnight) as  is_more_roomnight
                ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
                ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
                ,count(distinct case when ischange='true' and ret='true' then q_trace_id else null end) as a
                ,count(distinct if((ret='false' or ret is null),q_trace_id,null)) as b
                ,count(distinct q_trace_id) as c
        from(
            select dt,log_time,q_trace_id,ret,a.country_name,province_name,err_code,err_message,err_sys,ischange,a.user_id
                    ,case when b.grouptype = 'DC' then 'DC'
                         when supplier_id in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                         when c.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                         else '其他' end as supplier
                    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                          when e.area in ('欧洲','亚太','美洲') then e.area
                          else '其他' end as mdd
                    ,case when province_name in ('澳门','香港') then '港澳'  
                        when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                        when a.country_name in ('日本','韩国','泰国') then a.country_name 
                        else '其他' end as new_mdd
                    ,case when dt > f.min_order_date then '老客' else '新客' end as user_type
                    ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            from default.view_dw_user_app_booking_qta_di a
            left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) = b.d
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= '2026-02-26'
                 and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
                 and source='app_intl'
                 and platform in ('adr','ios')
                 and (province_name in ('香港','澳门','台湾') or a.country_name!='中国')
                 and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                 and q_trace_id not like 'f_inter_autotest%'
        )a
        group by 1,cube(supplier,new_mdd,user_type,is_more_roomnight)
    )a
) b on a.datas=b.booking_date and a.supplier = b.supplier and a.mdd=b.mdd and a.user_type=b.user_type and a.is_more_roomnight=b.is_more_roomnight
left join(
    select booking_date
         ,supplier
         ,is_more_roomnight
         ,user_type
         ,mdd
         ,round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
    from (
        select booking_date
                ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
                ,if(grouping(is_more_roomnight)=1,'ALL', is_more_roomnight) as  is_more_roomnight
                ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
                ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
            
                ,count(if((ret='false' or ret is null),user_id,null)) as total_submit_fail
                ,count(if((ret='false' or ret is null) and err_message='领券人与入住人不符',user_id,null)) as total_submit_coupon
                ,count(user_id) as total_submit_count
        from(
            select to_date(log_time) as booking_date
                    ,case when b.grouptype = 'DC' then 'DC'
                        when supplier_id in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                        when c.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                        else '其他' end as supplier
                    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他' end as mdd
                    ,case when province_name in ('澳门','香港') then '港澳'  
                        when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                        when a.country_name in ('日本','韩国','泰国') then a.country_name 
                        else '其他' end as new_mdd
                    ,case when dt > f.min_order_date then '老客' else '新客' end as user_type
                    ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
                    ,ret,err_message,a.user_id
            from default.dw_user_app_submit_qta_di a
            left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and to_date(log_time) = b.d
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= '2026-02-26'
                and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
                and source='app_intl'
                and platform in ('adr','ios','AndroidPhone','iPhone')
                and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
                and err_code not in( '-98','784','785')
        ) y group by 1,cube(supplier,new_mdd,user_type,is_more_roomnight)
    )a
) c on a.datas=c.booking_date and a.supplier = c.supplier and a.mdd=c.mdd and a.user_type=c.user_type and a.is_more_roomnight=c.is_more_roomnight
order by  1 desc
;

--- 6、货源


--- 7、订单价格带、星级、城市
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
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
            ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
            ,case when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
    from default.mdw_order_v3_international a 
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
        and order_date >= '2026-02-26'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info_app as ( --- q app 订单汇总
    select t1.order_date 
         ,if(grouping(t1.new_mdd)=1,'ALL', t1.new_mdd) as  mdd
         ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
         ,if(grouping(t1.per_type)=1,'ALL', t1.per_type) as  per_type
         ,if(grouping(t1.adr_type)=1,'ALL', t1.adr_type) as  adr_type
         ,sum(final_commission_after) as `Q_佣金_app`
         ,sum(init_gmv) as `Q_GMV_app`
         ,sum(coupon_substract_summary) as `Q_券额_app`
         ,count(distinct order_no) as `Q_订单量_app`
         ,count(distinct t1.user_id) as `Q_下单用户_app`
         ,sum(room_night) as `Q_间夜量_app`
         ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as `Q_用券订单量_app`
         ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as `Q_高星间夜量_app`
         ,sum(case when hotel_grade in (3) then room_night else 0 end ) as `Q_中星间夜量_app`
         ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as `Q_低星间夜量_app`
         ,count(distinct case when hotel_grade in (4,5) then order_no else 0 end ) as `Q_高星订单量_app`
         ,count(distinct case when hotel_grade in (3) then order_no else 0 end ) as `Q_中星订单量_app`
         ,count(distinct case when hotel_grade not in (3,4,5) then order_no else 0 end ) as `Q_低星订单量_app`
    from q_order_app t1
    group by t1.order_date,cube(t1.new_mdd,t1.user_type,t1.per_type,t1.adr_type)
)

/**************************************** c相关数据 ****************************************/ 
,c_user_type as(   --- 用于判定c新老客
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
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            -- ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 0 and 3    then '提前订1-3天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                  else '提前订31+'  
            end  per_type
            ,case when room_fee / extend_info['room_night'] < 400  then '1[0,400)'
                  when room_fee / extend_info['room_night'] >= 400 and  room_fee / extend_info['room_night'] < 800  then '2[400,800)'
                  when room_fee / extend_info['room_night'] >= 800 and  room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                  when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-02-26' 
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,c_order_info as(  ---- c订单汇总
    select dt
           ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
           ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
           ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
           ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
           ,count(order_no) as `C_订单量`
           ,sum(room_night) as `C_间夜量`
           ,sum(room_fee)as `C_GMV`
           ,sum(comission) as `C_佣金`
           ,sum(cqe) as `C_券额`
           ,count(distinct user_id)  `C_下单用户`
           ,sum(case when star in (4,5) then room_night else 0 end) as `C_高星间夜量`
           ,sum(case when star in (3) then room_night else 0 end) as `C_中星间夜量`
           ,sum(case when star not in (3,4,5) then room_night else 0 end) as `C_低星间夜量`
           ,count(distinct case when star in (4,5) then order_no else 0 end) as `C_高星订单量`
           ,count(distinct case when star in (3) then order_no else 0 end) as `C_中星订单量`
           ,count(distinct case when star not in (3,4,5) then order_no else 0 end) as `C_低星订单量`
    from c_order
    group by dt,cube(user_type, new_mdd, per_type, adr_type)
)


/******************************** QC分区域分新老结果数据 ********************************/ 
select t1.order_date
        ,t1.mdd
        ,t1.user_type
        ,t1.per_type
        ,t1.adr_type

        ,concat(round(`Q_间夜量_app` / `C_间夜量` *100, 2), '%') as `间夜QC`
        ,concat(round(`Q_佣金_app` / `C_佣金` *100, 2), '%') as `收益QC`
        ,concat(round((`Q_收益率_app` - `C_收益率`) *100, 2), '%')  as `收益率QC差`
        ,concat(round((`Q_券补贴率_app` - `C_券补贴率`) *100, 2), '%')  as `券补贴率QC差`
        ,concat(round(`Q_ADR_app` / `C_ADR` *100, 2), '%')   as `ADR_QC`
        ,concat(round(`Q_订单量_app` / `C_订单量` *100, 2), '%')  as `订单量QC`
        ,concat(round(`Q_单间夜_app` / `C_单间夜` *100, 2), '%') as `单间夜QC`

        ,`Q_间夜量_app`
        ,`C_间夜量`
        ,`C_佣金`
        ,`Q_佣金_app`

        ,concat(round(`Q_收益率_app` * 100, 2), '%') `Q_收益率_app` --佣金率
        ,concat(round(`C_收益率` * 100, 2), '%')     `C_收益率`     --佣金率

        ,concat(round(`Q_券补贴率_app` * 100, 2), '%') `Q_券补贴率_app`
        ,concat(round(`C_券补贴率` * 100, 2), '%')     `C_券补贴率`
        ,`Q_订单量_app`
        ,`C_订单量`

        ,`Q_GMV_app`
        ,`C_GMV`

        ,`Q_券额_app`
        ,`C_券额`

        ,`Q_下单用户_app`
        ,`C_下单用户`

        ,`Q_ADR_app`
        ,`C_ADR`


        ,`Q_高星间夜量_app`
        ,`C_高星间夜量`

        ,`Q_中星间夜量_app`
        ,`C_中星间夜量`

        ,`Q_低星间夜量_app`
        ,`C_低星间夜量`

        ,`Q_单间夜_app`
        ,`C_单间夜`
        ,`Q_高星订单量_app`
        ,`Q_中星订单量_app`
        ,`Q_低星订单量_app`
        ,`C_高星订单量`
        ,`C_中星订单量`
        ,`C_低星订单量`
from (---- 预定口径Q数据
    select order_date, mdd,user_type,per_type,adr_type
           ,`Q_间夜量_app`
           ,`Q_订单量_app`
           ,`Q_下单用户_app`
           ,`Q_GMV_app`
           ,`Q_佣金_app`
           ,`Q_券额_app`
           ,`Q_高星间夜量_app`
           ,`Q_中星间夜量_app`
           ,`Q_低星间夜量_app`
           ,`Q_高星订单量_app`
           ,`Q_中星订单量_app`
           ,`Q_低星订单量_app`
           ,`Q_间夜量_app` / `Q_订单量_app` `Q_单间夜_app`
           ,`Q_佣金_app` / `Q_GMV_app` `Q_收益率_app`
           ,`Q_券额_app` / `Q_GMV_app` `Q_券补贴率_app`
           ,`Q_GMV_app` / `Q_间夜量_app` `Q_ADR_app`
    from order_info_app
) t1
left join (---- 预定口径C数据
    select dt, mdd,user_type,per_type,adr_type
           ,`C_间夜量`
           ,`C_订单量`
           ,`C_下单用户`
           ,`C_GMV`
           ,`C_佣金`
           ,`C_券额`
           ,`C_高星间夜量`
           ,`C_中星间夜量`
           ,`C_低星间夜量`
           ,`C_高星订单量`
           ,`C_中星订单量`
           ,`C_低星订单量`
           ,`C_间夜量` / `C_订单量` `C_单间夜`
           ,`C_佣金` / `C_GMV` `C_收益率`
           ,`C_券额` / `C_GMV` `C_券补贴率`
           ,`C_GMV` / `C_间夜量` `C_ADR`
    from c_order_info
) t2   --- 预定口径C数据
on t1.order_date=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type and t1.per_type=t2.per_type and t1.adr_type=t2.adr_type
order by t1.order_date 
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when  user_type = '老客' then 3 end asc
;


--- 8、离店时间分布
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then '港澳'  
                   when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                   when a.country_name in ('日本','韩国','泰国') then a.country_name 
                   else '其他' end as new_mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,checkout_date
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
            ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
            ,case when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
    from default.mdw_order_v3_international a 
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
        and order_date >= '2026-02-26'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_info_app as ( --- q app 订单汇总
    select t1.order_date,mdd
         ,if(grouping(t1.checkout_date)=1,'ALL', t1.checkout_date) as  checkout_date
         ,count(distinct order_no) as `Q_订单量_app`
    from q_order_app t1 where user_type = '新客' and mdd in ('日本','泰国')
    group by t1.order_date,mdd,cube(t1.checkout_date)
)

/**************************************** c相关数据 ****************************************/ 
,c_user_type as(   --- 用于判定c新老客
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
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            -- ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,substr(checkout_date,1,10) checkout_date
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 0 and 3    then '提前订1-3天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                  else '提前订31+'  
            end  per_type
            ,case when room_fee / extend_info['room_night'] < 400  then '1[0,400)'
                  when room_fee / extend_info['room_night'] >= 400 and  room_fee / extend_info['room_night'] < 800  then '2[400,800)'
                  when room_fee / extend_info['room_night'] >= 800 and  room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                  when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-02-26' 
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,c_order_info as(  ---- c订单汇总
    select dt,mdd
           ,if(grouping(checkout_date)=1,'ALL', checkout_date) as  checkout_date
           ,count(distinct order_no) as `C_订单量`
    from c_order where user_type = '新客' and mdd in ('日本','泰国')
    group by dt,mdd,cube(checkout_date)
)

/******************************** QC分区域分新老结果数据 ********************************/ 
select t1.order_date
        ,t1.mdd
        ,t1.checkout_date
        ,`Q_订单量_app`
        ,`C_订单量`

from (---- 预定口径Q数据
    select order_date, mdd,checkout_date
           ,`Q_订单量_app`
    from order_info_app
) t1
left join (---- 预定口径C数据
    select dt, mdd,checkout_date
           ,`C_订单量`
    from c_order_info
) t2   --- 预定口径C数据
on t1.order_date=t2.dt and t1.mdd=t2.mdd and t1.checkout_date=t2.checkout_date
order by t1.order_date ,t1.checkout_date
;


--- 9、产品功能

