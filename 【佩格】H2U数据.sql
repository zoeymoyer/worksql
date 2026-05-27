
with uv as( --分日去重活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= date_sub(current_date, 3)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,h_uv as (
    select dt,user_name
    from ihotel_default.dw_qav_hotel_track_info_di_v1
    where dt >= date_sub(current_date, 3)
        and dt <= date_sub(current_date, 1)
        and key = 'hotel/home/searchCard/show/keyword'
        and get_json_object(value, '$.ext.tab') = 'Overseas'
        and user_name is not null and user_name not in ('null', 'NULL', '', ' ')
    group by 1,2
) 
,act_uv as (
    select dt,user_name
    from ihotel_default.dw_qav_hotel_track_info_di_v1
    where dt >= date_sub(current_date, 3)
        and dt <= date_sub(current_date, 1)
        and key in ('hotel/home/bottomEntrance/click/entrance','hotel/global/home/recommendHotelInter/click')
        and user_name is not null and user_name not in ('null', 'NULL', '', ' ')
    group by 1,2
)


select t1.dt,count(distinct t1.user_name) h_uv,count(distinct t2.user_name) has_behavior_uv,count(distinct t3.user_name) act_uv
        ,count(distinct case when t2.user_name is not null then t3.user_name end) has_behavior_act_uv,count(distinct case when t2.user_name is not null and t3.user_name is null then t1.user_name end) has_behavior_no_act_uv
        ,count(distinct case when t2.user_name is null then t3.user_name end) no_behavior_act_uv,count(distinct case when t2.user_name is null and t3.user_name is null then t1.user_name end) no_behavior_no_act_uv
from h_uv t1
left join act_uv t2 on t1.dt = t2.dt and t1.user_name = t2.user_name
left join uv t3 on t1.dt = t3.dt and t1.user_name = t3.user_name
group by 1
order by 1
;



-- H页和L页流量表去重 重合数
SELECT COUNT(*) AS overlap_count
FROM (
    SELECT DISTINCT user_name
    FROM ihotel_default.dw_user_app_log_search_di_v1
    WHERE user_name IS NOT NULL
  and dt = '2026-05-13'
) a
INNER JOIN (
    SELECT DISTINCT user_name
    FROM default.dw_qav_hotel_track_info_di
    WHERE dt = '20260513'
        AND key = 'hotel/home/searchCard/show/keyword'
        AND get_json_object(value, 'ext.tab') = 'Overseas'
        AND user_name IS NOT NULL
) b
ON a.user_name = b.user_name

-- H页uv
SELECT 
       dt,
        count(distinct user_name)
    FROM default.dw_qav_hotel_track_info_di
    WHERE dt BETWEEN '20260503' AND '20260517'
        AND key = 'hotel/home/searchCard/show/keyword'
        AND get_json_object(value, '$.ext.tab') = 'Overseas'
        AND user_name IS NOT NULL
        group by dt
        
        

;


with q_data as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when a.country_name = '日本'  then  '日本' else '非日本' end is_jp
           
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status,init_gmv,room_night
            --- 预定当日是否取消或拒单
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
                   and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                   and (refund_time is null or date(refund_time) > order_date) 
                   then 'Y' else 'N' 
            end is_not_cancel_d0 
            --- 是否取消订单，剔除了预定当日
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                   or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                   or (refund_time is not null and date(refund_time) > order_date) 
                   then 'Y' else 'N' 
            end is_cancel_d0  
            --- 是否不可取消订单，Y不可取消订单。 使用spark引擎，值=2为可取消订单，但存在null和空情况占比2%左右。数据最早看25年4月份之后
            ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end is_non_ref 
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
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-01-01' and date_sub(current_date, 1)
)
,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                  when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                  when c.area in ('欧洲','亚太','美洲') then c.area
                  else '其他' 
            end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,o.user_id,order_no,room_fee,order_status
            ,extend_info['room_night'] room_night
            --- 预定当日是否有取消单或被拒单
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' 
            end as supplier
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
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
)


select t1.checkout_date, t1.is_jp
      ,`Q订单`,`Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C非当日取消率`,`C不可取消订单`, `C不可取消订单占比`
      
from (
    select checkout_date
        ,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from  q_data
    group by checkout_date,cube(is_jp)
) t1 left join (
    select checkout_date
          ,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C非当日取消率`
    from c_order
    group by checkout_date,cube(is_jp)

)t2 on t1.checkout_date=t2.checkout_date and t1.is_jp=t2.is_jp 
order by 1,2
;

with user_type as(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
) 
,h_uv as (
    select dt,user_name
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                    when e.area in ('欧洲','亚太','美洲') then e.area
                    else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
    from ihotel_default.dw_qav_hotel_track_info_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_name = b.user_name 
    where dt >= '2026-05-12'
        and dt <= date_sub(current_date, 1)
        and key = 'hotel/home/searchCard/show/keyword'
        and get_json_object(value, '$.ext.tab') = 'Overseas'
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
) 
,uv as (----分日去重活跃用户
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
    where dt >= '2026-05-12' and  dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_order_app as (----订单明细表  app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-05-12' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
select  dt
        ,h_uv
        ,uv
        ,d_s_UV / s_all_UV   s2d
        ,b_ds_UV / d_s_UV   d2b
        ,o_ds_order / b_ds_UV  b2o
        ,uv / h_uv  h2u
        ,o_ds_order / h_uv  h2o
        ,s_all_UV,d_s_UV,b_ds_UV,o_ds_order
from (
    select dt
        ,sum(h_uv) h_uv
        ,sum(uv) uv
        ,sum(s_all_UV) s_all_UV
        ,sum(d_s_UV) d_s_UV
        ,sum(b_ds_UV) b_ds_UV
        ,sum(o_ds_order) o_ds_order
    from (
        select t1.dt,t2.mdd
                ,count(distinct t1.user_name) h_uv
                ,count(distinct t2.user_id) uv
                ,count(distinct case when search_pv >0 then  t2.user_id else null end )s_all_UV
                ,count(distinct case when detail_pv >0 and search_pv >0 then t2.user_id else null end) d_s_UV
                ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t2.user_id else null end ) b_ds_UV
                ,count(distinct case when t5.user_id is not null and detail_pv >0 and search_pv >0 then t5.order_no else null end ) o_ds_order
        from h_uv t1 
        left join uv t2 on t1.dt = t2.dt and t1.user_name = t2.user_name
        left join q_order_app t5 on t2.user_id=t5.user_id and t2.dt = t5.order_date and t2.mdd = t5.mdd
        group by 1,2
    )
    group by dt
)
order by 1
;

--- 分实验sdbo数据分析
with user_type as (
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
,biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date,
                user_info['orig_device_id'] as orig_device_id,user_id,user_name,
                count(order_no) as order_nos_90,
                sum(room_night) as room_nights_90
            from mdw_order_v3_international
            where dt = '%(DATE)s'
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2,3,4
        )a where room_nights_90>=15
    group by 1
)
,abtest AS (--- 实验明细
    select  dt,
            ab_version version,
            ab_exp_value AS user_id
            ,b.user_name
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    left join pub.dim_user_profile_nd b on a.ab_exp_value = b.user_id
    where a.dt >= '2026-05-12'  and dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '260330_ho_gj_PriceDetailOptimize'
        and ab_exp_value not in (select user_id from biguser)  --- 去除大单用户
    group by 1,2,3,4
)
,uv as (----分日去重活跃用户
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
    where dt >= '2026-05-12' and  dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_order_app as (----订单明细表  app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-05-12' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,ld_price_info as ( ---LD页税费明细
    select dt
            ,user_name
    from ihotel_default.dw_qav_hotel_track_info_di_v1
    where dt >= '2026-05-12'
    and key in(
        'ihotel/list/listPage/show/listPriceDetailShow'
        ,'ihotel/detail/detailPage/show/detailPriceDetailShow'
    )
    group by 1,2
)
,b_price_info as ( ---B页税费明细
    select dt
            ,user_name,count(1) as b_price_pv
    from ihotel_default.dw_qav_hotel_track_info_di_v1
    where dt >= '2026-05-12'
    and key in('ihotel/booking/bookingPage/show/feeInfoShow')
    group by 1,2
)
,sdbo_info as (  --- SDBO
    select dt,version
        ,sum(q_uv) q_uv

        ,sum(s_all_UV) s_all_UV
        ,sum(d_s_UV) d_s_UV
        ,sum(b_ds_UV) b_ds_UV
        ,sum(o_ds_order) o_ds_order

        ,sum(ld_UV_ld) ld_UV
        ,sum(s_all_UV_ld) s_all_UV_ld  
        ,sum(d_s_UV_ld) d_s_UV_ld
        ,sum(b_ds_UV_ld) b_ds_UV_ld
        ,sum(o_ds_order_ld) o_ds_order_ld  

        ,sum(b_UV_b) b_UV
        ,sum(b_ds_UV_b) b_ds_UV_b
        ,sum(o_ds_order_b) o_ds_order_b

        ,sum(case when user_type='新客' then b_UV_b else 0 end) b_UV_new
        ,sum(case when user_type='新客' then b_ds_UV_b else 0 end) b_ds_UV_new
        ,sum(case when user_type='新客' then o_ds_order_b else 0 end) o_ds_order_new
        ,sum(case when user_type='老客' then b_UV_b else 0 end) b_UV_old
        ,sum(case when user_type='老客' then b_ds_UV_b else 0 end) b_ds_UV_old
        ,sum(case when user_type='老客' then o_ds_order_b else 0 end) o_ds_order_old
    from (
        select t1.dt,version,t1.mdd,t1.user_type
                ,count(distinct t1.user_id) q_uv
                ,count(distinct case when search_pv >0 then  t1.user_id else null end )s_all_UV
                ,count(distinct case when detail_pv >0 and search_pv >0 then t1.user_id else null end) d_s_UV
                ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.user_id else null end ) b_ds_UV
                ,count(distinct case when t5.user_id is not null and detail_pv >0 and search_pv >0 then t5.order_no else null end ) o_ds_order

                ,count(distinct t3.user_name) ld_UV_ld
                ,count(distinct case when t3.user_name is not null and search_pv >0 then  t1.user_id else null end ) s_all_UV_ld
                ,count(distinct case when t3.user_name is not null and detail_pv >0 and search_pv >0 then t1.user_id else null end) d_s_UV_ld
                ,count(distinct case when t3.user_name is not null and booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.user_id else null end ) b_ds_UV_ld
                ,count(distinct case when t3.user_name is not null and t5.user_id is not null and detail_pv >0 and search_pv >0 then t5.order_no else null end ) o_ds_order_ld

                ,count(distinct t4.user_name) b_UV_b
                ,count(distinct case when t4.user_name is not null and booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.user_id else null end ) b_ds_UV_b
                ,count(distinct case when t4.user_name is not null and t5.user_id is not null and detail_pv >0 and search_pv >0 then t5.order_no else null end ) o_ds_order_b
        from uv t1 
        left join abtest t2 on t1.user_name=t2.user_name and t1.dt = t2.dt
        left join ld_price_info t3 on t1.user_name=t3.user_name and t1.dt = t3.dt
        left join b_price_info t4 on t1.user_name=t4.user_name and t1.dt = t4.dt
        left join q_order_app t5 on t1.user_id=t5.user_id and t1.dt = t5.order_date and t1.mdd = t5.mdd
        where t2.user_name is not null  --- 筛选实验用户
        group by 1,2,3,4
    )
    group by dt,version
)

select t1.*
    ,t2.b_price_uv,t2.b_price_pv
    ,t2.b_price_uv_new,t2.b_price_pv_new
    ,t2.b_price_uv_old,t2.b_price_pv_old
    ,t2.b_price_pv / t2.b_price_uv as b_price_puv
    ,t2.b_price_pv_new / t2.b_price_uv_new as b_price_puv_new
    ,t2.b_price_pv_old / t2.b_price_uv_old as b_price_puv_old
from (
    select dt
        ,version
        ,q_uv   `sdbo_uv`
        ,d_s_UV / s_all_UV   s2d
        ,b_ds_UV / d_s_UV   d2b
        ,o_ds_order / b_ds_UV  b2o
        ,ld_UV  `浏览LD页价格明细uv`
        ,d_s_UV_ld / s_all_UV_ld   `s2d_浏览LD页价格明细`
        ,b_ds_UV_ld / d_s_UV_ld   `d2b_浏览LD页价格明细`
        ,o_ds_order_ld / b_ds_UV_ld  `b2o_浏览LD页价格明细`
        ,b_UV  `浏览B页价格明细uv`
        ,o_ds_order_b / b_ds_UV_b  `b2o_浏览B页价格明细`
        ,o_ds_order_new / b_ds_UV_new  `b2o_浏览B页价格明细_新客`
        ,o_ds_order_old / b_ds_UV_old  `b2o_浏览B页价格明细_老客`
        ,s_all_UV
        ,d_s_UV
        ,b_ds_UV
        ,o_ds_order
        ,s_all_UV_ld
        ,d_s_UV_ld
        ,b_ds_UV_ld
        ,o_ds_order_ld
        ,b_ds_UV_b
        ,o_ds_order_b
        ,b_ds_UV_new
        ,o_ds_order_new
        ,b_ds_UV_old
        ,o_ds_order_old 
    from sdbo_info
)t1 
left join (
    select t1.dt,t3.version
        ,count(distinct t1.user_name) b_price_uv
        ,sum(t2.b_price_pv) b_price_pv
        ,count(distinct case when user_type='新客' then t1.user_name else null end) b_price_uv_new
        ,sum(case when user_type='新客' then t2.b_price_pv else 0 end) b_price_pv_new
        ,count(distinct case when user_type='老客' then t1.user_name else null end) b_price_uv_old
        ,sum(case when user_type='老客' then t2.b_price_pv else 0 end) b_price_pv_old
    from uv t1
    left join b_price_info t2 on t1.user_name = t2.user_name and t1.dt = t2.dt
    left join abtest t3 on t1.user_name = t3.user_name and t1.dt = t3.dt
    where t3.user_name is not null and t2.user_name is not null
    group by 1,2
)t2 on t1.dt = t2.dt and t1.version = t2.version
order by 1,2
;