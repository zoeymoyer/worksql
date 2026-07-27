
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
    select  user_id,orig_device_id
    from(
            select
                order_date,
                user_info['orig_device_id'] as orig_device_id,user_id,user_name,
                count(order_no) as order_nos_90,
                sum(room_night) as room_nights_90
            from default.mdw_order_v3_international
            where dt =  from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2,3,4
        )a where room_nights_90>=15
    group by 1,2
)
,ab_rule as (
    select concat(substr(rule.dt, 1, 4), '-', substr(rule.dt, 5, 2), '-', substr(rule.dt, 7, 2)) as dt
         ,ab_version       --- 实验版本
         ,ab_rule_version  --- 实验版本号
         ,device_id as user_id  -- user_id或者设备id，具体看分流维度
         ,b.user_name
    from (
        select ab_exp_id,
               ab_version,
               ab_rule_version,dt
        from default.ods_abtest_rule_info
        where dt >= '20260527' and dt <= replace(date_sub(current_date, 1), '-', '')
          and source = 'hotel'
          and ab_shuntbase = 'USERID'   --- APP_UID 和 USERID 分流
          and ab_exp_id = '260430_ho_gj_B2OD2Bbianjia'
    ) rule
    join (
        select expid,
               version,
               ruleversion,
               clientcode as device_id,
               dt,
               logdate
        from default.ods_abtest_sdk_log_endtime_hotel
        where dt >= '20260527' and dt <= replace(date_sub(current_date, 1), '-', '')
          and clientcode is not null
          and expid is not null
          and version is not null
          and ruleversion is not null
          and expid != ''
          and version != ''
          and clientcode not in (
              '0', '00000000', '00000000000000', '000000000000000',
              '0000000000000000', '0000000000000000000000000000000000000000',
              '', 'ctrip', 'elong', '352284040670808'
          )
          and (clientcode not like 'tc%' and clientcode not like 'wx%' and clientcode not like 'pd%')
          -- 分类是USERID，clientcode是user_id，分流是APP_UID，clientcode是orig_device_id
          and clientcode not in (select user_id from biguser) -- 去除大单用户   
    ) ab on ab.expid = rule.ab_exp_id
        and ab.version = rule.ab_version
        and ab.ruleversion = rule.ab_rule_version
        and ab.dt = rule.dt
    left join pub.dim_user_profile_nd b on ab.device_id = b.user_id
    group by 1, 2, 3, 4, 5
)
,q_order_app as (----订单明细表  app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,user_name
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
            ,get_json_object(extendinfomap,'$.traceId') as traceId
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
        and order_date >= '2026-05-27' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,track_base as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
           ,user_name
           ,key
           ,get_json_object(value, '$.ext.tipType') as tipType
           ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260527' and dt <= replace(date_sub(current_date, 1), '-', '')
        and key in (
            'ihotel/booking/bookingPage/monitor/bookingContainerShow'  -- B页曝光
            ,'ihotel/Booking/Access/show/priceChangePopupExposure'  -- 变价或房态变化弹窗曝光
        )
        and user_name is not null
    group by 1,2,3,4,5
)

select t1.dt,ab_version
    ,count(distinct case when t1.key='ihotel/booking/bookingPage/monitor/bookingContainerShow' then t1.user_name end) `B页曝光uv`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' then t1.user_name end) `变价或房态变化弹窗曝光uv`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' and tipType in ('zhangjia','jiangjia') then t1.user_name end) `变价弹窗曝光uv`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' and tipType = 'roomChange' then t1.user_name end) `房态变化弹窗曝光uv`
    
    ,count(distinct t3.order_no) `生单量`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' then t3.order_no end) `弹出后生单量`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' and tipType in ('zhangjia','jiangjia') then t3.order_no end) `弹出变价后生单量`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' and tipType = 'roomChange' then t3.order_no end) `弹出房态变化后生单量`

    ,count(distinct case when t1.key='ihotel/booking/bookingPage/monitor/bookingContainerShow' then t1.trace_id end) `B页曝光量`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' then t1.trace_id end) `变价或房态变化弹窗曝光量`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' and tipType in ('zhangjia','jiangjia') then t1.trace_id end) `变价弹窗曝光量`
    ,count(distinct case when t1.key='ihotel/Booking/Access/show/priceChangePopupExposure' and tipType in ('roomChange') then t1.trace_id end) `房态变化弹窗曝光量`
from track_base t1 
left join ab_rule t2 on t1.dt=t2.dt and t1.user_name=t2.user_name
left join q_order_app t3 on t1.dt=t3.order_date and t1.user_name=t3.user_name 
where t2.user_name is not null
group by 1,2
order by 1,2
;


, uv as (
    select dt
         , t.ab_version
         , t.ab_rule_version
         , case
               when province_name in ('澳门', '香港') then province_name
               when a.country_name in ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯') then a.country_name
               when e.area in ('欧洲', '亚太', '美洲') then e.area
               else '其他'
           end as mdd
         , case
               when dt > b.min_order_date then '老客'
               else '新客'
           end as user_type
         , a.user_id
         , a.user_name
         , if(no_user_id is null, '正常用户', '大单用户') as is_big_order_user
         , sum(search_pv) as search_pv
         , sum(detail_pv) as detail_pv
         , sum(booking_pv) as booking_pv
         , sum(order_pv) as order_pv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id
    left join no_user on a.user_id = no_user.no_user_id
    right join ab_rule t on t.ab_exp_value = a.orig_device_id
    where dt = '2026-05-18'
      and business_type = 'hotel'
      and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
      and (search_pv + detail_pv + booking_pv + order_pv) > 0
      and a.user_name is not null
      and a.user_name not in ('null', 'NULL', '', ' ')
      and a.user_id is not null
      and a.user_id not in ('null', 'NULL', '', ' ')
      and a.country_name = '日本'
      and if(no_user_id is null, '正常用户', '大单用户') = '正常用户'
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

, q_order_app as (
    select order_date
         , t.ab_version
         , t.ab_rule_version
         , case
               when province_name in ('澳门', '香港') then province_name
               when a.country_name in ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯') then a.country_name
               when e.area in ('欧洲', '亚太', '美洲') then e.area
               else '其他'
           end as mdd
         , case
               when order_date = b.min_order_date then '新客'
               else '老客'
           end as user_type
         , a.user_id
         , init_gmv
         , order_no
         , room_night
         , batch_series
         , hotel_grade
         , coupon_id
         , init_commission_after
         , case
               when coupon_id is not null
                   and batch_series not in ('MacaoDisco_ZK_5e27de', '2night_ZK_952825', '3night_ZK_ad8c83')
                   and batch_series not like '%23base_ZK_728810%'
                   and batch_series not like '%23extra_ZK_ce6f99%'
                   then 'Y'
               else 'N'
           end as is_user_conpon
         , case
               when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                   then (init_commission_after
                         + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0)
                         + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0)
                         + coalesce(ext_plat_certificate, 0))
               else init_commission_after + coalesce(ext_plat_certificate, 0)
           end as final_commission_after
         , case
               when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                   then (init_commission_after_new
                         + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0)
                         + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0)
                         + coalesce(ext_plat_certificate, 0))
               else init_commission_after_new + coalesce(ext_plat_certificate, 0)
           end as qyj
         , case
               when coalesce(four_a, third_a) is not null and dt <= '20221124'
                   then round(coalesce(
                                 ((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),
                                 (((bp + final_cost) * (1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)
                             ), 2)
               when coalesce(four_a, third_a) is not null and order_date <= '2024-03-29'
                   then coalesce(four_a_reduce, third_a_reduce) * room_night
               else coalesce(cashbackmap['follow_price_amount'] * room_night, 0)
           end as zbj
         , coalesce(get_json_object(extendinfomap, '$.frame_amount'), 0) * room_night as xyb
         , coalesce(cashbackmap['framework_amount'], 0) as qb
         , coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) as jf_amt
         , case
               when coupon_substract_summary is null
                    or batch_series like '%23base_ZK_728810%'
                    or batch_series like '%23extra_ZK_ce6f99%'
                   then 0
               else coalesce(coupon_substract_summary, 0)
           end as coupon_substract_summary
         , if(no_user_id is null, '正常用户', '大单用户') as is_big_order_user
    from default.mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join no_user on a.user_id = no_user.no_user_id
    right join ab_rule t on t.ab_exp_value = a.user_info['orig_device_id']
    where dt = '20260518'
      and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
      and terminal_channel_type = 'app'
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
      and (first_rejected_time is null or date(first_rejected_time) > order_date)
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid = '1'
      and order_date = '2026-05-18'
      and order_no <> '103576132435'
      and a.country_name = '日本'
      and if(no_user_id is null, '正常用户', '大单用户') = '正常用户'
)

, q_flow_info as (
    select t.dt
         , t.ab_version
         , t.ab_rule_version
         , t.uv
         , t.s_all_uv
         , t.d_all_uv
         , t.b_all_uv
         , t.d_s_uv
         , t.b_ds_uv
         , t.o_ds_order
         , case when t.s_all_uv > 0 then 1.0 * t.d_s_uv / t.s_all_uv else 0 end as s2d
         , case when t.d_s_uv > 0 then 1.0 * t.b_ds_uv / t.d_s_uv else 0 end as d2b
         , case when t.b_ds_uv > 0 then 1.0 * t.o_ds_order / t.b_ds_uv else 0 end as b2o
         , case when t.s_all_uv > 0 then 1.0 * t.o_ds_order / t.s_all_uv else 0 end as s2o
    from (
        select a.dt
             , a.ab_version
             , a.ab_rule_version
             , count(distinct a.user_id) as uv
             , count(distinct case when a.search_pv > 0 then a.user_id end) as s_all_uv
             , count(distinct case when a.detail_pv > 0 then a.user_id end) as d_all_uv
             , count(distinct case when a.booking_pv > 0 then a.user_id end) as b_all_uv
             , count(distinct case when a.detail_pv > 0 and a.search_pv > 0 then a.user_id end) as d_s_uv
             , count(distinct case when a.booking_pv > 0 and a.detail_pv > 0 and a.search_pv > 0 then a.user_id end) as b_ds_uv
             , count(distinct case when b.user_id is not null and a.detail_pv > 0 and a.search_pv > 0 then b.order_no end) as o_ds_order
        from uv a
        left join q_order_app b
            on a.dt = b.order_date
            and a.user_id = b.user_id
            and a.ab_version = b.ab_version
            and a.ab_rule_version = b.ab_rule_version
        group by a.dt, a.ab_version, a.ab_rule_version
    ) t
)

, q_order_info_app as (
    select order_date as dt
         , ab_version
         , ab_rule_version
         , sum(final_commission_after) as q_commission_app
         , sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_commission_c_view_app
         , sum(init_gmv) as q_gmv_app
         , sum(coupon_substract_summary) as q_coupon_amount_app
         , count(distinct order_no) as q_order_cnt_app
         , count(distinct user_id) as q_order_user_cnt_app
         , sum(room_night) as q_room_night_app
         , count(distinct case when is_user_conpon = 'Y' then order_no end) as q_coupon_order_cnt_app
    from q_order_app
    group by order_date, ab_version, ab_rule_version
)

, q_app_order as (
    select order_date
         , t.ab_version as ab_version
         , t.ab_rule_version as ab_rule_version
         , count(distinct order_no) as order_no_q
         , count(distinct case
                             when (first_cancelled_time is null or date(first_cancelled_time) > order_date)
                                 and (first_rejected_time is null or date(first_rejected_time) > order_date)
                                 and (refund_time is null or date(refund_time) > order_date)
                                 then order_no
                          end) as no_t0_cancel_order_no_q
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join no_user on a.user_id = no_user.no_user_id
    right join ab_rule t on t.ab_exp_value = a.user_info['orig_device_id']
    where dt = '20260518'
      and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_date = '2026-05-18'
      and order_no <> '103576132435'
      and if(no_user_id is null, '正常用户', '大单用户') = '正常用户'
      and a.country_name = '日本'
    group by 1, 2, 3
)

, q_data_info as (
    select f.dt
         , f.ab_version
         , f.ab_rule_version
         , coalesce(f.uv, 0) as uv
         , coalesce(f.s_all_uv, 0) as s_all_uv
         , coalesce(f.d_all_uv, 0) as d_all_uv
         , coalesce(f.b_all_uv, 0) as b_all_uv
         , coalesce(f.d_s_uv, 0) as d_s_uv
         , coalesce(f.b_ds_uv, 0) as b_ds_uv
         , coalesce(f.o_ds_order, 0) as o_ds_order
         , coalesce(f.s2d, 0) as s2d
         , coalesce(f.d2b, 0) as d2b
         , coalesce(f.b2o, 0) as b2o
         , coalesce(f.s2o, 0) as s2o
         , coalesce(o.q_room_night_app, 0) as q_room_night_app
         , coalesce(o.q_order_cnt_app, 0) as q_order_cnt_app
         , coalesce(o.q_order_user_cnt_app, 0) as q_order_user_cnt_app
         , coalesce(o.q_gmv_app, 0) as q_gmv_app
         , coalesce(o.q_commission_app, 0) as q_commission_app
         , coalesce(o.q_commission_c_view_app, 0) as q_commission_c_view_app
         , coalesce(o.q_coupon_amount_app, 0) as q_coupon_amount_app
         , coalesce(o.q_coupon_order_cnt_app, 0) as q_coupon_order_cnt_app
         , case when coalesce(f.uv, 0) > 0 then 1.0 * coalesce(o.q_order_cnt_app, 0) / f.uv else 0 end as q_cr_app
         , case when coalesce(o.q_order_cnt_app, 0) > 0 then 1.0 * coalesce(o.q_room_night_app, 0) / o.q_order_cnt_app else 0 end as q_avg_rn_per_order_app
         , case when coalesce(o.q_gmv_app, 0) > 0 then 1.0 * coalesce(o.q_commission_app, 0) / o.q_gmv_app else 0 end as q_take_rate_app
         , case when coalesce(o.q_gmv_app, 0) > 0 then 1.0 * coalesce(o.q_coupon_amount_app, 0) / o.q_gmv_app else 0 end as q_subsidy_rate_app
         , case when coalesce(o.q_room_night_app, 0) > 0 then 1.0 * coalesce(o.q_gmv_app, 0) / o.q_room_night_app else 0 end as q_adr_app
         , case when coalesce(o.q_order_cnt_app, 0) > 0 then 1.0 * coalesce(o.q_coupon_order_cnt_app, 0) / o.q_order_cnt_app else 0 end as q_coupon_order_rate_app
    from q_flow_info f
    left join q_order_info_app o
        on f.dt = o.dt
        and f.ab_version = o.ab_version
        and f.ab_rule_version = o.ab_rule_version
)

select t1.dt as dt
     , t1.ab_version as ab_version
     , case when t1.uv > 0 then 1.0 * t1.q_commission_app / t1.uv else 0 end as subsidy_per_uv
     , t1.uv
     , t1.s2d
     , t1.d2b
     , t1.b2o
     , t1.q_cr_app as cr
     , case when t1.uv > 0 then 1.0 * t1.q_order_user_cnt_app / t1.uv else 0 end as u2o
     , case when coalesce(t4.order_no_q, 0) > 0
            then 1.0 * (coalesce(t4.order_no_q, 0) - coalesce(t4.no_t0_cancel_order_no_q, 0)) / t4.order_no_q
            else 0
       end as `取消率`
     , t1.q_gmv_app as GMV
     , t1.q_commission_app as `佣金`
     , t1.q_room_night_app as `间夜量`
     , t1.q_order_cnt_app as `订单量`
     , t1.q_order_user_cnt_app as `生单用户数`
     , t1.q_adr_app as ADR
     , t1.q_avg_rn_per_order_app as `单订单间夜`
     , t1.q_coupon_amount_app as `券额`
     , t1.q_coupon_order_cnt_app as `用券订单量`
     , t1.q_subsidy_rate_app as `券补贴率`
     , t1.q_coupon_order_rate_app as `用券订单占比`
from q_data_info t1
left join q_app_order t4
    on t4.order_date = t1.dt
    and t1.ab_version = t4.ab_version
    and t1.ab_rule_version = t4.ab_rule_version   -- 修正：原为 t1.ab_rule_version = t4，缺少 .ab_rule_version