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
    where a.dt >= '2026-05-08'  and dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '260430_ho_gj_Unqualiroom'
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
        'ihotel/Detail/PriceList/show/sectionExposure'
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




--- 分数据分析
-- 实验是前端分类，分类依据uid
with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date
                ,user_info['orig_device_id'] as orig_device_id
                ,user_id
                ,user_name
                ,count(order_no) as order_nos_90
                ,sum(room_night) as room_nights_90
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
    select  a.dt,
            ab_version version,
            ab_exp_value AS uid
            -- ,b.user_name
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    -- left join pub.dim_user_profile_nd b on a.ab_exp_value = b.user_id
    where a.dt >= '2026-05-08'  and a.dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'uid' 
        and ab_exp_id = '260430_ho_gj_Unqualiroom'
        and ab_exp_value not in (select user_id from biguser)  --- 去除大单用户
    group by 1,2,3
    -- select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
    --       ,version
    --       ,clientcode as user_id
    --       ,user_name
    -- from default.ods_abtest_sdk_log_endtime_hotel a
    -- left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    -- where dt >= '20260508'
    --     and dt <= '20260524'
    --     and expid = '260430_ho_gj_Unqualiroom'
    -- group by 1,2,3,4
)

,display_table as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
            ,get_json_object(extendinfomap,'$.traceId') as traceId
            ,SUBSTRING_INDEX(room_id, '_', 1) AS room_id
            ,physical_room_id
            --- 不符房型
            ,case when ext_pricing_map['not_match_adult'] = 'true' then 'Y' else 'N' end as not_match_adult  
            ,uid,adults_num
    from ihotel_default.dw_hotel_price_display
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) >= '2026-05-08' and concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) <=  date_sub(current_date, 1)
        and get_json_object(extendinfomap,'$.traceId') is not null
        and SUBSTRING_INDEX(room_id, '_', 1) is not null
        and physical_room_id is not null
    group by 1,2,3,4,5,6,7
)
,q_order as (
    select  order_date
        ,get_json_object(extendinfomap,'$.traceId') as traceId
        ,user_info['orig_device_id'] as orig_device_id
        ,hotel_seq
        ,room_night
        ,a.order_no
        ,qta_product_id
        ,physical_room_id
        ,max_c
    from default.mdw_order_v3_international a
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and order_date >= date_sub(current_date, 30)
        and get_json_object(extendinfomap,'$.traceId') is not null
)

select t1.dt,version
    ,count(distinct t1.traceId) `D页曝光pv`
    ,count(distinct case when t1.not_match_adult = 'Y' then t1.traceId end) as `D页不符房型曝光pv`
    ,count(distinct t1.uid) as `D页曝光uv`
    ,count(distinct case when t1.not_match_adult = 'Y' then t1.uid end) as `D页不符房型曝光uv`
    ,count(distinct t2.order_no) as `D页订单量`
    ,count(distinct case when t1.not_match_adult = 'Y' then t2.order_no end) as `D页不符房型订单量`
    ,count(distinct case when t1.not_match_adult = 'Y'  and t1.adults_num > t2.max_c then t2.order_no end) as `D页不符房型订单量-成人数超标`
    ,count(distinct case when t1.not_match_adult = 'N'  then t1.room_id end) as `D页符合房型曝光数`
    ,count(distinct case when t1.not_match_adult = 'Y'  then t1.room_id end) as `D页不符房型曝光数`

    ,count(distinct case when t1.not_match_adult = 'Y' then t2.order_no end) / count(distinct case when t1.not_match_adult = 'Y' then t1.uid end) as `D页不符房型CR`
    ,count(distinct case when t1.not_match_adult = 'Y' then t1.traceId end) / count(distinct t1.traceId) as `D页不符房型曝光率`
    ,count(distinct case when t1.not_match_adult = 'Y' then t2.order_no end) / count(distinct t2.order_no) as `D页不符房型订单占比`
    ,count(distinct case when t1.not_match_adult = 'Y'  and t1.adults_num > t2.max_c then t2.order_no end) / count(distinct t2.order_no) as `D页不符房型订单占比-成人数超标` 
from display_table t1
left join q_order t2 on t1.traceId = t2.traceId and t1.room_id = t2.qta_product_id and t1.physical_room_id = t2.physical_room_id 
    -- and t1.dt=t2.order_date
left join abtest t3 on t1.uid = t3.uid and t1.dt = t3.dt
where t3.uid is not null
group by 1,2
order by 1,2 desc ;




with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date
                ,user_info['orig_device_id'] as orig_device_id
                ,user_id
                ,user_name
                ,count(order_no) as order_nos_90
                ,sum(room_night) as room_nights_90
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
, ab_rule as (select ab_exp_id
                , ab_version
                , ab_rule_version
                , device_id as ab_exp_value
        from (select ab_exp_id,
                        ab_version,
                        ab_rule_version
                from default.ods_abtest_rule_info
                where dt = '20260414'
                and source = 'hotel'
                and ab_shuntbase = 'APP_UID'
                and ab_exp_id = '251204_ho_gj_ai_compare_price'
                ) rule
                    join
                (select expid, version, ruleversion, clientcode as device_id, dt, logdate
                from default.ods_abtest_sdk_log_endtime_hotel
                where dt = '20260414'
                and clientcode is not NULL
                and expid is not NULL
                and version is not NULL
                and ruleversion is not NULL
                and expid != ''
                and version != ''
                and clientcode not in ('0', '00000000', '00000000000000', '000000000000000', '0000000000000000',
                                        '0000000000000000000000000000000000000000', '', 'ctrip', 'elong',
                                        '352284040670808')
                and (clientcode not like 'tc%' and clientcode not like 'wx%' and clientcode not like 'pd%')) ab
                on ab.expid = rule.ab_exp_id and ab.version = rule.ab_version and
                ab.ruleversion = rule.ab_rule_version
        group by 1, 2, 3, 4)
   , user_type as (select user_id
                        , min(order_date) as min_order_date
                   from default.mdw_order_v3_international --- 海外订单表
                   where dt = '20260414'
                     and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                     and terminal_channel_type in ('www', 'app', 'touch')
                     and order_status not in ('CANCELLED', 'REJECTED')
                     and is_valid = '1'
                   group by 1)
   , uv as (select dt
                 ,t.ab_version
                 ,t.ab_rule_version
                 , case
                       when province_name in ('澳门', '香港') then province_name
                       when a.country_name in
                            ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯')
                           then a.country_name
                       when e.area in ('欧洲', '亚太', '美洲') then e.area
                       else '其他' end                                         as mdd
                 , case when dt > b.min_order_date then '老客' else '新客' end as user_type
                 , a.user_id
                 , a.user_name
                 , if(no_user_id is null,'正常用户','大单用户') as is_big_order_user
                 , sum(search_pv)                                                 search_pv
                 , sum(detail_pv)                                                 detail_pv
                 , sum(booking_pv)                                                booking_pv
                 , sum(order_pv)                                                  order_pv
            from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
                     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
                     left join user_type b on a.user_id = b.user_id
                     left join no_user on a.user_id = no_user.no_user_id
                     right join ab_rule t on t.ab_exp_value = a.orig_device_id
            where dt = '2026-04-14'
              and business_type = 'hotel'
              and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
              and case
                      when province_name in ('台湾', '澳门', '香港') then province_name else a.country_name end = '韩国'
              and city_name = '首尔'
              and (search_pv + detail_pv + booking_pv + order_pv) > 0
              and a.user_name is not null
              and a.user_name not in ('null', 'NULL', '', ' ')
              and a.user_id is not null
              and a.user_id not in ('null', 'NULL', '', ' ')
            group by 1, 2, 3, 4, 5,6,7,8)
   , q_uv_info as
    (select dt
          , ab_version
          , ab_rule_version
          , is_big_order_user
          , count(distinct user_id) uv
     from uv
     where is_big_order_user = '正常用户'
     group by 1,2,3,4)
   , q_order_app as (
    select order_date
         ,t.ab_version
         ,t.ab_rule_version
         , case
               when province_name in ('澳门', '香港') then province_name
               when a.country_name in ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯')
                   then a.country_name
               when e.area in ('欧洲', '亚太', '美洲') then e.area
               else '其他' end                                                                          as mdd
         , case when order_date = b.min_order_date then '新客' else '老客' end                          as user_type
         , a.user_id
         , init_gmv
         , order_no
         , room_night
         , batch_series
         , hotel_grade
         , coupon_id
         , case
               when coupon_id is not null
                   and batch_series not in ('MacaoDisco_ZK_5e27de', '2night_ZK_952825', '3night_ZK_ad8c83')
                   and batch_series not like '%23base_ZK_728810%'
                   and batch_series not like '%23extra_ZK_ce6f99%'
                   then 'Y'
               else 'N' end                                                                                is_user_coupon        --- 是否用券
         , case
               when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                   then (init_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) +
                         coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) +
                         coalesce(ext_plat_certificate, 0))
               else init_commission_after + coalesce(ext_plat_certificate, 0) end                       as init_commission_after --- Q佣金

         , coalesce(get_json_object(extendinfomap, '$.V2_BEAT_AMOUNT_AF'), 0) * room_night              as pricing_subsidy_amount
         , case
               when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or
                     batch_series like '%23extra_ZK_ce6f99%') then 0
               else nvl(coupon_substract_summary, 0) end                                                as coupon_subsidy_amount
         , coalesce(cashbackmap['voucher_pack_price'], 0)                                               as voucher_pack_income
         , coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) as point_subsidy_amount
         , case
               when array_contains(supplier_promotion_code, '2913') and qta_supplier_id = '1615667' then coalesce(
                       get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),
                       0) end                                                                           as multi_point_subsidy_amount
         , case
               when supplier_code in
                    ('hca9008oc4l', 'hca908oh60s', 'hca908oh60t', 'hca9008pb7m', 'hca9008pb7k', 'hca908pb70p',
                     'hca908pb70o', 'hca908pb70q', 'hca908pb70s', 'hca908pb70r', 'hca908lp9aj', 'hca908lp9ag',
                     'hca908lp9ai', 'hca908lp9ah', 'hca9008lp9v', 'hca908lp9ak', 'hca908lp9al', 'hca908lp9am',
                     'hca908lp9an', 'hca1f71a00i', 'hca1f71a00j')
                   then coalesce(follow_price_amount, 0) end                                            as follow_price_subsidy_amount
         , coalesce(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) * room_night           as bp_adv_amount_realized
         ,if(no_user_id is null,'正常用户','大单用户') is_big_order_user
    from default.mdw_order_v3_international a
             left join user_type b on a.user_id = b.user_id
             left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
             left join no_user on a.user_id = no_user.no_user_id
             right join ab_rule t on t.ab_exp_value = a.user_info['orig_device_id']
    where a.dt = '20260414'
      and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
      and case
              when province_name in ('台湾', '澳门', '香港') then province_name else a.country_name end = '韩国'
      and city_name = '首尔'
      and terminal_channel_type = 'app'
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
      and (first_rejected_time is null or date(first_rejected_time) > order_date)
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid = '1'
      and order_date = '2026-04-14'
      and order_no <> '103576132435')
   , order_info_app as (
    select order_date
         , ab_version
         , ab_rule_version
         , sum(init_commission_after)                                                        as q_commission_app       -- Q_佣金_app
         , sum(init_gmv)                                                                     as q_gmv_app              -- Q_GMV_app
         , sum(coupon_subsidy_amount)                                                                       as q_coupon_amount_app    -- Q_券额_app
         , count(distinct order_no)                                                          as q_order_cnt_app        -- Q_订单量_app
         , count(distinct t1.user_id)                                                        as q_order_user_cnt_app   -- Q_下单用户_app
         , sum(room_night)                                                                   as q_room_night_app       -- Q_间夜量_app
         , count(distinct case when is_user_coupon = 'Y' then order_no else null end)        as q_coupon_order_cnt_app -- Q_用券订单量_app
         , sum(pricing_subsidy_amount) + sum(coupon_subsidy_amount) + sum(point_subsidy_amount) - sum(multi_point_subsidy_amount)  + sum(follow_price_subsidy_amount)  as `平台补贴额`
         , (sum(pricing_subsidy_amount) + sum(coupon_subsidy_amount) + sum(point_subsidy_amount) - sum(multi_point_subsidy_amount) + sum(follow_price_subsidy_amount)  ) /
           sum(init_gmv)                                                                     as `平台补贴率`
         , sum(init_commission_after) / sum(init_gmv) +
           (sum(pricing_subsidy_amount) + sum(coupon_subsidy_amount) + sum(point_subsidy_amount) - sum(multi_point_subsidy_amount) + sum(follow_price_subsidy_amount) - sum(bp_adv_amount_realized) ) /
           sum(init_gmv)                                                                     as `补贴前佣金率`
         , sum(init_commission_after) + sum(pricing_subsidy_amount) + sum(coupon_subsidy_amount) + sum(point_subsidy_amount) - sum(multi_point_subsidy_amount) + sum(follow_price_subsidy_amount) - sum(bp_adv_amount_realized)     as `补贴前佣金额`
    from q_order_app t1
    where is_big_order_user = '正常用户'
    group by 1, 2, 3)
   , q_data_info as (select t1.dt
                          , t1.ab_version
                          , t1.ab_rule_version
                          , coalesce(t1.uv, 0)                                                 as uv
                          , coalesce(t4.q_room_night_app, 0)                                   as q_room_night_app               -- Q_间夜量_app
                          , coalesce(t4.q_order_cnt_app, 0)                                    as q_order_cnt_app                -- Q_订单量_app
                          , coalesce(t4.q_order_user_cnt_app, 0)                               as q_order_user_cnt_app           -- Q_下单用户_app
                          , coalesce(t4.q_gmv_app, 0)                                          as q_gmv_app                      -- Q_GMV_app
                          , coalesce(t4.q_commission_app, 0)                                   as q_commission_app               -- Q_佣金_app
                          , coalesce(t4.q_coupon_amount_app, 0)                                as q_coupon_amount_app            -- Q_券额_app
                          , coalesce(t4.q_order_cnt_app / t1.uv, 0)                            as q_cr_app                       -- Q_CR_app
                          , coalesce(t4.q_room_night_app, 0) / coalesce(t4.q_order_cnt_app, 0) as q_avg_rn_per_order_app         -- Q_单间夜_app
                          , coalesce(t4.q_commission_app, 0) / coalesce(t4.q_gmv_app, 0)       as q_take_rate_app                -- Q_收益率_app
                          , coalesce(t4.q_coupon_amount_app, 0) / coalesce(t4.q_gmv_app, 0)    as q_subsidy_rate_app             -- Q_券补贴率_app
                          , coalesce(t4.q_gmv_app, 0) / coalesce(t4.q_room_night_app, 0)       as q_adr_app                      -- Q_ADR_app
                          , coalesce(t4.q_coupon_order_cnt_app, 0) /
                            coalesce(t4.q_order_cnt_app, 0)                                    as q_coupon_order_rate_app        -- Q_用券订单占比_app
                          , coalesce(t4.`平台补贴额`, 0)                                       as platform_subsidy_amount        -- 平台补贴额
                          , coalesce(t4.`平台补贴率`, 0)                                       as platform_subsidy_rate          -- 平台补贴率
                          , coalesce(t4.`补贴前佣金率`, 0)                                     as commission_rate_before_subsidy -- 补贴前佣金率
                          , coalesce(t4.`补贴前佣金额`, 0)                                     as commission_after_subsidy  -- 补贴前佣金额
                     from q_uv_info t1
                              left join order_info_app t4 on t1.dt = t4.order_date
                         and t1.ab_version = t4.ab_version
                         and t1.ab_rule_version = t4.ab_rule_version
)

select dt
     , ab_version
     , ab_rule_version
     , uv
     , q_order_user_cnt_app                            -- Q_下单用户_app
     , q_order_cnt_app                                 -- Q_订单量_app
     , q_order_user_cnt_app / uv as U2O
     , q_cr_app                                        -- Q_CR_app
     , q_room_night_app                                -- Q_间夜量_app
     , q_avg_rn_per_order_app                          -- Q_单间夜_app
     , q_gmv_app                                       -- Q_GMV_app
     , q_adr_app                                       -- Q_ADR_app
     , q_commission_app                                -- Q_佣金_app
     , q_take_rate_app                                 -- Q_收益率_app(佣金率)

     , platform_subsidy_amount                         -- 平台补贴额
     , platform_subsidy_rate                           -- 平台补贴率
     , commission_rate_before_subsidy                  -- 补贴前佣金率
     , commission_after_subsidy                   -- 补贴前佣金额

     , q_room_night_app / uv     as room_nights_per_uv -- 单UV间夜
     , q_gmv_app / uv            as gmv_per_uv         -- 单UVGMV
     , q_commission_app / uv     as revenue_per_uv     -- 单UV收益
     , platform_subsidy_amount / uv                    --单UV补贴
     , q_commission_app / q_room_night_app             --单间夜收益
     , platform_subsidy_amount / q_room_night_app      --单间夜补贴

from q_data_info