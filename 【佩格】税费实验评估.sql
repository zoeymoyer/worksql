
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
    where a.dt >= '2026-05-07' and dt <= date_sub(current_date, 1)
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
    where dt >= '2026-05-07' and  dt <= date_sub(current_date, 1)
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
        and order_date >= '2026-05-07' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,ld_price_info as ( ---LD页税费明细
    select dt
            ,user_name
    from ihotel_default.dw_qav_hotel_track_info_di_v1
    where dt >= '2026-05-07'
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
    where dt >= '2026-05-07'
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




