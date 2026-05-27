-- 1、实验数据
with abtest as (
    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
          ,version
          ,clientcode as user_id
          ,user_name
    from default.ods_abtest_sdk_log_endtime_hotel a
    left join pub.dim_user_profile_nd b on a.clientcode = b.user_id
    where dt >= '20260425'
        and dt <= '20260525'
        and expid = '250804_ho_gj_iHotel_proPay'
    group by 1,2,3,4
)
,zhunru as (
    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
          ,user_name
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260425'
        and dt <= '20260525'
        and key = 'ihotel/Booking/payLater/show/entry'
    group by 1,2
)
,dingdan as (
    select order_date
            ,user_info['orig_device_id'] as uid
            ,user_id,user_name
            ,order_no
            ,init_gmv
            ,room_night
            ,order_status
            ,ext_flag_map['post_pay_flag'] as post_pay_flag
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + coalesce(ext_plat_certificate, 0))
                else init_commission_after + coalesce(ext_plat_certificate, 0)
            end as final_commission_after   -- Q佣金
            ,case when ext_flag_map['pay_after_stay_flag'] = 'true' then '后付订单'
                when ext_flag_map['pay_after_stay_flag'] = 'false' then '非后付订单'
                else '其他'
            end as is_pay_after   -- 是否后付订单
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
                   and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                   and (refund_time is null or date(refund_time) > order_date) 
                   then 'Y' else 'N' 
            end is_not_cancel_d0 -- D0是否未取消
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + coalesce(ext_plat_certificate, 0))
                else final_commission_after + coalesce(ext_plat_certificate, 0)
            end as ld_commission   -- Q离店佣金
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and is_valid = '1'
        and order_date >= '2026-04-25'
        and order_date <= '2026-05-25'
)
,liuliang as (
    select dt
          ,user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1
    where dt >= '2026-04-25'
        and dt <= '2026-05-25'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and user_name is not null and user_name not in ('null', 'NULL', '', ' ')
        and user_id is not null and user_id not in ('null', 'NULL', '', ' ')
        and booking_pv > 0 
    group by 1,2
)


select t1.dt as `日期`
      ,t1.version as `实验分组`
      ,count(distinct t1.user_id) as `分组用户量`
      ,count(distinct t2.user_name) as `B页UV`
      ,count(distinct t4.user_name) as `准入UV`
      ,count(distinct case when t3.is_not_cancel_d0='Y' then t3.order_no  end) as `订单`
      ,sum(case when t3.is_not_cancel_d0='Y' then t3.final_commission_after end) as `佣金`
      ,sum(case when t3.is_not_cancel_d0='Y' then t3.init_gmv else 0 end) as `GMV`

      ,count(distinct case when t3.is_not_cancel_d0='Y' and t3.is_pay_after = '后付订单' then t3.order_no else null end) as `后付订单`
      ,sum(case when t3.is_not_cancel_d0='Y' and t3.is_pay_after = '后付订单' then t3.final_commission_after else 0 end) as `后付订单佣金`
      ,sum(case when t3.is_not_cancel_d0='Y' and t3.is_pay_after = '后付订单' then t3.init_gmv else 0 end) as `后付订单GMV`

    --   ,count(distinct case when t3.order_status = 'CHECKED_OUT' then t3.order_no else null end) as `离店订单`
    --   ,sum(case when t3.order_status = 'CHECKED_OUT' then t3.init_gmv else null end) as `离店GMV`
    --   ,sum(case when t3.order_status = 'CHECKED_OUT' then t3.final_commission_after else null end) as `离店佣金`

    --   ,count(distinct case when t3.order_status not in ('CANCELLED', 'REJECTED') then t3.order_no else null end) as `未离店订单`
    --   ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') then t3.init_gmv else null end) as `未离店GMV`
    --   ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') then t3.final_commission_after else null end) as `未离店佣金`

    --   ,count(distinct case when t3.order_status = 'CHECKED_OUT' and t3.post_pay_flag in (2, 3, 5,7) then t3.order_no else null end) as `已离店后付扣款失败订单数`
    --   ,sum(case when t3.order_status = 'CHECKED_OUT' and t3.post_pay_flag in (2, 3, 5,7) then t3.init_gmv else 0 end) as `已离店后付扣款失败GMV`
    --   ,sum(case when t3.order_status = 'CHECKED_OUT' and t3.post_pay_flag in (2, 3, 5,7) then t3.final_commission_after else 0 end) as `已离店后付扣款失败佣金`
      
    --   ,count(distinct case when t3.order_status not in ('CANCELLED', 'REJECTED') and t3.post_pay_flag in (2, 3, 5,7) then t3.order_no else null end) as `未离店后付扣款失败订单数`
    --   ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') and t3.post_pay_flag in (2, 3, 5,7) then t3.init_gmv else 0 end) as `未离店后付扣款失败GMV`
    --   ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') and t3.post_pay_flag in (2, 3, 5,7) then t3.final_commission_after else 0 end) as `未离店后付扣款失败佣金`
from abtest t1
left join liuliang t2 on t1.user_name = t2.user_name and t1.dt = t2.dt
left join dingdan t3 on t2.user_name = t3.user_name and t2.dt = t3.order_date
left join zhunru t4 on t2.user_name = t4.user_name and t2.dt = t4.dt
group by 1,2
;

---2、后付订单离店情况
with dingdan as (
    select order_date,checkout_date
            ,user_id,user_name
            ,order_no
            ,init_gmv
            ,room_night
            ,order_status
            ,ext_flag_map['post_pay_flag'] as post_pay_flag
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + coalesce(ext_plat_certificate, 0))
                else init_commission_after + coalesce(ext_plat_certificate, 0)
            end as final_commission_after   -- Q佣金  
            ,final_payamount_price    --- 用户最终支付金额    
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and is_valid = '1'
        and order_date >= '2026-04-25'
        and order_date <= '2026-05-25'
        and ext_flag_map['pay_after_stay_flag'] = 'true'  -- 后付订单
)
select sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5,7) then final_payamount_price end) `已离店扣款失败GMV`
    ,sum(case when order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT') then final_payamount_price end) `未离店GMV`
    ,sum(case when order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT') then final_payamount_price end) * 0.0013 `未离店预计坏账金额`
from dingdan
;
;

--- 单订单收益	  
select dt 
      ,(dayofweek(dt) + 5) % 7 + 1   as "星期"
      ,mdd "目的地"
      ,user_type  "用户类型"
      
      --- QC 对比指标
      ,qc_rn_rate as "间夜QC"
      ,qc_avg_rn as "单间夜QC"
      ,qc_revenue as "收益QC"
      ,qc_take_rate_diff as "收益率QC差"
      ,qc_adr as "ADR_QC"
      ,qc_subsidy_rate_diff as "券补贴率QC差"
      ,qc_order_cnt as "订单量QC"
      --- 基础间夜量与佣金
      ,q_room_night as "Q_间夜量"
      ,q_room_night_app as "Q_间夜量_app"
      ,c_room_night as "C_间夜量"
      ,q_commission as "Q_佣金"
      ,c_commission as "C_佣金"
      ,q_commission_app as "Q_佣金_app"
      ,q_commission_app / q_order_cnt_app as "单订单收益"
      ,q_commission_app / q_room_night_app as "单间夜收益" 
      --- 订单量与GMV
      ,q_order_cnt as "Q_订单量"
      ,q_order_cnt_app as "Q_订单量_app"
      ,c_order_cnt as "C_订单量"
      ,q_gmv as "Q_GMV"
      ,q_gmv_app as "Q_GMV_app"
      ,c_gmv as "C_GMV"
      --- 单间夜及取消率
      ,q_avg_rn_per_order as "Q_单间夜"
      ,q_avg_rn_per_order_app as "Q_单间夜_app"
      ,c_avg_rn_per_order as "C_单间夜"
      ,q_cancel_rate as "Q_取消率"
      ,q_cancel_rate_app as "Q_取消率_app"
      ,c_cancel_rate as "C_取消率"
from ihotel_default.ads_ihotel_qc_checkout_metrics_di where dt >= '2026-01-01' and mdd='ALL' and user_type='ALL'
order by 1 desc    
;


with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  orig_device_id,user_name
    from(
            select
                order_date,user_name,
                user_info['orig_device_id'] as orig_device_id,
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
            group by 1,2,3
        )a where room_nights_90>=15
    group by 1,2
)
,abtest as (
    SELECT  dt,
            ab_version version,
            ab_exp_value AS user_id,
            b.user_name
    FROM ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    left join pub.dim_user_profile_nd b on a.ab_exp_value = b.user_id
    WHERE a.dt between '2026-04-25'  AND '2026-05-25'
         and user_id_type = 'user_id' 
        and ab_exp_id = '250804_ho_gj_iHotel_proPay'
    group by 1,2,3,4
)
,zhunru as (
    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
          ,user_name
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260425'
        and dt <= '20260525'
        and key = 'ihotel/Booking/payLater/show/entry'
    group by 1,2
)
,dingdan as (
    select order_date
            ,user_info['orig_device_id'] as uid
            ,user_id,user_name
            ,order_no
            ,init_gmv
            ,room_night
            ,order_status
            ,ext_flag_map['post_pay_flag'] as post_pay_flag
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + coalesce(ext_plat_certificate, 0))
                else init_commission_after + coalesce(ext_plat_certificate, 0)
            end as final_commission_after   -- Q佣金
            ,case when ext_flag_map['pay_after_stay_flag'] = 'true' then '后付订单'
                when ext_flag_map['pay_after_stay_flag'] = 'false' then '非后付订单'
                else '其他'
            end as is_pay_after   -- 是否后付订单
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
                   and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                   and (refund_time is null or date(refund_time) > order_date) 
                   then 'Y' else 'N' 
            end is_not_cancel_d0 -- D0是否未取消
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + coalesce(ext_plat_certificate, 0))
                else final_commission_after + coalesce(ext_plat_certificate, 0)
            end as ld_commission   -- Q离店佣金
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and is_valid = '1'
        and order_date >= '2026-04-25'
        and order_date <= '2026-05-25'
)
,liuliang as (
    select dt
          ,user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1
    where dt >= '2026-04-25'
        and dt <= '2026-05-25'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and user_name is not null and user_name not in ('null', 'NULL', '', ' ')
        and user_id is not null and user_id not in ('null', 'NULL', '', ' ')
        and booking_pv > 0 
    group by 1,2
)

select t1.dt as `日期`
      ,t1.version as `实验分组`
      ,count(distinct t1.user_id) as `分组用户量`
      ,count(distinct t2.user_name) as `B页UV`
      ,count(distinct t4.user_name) as `准入UV`
      ,count(distinct case when t3.is_not_cancel_d0='Y' then t3.order_no  end) as `订单`
      ,sum(case when t3.is_not_cancel_d0='Y' then t3.final_commission_after end) as `佣金`
      ,sum(case when t3.is_not_cancel_d0='Y' then t3.init_gmv else 0 end) as `GMV`

      ,count(distinct case when t3.is_not_cancel_d0='Y' and t3.is_pay_after = '后付订单' then t3.order_no else null end) as `后付订单`
      ,sum(case when t3.is_not_cancel_d0='Y' and t3.is_pay_after = '后付订单' then t3.final_commission_after else 0 end) as `后付订单佣金`
      ,sum(case when t3.is_not_cancel_d0='Y' and t3.is_pay_after = '后付订单' then t3.init_gmv else 0 end) as `后付订单GMV`

      ,count(distinct case when t3.order_status = 'CHECKED_OUT' then t3.order_no else null end) as `离店订单`
      ,sum(case when t3.order_status = 'CHECKED_OUT' then t3.init_gmv else null end) as `离店GMV`
      ,sum(case when t3.order_status = 'CHECKED_OUT' then t3.final_commission_after else null end) as `离店佣金`

      ,count(distinct case when t3.order_status not in ('CANCELLED', 'REJECTED') then t3.order_no else null end) as `未离店订单`
      ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') then t3.init_gmv else null end) as `未离店GMV`
      ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') then t3.final_commission_after else null end) as `未离店佣金`

      ,count(distinct case when t3.order_status = 'CHECKED_OUT' and t3.post_pay_flag in (2, 3, 5) then t3.order_no else null end) as `已离店后付扣款失败订单数`
      ,sum(case when t3.order_status = 'CHECKED_OUT' and t3.post_pay_flag in (2, 3, 5) then t3.init_gmv else 0 end) as `已离店后付扣款失败GMV`
      ,sum(case when t3.order_status = 'CHECKED_OUT' and t3.post_pay_flag in (2, 3, 5) then t3.final_commission_after else 0 end) as `已离店后付扣款失败佣金`
      
      ,count(distinct case when t3.order_status not in ('CANCELLED', 'REJECTED') and t3.post_pay_flag in (2, 3, 5) then t3.order_no else null end) as `未离店后付扣款失败订单数`
      ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') and t3.post_pay_flag in (2, 3, 5) then t3.init_gmv else 0 end) as `未离店后付扣款失败GMV`
      ,sum(case when t3.order_status not in ('CANCELLED', 'REJECTED') and t3.post_pay_flag in (2, 3, 5) then t3.final_commission_after else 0 end) as `未离店后付扣款失败佣金`
from abtest t1
left join liuliang t2 on t1.user_name = t2.user_name and t1.dt = t2.dt
left join dingdan t3 on t2.user_name = t3.user_name and t2.dt = t3.order_date
left join zhunru t4 on t2.user_name = t4.user_name and t2.dt = t4.dt
left join biguser on t1.user_name = biguser.user_name 
where biguser.user_name is null  -- 过滤掉新逻辑大单用户
group by 1,2
;





select checkout_date
    ,is_pay_after
    ,count(distinct order_no) as order_num
    ,sum(init_gmv) as gmv
    ,sum(final_commission_after) as commission
from (
    select order_date
          ,user_info['orig_device_id'] as uid
          ,user_id,user_name
          ,order_no
          ,init_gmv,checkout_date
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after + coalesce(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + coalesce(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + coalesce(ext_plat_certificate, 0))
                else final_commission_after + coalesce(ext_plat_certificate, 0)
           end as final_commission_after   -- Q佣金
          ,case when ext_flag_map['pay_after_stay_flag'] = 'true' then '后付订单'
                when ext_flag_map['pay_after_stay_flag'] = 'false' then '非后付订单'
                else '其他'
           end as is_pay_after   -- 是否后付订单
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
        and checkout_date >= '2026-04-25'
        and checkout_date <= '2026-05-25'
        and ext_flag_map['pay_after_stay_flag'] = 'true'
)
group by 1,2 