---- 1、整体晨报数据
select 
    dt
    ,mdd	as	"目的地"
    ,user_type	as	"用户类型"
    ,qc_rn_rate	as	"间夜QC"
    ,qc_traffic_rate	as	"流量QC"
    ,qc_cr	as	"转化QC"
    ,qc_revenue	as	"收益QC"
    ,qc_take_rate_diff	as	"收益率QC差"
    ,qc_subsidy_rate_diff	as	"券补贴率QC差"
    ,qc_adr	as	"ADR_QC"
    ,qc_order_cnt	as	"订单量QC"
    ,qc_avg_rn	as	"单间夜QC"
    ,uv	as	"Q_DAU"
    ,c_uv	as	"C_DAU"
    ,q_room_night	as	"Q_间夜量"
    ,q_room_night_app	as	"Q_间夜量_app"
    ,c_room_night	as	"C_间夜量"
    ,q_commission	as	"Q_佣金"
    ,c_commission	as	"C_佣金"
    ,q_commission_app	as	"Q_佣金_app"
    ,q_traffic_rate	as	"Q_流量占比"
    ,q_cr	as	"Q_CR"
    ,q_cr_app	as	"Q_CR_app"
    ,c_cr	as	"C_CR"
    ,q_take_rate	as	"Q_收益率"
    ,q_take_rate_app	as	"Q_收益率_app"
    ,c_take_rate	as	"C_收益率"
    ,q_rn_rate_app	as	"Q_间夜占比_app"
    ,q_order_cnt_rate_app	as	"Q_订单量占比_app"
    ,q_gmv_rate_app	as	"Q_GMV占比_app"
    ,q_commission_rate_app	as	"Q_佣金占比_app"
    ,q_coupon_amt_rate_app	as	"Q_券额占比_app"
    ,q_rn_rate	as	"Q_间夜占比"
    ,q_order_cnt_rate	as	"Q_订单量占比"
    ,q_gmv_rate	as	"Q_GMV占比"
    ,q_commission_rate	as	"Q_佣金占比"
    ,q_coupon_amt_rate	as	"Q_券额占比"
    ,q_subsidy_rate	as	"Q_券补贴率"
    ,q_subsidy_rate_app	as	"Q_券补贴率_app"
    ,c_subsidy_rate	as	"C_券补贴率"
    ,q_order_cnt	as	"Q_订单量"
    ,q_order_cnt_app	as	"Q_订单量_app"
    ,c_order_cnt	as	"C_订单量"
    ,q_gmv	as	"Q_GMV"
    ,q_gmv_app	as	"Q_GMV_app"
    ,c_gmv	as	"C_GMV"
    ,q_coupon_amount	as	"Q_券额"
    ,q_coupon_amount_app	as	"Q_券额_app"
    ,c_coupon_amount	as	"C_券额"
    ,q_order_user_cnt	as	"Q_下单用户"
    ,q_order_user_cnt_app	as	"Q_下单用户_app"
    ,c_order_user_cnt	as	"C_下单用户"
    ,q_adr	as	"Q_ADR"
    ,q_adr_app	as	"Q_ADR_app"
    ,c_adr	as	"C_ADR"
    ,q_coupon_order_rate	as	"Q_用券订单占比"
    ,q_coupon_order_rate_app	as	"Q_用券订单占比_app"
    ,q_high_star_rn	as	"Q_高星间夜量"
    ,q_high_star_rn_app	as	"Q_高星间夜量_app"
    ,c_high_star_rn	as	"C_高星间夜量"
    ,q_mid_star_rn	as	"Q_中星间夜量"
    ,q_mid_star_rn_app	as	"Q_中星间夜量_app"
    ,c_mid_star_rn	as	"C_中星间夜量"
    ,q_low_star_rn	as	"Q_低星间夜量"
    ,q_low_star_rn_app	as	"Q_低星间夜量_app"
    ,c_low_star_rn	as	"C_低星间夜量"
    ,q_avg_rn_per_order	as	"Q_单间夜"
    ,q_avg_rn_per_order_app	as	"Q_单间夜_app"
    ,c_avg_rn_per_order	as	"C_单间夜"
    ,s_all_uv	as	"Q_SUV"
    ,d_s_uv	as	"Q_DUV"
    ,b_ds_uv	as	"Q_BUV"
    ,o_ds_order	as	"Q_生单量"
    ,s2d	as	"s2d"
    ,d2b	as	"d2b"
    ,b2o	as	"b2o"
    ,s2o	as	"s2o"
    ,s_all_uv_c	as	"C_SUV"
    ,d_s_uv_c	as	"C_DUV"
    ,b_ds_uv_c	as	"C_BUV"
    ,o_ds_order_c	as	"C_生单量"
    ,s2d_c	as	"s2d_c"
    ,d2b_c	as	"d2b_c"
    ,b2o_c	as	"b2o_c"
    ,s2o_c	as	"s2o_c"
    ,s2d_qc	as	"s2d_qc"
    ,d2b_qc	as	"d2b_qc"
    ,b2o_qc	as	"b2o_qc"
    ,s2o_qc	as	"s2o_qc"
from ihotel_default.ads_intl_hotel_qc_monitor_di
where dt >= date_sub(current_date, 30)
order by 1 desc
;

--- 2、用户端数据播报
with data_info as (
    select dt,mdd,user_type,qc_rn_rate,qc_traffic_rate,qc_cr,qc_revenue,qc_take_rate_diff,qc_subsidy_rate_diff,qc_adr,qc_order_cnt,qc_avg_rn,uv,c_uv,q_room_night,q_room_night_app,c_room_night,q_commission,c_commission,q_commission_app,q_traffic_rate,q_cr,q_cr_app,c_cr,q_take_rate,q_take_rate_app,c_take_rate,q_rn_rate_app,q_order_cnt_rate_app,q_gmv_rate_app,q_commission_rate_app,q_coupon_amt_rate_app,q_rn_rate,q_order_cnt_rate,q_gmv_rate,q_commission_rate,q_coupon_amt_rate,q_subsidy_rate,q_subsidy_rate_app,c_subsidy_rate,q_order_cnt,q_order_cnt_app,c_order_cnt,q_gmv,q_gmv_app,c_gmv,q_coupon_amount,q_coupon_amount_app,c_coupon_amount,q_order_user_cnt,q_order_user_cnt_app,c_order_user_cnt,q_adr,q_adr_app,c_adr,q_coupon_order_rate,q_coupon_order_rate_app,q_high_star_rn,q_high_star_rn_app,c_high_star_rn,q_mid_star_rn,q_mid_star_rn_app,c_mid_star_rn,q_low_star_rn,q_low_star_rn_app,c_low_star_rn,q_avg_rn_per_order,q_avg_rn_per_order_app,c_avg_rn_per_order,s_all_uv,d_s_uv,b_ds_uv,o_ds_order,s2d,d2b,b2o,s2o,s_all_uv_c,d_s_uv_c,b_ds_uv_c,o_ds_order_c,s2d_c,d2b_c,b2o_c,s2o_c,s2d_qc,d2b_qc,b2o_qc,s2o_qc
    from ihotel_default.ads_intl_hotel_qc_monitor_di
    where mdd='ALL'
)

select t1.dt "日期"
      ,wkd "星期"
      ,t1.qc_rn_rate "间夜QC"
      ,t1.qc_cr "转化QC"
      ,t1.qc_cr / t2.qc_cr - 1 "转化-YOY"
      ,t1.qc_cr_nu "新客转化QC"
      ,t1.qc_cr_nu / t2.qc_cr_nu - 1 "新客转化-YOY" 
      ,t1.qc_cr_old "老客转化QC"
      ,t1.qc_cr_old / t2.qc_cr_old - 1 "老客转化-YOY"
      ,t1.q_cr_app "Q-CR"
      ,t1.s2d_qc "S2D-QC"
      ,t1.s2d_qc / t2.s2d_qc - 1 "S2D-YOY"
      ,t1.d2b_qc "D2B-QC"
      ,t1.d2b_qc / t2.d2b_qc - 1 "D2B-YOY"
      ,t1.b2o_qc "B2O-QC"
      ,t1.b2o_qc / t2.b2o_qc - 1 "B2O-YOY"
from (
    select dt
        ,pmod(datediff(dt, '2018-06-25'), 7)+1  as wkd
        ,max(case when user_type='ALL' then qc_rn_rate end) qc_rn_rate
        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old

        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app

        ,max(case when user_type='ALL' then s2d_qc end)s2d_qc
        ,max(case when user_type='ALL' then d2b_qc end)d2b_qc
        ,max(case when user_type='ALL' then b2o_qc end)b2o_qc
    from data_info 
    where dt >= date_sub(current_date, 30)
    group by 1

) t1 left join (
    select dt
        ,max(case when user_type='ALL' then qc_rn_rate end) qc_rn_rate
        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old

        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app

        ,max(case when user_type='ALL' then s2d_qc end)s2d_qc
        ,max(case when user_type='ALL' then d2b_qc end)d2b_qc
        ,max(case when user_type='ALL' then b2o_qc end)b2o_qc
    from data_info 
    group by 1 
) t2 on add_months(t1.dt, -12) = t2.dt
order by 1 desc
;


--- 2、用户端数据播报
with data_info as (
    select dt,mdd,user_type,qc_rn_rate,qc_traffic_rate,qc_cr,qc_revenue,qc_take_rate_diff,qc_subsidy_rate_diff,qc_adr,qc_order_cnt,qc_avg_rn,uv,c_uv,q_room_night,q_room_night_app,c_room_night,q_commission,c_commission,q_commission_app,q_traffic_rate,q_cr,q_cr_app,c_cr,q_take_rate,q_take_rate_app,c_take_rate,q_rn_rate_app,q_order_cnt_rate_app,q_gmv_rate_app,q_commission_rate_app,q_coupon_amt_rate_app,q_rn_rate,q_order_cnt_rate,q_gmv_rate,q_commission_rate,q_coupon_amt_rate,q_subsidy_rate,q_subsidy_rate_app,c_subsidy_rate,q_order_cnt,q_order_cnt_app,c_order_cnt,q_gmv,q_gmv_app,c_gmv,q_coupon_amount,q_coupon_amount_app,c_coupon_amount,q_order_user_cnt,q_order_user_cnt_app,c_order_user_cnt,q_adr,q_adr_app,c_adr,q_coupon_order_rate,q_coupon_order_rate_app,q_high_star_rn,q_high_star_rn_app,c_high_star_rn,q_mid_star_rn,q_mid_star_rn_app,c_mid_star_rn,q_low_star_rn,q_low_star_rn_app,c_low_star_rn,q_avg_rn_per_order,q_avg_rn_per_order_app,c_avg_rn_per_order,s_all_uv,d_s_uv,b_ds_uv,o_ds_order,s2d,d2b,b2o,s2o,s_all_uv_c,d_s_uv_c,b_ds_uv_c,o_ds_order_c,s2d_c,d2b_c,b2o_c,s2o_c,s2d_qc,d2b_qc,b2o_qc,s2o_qc
    from ihotel_default.ads_intl_hotel_qc_monitor_di
    where mdd='ALL'
)

select t1.dt "日期"
      ,wkd "星期"
      ,t1.qc_rn_rate "间夜QC"
      ,t1.qc_cr "转化QC"
      ,t1.qc_cr / t2.qc_cr - 1 "转化-YOY"
      ,t1.qc_cr_nu "新客转化QC"
      ,t1.qc_cr_nu / t2.qc_cr_nu - 1 "新客转化-YOY" 
      ,t1.qc_cr_old "老客转化QC"
      ,t1.qc_cr_old / t2.qc_cr_old - 1 "老客转化-YOY"
      ,t1.q_cr_app "Q-CR"
      ,t1.s2d_qc "S2D-QC"
      ,t1.s2d_qc / t2.s2d_qc - 1 "S2D-YOY"
      ,t1.d2b_qc "D2B-QC"
      ,t1.d2b_qc / t2.d2b_qc - 1 "D2B-YOY"
      ,t1.b2o_qc "B2O-QC"
      ,t1.b2o_qc / t2.b2o_qc - 1 "B2O-YOY"
from (
    select dt
        ,pmod(datediff(dt, '2018-06-25'), 7)+1  as wkd
        ,max(case when user_type='ALL' then qc_rn_rate end) qc_rn_rate
        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old

        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app

        ,max(case when user_type='ALL' then s2d_qc end)s2d_qc
        ,max(case when user_type='ALL' then d2b_qc end)d2b_qc
        ,max(case when user_type='ALL' then b2o_qc end)b2o_qc
    from data_info 
    where dt >= date_sub(current_date, 30)
    group by 1

) t1 left join (
    select dt
        ,max(case when user_type='ALL' then qc_rn_rate end) qc_rn_rate
        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old

        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app

        ,max(case when user_type='ALL' then s2d_qc end)s2d_qc
        ,max(case when user_type='ALL' then d2b_qc end)d2b_qc
        ,max(case when user_type='ALL' then b2o_qc end)b2o_qc
    from data_info 
    group by 1 
) t2 on add_months(t1.dt, -12) = t2.dt
order by 1 desc
;



--- 3、流量数据播报
with data_info as (
    select dt,mdd,user_type,qc_rn_rate,qc_traffic_rate,qc_cr,qc_revenue,qc_take_rate_diff,qc_subsidy_rate_diff,qc_adr,qc_order_cnt,qc_avg_rn,uv,c_uv,q_room_night,q_room_night_app,c_room_night,q_commission,c_commission,q_commission_app,q_traffic_rate,q_cr,q_cr_app,c_cr,q_take_rate,q_take_rate_app,c_take_rate,q_rn_rate_app,q_order_cnt_rate_app,q_gmv_rate_app,q_commission_rate_app,q_coupon_amt_rate_app,q_rn_rate,q_order_cnt_rate,q_gmv_rate,q_commission_rate,q_coupon_amt_rate,q_subsidy_rate,q_subsidy_rate_app,c_subsidy_rate,q_order_cnt,q_order_cnt_app,c_order_cnt,q_gmv,q_gmv_app,c_gmv,q_coupon_amount,q_coupon_amount_app,c_coupon_amount,q_order_user_cnt,q_order_user_cnt_app,c_order_user_cnt,q_adr,q_adr_app,c_adr,q_coupon_order_rate,q_coupon_order_rate_app,q_high_star_rn,q_high_star_rn_app,c_high_star_rn,q_mid_star_rn,q_mid_star_rn_app,c_mid_star_rn,q_low_star_rn,q_low_star_rn_app,c_low_star_rn,q_avg_rn_per_order,q_avg_rn_per_order_app,c_avg_rn_per_order,s_all_uv,d_s_uv,b_ds_uv,o_ds_order,s2d,d2b,b2o,s2o,s_all_uv_c,d_s_uv_c,b_ds_uv_c,o_ds_order_c,s2d_c,d2b_c,b2o_c,s2o_c,s2d_qc,d2b_qc,b2o_qc,s2o_qc
    from ihotel_default.ads_intl_hotel_qc_monitor_di
    where mdd='ALL'
    and dt >= date_sub(current_date, 45)
)

select t1.dt,t1.wkd
    ,concat(round(t1.qc_traffic_rate * 100, 2), '%') "流量QC"
    ,concat(round(t1.qc_traffic_rate_nu * 100, 2), '%') "新客流量QC"
    ,concat(round(t1.qc_traffic_rate_old * 100, 2), '%') "老客流量QC"

    ,case when        round((t1.qc_traffic_rate - t2.qc_traffic_rate) * 100, 2) < 0 
          then concat(round((t1.qc_traffic_rate - t2.qc_traffic_rate) * 100, 2), 'pp')
     else concat('+', round((t1.qc_traffic_rate - t2.qc_traffic_rate) * 100, 2), 'pp') end "流量QC日环比"
    ,case when        round((t1.qc_traffic_rate - t3.qc_traffic_rate) * 100, 2) < 0 
          then concat(round((t1.qc_traffic_rate - t3.qc_traffic_rate) * 100, 2), 'pp')
     else concat('+', round((t1.qc_traffic_rate - t3.qc_traffic_rate) * 100, 2), 'pp') end "流量QC周同比"
    
    ,case when        round((t1.qc_traffic_rate_nu - t2.qc_traffic_rate_nu) * 100, 2) < 0 
          then concat(round((t1.qc_traffic_rate_nu - t2.qc_traffic_rate_nu) * 100, 2), 'pp')
     else concat('+', round((t1.qc_traffic_rate_nu - t2.qc_traffic_rate_nu) * 100, 2), 'pp') end "新客流量QC日环比"
    ,case when        round((t1.qc_traffic_rate_nu - t3.qc_traffic_rate_nu) * 100, 2) < 0 
          then concat(round((t1.qc_traffic_rate_nu - t3.qc_traffic_rate_nu) * 100, 2), 'pp')
     else concat('+', round((t1.qc_traffic_rate_nu - t3.qc_traffic_rate_nu) * 100, 2), 'pp') end "新客流量QC周同比"
    
    ,case when        round((t1.qc_traffic_rate_old - t2.qc_traffic_rate_old) * 100, 2) < 0 
          then concat(round((t1.qc_traffic_rate_old - t2.qc_traffic_rate_old) * 100, 2), 'pp')
     else concat('+', round((t1.qc_traffic_rate_old - t2.qc_traffic_rate_old) * 100, 2), 'pp') end "老客流量QC日环比"
    ,case when        round((t1.qc_traffic_rate_old - t3.qc_traffic_rate_old) * 100, 2) < 0 
          then concat(round((t1.qc_traffic_rate_old - t3.qc_traffic_rate_old) * 100, 2), 'pp')
     else concat('+', round((t1.qc_traffic_rate_old - t3.qc_traffic_rate_old) * 100, 2), 'pp') end "老客流量QC周同比"
    
    
    ,concat(round(t1.qc_cr * 100, 2), '%')    "转化QC"
    ,concat(round(t1.qc_cr_nu * 100, 2), '%') "新客转化QC"
    ,concat(round(t1.qc_cr_old * 100, 2), '%') "老客转化QC"

    ,case when        round((t1.qc_cr - t2.qc_cr) * 100, 2) < 0 
          then concat(round((t1.qc_cr - t2.qc_cr) * 100, 2), 'pp')
     else concat('+', round((t1.qc_cr - t2.qc_cr) * 100, 2), 'pp') end "转化QC日环比"
    ,case when        round((t1.qc_cr - t3.qc_cr) * 100, 2) < 0 
          then concat(round((t1.qc_cr - t3.qc_cr) * 100, 2), 'pp')
     else concat('+', round((t1.qc_cr - t3.qc_cr) * 100, 2), 'pp') end "转化QC周同比"

    ,case when        round((t1.qc_cr_nu - t2.qc_cr_nu) * 100, 2) < 0 
          then concat(round((t1.qc_cr_nu - t2.qc_cr_nu) * 100, 2), 'pp')
     else concat('+', round((t1.qc_cr_nu - t2.qc_cr_nu) * 100, 2), 'pp') end "新客转化QC日环比"
    ,case when        round((t1.qc_cr_nu - t3.qc_cr_nu) * 100, 2) < 0 
          then concat(round((t1.qc_cr_nu - t3.qc_cr_nu) * 100, 2), 'pp')
     else concat('+', round((t1.qc_cr_nu - t3.qc_cr_nu) * 100, 2), 'pp') end "新客转化QC周同比"

    ,case when        round((t1.qc_cr_old - t2.qc_cr_old) * 100, 2) < 0 
          then concat(round((t1.qc_cr_old - t2.qc_cr_old) * 100, 2), 'pp')
     else concat('+', round((t1.qc_cr_old - t2.qc_cr_old) * 100, 2), 'pp') end "老客转化QC日环比"
    ,case when        round((t1.qc_cr_old - t3.qc_cr_old) * 100, 2) < 0 
          then concat(round((t1.qc_cr_old - t3.qc_cr_old) * 100, 2), 'pp')
     else concat('+', round((t1.qc_cr_old - t3.qc_cr_old) * 100, 2), 'pp') end "老客转化QC周同比"

    ,concat(round(t1.q_cr_app * 100, 2), '%') "Q_CR"
    ,concat(round(t1.c_cr * 100, 2), '%') "C_CR"
    ,t1.uv
    ,t1.c_uv
from (
    select dt
        ,pmod(datediff(dt, '2018-06-25'), 7)+1  as wkd
        ,max(case when user_type='ALL' then qc_traffic_rate end) qc_traffic_rate

        ,max(case when user_type='新客' then qc_traffic_rate end)qc_traffic_rate_nu
        ,max(case when user_type='老客' then qc_traffic_rate end)qc_traffic_rate_old

        ,max(case when user_type='ALL' then uv end)uv

        ,max(case when user_type='ALL' then c_uv end)c_uv
        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app
        ,max(case when user_type='ALL' then c_cr end)c_cr

        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old
    from data_info 
    where dt >= date_sub(current_date, 30)
    group by 1

) t1 left join (
    select dt
        ,max(case when user_type='ALL' then qc_traffic_rate end) qc_traffic_rate

        ,max(case when user_type='新客' then qc_traffic_rate end)qc_traffic_rate_nu
        ,max(case when user_type='老客' then qc_traffic_rate end)qc_traffic_rate_old

        ,max(case when user_type='ALL' then uv end)uv

        ,max(case when user_type='ALL' then c_uv end)c_uv
        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app
        ,max(case when user_type='ALL' then c_cr end)c_cr

        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old
    from data_info 
    group by 1
) t2 on datediff(t1.dt, t2.dt) = 1
left join (
    select dt
        ,max(case when user_type='ALL' then qc_traffic_rate end) qc_traffic_rate

        ,max(case when user_type='新客' then qc_traffic_rate end)qc_traffic_rate_nu
        ,max(case when user_type='老客' then qc_traffic_rate end)qc_traffic_rate_old

        ,max(case when user_type='ALL' then uv end)uv

        ,max(case when user_type='ALL' then c_uv end)c_uv
        ,max(case when user_type='ALL' then q_cr_app end)q_cr_app
        ,max(case when user_type='ALL' then c_cr end)c_cr

        ,max(case when user_type='ALL' then qc_cr end)qc_cr

        ,max(case when user_type='新客' then qc_cr end)qc_cr_nu
        ,max(case when user_type='老客' then qc_cr end)qc_cr_old
    from data_info 
    group by 1
) t3 on datediff(t1.dt, t3.dt) = 7
order by 1 desc
;
