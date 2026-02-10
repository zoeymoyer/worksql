with user_type as
(
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

,pay_after_order as (
    ---后付订单明细
    SELECT  order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type
            ,order_no
            ,checkout_date
            ,city_name
            ,a.province_name
            ,a.country_name
            ,a.user_id
            ,user_name
            ,order_status
            ,customer_names
            ,init_gmv 
            ,room_night
            ,case when (coupon_substract is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                else coalesce(coupon_substract, 0) end as q_qe   --`Q_券额`
            ,ext_flag_map['sub_auth_type'] sub_auth_type
            ,ext_flag_map['post_pay_flag'] post_pay_flag
            ,ext_flag_map['selected_payment_type'] selected_payment_type
            ,case when ext_flag_map['pay_after_stay_flag'] = 'true' then '后付订单'
                when ext_flag_map['pay_after_stay_flag'] = 'false' then '非后付订单'
                else '其他'
                end as  is_pay_after ---`是否后付订单`
            ,case when ext_flag_map['selected_payment_type'] = 1 then '标准后付'
                when ext_flag_map['selected_payment_type'] = 2 then '拿去花后付'
                else '其他'
                end as  pay_after_type  ---`后付支付方式`
            ,case when ext_flag_map['sub_auth_type'] = 1 then '微信免密'
                when ext_flag_map['sub_auth_type'] = 2 then '微信支付分'
                when ext_flag_map['sub_auth_type'] = 3 then '支付宝免密'
                when ext_flag_map['sub_auth_type'] = 4 then '支付宝芝麻分'
                when ext_flag_map['sub_auth_type'] = 5 then '支付宝预授权'
                when ext_flag_map['sub_auth_type'] = 6 then '银行卡'
                when ext_flag_map['sub_auth_type'] = 7 then '拿去花'
                when ext_flag_map['sub_auth_type'] = 99 then '其他'
                else '未知'
                end as pay_auth_after   --- `后付授权的支付方式`
            ,case when ext_flag_map['post_pay_flag'] = 1 then '用户扣款成功'
                when ext_flag_map['post_pay_flag'] = 2 then '垫资扣款成功'
                when ext_flag_map['post_pay_flag'] = 3 then '垫资扣款失败'
                when ext_flag_map['post_pay_flag'] = 4 then '用户向垫资扣款成功'
                when ext_flag_map['post_pay_flag'] = 5 then '用户向垫资扣款失败'
                when ext_flag_map['post_pay_flag'] is null then '未扣款'
                else '其他'
                end as  post_pay_flag_type --- `扣款状态`
    FROM default.mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name  
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
    and order_status not in ('CANCELLED','REJECTED')
    --and order_status = 'CHECKED_OUT'
    and is_valid = 1
    and ext_flag_map['selected_payment_type'] = 1   --- 标准后付
    and ext_flag_map['pay_after_stay_flag'] = 'true' --- 后付订单
    and order_date >= '2025-10-20' and order_date <= date_sub(current_date, 1)
    order by order_date desc
)

select order_date
      ,count(distinct order_no) `后付订单`
      ,count(distinct case when order_status = 'CHECKED_OUT' then order_no end) `后付离店订单`
      ,sum(case when order_status = 'CHECKED_OUT' then init_gmv end) `后付离店订单GMV`
      ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (1,4) then order_no end) `后付离店已扣款订单`
      ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (1,4) then init_gmv end) `后付离店已扣款订单GMV`
      ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) then order_no end) `后付离店扣款失败订单`
      ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) then init_gmv end) `后付离店扣款失败订单GMV`
from pay_after_order
group by 1
;





with pay_after_order as (
    ---后付订单明细
    SELECT  dt
            ,order_date
            ,order_no
            ,checkout_date
            ,city_name
            ,a.province_name
            ,a.country_name
            ,a.user_id
            ,user_name
            ,order_status
            ,customer_names
            ,init_gmv 
            ,room_night
            ,case when (coupon_substract is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                else coalesce(coupon_substract, 0) end as q_qe   --`Q_券额`
            ,ext_flag_map['sub_auth_type'] sub_auth_type
            ,ext_flag_map['post_pay_flag'] post_pay_flag
            ,ext_flag_map['selected_payment_type'] selected_payment_type
            ,case when ext_flag_map['pay_after_stay_flag'] = 'true' then '后付订单'
                when ext_flag_map['pay_after_stay_flag'] = 'false' then '非后付订单'
                else '其他'
                end as  is_pay_after ---`是否后付订单`
            ,case when ext_flag_map['selected_payment_type'] = 1 then '标准后付'
                when ext_flag_map['selected_payment_type'] = 2 then '拿去花后付'
                else '其他'
                end as  pay_after_type  ---`后付支付方式`
            ,case when ext_flag_map['sub_auth_type'] = 1 then '微信免密'
                when ext_flag_map['sub_auth_type'] = 2 then '微信支付分'
                when ext_flag_map['sub_auth_type'] = 3 then '支付宝免密'
                when ext_flag_map['sub_auth_type'] = 4 then '支付宝芝麻分'
                when ext_flag_map['sub_auth_type'] = 5 then '支付宝预授权'
                when ext_flag_map['sub_auth_type'] = 6 then '银行卡'
                when ext_flag_map['sub_auth_type'] = 7 then '拿去花'
                when ext_flag_map['sub_auth_type'] = 99 then '其他'
                else '未知'
                end as pay_auth_after   --- `后付授权的支付方式`
            ,case when ext_flag_map['post_pay_flag'] = 1 then '用户扣款成功'
                when ext_flag_map['post_pay_flag'] = 2 then '垫资扣款成功'
                when ext_flag_map['post_pay_flag'] = 3 then '垫资扣款失败'
                when ext_flag_map['post_pay_flag'] = 4 then '用户向垫资扣款成功'
                when ext_flag_map['post_pay_flag'] = 5 then '用户向垫资扣款失败'
                when ext_flag_map['post_pay_flag'] is null then '未扣款'
                else '其他'
                end as  post_pay_flag_type --- `扣款状态`
    FROM default.mdw_order_v3_international a
    WHERE dt = '%(DATE)s'
    and order_status not in ('CANCELLED', 'REJECTED')
    --and order_status = 'CHECKED_OUT'
    and is_valid = 1
    and ext_flag_map['pay_after_stay_flag'] = 'true' --- 后付订单
    and order_date >= '2025-10-20' and order_date <= date_sub(current_date, 1)
)


select dt
      ,after_order                as `后付订单总量`
      ,checkout_order             as `后付离店订单总量`
      ,non_checkout_order         as `后付未离店订单总量`
      ,round(after_gmv)           as `后付订单GMV`
      ,round(checkout_gmv)        as `后付离店订单GMV`
      ,round(non_checkout_gmv)    as `后付未离店订单GMV`
      ,checkout_pay_order         as `后付离店已扣款订单总量`
      ,round(checkout_pay_gmv)    as `后付离店已扣款订单GMV`
      ,checkout_no_pay_order      as `后付离店未扣款订单总量`
      ,round(checkout_no_pay_gmv) as `后付离店未扣款订单GMV`
      ,concat(round(checkout_no_pay_bad_order / checkout_order_q * 100, 2), '%') as `坏账率-订单`
      ,concat(round(checkout_no_pay_order_t1 /  checkout_order_q * 100, 2), '%') as `T1坏账率-订单`
      ,concat(round(checkout_no_pay_order_t3 /  checkout_order_q * 100, 2), '%') as `T3坏账率-订单`
      ,concat(round(checkout_no_pay_order_t7 /  checkout_order_q * 100, 2), '%') as `T7坏账率-订单`
      ,concat(round(checkout_no_pay_order_t15 / checkout_order_q * 100, 2), '%') as `T15坏账率-订单`
      ,concat(round(checkout_no_pay_order_t30 / checkout_order_q * 100, 2), '%') as `T30坏账率-订单`
      ,concat(round(checkout_no_pay_order_t60 / checkout_order_q * 100, 2), '%') as `T60坏账率-订单`
      ,concat(round(checkout_no_pay_order_t90 / checkout_order_q * 100, 2), '%') as `T90坏账率-订单`
      
      ,concat(round(checkout_no_pay_bad_gmv / checkout_gmv_q * 100, 2), '%') as `坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t1 /  checkout_gmv_q * 100, 2), '%') as `T1坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t3 /  checkout_gmv_q * 100, 2), '%') as `T3坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t7 /  checkout_gmv_q * 100, 2), '%') as `T7坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t15 / checkout_gmv_q * 100, 2), '%') as `T15坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t30 / checkout_gmv_q * 100, 2), '%') as `T30坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t60 / checkout_gmv_q * 100, 2), '%') as `T60坏账率-GMV`
      ,concat(round(checkout_no_pay_gmv_t90 / checkout_gmv_q * 100, 2), '%') as `T90坏账率-GMV`

from (
    select dt
        ,count(distinct order_no) after_order   --- 后付订单总量
        ,count(distinct case when order_status = 'CHECKED_OUT' then order_no end) checkout_order --- 后付离店订单总量
        ,count(distinct case when order_status != 'CHECKED_OUT' then order_no end) non_checkout_order --- 后付未离店订单总量
        ,sum(init_gmv) after_gmv   --- 后付订单GMV
        ,sum(case when order_status = 'CHECKED_OUT' then init_gmv end) checkout_gmv --- 后付离店订单GMV
        ,sum(case when order_status != 'CHECKED_OUT' then init_gmv end) non_checkout_gmv --- 后付未离店订单GMV

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (1,4)  then order_no end) checkout_pay_order  --- 后付离店已扣款订单
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (1,4)  then init_gmv end) checkout_pay_gmv  --- 后付离店已扣款订单GMV

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5)  then order_no end) checkout_no_pay_order  --- 后付离店扣款失败订单
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5)  then init_gmv end) checkout_no_pay_gmv  --- 后付离店扣款失败订单GMV

        ,count(distinct case when order_status = 'CHECKED_OUT' and selected_payment_type = 1 then order_no end) checkout_order_q --- 后付离店订单标准后付
        ,sum(case when order_status = 'CHECKED_OUT' and selected_payment_type = 1 then init_gmv end) checkout_gmv_q --- 后付未离店订单GMV标准后付

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and selected_payment_type = 1 then order_no end)  checkout_no_pay_bad_order --- 后付离店扣款失败订单
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and selected_payment_type = 1 then init_gmv end)  checkout_no_pay_bad_gmv  --- 后付离店扣款失败订单

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 1 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t1  --- 后付离店扣款失败订单逾期1天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 1  and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t1  --- 后付离店扣款失败订单GMV逾期1天

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 3 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t3  --- 后付离店扣款失败订单逾期3天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 3 and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t3  --- 后付离店扣款失败订单GMV逾期3天

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 7 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t7  --- 后付离店扣款失败订单逾期7天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 7 and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t7  --- 后付离店扣款失败订单GMV逾期7天

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 15 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t15  --- 后付离店扣款失败订单逾期15天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 15 and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t15  --- 后付离店扣款失败订单GMV逾期15天

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 30 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t30  --- 后付离店扣款失败订单逾期30天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 30 and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t30  --- 后付离店扣款失败订单GMV逾期30天

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 60 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t60  --- 后付离店扣款失败订单逾期60天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 60 and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t60  --- 后付离店扣款失败订单GMV逾期60天

        ,count(distinct case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 90 and selected_payment_type = 1 then order_no end) checkout_no_pay_order_t90  --- 后付离店扣款失败订单逾期90天
        ,sum(case when order_status = 'CHECKED_OUT' and post_pay_flag in (2,3,5) and datediff(date_sub(current_date, 0), checkout_date) >= 90 and selected_payment_type = 1 then init_gmv end) checkout_no_pay_gmv_t90  --- 后付离店扣款失败订单GMV逾期90天

    from pay_after_order
    group by 1
)