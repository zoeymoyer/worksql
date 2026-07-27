--- 1、小红书产品力数据整体
with uv as(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= date_sub(current_date, 14)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,red as(
    select  flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub(current_date, 30) and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
    group by 1,2
)
,user_xhs as (
    select  uv.dt
           ,uv.user_id
    from uv uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    group by 1,2
)
,qc_conpon_xhs as (
    select t1.dt
        ,concat(round(count(distinct case when qunar_price_info['qunar_coupon_name'] is not null then id end )/count(distinct id)*100,1),'%')`用券占比（整体）`
    from default.dwd_hotel_cq_compare_price_result_intl_hi t1
    join user_xhs t2 on t1.user_id = t2.user_id and t1.dt = replace(t2.dt, '-', '')
    where t1.dt between '%(DATE_14)s' and '%(DATE)s'
        and ctrip_room_status = 'true'
        and qunar_room_status = 'true'
        and room_type_cover = 'Qmeet'
        and business_type = 'intl_crawl_cq_api_userview'
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        --and province_name = '香港'
    group by 1
)
,qc_price_xhs as (
    select order_date
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `比价次数`
        ,count(distinct user_id) `比价用户数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.01 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-1%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.02 and pay_price_diff/ctrip_pay_price <= -0.01 then id end)  `支付价beat1-2%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= -0.02 then id end)  `支付价beat2-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as order_date
            ,case when a.province_name in ('澳门','香港') then a.province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when a.identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,a.id,a.pay_price_diff,a.ctrip_pay_price,a.pay_price_compare_result,a.business_type,a.check_out,a.check_in
            ,a.user_id
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        join user_xhs t2 on a.user_id = t2.user_id and a.dt = replace(t2.dt, '-', '')
        where a.dt >= '%(DATE_14)s' and a.dt <= '%(DATE)s'
            and business_type in ('intl_crawl_cq_api_userview')
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            --and province_name = '香港'
    )t
    group by 1
)

select order_date
      ,`比价用户数`
      ,`比价次数`
      ,`用券占比（整体）`
      
      ,concat(round(`支付价lose率` * 100, 2), '%') as `支付价lose率`
      ,concat(round(`支付价lose深度` * 100, 2), '%') as `支付价lose深度`
      ,concat(round(`支付价beat数`  / `比价次数` * 100, 2), '%') as `beat率`
      ,concat(round(- `支付价beat深度` * 100, 2), '%') as `支付价beat深度`
      ,concat(round(`支付价beat0-1%次数`   / `比价次数` * 100, 2), '%') as `支付价beat0-1%率`
      ,concat(round(`支付价beat1-2%次数`   / `比价次数` * 100, 2), '%') as `支付价beat1-2%率`
      ,concat(round(`支付价beat2-3%次数`   / `比价次数` * 100, 2), '%') as `支付价beat2-3%率`
      ,concat(round(`支付价beat3-4%次数`   / `比价次数` * 100, 2), '%') as `支付价beat3-4%率`
      ,concat(round(`支付价beat4-5%次数`   / `比价次数` * 100, 2), '%') as `支付价beat4-5%率`
      ,concat(round(`支付价beat5-6%次数`   / `比价次数` * 100, 2), '%') as `支付价beat5-6%率`
      ,concat(round(`支付价beat6-7%次数`   / `比价次数` * 100, 2), '%') as `支付价beat6-7%率`
      ,concat(round(`支付价beat7-8%次数`   / `比价次数` * 100, 2), '%') as `支付价beat7-8%率`
      ,concat(round(`支付价beat8%以上次数`  / `比价次数` * 100, 2), '%') as `支付价beat8%以上率`
from qc_price_xhs t1 
left join qc_conpon_xhs t2 on replace(t1.order_date, '-', '') = t2.dt
order by 1 desc
;





-- 2、小红书香港产品力数据
with uv as(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= date_sub(current_date, 14)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,red as(
    select  flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub(current_date, 30) and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
    group by 1,2
)
,user_xhs as (
    select  uv.dt
           ,uv.user_id
    from uv uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    group by 1,2
)
,qc_conpon_xhs as (
    select t1.dt
        ,concat(round(count(distinct case when qunar_price_info['qunar_coupon_name'] is not null then id end )/count(distinct id)*100,1),'%')`用券占比（整体）`
    from default.dwd_hotel_cq_compare_price_result_intl_hi t1
    join user_xhs t2 on t1.user_id = t2.user_id and t1.dt = replace(t2.dt, '-', '')
    where t1.dt between '%(DATE_14)s' and '%(DATE)s'
        and ctrip_room_status = 'true'
        and qunar_room_status = 'true'
        and room_type_cover = 'Qmeet'
        and business_type = 'intl_crawl_cq_api_userview'
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and province_name = '香港'
    group by 1
)
,qc_price_xhs as (
    select order_date
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `比价次数`
        ,count(distinct user_id) `比价用户数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.01 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-1%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.02 and pay_price_diff/ctrip_pay_price <= -0.01 then id end)  `支付价beat1-2%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= -0.02 then id end)  `支付价beat2-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as order_date
            ,case when a.province_name in ('澳门','香港') then a.province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when a.identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,a.id,a.pay_price_diff,a.ctrip_pay_price,a.pay_price_compare_result,a.business_type,a.check_out,a.check_in
            ,a.user_id
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        join user_xhs t2 on a.user_id = t2.user_id and a.dt = replace(t2.dt, '-', '')
        where a.dt >= '%(DATE_14)s' and a.dt <= '%(DATE)s'
            and business_type in ('intl_crawl_cq_api_userview')
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            and province_name = '香港'
    )t
    group by 1
)

select order_date
      ,`比价用户数`
      ,`比价次数`
      ,`用券占比（整体）`
      
      ,concat(round(`支付价lose率` * 100, 2), '%') as `支付价lose率`
      ,concat(round(`支付价lose深度` * 100, 2), '%') as `支付价lose深度`
      ,concat(round(`支付价beat数`  / `比价次数` * 100, 2), '%') as `beat率`
      ,concat(round(- `支付价beat深度` * 100, 2), '%') as `支付价beat深度`
      ,concat(round(`支付价beat0-1%次数`   / `比价次数` * 100, 2), '%') as `支付价beat0-1%率`
      ,concat(round(`支付价beat1-2%次数`   / `比价次数` * 100, 2), '%') as `支付价beat1-2%率`
      ,concat(round(`支付价beat2-3%次数`   / `比价次数` * 100, 2), '%') as `支付价beat2-3%率`
      ,concat(round(`支付价beat3-4%次数`   / `比价次数` * 100, 2), '%') as `支付价beat3-4%率`
      ,concat(round(`支付价beat4-5%次数`   / `比价次数` * 100, 2), '%') as `支付价beat4-5%率`
      ,concat(round(`支付价beat5-6%次数`   / `比价次数` * 100, 2), '%') as `支付价beat5-6%率`
      ,concat(round(`支付价beat6-7%次数`   / `比价次数` * 100, 2), '%') as `支付价beat6-7%率`
      ,concat(round(`支付价beat7-8%次数`   / `比价次数` * 100, 2), '%') as `支付价beat7-8%率`
      ,concat(round(`支付价beat8%以上次数`  / `比价次数` * 100, 2), '%') as `支付价beat8%以上率`
from qc_price_xhs t1 
left join qc_conpon_xhs t2 on replace(t1.order_date, '-', '') = t2.dt
order by 1 desc
;


--- 3、小红书整体券明细
with uv as(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= date_sub(current_date, 14)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,red as(
    select  flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub(current_date, 30) and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
    group by 1,2
)
,user_xhs as (
    select  uv.dt
           ,uv.user_id
    from uv uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    group by 1,2
)
,quan as (
    select dt
        ,qunar_price_info['qunar_coupon_batch_num'] as `Q优惠券ID`
        ,qunar_price_info['qunar_coupon_name'] as `Q优惠券名称`
        ,sum(get_json_object(regexp_extract(qunar_promotion, concat('"name":"', qunar_price_info['qunar_coupon_name'], '"[^}]*"amount":"([^"]+)"'), 1), '$')) as coupon_amount
        ,sum(qunar_before_coupons_cashback_price) as `Q券前支付价`
        ,count(id) as `Q优惠券比价次数`
    from default.dwd_hotel_cq_compare_price_result_intl_hi t1
    join user_xhs t2 on t1.user_id = t2.user_id and t1.dt = replace(t2.dt, '-', '')
    where t1.dt between '%(DATE_7)s' and '%(DATE)s'
        and t1.ctrip_room_status = 'true'
        and t1.qunar_room_status = 'true'
        and t1.room_type_cover = 'Qmeet'
        and t1.business_type = 'intl_crawl_cq_api_userview'
        and t1.compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
    group by 1,2,3
)
,compare as (
    select dt
        ,count(id) as `总比价次数`
    from default.dwd_hotel_cq_compare_price_result_intl_hi t1
    join user_xhs t2 on t1.user_id = t2.user_id and t1.dt = replace(t2.dt, '-', '')
    where t1.dt between '%(DATE_7)s' and '%(DATE)s'
        and t1.ctrip_room_status = 'true'
        and t1.qunar_room_status = 'true'
        and t1.room_type_cover = 'Qmeet'
        and t1.business_type = 'intl_crawl_cq_api_userview'
        and t1.compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
    group by 1
)

select t1.dt
    ,`Q优惠券ID`
    ,`Q优惠券名称`
    ,`Q优惠券比价次数`
    ,`总比价次数`
    ,concat(round(coupon_amount / `Q券前支付价` * 100, 1), '%') as `补贴率`
    ,concat(round(`Q优惠券比价次数` / `总比价次数` * 100, 1), '%') as `用券占比`
from quan t1
left join compare t2
    on t1.dt = t2.dt
order by 1 desc
;