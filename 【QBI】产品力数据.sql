--- 3、产品力数据
with qc_price as (
    select order_date
        ,business_type_name
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
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
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result,business_type,check_out,check_in
            ,case when business_type = 'intl_crawl_cq_spa' then '抓取'
                  when business_type = 'intl_crawl_cq_api_order' then '生单'
                  when business_type = 'intl_crawl_cq_api_userview_acc' then '主站模拟券后'
                  else '其他' end as business_type_name
            -- 【新增】: 使用解析后的 order_date 和 check_in 日期计算提前订分布
            ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) < 0 or datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) = 0 then '凌晨订&当天订'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 1 and 3    then '提前订1-3天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 4 and 7    then '提前订4-7天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14   then '提前订8-14天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
             end as per_type
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260410' and dt <= replace(date_sub(current_date, 1),'-','')
            -- and business_type = 'intl_crawl_cq_spa'  -- intl_crawl_cq_spa 抓取  intl_crawl_cq_api_order 生单  intl_crawl_cq_api_userview 主站（流量） intl_crawl_cq_api_userview_acc 主站模拟券后价
            and business_type in ('intl_crawl_cq_spa', 'intl_crawl_cq_api_order', 'intl_crawl_cq_api_userview_acc')
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
    )t
    group by 1,2,cube(user_type,mdd)
)

select order_date
      ,business_type_name
      ,mdd
      ,user_type
      ,`支付价lose率`
      ,`支付价lose深度`
      ,`支付价beat深度`
      ,`支付价beat数`         / `支付价抓取次数`  `beat率`
      ,`支付价beat0-3%次数`   / `支付价抓取次数`  `支付价beat0-3%率`
      ,`支付价beat3-4%次数`   / `支付价抓取次数`  `支付价beat3-4%率`
      ,`支付价beat4-5%次数`   / `支付价抓取次数`  `支付价beat4-5%率`
      ,`支付价beat5-6%次数`   / `支付价抓取次数`  `支付价beat5-6%率`
      ,`支付价beat6-7%次数`   / `支付价抓取次数`  `支付价beat6-7%率`
      ,`支付价beat7-8%次数`   / `支付价抓取次数`  `支付价beat7-8%率`
      ,`支付价beat8%以上次数`  / `支付价抓取次数`  `支付价beat8%以上率`
from qc_price
order by 1,2,3,4
;





with abt as ( 
    select  ab_exp_value as user_id,dt
       ,case when ab_version in ('A') then 'A空白组85%' 
             when ab_version in ('B') then 'B实验组5%' 
             when ab_version in ('C') then 'C对照组5%' 
             when ab_version in ('D') then 'D对照组5%' 
             else 'null' end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt  >= '2026-02-08'  and dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
    group by 1,2,3
)
,q_order_info as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id  and a.order_date=t.dt
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and t.ab_type is not null
)
,q_order_app as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
    from default.mdw_order_v3_international a 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_uv as (
    select 
        t.ab_type,a.dt
        ,count(distinct a.user_id) as `q_uv`
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id  and a.dt=t.dt
    where a.dt  >= '2026-02-08'  and a.dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
    group by 1,2
)
,q_order as (
    select 
        ab_type,order_date
        ,count(distinct user_id) as `q_生单用户`   
        ,count(order_no) as `q_订单量` 
        ,sum(room_night) as `q_间夜量` 
        ,sum(ld_yj) as `q_收益额`
        ,sum(init_gmv) as `q_GMV`
        ,sum(coupon_substract_summary) as `q_券补贴额`
        ,sum(q_jf) as `q_积分补贴额`
        ,sum(case when use_promotion_enjoy_first is not null then promotionAmount end) as `q_优惠先享补贴额`
        ,count(case when use_promotion_enjoy_first is not null then order_no end) as `q_先享订单量` 
    from q_order_info
    group by 1,2
)
,q_order_reorder as (
    select 
        t1.ab_type,t1.order_date
        ,count(distinct t1.user_id) as `复购生单用户`   
        ,count(distinct t2.order_no) as `复购订单量` 
        ,sum(t2.ld_yj) as `复购收益`
        ,sum(t2.room_night) as `复购间夜`
    from q_order_info t1 
    left join q_order_app t2 on t1.user_id=t2.user_id and t2.order_time > t1.order_time and datediff(t2.order_date, t1.order_date) <= 7 
    where t2.user_id is not null 
          ---- 限定7天之前的复购（满7日）
          and t1.order_date <= date_sub(current_date, 8) 
    group by 1,2
)

select a.order_date,
    a.ab_type as `实验分组`,
    b.`q_uv` as uv,
    a.`q_生单用户` as `生单用户`,
    a.`q_订单量` as `订单量`,
    a.`q_间夜量` as `间夜量`,
    round(a.`q_收益额`) as `收益额`,
    round(a.`q_GMV`) as `GMV`,
    round(a.`q_券补贴额`,1) as `券补贴额`,
    round(a.`q_积分补贴额`,1) as `积分补额`,
    round(a.`q_优惠先享补贴额`,1) as `优惠先享补贴额`,
    concat(round((a.`q_生单用户`/b.`q_uv`)*100,2),'%') as U2O,
    concat(round((a.`q_订单量`/b.`q_uv`)*100,2),'%') as CR,
    concat(round((a.`q_收益额`/a.`q_GMV`)*100,2),'%') as `佣金率`,
    concat(round((a.`q_券补贴额`/a.`q_GMV`)*100,2),'%') as `券补贴率`,
    concat(round((a.`q_优惠先享补贴额`/a.`q_GMV`)*100,2),'%') as `优惠先享补贴率`,
    round(a.`q_间夜量`/b.`q_uv`,4) as `单UV间夜`,
    round(a.`q_收益额`/b.`q_uv`,4) as `单UV收益`,
    round(a.`q_优惠先享补贴额`/b.`q_uv`,4) as `单UV先享`,
    a.`q_先享订单量` as `先享订单量`,
    concat(round((a.`q_先享订单量`/a.`q_订单量`)*100,2),'%') as `先享订单占比`,
    c.`复购生单用户` as `复购生单用户`,
    c.`复购订单量` as `复购订单量`,
    c.`复购收益` as `复购收益`,
    c.`复购间夜` as `复购间夜`
from q_order a
left join q_uv b on a.ab_type = b.ab_type and a.order_date=b.dt
left join q_order_reorder c on a.ab_type = c.ab_type and a.order_date=c.order_date
order by a.ab_type asc,a.order_date asc
;


