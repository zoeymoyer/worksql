-- 多房型
with abt as (
    select distinct
            ab_exp_value as user_id
            ,case when ab_version = 'A' then 'A空白组40%'
                  when ab_version = 'B' then 'B实验组20%'
                  when ab_version = 'C' then 'C对照组20%'
                  when ab_version = 'D' then 'D对照组20%'
                  else '其他'
             end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt between '2026-04-25' and '2026-05-07'
        and type = 'flow'
        and user_id_type = 'user_id'
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
)
,q_uv as (
    select  a.dt as stat_date
            ,t.ab_type
            ,count(distinct a.user_id) as q_uv
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    inner join abt t
        on a.user_id = t.user_id
    where a.dt between '2026-04-25' and '2026-05-07'
        and a.business_type = 'hotel'
        and (a.province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        and a.device_id is not null
        and a.device_id <> ''
        and a.hotel_seq is not null
    group by 1,2
)
,detail_room_uv as (
    select  regexp_replace(p.dt, '(\\d{4})(\\d{2})(\\d{2})', '$1-$2-$3') as stat_date
            ,t.ab_type
            ,count(distinct case when cast(p.physical_room_index as int) = 1 then p.user_id end) as low_price_uv
            ,count(distinct case when cast(p.physical_room_index as int) <> 1 then p.user_id end) as non_low_price_uv
    from ihotel_default.dw_hotel_price_display p
    inner join abt t
        on p.user_id = t.user_id
    where p.dt between regexp_replace('2026-04-25', '-', '') and regexp_replace('2026-05-07', '-', '')
        and p.user_id is not null
        and p.hotel_seq is not null
        and p.physical_room_id is not null
        and p.physical_room_index is not null
        and (p.province_name in ('台湾','澳门','香港') or p.country_name != '中国')
    group by 1,2
)
,uv_metrics as (
    select  u.ab_type
            ,sum(u.q_uv) as all_uv
            ,sum(nvl(d.low_price_uv, 0)) as low_price_uv
            ,sum(nvl(d.non_low_price_uv, 0)) as non_low_price_uv
    from q_uv u
    left join detail_room_uv d
        on u.stat_date = d.stat_date
        and u.ab_type = d.ab_type
    group by 1
)
,detail_lowest_room as (
    select distinct
            p.dt as view_dt
            ,p.user_id
            ,p.hotel_seq
            ,p.physical_room_id
    from ihotel_default.dw_hotel_price_display p
    where p.dt between regexp_replace('2026-04-25', '-', '') and regexp_replace('2026-05-07', '-', '')
        and cast(p.physical_room_index as int) = 1
        and p.user_id is not null
        and p.hotel_seq is not null
        and p.physical_room_id is not null
)
,order_base as (
    select  a.order_no
            ,a.user_id
            ,a.order_date
            ,a.hotel_seq
            ,a.physical_room_id
            ,a.room_night
            ,a.init_gmv
            ,a.init_commission_after
            ,a.ext_plat_certificate
            ,a.coupon_substract_summary
            ,a.promotion_score_info
            ,a.ext_flag_map
            ,t.ab_type
            ,nvl(get_json_object(a.ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) as enjoy_first_amount
            ,nvl(get_json_object(a.promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) as point_subsidy_amount
    from default.mdw_order_v3_international a
    inner join abt t
        on a.user_id = t.user_id
    where a.dt = regexp_replace('2026-05-07', '-', '')
        and (a.province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        and a.is_valid = '1'
        and a.terminal_channel_type = 'app'
        and a.order_status not in ('CANCELLED','REJECTED')
        and a.order_date between '2026-04-25' and '2026-05-07'
        and a.order_no <> '103576132435'
)
,low_price_order as (
    select distinct
            a.order_no
    from order_base a
    inner join detail_lowest_room p
        on a.user_id = p.user_id
        and a.hotel_seq = p.hotel_seq
        and a.physical_room_id = p.physical_room_id
        and regexp_replace(a.order_date, '-', '') = p.view_dt
)
,order_tag as (
    select  a.*
            ,case when l.order_no is not null then 1 else 0 end as is_low_price_order
    from order_base a
    left join low_price_order l
        on a.order_no = l.order_no
)
,order_metrics as (
    select  ab_type
            ,room_scope
            ,count(distinct user_id) as order_user_cnt
            ,count(distinct order_no) as order_cnt
            ,sum(room_night) as room_night
            ,sum(init_gmv) as gmv
            ,sum(nvl(init_commission_after, 0) + nvl(ext_plat_certificate, 0)) as revenue
            ,sum(nvl(coupon_substract_summary, 0)) as coupon_subsidy_amount
            ,sum(nvl(point_subsidy_amount, 0)) as point_subsidy_amount
            ,sum(nvl(enjoy_first_amount, 0)) as enjoy_first_subsidy_amount
            ,sum(nvl(coupon_substract_summary, 0) + nvl(point_subsidy_amount, 0) + nvl(enjoy_first_amount, 0)) as total_subsidy_amount
            ,count(distinct case when enjoy_first_amount > 0 then order_no end) as enjoy_first_order_cnt
    from (
        select  *
                ,'所有房型' as room_scope
        from order_tag

        union all

        select  *
                ,'最低价房型' as room_scope
        from order_tag
        where is_low_price_order = 1

        union all

        select  *
                ,'非最低价房型' as room_scope
        from order_tag
        where is_low_price_order = 0
    ) x
    group by 1,2
)
,final_metrics as (
    select  u.ab_type
            ,'所有房型' as room_scope
            ,u.all_uv as uv
            ,m.order_user_cnt
            ,m.order_cnt
            ,m.room_night
            ,m.gmv
            ,m.revenue
            ,m.coupon_subsidy_amount
            ,m.point_subsidy_amount
            ,m.enjoy_first_subsidy_amount
            ,m.total_subsidy_amount
            ,m.enjoy_first_order_cnt
    from uv_metrics u
    left join order_metrics m
        on u.ab_type = m.ab_type
        and m.room_scope = '所有房型'

    union all

    select  u.ab_type
            ,'最低价房型' as room_scope
            ,u.low_price_uv as uv
            ,m.order_user_cnt
            ,m.order_cnt
            ,m.room_night
            ,m.gmv
            ,m.revenue
            ,m.coupon_subsidy_amount
            ,m.point_subsidy_amount
            ,m.enjoy_first_subsidy_amount
            ,m.total_subsidy_amount
            ,m.enjoy_first_order_cnt
    from uv_metrics u
    left join order_metrics m
        on u.ab_type = m.ab_type
        and m.room_scope = '最低价房型'

    union all

    select  u.ab_type
            ,'非最低价房型' as room_scope
            ,u.non_low_price_uv as uv
            ,m.order_user_cnt
            ,m.order_cnt
            ,m.room_night
            ,m.gmv
            ,m.revenue
            ,m.coupon_subsidy_amount
            ,m.point_subsidy_amount
            ,m.enjoy_first_subsidy_amount
            ,m.total_subsidy_amount
            ,m.enjoy_first_order_cnt
    from uv_metrics u
    left join order_metrics m
        on u.ab_type = m.ab_type
        and m.room_scope = '非最低价房型'
)

select  ab_type as `实验分组`
        ,room_scope as `房型口径`
        ,sum(nvl(uv, 0)) as `UV`
        ,sum(nvl(order_user_cnt, 0)) as `下单用户量`
        ,sum(nvl(order_cnt, 0)) as `订单量`
        ,sum(nvl(room_night, 0)) as `间夜量`
        ,round(sum(nvl(gmv, 0)), 1) as `GMV`
        ,round(sum(nvl(revenue, 0)), 1) as `收益额`
        ,round(sum(nvl(coupon_subsidy_amount, 0)), 1) as `券补贴金额`
        ,round(sum(nvl(point_subsidy_amount, 0)), 1) as `积分补贴金额`
        ,round(sum(nvl(enjoy_first_subsidy_amount, 0)), 1) as `优惠先享补贴金额`
        ,round(sum(nvl(total_subsidy_amount, 0)), 1) as `整体补贴金额`
        ,concat(round(sum(nvl(order_user_cnt, 0)) / sum(nvl(uv, 0)) * 100, 2), '%') as `转化率`
        ,concat(round(sum(nvl(order_cnt, 0)) / sum(nvl(uv, 0)) * 100, 2), '%') as `订单CR`
        ,round(sum(nvl(gmv, 0)) / sum(nvl(order_cnt, 0)), 4) as `订单均价`
        ,round(sum(nvl(revenue, 0)) / sum(nvl(order_cnt, 0)), 4) as `单订单收益`
        ,concat(round(sum(nvl(revenue, 0)) / sum(nvl(gmv, 0)) * 100, 2), '%') as `佣金率`
        ,concat(round(sum(nvl(total_subsidy_amount, 0)) / sum(nvl(gmv, 0)) * 100, 2), '%') as `整体补贴率`
        ,concat(round(sum(nvl(enjoy_first_subsidy_amount, 0)) / sum(nvl(gmv, 0)) * 100, 2), '%') as `优惠先享补贴率`
        ,sum(nvl(enjoy_first_order_cnt, 0)) as `优惠先享订单量`
        ,concat(round(sum(nvl(enjoy_first_order_cnt, 0)) / sum(nvl(order_cnt, 0)) * 100, 2), '%') as `优惠先享订单占比`
from final_metrics
where ab_type in ('A空白组40%','B实验组20%','C对照组20%','D对照组20%')
group by 1,2
order by case when ab_type = 'A空白组40%' then 1
              when ab_type = 'B实验组20%' then 2
              when ab_type = 'C对照组20%' then 3
              when ab_type = 'D对照组20%' then 4
              else 5
         end
        ,case when room_scope = '所有房型' then 1
              when room_scope = '最低价房型' then 2
              when room_scope = '非最低价房型' then 3
              else 4
         end
;



-- 优惠先享20%流量阶段实验数据-12天
-- 所有房型
with abt as (
    select distinct
            ab_exp_value as user_id
            ,case when ab_version in ('A') then 'A空白组40%'
                  when ab_version in ('B') then 'B实验组20%'
                  when ab_version in ('C') then 'C对照组20%'
                  when ab_version in ('D') then 'D对照组20%'
                  else 'null'
             end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt between '2026-04-25' and '2026-05-07'
        and type = 'flow'
        and user_id_type = 'user_id'
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
)
,hotel_share as (
    select  dt
            ,hotel_seq
            ,qc_hotel_type
    from ihotel_default.dim_baseinfo_hotel_share_compare_category
    where dt between '2026-04-25' and '2026-05-07'
)
,compare_type as (
    -- 根据酒店的比价竞争情况进行分类
    -- QC竞争（携程占比≥45%）
    -- 多元竞争（携程占比30%-45%）
    -- 非C竞争（携程占比<30%）
    select distinct
            hotel_seq
            ,case when c_qcnt / q_cnt >= 0.45 then 'QC竞争'
                  when c_qcnt / q_cnt >= 0.3 then '多元竞争'
                  else '非C竞争'
             end as compare_type
    from (
        select  hotel_seq
                ,count(case when compare_plat like '%携程%' then 1 end) as c_qcnt
                ,count(case when compare_plat like '%美团%' then 1 end) as mt_qcnt
                ,count(case when compare_plat like '%同程%' then 1 end) as te_qcnt
                ,count(case when compare_plat like '%飞猪%' then 1 end) as fg_qcnt
                ,count(case when compare_plat like '%Agoda%' then 1 end) as a_qcnt
                ,count(case when compare_plat like '%Booking%' then 1 end) as b_qcnt
                ,count(case when compare_plat like '%其他%' then 1 end) as other_qcnt
                ,count(1) as q_cnt
        from fuwu.inter_hotel_bijia_ups_scene_mingxi
        where dt between date_add('2026-05-06', -366) and '2026-05-07'
            and is_compare = '1'
            and compare_plat is not null
        group by 1
    ) q1
    where q_cnt >= 20
)
,q_uv as (
    select  a.dt as order_date
            ,t.ab_type
            -- ,s.qc_hotel_type
            -- ,u.compare_type
            ,count(distinct a.user_id) as `q_uv`
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join abt t
        on a.user_id = t.user_id
    left join hotel_share s
        on a.hotel_seq = s.hotel_seq
        and a.dt = s.dt
    left join compare_type u
        on a.hotel_seq = u.hotel_seq
    where a.dt between '2026-04-25' and '2026-05-07'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        and a.device_id is not null
        and a.device_id <> ''
        and t.ab_type is not null
        and a.hotel_seq is not null
    group by 1,2
)
,q_order as (
    select  a.order_date
            ,t.ab_type
            -- ,s.qc_hotel_type
            -- ,u.compare_type
            ,count(distinct a.user_id) as `q_用户量`
            ,count(order_no) as `q_订单量`
            ,sum(room_night) as `q_间夜量`
            ,sum(init_commission_after) + sum(nvl(ext_plat_certificate, 0)) as `q_收益额`
            ,sum(init_gmv) as `q_GMV`
            ,sum(nvl(coupon_substract_summary, 0)) as `q_券补贴额`
            ,sum(nvl(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0)) as `q_积分补贴额`
            ,sum(nvl(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0)) as `q_优惠先享补贴额`
            ,count(case when nvl(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) > 0 then order_no end) as `q_先享订单量`
    from default.mdw_order_v3_international a
    left join abt t
        on a.user_id = t.user_id
    left join hotel_share s
        on a.hotel_seq = s.hotel_seq
        and a.order_date = s.dt
    left join compare_type u
        on a.hotel_seq = u.hotel_seq
    where a.dt = regexp_replace('2026-05-07', '-', '')
        and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        and is_valid = '1'
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and order_date between '2026-04-25' and '2026-05-07'
        and t.ab_type is not null
        and order_no <> '103576132435'
    group by 1,2
)

select  a.ab_type as `实验分组`
        -- ,a.qc_hotel_type as `QC酒店市占分层`
        -- ,a.compare_type as `UPS比价对象`
        ,sum(b.`q_uv`) as uv
        ,sum(a.`q_用户量`) as `o用户量`
        ,sum(a.`q_订单量`) as `订单量`
        ,sum(a.`q_间夜量`) as `间夜量`
        ,round(sum(a.`q_收益额`), 1) as `收益额`
        ,round(sum(a.`q_GMV`), 1) as `GMV`
        ,round(sum(a.`q_券补贴额`), 1) as `券补贴额`
        ,round(sum(a.`q_积分补贴额`), 1) as `积分补额`
        ,round(sum(a.`q_优惠先享补贴额`), 1) as `优惠先享补贴额`
        ,concat(round(sum(a.`q_用户量`) / sum(b.`q_uv`) * 100, 2), '%') as U2O
        ,concat(round(sum(a.`q_订单量`) / sum(b.`q_uv`) * 100, 2), '%') as CR
        ,concat(round(sum(a.`q_收益额`) / sum(a.`q_GMV`) * 100, 2), '%') as `佣金率`
        ,concat(round(sum(a.`q_券补贴额`) / sum(a.`q_GMV`) * 100, 2), '%') as `券补贴率`
        ,concat(round(sum(a.`q_优惠先享补贴额`) / sum(a.`q_GMV`) * 100, 2), '%') as `优惠先享补贴率`
        ,round(sum(a.`q_间夜量`) / sum(b.`q_uv`), 4) as `单UV间夜`
        ,round(sum(a.`q_收益额`) / sum(b.`q_uv`), 4) as `单UV收益`
        ,round(sum(a.`q_优惠先享补贴额`) / sum(b.`q_uv`), 4) as `单UV先享`
        ,sum(a.`q_先享订单量`) as `先享订单量`
        ,concat(round(sum(a.`q_先享订单量`) / sum(a.`q_订单量`) * 100, 2), '%') as `先享订单占比`
from q_order a
left join q_uv b
    on a.order_date = b.order_date
    and a.ab_type = b.ab_type
group by 1
order by a.ab_type asc
;



--- 优惠先享实验评估 0617 14天数据
with abt as (
    select  ab_exp_value as user_id
            ,case when ab_version = 'A' then 'A空白组40%'
                  when ab_version = 'B' then 'B实验组20%'
                  when ab_version = 'C' then 'C对照组20%'
                  when ab_version = 'D' then 'D对照组20%'
                  else '其他'
             end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt between '2026-04-25' and '2026-05-07'
        and type = 'flow'
        and user_id_type = 'user_id'
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
    group by 1,2
)
,q_order_app as (--- 订单数据
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,CASE
                WHEN ext_flag_map['pay_after_stay_flag'] = 'true' THEN '后付订单'
                WHEN ext_flag_map['pay_after_stay_flag'] = 'false' THEN '非后付订单'
                ELSE '其他'
            END AS `是否后付订单`
            ,CASE
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 1 THEN '用户扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 2 THEN '垫资扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 3 THEN '垫资扣款失败'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 4 THEN '用户向垫资扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 5 THEN '用户向垫资扣款失败'
                WHEN ext_flag_map['post_pay_flag'] IS NULL THEN '未扣款'
                ELSE '其他'
            END AS `当前扣款状态`
    from default.mdw_order_v3_international a 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-04-25'  
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order_ab as (--- 订单数据
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-04-25'  and order_date <= '2026-05-07'
        and order_no <> '103576132435'
        and t.ab_type is not null
)
,q_uv as (-- 流量
    select dt
        ,t.ab_type
        ,a.user_id
        ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id 
    where a.dt  >= '2026-04-25'  and a.dt <= '2026-05-07'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
    group by 1,2,3,4
)
,d_exp as (-- D页优惠先享飘条曝光
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
            ,user_name
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260425'  and dt <= '20260507'
    and key = 'ihotel/Detail/PriceList/show/ifBenefitfrontBanner'   ----D页优惠先享飘条曝光
    group by 1,2
)
,q_reorder_detail as (
    select
        t1.ab_type
        ,t1.order_date
        ,t1.user_id
        ,t2.order_no as reorder_no
        ,t2.q_yj
        ,t2.room_night
    from (
        select
            ab_type
            ,order_date
            ,user_id
            ,min(order_time) as order_time
        from q_order_ab
        where order_date <= date_sub(current_date, 8)
        group by 1,2,3
    ) t1
    join q_order_app t2 
        on t1.user_id = t2.user_id
        and t2.order_time > t1.order_time
        and datediff(t2.order_date, t1.order_date) <= 7
)
,q_order_reorder as (-- 复购订单
    select  ab_type
            ,order_date
            ,count(distinct user_id) as `复购生单用户`
            ,count(distinct reorder_no) as `复购订单量`
    from q_reorder_detail
    group by 1,2
)
,q_order_reorder_yj as (-- 复购订单佣金
    select  ab_type
            ,sum(max_q_yj) as `复购收益`
            ,sum(max_room_night) as `复购间夜`
    from (
        select ab_type
            ,reorder_no
            ,max(q_yj) as max_q_yj
            ,max(room_night) as max_room_night
        from q_reorder_detail
        group by 1,2
    ) 
    group by 1
)
,q_ab_info as (
    select t1.dt,t1.ab_type
            ,count(distinct t1.user_id) as `uv`
            ,count(distinct t2.order_no) as `订单量`
            ,count(distinct t2.user_id) as `生单uv`
            ,sum(t2.q_yj) as `收益`
            ,sum(t2.room_night) as `间夜`
            ,sum(t2.init_gmv) as `gmv`
            ,sum(t2.coupon_substract_summary) as `券额`
            ,count(distinct case when t2.use_promotion_enjoy_first is not null then t2.order_no end) as `先享订单量` 
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.room_night end) as `先享间夜量`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.q_yj end) as `先享收益额`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.init_gmv end) as `先享GMV`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.coupon_substract_summary end) as `先享券补贴额`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.promotionAmount end) as `先享补贴金额`
            ,count(distinct case when t3.user_name is not null then t1.user_id end) as `先享曝光uv`
            ,count(distinct case when t3.user_name is not null then t2.order_no end) as `先享曝光订单量`
            ,count(distinct case when t3.user_name is not null then t2.user_id end) as `先享曝光生单UV`
            ,sum(case when t3.user_name is not null then t2.q_yj end) as `先享曝光收益`
            ,sum(case when t3.user_name is not null then t2.room_night end) as `先享曝光间夜`
            ,sum(case when t3.user_name is not null then t2.init_gmv end) as `先享曝光GMV`
            ,sum(case when t3.user_name is not null then t2.coupon_substract_summary end) as `先享曝光券额`
    from q_uv t1
    left join q_order_ab t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date and t1.ab_type = t2.ab_type
    left join d_exp t3 on t1.user_name = t3.user_name and t1.dt = t3.dt 
    group by 1,2
)

select t1.ab_type
       ,sum(`uv`) as `uv`
       ,sum(`订单量`) as `订单量`
       ,sum(`生单uv`) as `生单uv`
       ,sum(`收益`) as `收益`
       ,sum(`间夜`) as `间夜`
       ,sum(`gmv`) as `gmv`
       ,sum(`券额`) as `券额`
       ,sum(`先享订单量`) as `先享订单量`
       ,sum(`先享间夜量`) as `先享间夜量`
       ,sum(`先享收益额`) as `先享收益额`
       ,sum(`先享GMV`) as `先享GMV`
       ,sum(`先享券补贴额`) as `先享券补贴额`
       ,sum(`先享补贴金额`) as `先享补贴金额`
       ,sum(`先享曝光uv`) as `先享曝光uv`
       ,sum(`先享曝光订单量`) as `先享曝光订单量`
       ,sum(`先享曝光生单UV`) as `先享曝光生单UV`
       ,sum(`先享曝光收益`) as `先享曝光收益`
       ,sum(`先享曝光间夜`) as `先享曝光间夜`
       ,sum(`先享曝光GMV`) as `先享曝光GMV`
       ,sum(`先享曝光券额`) as `先享曝光券额`
       ,sum(`复购生单用户`) as `复购生单用户`
       ,sum(`复购订单量`) as `复购订单量`
       ,max(`复购收益`) as `复购收益`
       ,max(`复购间夜`) as `复购间夜`
from q_ab_info t1 
left join q_order_reorder t2 on t1.ab_type = t2.ab_type and t1.dt = t2.order_date
left join q_order_reorder_yj t4 on t1.ab_type = t4.ab_type
group by 1
;


--- 优惠先享实验评估 0617 14天数据 new
with abt as (
    select  ab_exp_value as user_id
            ,case when ab_version = 'A' then 'A空白组40%'
                  when ab_version = 'B' then 'B实验组20%'
                  when ab_version = 'C' then 'C对照组20%'
                  when ab_version = 'D' then 'D对照组20%'
                  else '其他'
             end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt between '2026-06-25' and '2026-06-29'
        and type = 'flow'
        and user_id_type = 'user_id'
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
    group by 1,2
)
,q_order_app as (--- 订单数据
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,CASE
                WHEN ext_flag_map['pay_after_stay_flag'] = 'true' THEN '后付订单'
                WHEN ext_flag_map['pay_after_stay_flag'] = 'false' THEN '非后付订单'
                ELSE '其他'
            END AS `是否后付订单`
            ,CASE
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 1 THEN '用户扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 2 THEN '垫资扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 3 THEN '垫资扣款失败'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 4 THEN '用户向垫资扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 5 THEN '用户向垫资扣款失败'
                WHEN ext_flag_map['post_pay_flag'] IS NULL THEN '未扣款'
                ELSE '其他'
            END AS `当前扣款状态`
    from default.mdw_order_v3_international a 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-06-25'  
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_order_ab as (--- 订单数据
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-06-25'  and order_date <= '2026-06-29'
        and order_no <> '103576132435'
        and t.ab_type is not null
        and province_name = '香港'
)
,q_uv as (-- 流量
    select dt
        ,t.ab_type
        ,a.user_id
        ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id 
    where a.dt  >= '2026-06-25'  and a.dt <= '2026-06-29'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
        and province_name = '香港'
    group by 1,2,3,4
)
,d_exp as (-- D页优惠先享飘条曝光
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
            ,user_name
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260625'  and dt <= '20260629'
    and key = 'ihotel/Detail/PriceList/show/ifBenefitfrontBanner'   ----D页优惠先享飘条曝光
    group by 1,2
)
,q_reorder_detail as (
    select
        ab_type
        ,order_date
        ,user_id
        ,reorder_no
        ,q_yj
        ,room_night
    from (
        select
            t1.ab_type
            ,t1.order_date
            ,t1.user_id
            ,t2.order_no as reorder_no
            ,t2.q_yj
            ,t2.room_night
            ,row_number() over(
                partition by t1.ab_type,t2.order_no
                order by t1.order_time desc
            ) as rn
        from (
            select
                ab_type
                ,order_date
                ,user_id
                ,min(order_time) as order_time
            from q_order_ab
            group by 1,2,3
        ) t1
        join q_order_app t2 
            on t1.user_id = t2.user_id
            and t2.order_time > t1.order_time
            and datediff(t2.order_date, t1.order_date) <= 7
        where t1.order_date <= date_sub(current_date, 8)
    ) a
    where rn = 1
)
,q_order_reorder as (-- 复购订单
    select  ab_type
            ,order_date
            ,count(distinct user_id) as `复购生单用户`
            ,count(distinct reorder_no) as `复购订单量`
            ,sum(q_yj) as `复购收益`
            ,sum(room_night) as `复购间夜`
    from q_reorder_detail
    group by 1,2
)
,q_ab_info as (
    select t1.dt,t1.ab_type
            ,count(distinct t1.user_id) as `uv`
            ,count(distinct t2.order_no) as `订单量`
            ,count(distinct t2.user_id) as `生单uv`
            ,sum(t2.q_yj) as `收益`
            ,sum(t2.room_night) as `间夜`
            ,sum(t2.init_gmv) as `gmv`
            ,sum(t2.coupon_substract_summary) as `券额`
            ,count(distinct case when t2.use_promotion_enjoy_first is not null then t2.order_no end) as `先享订单量` 
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.room_night end) as `先享间夜量`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.q_yj end) as `先享收益额`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.init_gmv end) as `先享GMV`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.coupon_substract_summary end) as `先享券补贴额`
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.promotionAmount end) as `先享补贴金额`
            ,count(distinct case when t3.user_name is not null then t1.user_id end) as `先享曝光uv`
            ,count(distinct case when t3.user_name is not null then t2.order_no end) as `先享曝光订单量`
            ,count(distinct case when t3.user_name is not null then t2.user_id end) as `先享曝光生单UV`
            ,sum(case when t3.user_name is not null then t2.q_yj end) as `先享曝光收益`
            ,sum(case when t3.user_name is not null then t2.room_night end) as `先享曝光间夜`
            ,sum(case when t3.user_name is not null then t2.init_gmv end) as `先享曝光GMV`
            ,sum(case when t3.user_name is not null then t2.coupon_substract_summary end) as `先享曝光券额`
    from q_uv t1
    left join q_order_ab t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date and t1.ab_type = t2.ab_type
    left join d_exp t3 on t1.user_name = t3.user_name and t1.dt = t3.dt 
    group by 1,2
)

select t1.ab_type
       ,sum(`uv`) as `uv`
       ,sum(`订单量`) as `订单量`
       ,sum(`生单uv`) as `生单uv`
       ,sum(`收益`) as `收益`
       ,sum(`间夜`) as `间夜`
       ,sum(`gmv`) as `gmv`
       ,sum(`券额`) as `券额`
       ,sum(`先享订单量`) as `先享订单量`
       ,sum(`先享间夜量`) as `先享间夜量`
       ,sum(`先享收益额`) as `先享收益额`
       ,sum(`先享GMV`) as `先享GMV`
       ,sum(`先享券补贴额`) as `先享券补贴额`
       ,sum(`先享补贴金额`) as `先享补贴金额`
       ,sum(`先享曝光uv`) as `先享曝光uv`
       ,sum(`先享曝光订单量`) as `先享曝光订单量`
       ,sum(`先享曝光生单UV`) as `先享曝光生单UV`
       ,sum(`先享曝光收益`) as `先享曝光收益`
       ,sum(`先享曝光间夜`) as `先享曝光间夜`
       ,sum(`先享曝光GMV`) as `先享曝光GMV`
       ,sum(`先享曝光券额`) as `先享曝光券额`
       ,sum(`复购生单用户`) as `复购生单用户`
       ,sum(`复购订单量`) as `复购订单量`
       ,sum(`复购收益`) as `复购收益`
       ,sum(`复购间夜`) as `复购间夜`
from q_ab_info t1 
left join q_order_reorder t2 on t1.ab_type = t2.ab_type and t1.dt = t2.order_date
group by 1
;


with q_order as (
    select  order_date
            ,case when province_name in ('澳门','香港') then province_name when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,order_no
            ,checkout_date
            ,city_name
            ,a.province_name
            ,a.country_name
            ,user_id
            ,order_status
            ,init_gmv
            ,room_night
            ,cast(nvl(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'),0) as double) as `优惠先享金额`
            ,ext_flag_map['sub_auth_type'] as sub_auth_type
            ,ext_flag_map['post_pay_flag'] as post_pay_flag
            ,ext_flag_map['selected_payment_type'] as selected_payment_type
            ,case when ext_flag_map['pay_after_stay_flag'] = 'true' then '后付订单'
                when ext_flag_map['pay_after_stay_flag'] = 'false' then '非后付订单'
                else '其他'
            end as `是否后付订单`
            ,case when cast(ext_flag_map['selected_payment_type'] as int) = 1 then '标准后付'
                when cast(ext_flag_map['selected_payment_type'] as int) = 2 then '拿去花后付'
                else '其他'
            end as `后付支付方式`
            ,case when cast(ext_flag_map['sub_auth_type'] as int) = 1 then '微信免密'
                when cast(ext_flag_map['sub_auth_type'] as int) = 2 then '微信支付分'
                when cast(ext_flag_map['sub_auth_type'] as int) = 3 then '支付宝免密'
                when cast(ext_flag_map['sub_auth_type'] as int) = 4 then '支付宝芝麻分'
                when cast(ext_flag_map['sub_auth_type'] as int) = 5 then '支付宝预授权'
                when cast(ext_flag_map['sub_auth_type'] as int) = 6 then '银行卡'
                when cast(ext_flag_map['sub_auth_type'] as int) = 7 then '拿去花'
                when cast(ext_flag_map['sub_auth_type'] as int) = 99 then '其他'
                else '未知'
            end as `后付授权支付方式`
            ,case when cast(ext_flag_map['post_pay_flag'] as int) = 1 then '用户扣款成功'
                when cast(ext_flag_map['post_pay_flag'] as int) = 2 then '垫资扣款成功'
                when cast(ext_flag_map['post_pay_flag'] as int) = 3 then '垫资扣款失败'
                when cast(ext_flag_map['post_pay_flag'] as int) = 4 then '用户向垫资扣款成功'
                when cast(ext_flag_map['post_pay_flag'] as int) = 5 then '用户向垫资扣款失败'
                when ext_flag_map['post_pay_flag'] is null then '未扣款'
                else '其他'
            end as `当前扣款状态`
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        and is_valid = 1
        and order_date >= '2026-04-25'
        and order_date <=  '2026-05-07'
        and order_status not in ('CANCELLED','REJECTED')
        and ext_flag_map['use_promotion_enjoy_first'] is not null  --- 优惠先享
)
,task_info as (
    select to_date(gmt_create) gmt,order_id
            ,case when status='2' then '进行中' 
                  when status='3' then '成功'
                  when status='4' then '失败' 
            else '其他' end as status_type
            ,status
    from ihotel_default.ods_mkt_peach_promotion_task_merge_da 
    where dt=date_sub(current_date, 1) 
        and task_type = '2'
        and status <> '1'  -- 1初始化 2进行中 3成功 4失败
)

select t1.order_date,order_no,order_status,init_gmv,`优惠先享金额`,t2.status_type,`是否后付订单`, `后付授权支付方式`,`当前扣款状态`
from q_order t1 
left join task_info t2 on t1.order_no = t2.order_id
;





select order_date,a.user_id,order_no,order_time
        ,room_night,init_gmv,order_status
        ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
        ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
           
from default.mdw_order_v3_international a 
where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
    and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
    and is_valid = '1'
    and terminal_channel_type = 'app' 
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    and order_date >= '2026-05-01'  
    and order_date <= date_sub(current_date, 1)
    and order_no <> '103576132435'
    and ext_flag_map['use_promotion_enjoy_first']  is not null
;





--- 优惠先享实验评估 new
with abt as (
    select  ab_exp_value as user_id
            ,case when ab_version = 'A' then '实验组A'
                  when ab_version = 'B' then '实验组B'
                  when ab_version in ('C', 'D') then '空白对照组'
                  else '其他'
             end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt >= '2026-06-26'
        and type = 'flow'
        and user_id_type = 'user_id'
        and ab_exp_id = '260618_ho_gj_Holidayyhxxtest'
    group by 1,2
)
,q_order_app as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount
            ,CASE
                WHEN ext_flag_map['pay_after_stay_flag'] = 'true' THEN '后付订单'
                WHEN ext_flag_map['pay_after_stay_flag'] = 'false' THEN '非后付订单'
                ELSE '其他'
            END AS "是否后付订单"
            ,CASE
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 1 THEN '用户扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 2 THEN '垫资扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 3 THEN '垫资扣款失败'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 4 THEN '用户向垫资扣款成功'
                WHEN CAST(ext_flag_map['post_pay_flag'] AS INT) = 5 THEN '用户向垫资扣款失败'
                WHEN ext_flag_map['post_pay_flag'] IS NULL THEN '未扣款'
                ELSE '其他'
            END AS "当前扣款状态"
    from default.mdw_order_v3_international a 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-06-26' 

        and order_no <> '103576132435'
)
,q_order_ab as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf
            ,get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF') as q_v2_beat_amount_af
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
            ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-06-26'
        and order_no <> '103576132435'
        and t.ab_type is not null
        and province_name = '香港'
)
,q_uv as (
    select dt
        ,t.ab_type
        ,a.user_id
        ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id 
    where a.dt  >= '2026-06-26'
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
        and province_name = '香港'
    group by 1,2,3,4
)
,d_exp as (
    select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
            ,user_name
    from default.dw_qav_ihotel_track_info_di
    where dt >= '2026-06-26'
    and key = 'ihotel/Detail/PriceList/show/ifBenefitfrontBanner'
    group by 1,2
)
,q_reorder_detail as (
    select
        ab_type
        ,order_date
        ,user_id
        ,reorder_no
        ,q_yj
        ,room_night
    from (
        select
            t1.ab_type
            ,t1.order_date
            ,t1.user_id
            ,t2.order_no as reorder_no
            ,t2.q_yj
            ,t2.room_night
            ,row_number() over(
                partition by t1.ab_type,t2.order_no
                order by t1.order_time desc
            ) as rn
        from (
            select
                ab_type
                ,order_date
                ,user_id
                ,min(order_time) as order_time
            from q_order_ab
            group by 1,2,3
        ) t1
        join q_order_app t2 
            on t1.user_id = t2.user_id
            and t2.order_time > t1.order_time
            and datediff(t2.order_date, t1.order_date) <= 7
    ) a
    where rn = 1
)
,q_order_reorder as (
    select  ab_type
            ,order_date
            ,count(distinct user_id) as "复购生单用户"
            ,count(distinct reorder_no) as "复购订单量"
            ,sum(q_yj) as "复购收益"
            ,sum(room_night) as "复购间夜"
    from q_reorder_detail
    group by 1,2
)
,q_ab_info as (
    select t1.dt,t1.ab_type
            ,count(distinct t1.user_id) as "uv"
            ,count(distinct t2.order_no) as "订单量"
            ,count(distinct t2.user_id) as "生单uv"
            ,sum(t2.q_yj) as "收益"
            ,sum(t2.room_night) as "间夜"
            ,sum(t2.init_gmv) as "gmv"
            ,sum(t2.coupon_substract_summary) as "券额"
            ,count(distinct case when t2.use_promotion_enjoy_first is not null then t2.order_no end) as "先享订单量" 
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.room_night end) as "先享间夜量"
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.q_yj end) as "先享收益额"
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.init_gmv end) as "先享GMV"
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.coupon_substract_summary end) as "先享券补贴额"
            ,sum(case when t2.use_promotion_enjoy_first is not null then t2.promotionAmount end) as "先享补贴金额"
            ,count(distinct case when t3.user_name is not null then t1.user_id end) as "先享曝光uv"
            ,count(distinct case when t3.user_name is not null then t2.order_no end) as "先享曝光订单量"
            ,count(distinct case when t3.user_name is not null then t2.user_id end) as "先享曝光生单UV"
            ,sum(case when t3.user_name is not null then t2.q_yj end) as "先享曝光收益"
            ,sum(case when t3.user_name is not null then t2.room_night end) as "先享曝光间夜"
            ,sum(case when t3.user_name is not null then t2.init_gmv end) as "先享曝光GMV"
            ,sum(case when t3.user_name is not null then t2.coupon_substract_summary end) as "先享曝光券额"

            ,sum(t2.q_jf) as "积分补贴额"
            ,sum(coalesce(t2.q_v2_beat_amount_af, 0) * t2.room_night) as "定价补贴额"
    from q_uv t1
    left join q_order_ab t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date and t1.ab_type = t2.ab_type
    left join d_exp t3 on t1.user_name = t3.user_name and t1.dt = t3.dt 
    group by 1,2
)

select t1.ab_type
       ,sum("uv") as "uv"
       ,sum("生单uv") as "生单uv"
       ,sum("订单量") as "订单量"
       ,sum("收益") as "收益"
       ,sum("间夜") as "间夜"
       ,sum("gmv") as "gmv"
       ,sum("券额") as "券额"
       ,concat(round(sum("券额") / sum("gmv") * 100, 2), '%') as "券补率"
       ,concat(round(sum("收益") / sum("gmv") * 100, 2), '%') as "佣金率"
       ,concat(round(sum("生单uv") / sum("uv") * 100, 2), '%') as "u2o"
       ,concat(round(sum("订单量") / sum("uv") * 100, 2), '%') as "cr"
       ,round(sum("收益") / sum("订单量"), 4) as "单订单收益"

       ,sum("复购生单用户") as "复购生单用户"
       ,sum("复购订单量") as "复购订单量"
       ,sum("复购收益") as "复购收益"
       ,round(sum("复购订单量") / sum("复购生单用户"), 4) as "复购单用户订单"
       ,round(sum("复购收益") / sum("复购订单量"), 4) as "复购单订单收益"
       ,concat(round(sum("复购生单用户") / sum("uv") * 100, 2), '%') as "复购u2o"

       ,sum("先享曝光uv") as "先享曝光uv"
       ,sum("先享曝光生单UV") as "先享曝光生单UV"
       ,sum("先享曝光订单量") as "先享曝光订单量"
       ,sum("先享订单量") as "先享订单量"
       ,sum("先享补贴金额") as "先享补贴金额"
       ,concat(round(sum("先享曝光生单UV") / sum("先享曝光uv") * 100, 2), '%') as "先享曝光u2o"
       ,concat(round(sum("先享曝光订单量") / sum("先享曝光uv") * 100, 2), '%') as "先享曝光cr"
       ,concat(round(sum("先享订单量") / sum("先享曝光订单量") * 100, 2), '%') as "先享订单占比"
       ,concat(round(sum("先享曝光uv") / sum("uv") * 100, 2), '%') as "先享曝光占比"

       ,sum("先享间夜量") as "先享间夜量"
       ,sum("先享收益额") as "先享收益额"
       ,sum("先享GMV") as "先享GMV"
       ,sum("先享券补贴额") as "先享券补贴额"   
       ,sum("先享曝光收益") as "先享曝光收益"
       ,sum("先享曝光间夜") as "先享曝光间夜"
       ,sum("先享曝光GMV") as "先享曝光GMV"
       ,sum("先享曝光券额") as "先享曝光券额"     
       ,sum("复购间夜") as "复购间夜"

       ,sum("积分补贴额") as "积分补贴额"
       ,sum("定价补贴额") as "定价补贴额"
from q_ab_info t1 
left join q_order_reorder t2 on t1.ab_type = t2.ab_type and t1.dt = t2.order_date
group by 1
;