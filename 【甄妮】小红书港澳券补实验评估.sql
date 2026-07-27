
--- 港澳小红书实验评估 new 0715
with abt as (
    select  dt,ab_exp_value as user_id
            ,case when ab_version = 'A' then '实验组A'
                  when ab_version = 'B' then '实验组B'
                  when ab_version in ('C') then '对照组C'
                  when ab_version in ('D') then '对照组D'
                  else '其他'
             end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt >= '2026-07-02' 
        and type = 'flow'
        and user_id_type = 'user_id'
        and ab_exp_id = '260626_ho_gj_xianggang_xhs_929395'
    group by 1,2,3
)
,red as (-- 小红书
    select flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2026-06-01' and date_sub(current_date,1)
    and query_platform = 'redbook'
    group by 1,2
)
,q_uv_ab as (-- Q流量
    select a.dt
        ,t.ab_type
        ,a.user_id
        ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id  and a.dt=t.dt
    where a.dt  >= '2026-07-02' 
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and province_name in ('香港', '澳门')
        and t.ab_type is not null
    group by 1,2,3,4
)
,q_xhs_uv_ab as (-- Q小红书渠道流量
    select uv.dt,uv.user_id,uv.user_name,uv.ab_type
    from q_uv_ab uv
    left join red r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    group by 1,2,3,4
)
,q_order_ab as (-- Q订单
    select order_date,a.user_id,a.user_name
            ,order_no,order_time,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (coupon_substract_summary is null  or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0
                else coalesce(coupon_substract_summary,0) end as coupon_substract_summary  -- 券补
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
            ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id and t.dt=a.order_date
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-07-02' 
        and order_no <> '103576132435'
        and t.ab_type is not null
        and province_name in ('香港', '澳门')
)

select dt,ab_type
       ,`uv`
       ,`生单uv`
       ,`订单量`
       ,`收益`
       ,`间夜`
       ,`gmv`
       ,`券额`
       ,`积分补贴额`
       ,`定价补贴额`
       ,concat(round(`券额` / `gmv` * 100, 2), '%') as `券补率`
       ,concat(round(`收益` / `gmv` * 100, 2), '%') as `佣金率`
       ,concat(round(`生单uv` / `uv` * 100, 2), '%') as `u2o`
       ,concat(round(`订单量` / `uv` * 100, 2), '%') as `cr`
       ,round(`间夜` / `uv`, 4) as `单UV间夜`
       ,round(`收益` / `uv`, 4) as `单UV收益`
       ,round((`券额`+`积分补贴额`+`定价补贴额`) / `uv`, 4) as `单UV补贴`
from (
    select t1.dt,t1.ab_type
        ,count(distinct t1.user_id) as `uv`
        ,count(distinct t2.order_no) as `订单量`
        ,count(distinct t2.user_id) as `生单uv`
        ,sum(t2.q_yj) as `收益`
        ,sum(t2.room_night) as `间夜`
        ,sum(t2.init_gmv) as `gmv`
        ,sum(t2.coupon_substract_summary) as `券额`
        ,sum(t2.jf_amt) as `积分补贴额`
        ,sum(t2.djb_amt) as `定价补贴额`
    from q_xhs_uv_ab t1
    left join q_order_ab t2 on t1.user_id = t2.user_id and t1.dt = t2.order_date and t1.ab_type = t2.ab_type
    group by 1,2
) t
order by 1,2
;
