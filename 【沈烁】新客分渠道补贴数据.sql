with user_type as(
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
,q_channel_uv as (--- Q分渠道明细
    select dt,channel,user_name
    from ihotel_default.dwd_flow_ug_channel_di
    where dt >= '2025-01-01' and dt <= date_sub(current_date, 1)
    group by 1,2,3
)
,uv as (----分日去重活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_order as ( --- Q侧订单
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          ,a.user_id,a.order_no, a.init_gmv, a.room_night,a.user_name
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary  -- 券补
        ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
        ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补 = 协议补+平台补
        ,coalesce(get_json_object(extendinfomap,'$.platform_amount'),0) * room_night  plat_amt  --- 平台补
        ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0) * room_night + coalesce(cashbackmap['framework_amount'],0)  xyb_amt  --- 协议补
        ,case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
                then coalesce(follow_price_amount,0) end zjb_amt --- 追价补
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,uv_agg as (
    select  substr(a.dt, 1, 7) as mth
            ,if(grouping(c.channel) = 1, 'ALL', c.channel) as channel
            ,count(1) as dau
    from uv a
    left join q_channel_uv c on a.dt = c.dt and a.user_name = c.user_name
    where user_type = '新客'
    group by 
    grouping sets (
        (substr(a.dt, 1, 7),c.channel),
        (substr(a.dt, 1, 7))
    )
)
,order_agg as (
    select  substr(a.order_date, 1, 7) as mth
            ,if(grouping(c.channel) = 1, 'ALL', c.channel) as channel
            ,count(distinct a.user_id) as order_uv
            ,count(distinct a.order_no) as order_cnt
            ,sum(a.room_night) as room_night
            ,sum(a.init_gmv) as gmv
            ,sum(a.final_commission_after) as commission
            ,sum(a.coupon_substract_summary) as coupon_amt
            ,sum(a.jf_amt) as jf_amt
            ,sum(a.djb_amt) as djb_amt
            -- ,sum(a.plat_amt) as plat_amt
            -- ,sum(a.xyb_amt) as xyb_amt
            -- ,sum(coalesce(a.zjb_amt, 0)) as zjb_amt
    from q_order a
    left join q_channel_uv c on a.order_date = c.dt and a.user_name = c.user_name
    where user_type = '新客'
    group by 
    grouping sets (
        (substr(a.order_date, 1, 7),c.channel),
        (substr(a.order_date, 1, 7))
    )
)

select  coalesce(a.mth, b.mth) as `月份`
        ,coalesce(a.channel, b.channel) as `渠道`
        ,coalesce(a.dau, 0) as `DAU`
        ,coalesce(b.order_uv, 0) as `生单UV`
        ,coalesce(b.order_cnt, 0) as `订单量`
        ,coalesce(b.room_night, 0) as `间夜量`
        ,coalesce(b.gmv, 0) as `GMV`
        ,coalesce(b.commission, 0) as `佣金`
        ,coalesce(b.coupon_amt, 0) as `券补`
        ,coalesce(b.jf_amt, 0) as `积分补`
        ,coalesce(b.djb_amt, 0) as `定价补`
        -- ,coalesce(b.plat_amt, 0) as `平台补`
        -- ,coalesce(b.xyb_amt, 0) as `协议补`
        -- ,coalesce(b.zjb_amt, 0) as `追加补`
from uv_agg a
left join order_agg b  on a.mth = b.mth  and a.channel = b.channel
order by `月份` desc
        ,`渠道`
        ,`用户类型`
;







select checkout_date
		,sum(order_cnt)`订单量`
    ,sum(room_night)`间夜量`
    ,sum(fx_amt)`返现金额`
    ,sum(lingqu_order_cnt)`挽留成功订单量`
    ,sum(case when lingqu_status='6月领取' then lingqu_order_cnt end) `6月领取挽留成功订单量`
    ,round(sum(case when lingqu_status='6月领取' then lingqu_order_cnt end) / sum(lingqu_order_cnt),2) `6月领取挽留成功占比`
    ,sum(lingqu_room_night)`挽留成功间夜量`
from (

select checkout_date
       ,case when lingqu_date is null then '未领取'
             when lingqu_date >= '2026-06-02' then '6月领取'
             else '非6月领取' end as lingqu_status
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
        ,sum(fx) as fx_amt
        ,count(distinct case when fx > 0 then order_no end) as lingqu_order_cnt
        ,sum(case when fx > 0 then room_night end) as lingqu_room_night
from (
    select checkout_date
            ,order_no
            ,room_night
            ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')  fx
            ,concat(substr(ext_flag_map['cancel_red_packet_join_activity_time'],1,4),'-',substr(ext_flag_map['cancel_red_packet_join_activity_time'],5,2),'-',substr(ext_flag_map['cancel_red_packet_join_activity_time'],7,2)) as lingqu_date
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and checkout_date >= '2026-06-01' and checkout_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
group by 1,2
) group by 1 order by 1 desc