--- Q分新老x目的地x渠道ARPU数据
with user_type as (-----新老客
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
,q_order as (----Q订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,channel_info as (---渠道信息表
    select dt,user_name,channel 
    from ihotel_default.dwd_flow_ug_channel_di 
    group by 1,2,3
)
,user_cohort_segments as (
    --- 在用户粒度上提前进行多维展开并去重
    select 
         t1.order_date
        ,t1.user_name
        ,if(grouping(t1.mdd)=1, 'ALL', t1.mdd) as mdd
        ,if(grouping(t1.user_type)=1, 'ALL', t1.user_type) as user_type
        ,if(grouping(t2.channel)=1, 'ALL', t2.channel) as channel
    from q_order t1
    left join channel_info t2 on t1.user_name = t2.user_name and t1.order_date = t2.dt
    group by t1.order_date, t1.user_name, t1.mdd, t1.user_type, t2.channel
    grouping sets (
        (t1.order_date, t1.user_name),                                      -- 对应大盘 ()
        (t1.order_date, t1.user_name, t1.mdd),                              -- 仅分目的地
        (t1.order_date, t1.user_name, t1.user_type),                        -- 仅分新老客
        (t1.order_date, t1.user_name, t2.channel),                          -- 仅分渠道
        (t1.order_date, t1.user_name, t1.mdd, t1.user_type),                -- 目的地 + 新老客
        (t1.order_date, t1.user_name, t1.mdd, t2.channel),                  -- 目的地 + 渠道
        (t1.order_date, t1.user_name, t1.user_type, t2.channel)             -- 新老客 + 渠道
    )
)

select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,t1.channel
        ,uv
        --- ARPU数据
        ,yj0 / uv   ARPU0
        ,yj1 / uv   ARPU1
        ,yj2 / uv   ARPU2
        ,yj3 / uv   ARPU3
        ,yj4 / uv   ARPU4
        ,yj5 / uv   ARPU5
        ,yj6 / uv   ARPU6
        ,yj7 / uv   ARPU7
        ,yj30 / uv  ARPU30
        ,yj60 / uv  ARPU60
        ,yj90 / uv  ARPU90
        ,yj180 / uv ARPU180
        --- 单用户订单数据
        ,order_cnt0 / uv order_cnt_per_user0
        ,order_cnt1 / uv order_cnt_per_user1
        ,order_cnt2 / uv order_cnt_per_user2
        ,order_cnt3 / uv order_cnt_per_user3
        ,order_cnt4 / uv order_cnt_per_user4
        ,order_cnt5 / uv order_cnt_per_user5
        ,order_cnt6 / uv order_cnt_per_user6
        ,order_cnt7 / uv order_cnt_per_user7
        ,order_cnt30 / uv order_cnt_per_user30
        ,order_cnt60 / uv order_cnt_per_user60
        ,order_cnt90 / uv order_cnt_per_user90
        ,order_cnt180 / uv order_cnt_per_user180
        --- 单订单间夜数
        ,room_night0 / order_cnt0 room_night_per_order0
        ,room_night1 / order_cnt1 room_night_per_order1
        ,room_night2 / order_cnt2 room_night_per_order2
        ,room_night3 / order_cnt3 room_night_per_order3
        ,room_night4 / order_cnt4 room_night_per_order4
        ,room_night5 / order_cnt5 room_night_per_order5
        ,room_night6 / order_cnt6 room_night_per_order6
        ,room_night7 / order_cnt7 room_night_per_order7
        ,room_night30 / order_cnt30 room_night_per_order30
        ,room_night60 / order_cnt60 room_night_per_order60
        ,room_night90 / order_cnt90 room_night_per_order90
        ,room_night180 / order_cnt180 room_night_per_order180
        --- ADR
        ,init_gmv0 / room_night0 adr0
        ,init_gmv1 / room_night1 adr1
        ,init_gmv2 / room_night2 adr2   
        ,init_gmv3 / room_night3 adr3
        ,init_gmv4 / room_night4 adr4
        ,init_gmv5 / room_night5 adr5
        ,init_gmv6 / room_night6 adr6
        ,init_gmv7 / room_night7 adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv60 / room_night60 adr60
        ,init_gmv90 / room_night90 adr90
        ,init_gmv180 / room_night180 adr180
        --- 佣金率
        ,case when init_gmv0 = 0 then 0 else yj0 / init_gmv0 end commission_rate0
        ,case when init_gmv1 = 0 then 0 else yj1 / init_gmv1 end commission_rate1
        ,case when init_gmv2 = 0 then 0 else yj2 / init_gmv2 end commission_rate2
        ,case when init_gmv3 = 0 then 0 else yj3 / init_gmv3 end commission_rate3
        ,case when init_gmv4 = 0 then 0 else yj4 / init_gmv4 end commission_rate4
        ,case when init_gmv5 = 0 then 0 else yj5 / init_gmv5 end commission_rate5
        ,case when init_gmv6 = 0 then 0 else yj6 / init_gmv6 end commission_rate6
        ,case when init_gmv7 = 0 then 0 else yj7 / init_gmv7 end commission_rate7
        ,case when init_gmv30 = 0 then 0 else yj30 / init_gmv30 end commission_rate30
        ,case when init_gmv60 = 0 then 0 else yj60 / init_gmv60 end commission_rate60
        ,case when init_gmv90 = 0 then 0 else yj90 / init_gmv90 end commission_rate90
        ,case when init_gmv180 = 0 then 0 else yj180 / init_gmv180 end commission_rate180
        --- 补贴率
        ,case when init_gmv0 = 0 then 0 else qe0 / init_gmv0 end subsidy_rate0
        ,case when init_gmv1 = 0 then 0 else qe1 / init_gmv1 end subsidy_rate1
        ,case when init_gmv2 = 0 then 0 else qe2 / init_gmv2 end subsidy_rate2
        ,case when init_gmv3 = 0 then 0 else qe3 / init_gmv3 end subsidy_rate3
        ,case when init_gmv4 = 0 then 0 else qe4 / init_gmv4 end subsidy_rate4
        ,case when init_gmv5 = 0 then 0 else qe5 / init_gmv5 end subsidy_rate5
        ,case when init_gmv6 = 0 then 0 else qe6 / init_gmv6 end subsidy_rate6
        ,case when init_gmv7 = 0 then 0 else qe7 / init_gmv7 end subsidy_rate7
        ,case when init_gmv30 = 0 then 0 else qe30 / init_gmv30 end subsidy_rate30
        ,case when init_gmv60 = 0 then 0 else qe60 / init_gmv60 end subsidy_rate60
        ,case when init_gmv90 = 0 then 0 else qe90 / init_gmv90 end subsidy_rate90
        ,case when init_gmv180 = 0 then 0 else qe180 / init_gmv180 end subsidy_rate180
        --- 单间夜补贴
        ,case when room_night0 = 0 then 0 else qe0 / room_night0 end subsidy_per_room_night0
        ,case when room_night1 = 0 then 0 else qe1 / room_night1 end subsidy_per_room_night1
        ,case when room_night2 = 0 then 0 else qe2 / room_night2 end subsidy_per_room_night2
        ,case when room_night3 = 0 then 0 else qe3 / room_night3 end subsidy_per_room_night3
        ,case when room_night4 = 0 then 0 else qe4 / room_night4 end subsidy_per_room_night4
        ,case when room_night5 = 0 then 0 else qe5 / room_night5 end subsidy_per_room_night5
        ,case when room_night6 = 0 then 0 else qe6 / room_night6 end subsidy_per_room_night6
        ,case when room_night7 = 0 then 0 else qe7 / room_night7 end subsidy_per_room_night7
        ,case when room_night30 = 0 then 0 else qe30 / room_night30 end subsidy_per_room_night30
        ,case when room_night60 = 0 then 0 else qe60 / room_night60 end subsidy_per_room_night60
        ,case when room_night90 = 0 then 0 else qe90 / room_night90 end subsidy_per_room_night90
        ,case when room_night180 = 0 then 0 else qe180 / room_night180 end subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        -- 离店佣金
        ,ldyj0, ldyj1, ldyj2, ldyj3, ldyj4, ldyj5, ldyj6, ldyj7, ldyj30, ldyj60, ldyj90, ldyj180
from  (
    select t1.order_date,t1.user_type,t1.mdd,t1.channel
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0   then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1  then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2  then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3  then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4  then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5  then final_commission_after end) yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6  then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7  then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then final_commission_after end) yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then final_commission_after end) yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then final_commission_after end) yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then final_commission_after end) yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then init_gmv end) init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then init_gmv end) init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then init_gmv end) init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then init_gmv end) init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then init_gmv end) init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then init_gmv end) init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then init_gmv end) init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then init_gmv end) init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then init_gmv end) init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then coupon_substract_summary end) qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then coupon_substract_summary end) qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then coupon_substract_summary end) qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then coupon_substract_summary end) qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then coupon_substract_summary end) qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then coupon_substract_summary end) qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then coupon_substract_summary end) qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then coupon_substract_summary end) qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then coupon_substract_summary end) qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then coupon_substract_summary end) qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then coupon_substract_summary end) qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then order_no end) order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then order_no end) order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then order_no end) order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then order_no end) order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then order_no end) order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then order_no end) order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then order_no end) order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then order_no end) order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then order_no end) order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) order_cnt180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) ldyj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) ldyj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) ldyj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) ldyj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) ldyj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) ldyj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) ldyj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) ldyj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then ldyj end) ldyj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then ldyj end) ldyj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then ldyj end) ldyj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) ldyj180
    from order_info t1 
    left join q_order t2 on t1.user_name=t2.user_name and t2.order_date >= t1.order_date
    group by
        t1.order_date, 
        t1.mdd, 
        t1.user_type, 
        t1.channel
) t1 
order by order_date, user_type, mdd, channel
;



--- 2、C分新老x目的地ARPU数据
with c_user_type as(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as order_date
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            -- ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
            ,get_json_object(orig_discount_detail, '$.detail[1].amount') as cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join (
        select distinct order_no as order_no_oc
            ,orig_discount_detail
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da_patch
        where dt = '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            -- and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
            and terminal_channel_type = 'app'
            -- and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and order_status <> 'C'
            and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= date_sub(current_date, 1)
    ) oc
    on o.order_no = oc.order_no_oc
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
    --   and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01'
      and order_status <> 'C'
      and substr(order_date,1,10) <= date_sub(current_date, 1)

)
,user_cohort_segments as (
    select 
         t1.order_date
        ,t1.user_id
        ,if(grouping(t1.mdd)=1, 'ALL', t1.mdd) as mdd
        ,if(grouping(t1.user_type)=1, 'ALL', t1.user_type) as user_type
    from c_order t1
    group by t1.order_date, t1.user_id, t1.mdd, t1.user_type
    grouping sets (
        (t1.order_date, t1.user_id),                                -- 对应大盘 ()
        (t1.order_date, t1.user_id, t1.mdd),                        -- 仅分目的地
        (t1.order_date, t1.user_id, t1.user_type),                  -- 仅分新老客
        (t1.order_date, t1.user_id, t1.mdd, t1.user_type)           -- 目的地 + 新老客
    )
)

select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,uv
        --- ARPU数据
        ,yj0 / uv   ARPU0
        ,yj1 / uv   ARPU1
        ,yj2 / uv   ARPU2
        ,yj3 / uv   ARPU3
        ,yj4 / uv   ARPU4
        ,yj5 / uv   ARPU5
        ,yj6 / uv   ARPU6
        ,yj7 / uv   ARPU7
        ,yj30 / uv  ARPU30
        ,yj60 / uv  ARPU60
        ,yj90 / uv  ARPU90
        ,yj180 / uv ARPU180
        --- 单用户订单数据
        ,order_cnt0 / uv order_cnt_per_user0
        ,order_cnt1 / uv order_cnt_per_user1
        ,order_cnt2 / uv order_cnt_per_user2
        ,order_cnt3 / uv order_cnt_per_user3
        ,order_cnt4 / uv order_cnt_per_user4
        ,order_cnt5 / uv order_cnt_per_user5
        ,order_cnt6 / uv order_cnt_per_user6
        ,order_cnt7 / uv order_cnt_per_user7
        ,order_cnt30 / uv order_cnt_per_user30
        ,order_cnt60 / uv order_cnt_per_user60
        ,order_cnt90 / uv order_cnt_per_user90
        ,order_cnt180 / uv order_cnt_per_user180
        --- 单订单间夜数
        ,room_night0 / order_cnt0 room_night_per_order0
        ,room_night1 / order_cnt1 room_night_per_order1
        ,room_night2 / order_cnt2 room_night_per_order2
        ,room_night3 / order_cnt3 room_night_per_order3
        ,room_night4 / order_cnt4 room_night_per_order4
        ,room_night5 / order_cnt5 room_night_per_order5
        ,room_night6 / order_cnt6 room_night_per_order6
        ,room_night7 / order_cnt7 room_night_per_order7
        ,room_night30 / order_cnt30 room_night_per_order30
        ,room_night60 / order_cnt60 room_night_per_order60
        ,room_night90 / order_cnt90 room_night_per_order90
        ,room_night180 / order_cnt180 room_night_per_order180
        --- ADR
        ,init_gmv0 / room_night0 adr0
        ,init_gmv1 / room_night1 adr1
        ,init_gmv2 / room_night2 adr2   
        ,init_gmv3 / room_night3 adr3
        ,init_gmv4 / room_night4 adr4
        ,init_gmv5 / room_night5 adr5
        ,init_gmv6 / room_night6 adr6
        ,init_gmv7 / room_night7 adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv60 / room_night60 adr60
        ,init_gmv90 / room_night90 adr90
        ,init_gmv180 / room_night180 adr180
        --- 佣金率
        ,case when init_gmv0 = 0 then 0 else yj0 / init_gmv0 end commission_rate0
        ,case when init_gmv1 = 0 then 0 else yj1 / init_gmv1 end commission_rate1
        ,case when init_gmv2 = 0 then 0 else yj2 / init_gmv2 end commission_rate2
        ,case when init_gmv3 = 0 then 0 else yj3 / init_gmv3 end commission_rate3
        ,case when init_gmv4 = 0 then 0 else yj4 / init_gmv4 end commission_rate4
        ,case when init_gmv5 = 0 then 0 else yj5 / init_gmv5 end commission_rate5
        ,case when init_gmv6 = 0 then 0 else yj6 / init_gmv6 end commission_rate6
        ,case when init_gmv7 = 0 then 0 else yj7 / init_gmv7 end commission_rate7
        ,case when init_gmv30 = 0 then 0 else yj30 / init_gmv30 end commission_rate30
        ,case when init_gmv60 = 0 then 0 else yj60 / init_gmv60 end commission_rate60
        ,case when init_gmv90 = 0 then 0 else yj90 / init_gmv90 end commission_rate90
        ,case when init_gmv180 = 0 then 0 else yj180 / init_gmv180 end commission_rate180
        --- 补贴率
        ,case when init_gmv0 = 0 then 0 else qe0 / init_gmv0 end subsidy_rate0
        ,case when init_gmv1 = 0 then 0 else qe1 / init_gmv1 end subsidy_rate1
        ,case when init_gmv2 = 0 then 0 else qe2 / init_gmv2 end subsidy_rate2
        ,case when init_gmv3 = 0 then 0 else qe3 / init_gmv3 end subsidy_rate3
        ,case when init_gmv4 = 0 then 0 else qe4 / init_gmv4 end subsidy_rate4
        ,case when init_gmv5 = 0 then 0 else qe5 / init_gmv5 end subsidy_rate5
        ,case when init_gmv6 = 0 then 0 else qe6 / init_gmv6 end subsidy_rate6
        ,case when init_gmv7 = 0 then 0 else qe7 / init_gmv7 end subsidy_rate7
        ,case when init_gmv30 = 0 then 0 else qe30 / init_gmv30 end subsidy_rate30
        ,case when init_gmv60 = 0 then 0 else qe60 / init_gmv60 end subsidy_rate60
        ,case when init_gmv90 = 0 then 0 else qe90 / init_gmv90 end subsidy_rate90
        ,case when init_gmv180 = 0 then 0 else qe180 / init_gmv180 end subsidy_rate180
        --- 单间夜补贴
        ,case when room_night0 = 0 then 0 else qe0 / room_night0 end subsidy_per_room_night0
        ,case when room_night1 = 0 then 0 else qe1 / room_night1 end subsidy_per_room_night1
        ,case when room_night2 = 0 then 0 else qe2 / room_night2 end subsidy_per_room_night2
        ,case when room_night3 = 0 then 0 else qe3 / room_night3 end subsidy_per_room_night3
        ,case when room_night4 = 0 then 0 else qe4 / room_night4 end subsidy_per_room_night4
        ,case when room_night5 = 0 then 0 else qe5 / room_night5 end subsidy_per_room_night5
        ,case when room_night6 = 0 then 0 else qe6 / room_night6 end subsidy_per_room_night6
        ,case when room_night7 = 0 then 0 else qe7 / room_night7 end subsidy_per_room_night7
        ,case when room_night30 = 0 then 0 else qe30 / room_night30 end subsidy_per_room_night30
        ,case when room_night60 = 0 then 0 else qe60 / room_night60 end subsidy_per_room_night60
        ,case when room_night90 = 0 then 0 else qe90 / room_night90 end subsidy_per_room_night90
        ,case when room_night180 = 0 then 0 else qe180 / room_night180 end subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
from  (
    select t1.order_date,t1.user_type,t1.mdd
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0     then comission end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1    then comission end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2    then comission end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3    then comission end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4    then comission end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5    then comission end) yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6    then comission end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7    then comission end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then comission end) yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then comission end) yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then comission end) yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then comission end) yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_fee end) init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_fee end) init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_fee end) init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_fee end) init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_fee end) init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_fee end) init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_fee end) init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_fee end) init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_fee end) init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_fee end) init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_fee end) init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_fee end) init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then cqe end) qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then cqe end) qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then cqe end) qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then cqe end) qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then cqe end) qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then cqe end) qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then cqe end) qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then cqe end) qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then cqe end) qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then cqe end) qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then cqe end) qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then cqe end) qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then order_no end) order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then order_no end) order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then order_no end) order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then order_no end) order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then order_no end) order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then order_no end) order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then order_no end) order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then order_no end) order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then order_no end) order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) order_cnt180
    from user_cohort_segments t1 
    left join c_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by t1.order_date,t1.user_type,t1.mdd
) t1 
order by order_date, user_type, mdd
;


--- 3、小红书短视频ARPU数据
with user_type as (-----新老客
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
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,video_narrow as(   -- 短视频窄口径 短视频数据最早看25年4月数据，且需要前7天数据，所以从25年3月开始取
    select distinct t1.dt,user_name,member_name
    from (
        SELECT  query
                ,user_name
                ,dt
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-03-01' and '%(FORMAT_DATE)s' 
            and coalesce(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select 
            t1.dt
            ,t1.query
            ,member_name
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >=  '2025-03-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    where t3.query is null
)
,video_wide as(   -- 短视频宽口径 短视频数据最早看25年4月数据，且需要前7天数据，所以从25年3月开始取
    select distinct t1.dt,user_name
    from (
        SELECT  query
                ,user_name
                ,dt
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-03-01' and '%(FORMAT_DATE)s' 
            and coalesce(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join (
        select 
            t1.dt
            ,t1.query
            ,member_name
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >=  '2025-03-01'
        -- and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    -- left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    -- where t3.query is null
)
,red_wide as( -- 小红书宽口径
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2023-12-01' and date_sub(current_date,1)
    -- and business_type = 'hotel-inter'  --宽口径不用该字段
    and query_platform = 'redbook'
)
,red_narrow as( -- 小红书窄口径
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2023-12-01' and date_sub(current_date,1)
    and business_type = 'hotel-inter'  
    and query_platform = 'redbook'
)
,video_narrow_info as (-- 短视频窄口径生单人群
    select ord.order_date,ord.user_name,ord.user_type,ord.mdd
    from q_order ord 
    left join video_narrow r on ord.user_name = r.user_name
    where r.dt >= date_sub(ord.order_date, 7) and r.dt <= ord.order_date and r.user_name is not null
    group by 1,2,3,4
)
,video_wide_info as (-- 短视频宽口径生单人群
    select ord.order_date,ord.user_name,ord.user_type,ord.mdd
    from q_order ord 
    left join video_wide r on ord.user_name = r.user_name
    where r.dt >= date_sub(ord.order_date, 7) and r.dt <= ord.order_date and r.user_name is not null
    group by 1,2,3,4
)
,red_narrow_info as (--- 小红书窄口径生单人群
    select ord.order_date,ord.user_name,ord.user_type,ord.mdd
    from q_order ord 
    left join red_narrow r on ord.user_name = r.user_name
    where r.dt >= date_sub(ord.order_date, 7) and r.dt <= ord.order_date and r.user_name is not null
    group by 1,2,3,4
)
,red_wide_info as (--- 小红书宽口径生单人群
    select ord.order_date,ord.user_name,ord.user_type,ord.mdd
    from q_order ord 
    left join red_wide r on ord.user_name = r.user_name
    where r.dt >= date_sub(ord.order_date, 7) and r.dt <= ord.order_date and r.user_name is not null
    group by 1,2,3,4
)

--- 小红书宽口径渠道
select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,t1.channel
        ,uv
        --- ARPU数据
        ,yj0 / uv   ARPU0
        ,yj1 / uv   ARPU1
        ,yj2 / uv   ARPU2
        ,yj3 / uv   ARPU3
        ,yj4 / uv   ARPU4
        ,yj5 / uv   ARPU5
        ,yj6 / uv   ARPU6
        ,yj7 / uv   ARPU7
        ,yj30 / uv  ARPU30
        ,yj60 / uv  ARPU60
        ,yj90 / uv  ARPU90
        ,yj180 / uv ARPU180
        --- 单用户订单数据
        ,order_cnt0 / uv order_cnt_per_user0
        ,order_cnt1 / uv order_cnt_per_user1
        ,order_cnt2 / uv order_cnt_per_user2
        ,order_cnt3 / uv order_cnt_per_user3
        ,order_cnt4 / uv order_cnt_per_user4
        ,order_cnt5 / uv order_cnt_per_user5
        ,order_cnt6 / uv order_cnt_per_user6
        ,order_cnt7 / uv order_cnt_per_user7
        ,order_cnt30 / uv order_cnt_per_user30
        ,order_cnt60 / uv order_cnt_per_user60
        ,order_cnt90 / uv order_cnt_per_user90
        ,order_cnt180 / uv order_cnt_per_user180
        --- 单订单间夜数
        ,room_night0 / order_cnt0 room_night_per_order0
        ,room_night1 / order_cnt1 room_night_per_order1
        ,room_night2 / order_cnt2 room_night_per_order2
        ,room_night3 / order_cnt3 room_night_per_order3
        ,room_night4 / order_cnt4 room_night_per_order4
        ,room_night5 / order_cnt5 room_night_per_order5
        ,room_night6 / order_cnt6 room_night_per_order6
        ,room_night7 / order_cnt7 room_night_per_order7
        ,room_night30 / order_cnt30 room_night_per_order30
        ,room_night60 / order_cnt60 room_night_per_order60
        ,room_night90 / order_cnt90 room_night_per_order90
        ,room_night180 / order_cnt180 room_night_per_order180
        --- ADR
        ,init_gmv0 / room_night0 adr0
        ,init_gmv1 / room_night1 adr1
        ,init_gmv2 / room_night2 adr2   
        ,init_gmv3 / room_night3 adr3
        ,init_gmv4 / room_night4 adr4
        ,init_gmv5 / room_night5 adr5
        ,init_gmv6 / room_night6 adr6
        ,init_gmv7 / room_night7 adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv60 / room_night60 adr60
        ,init_gmv90 / room_night90 adr90
        ,init_gmv180 / room_night180 adr180
        --- 佣金率
        ,case when init_gmv0 = 0 then 0 else yj0 / init_gmv0 end commission_rate0
        ,case when init_gmv1 = 0 then 0 else yj1 / init_gmv1 end commission_rate1
        ,case when init_gmv2 = 0 then 0 else yj2 / init_gmv2 end commission_rate2
        ,case when init_gmv3 = 0 then 0 else yj3 / init_gmv3 end commission_rate3
        ,case when init_gmv4 = 0 then 0 else yj4 / init_gmv4 end commission_rate4
        ,case when init_gmv5 = 0 then 0 else yj5 / init_gmv5 end commission_rate5
        ,case when init_gmv6 = 0 then 0 else yj6 / init_gmv6 end commission_rate6
        ,case when init_gmv7 = 0 then 0 else yj7 / init_gmv7 end commission_rate7
        ,case when init_gmv30 = 0 then 0 else yj30 / init_gmv30 end commission_rate30
        ,case when init_gmv60 = 0 then 0 else yj60 / init_gmv60 end commission_rate60
        ,case when init_gmv90 = 0 then 0 else yj90 / init_gmv90 end commission_rate90
        ,case when init_gmv180 = 0 then 0 else yj180 / init_gmv180 end commission_rate180
        --- 补贴率
        ,case when init_gmv0 = 0 then 0 else qe0 / init_gmv0 end subsidy_rate0
        ,case when init_gmv1 = 0 then 0 else qe1 / init_gmv1 end subsidy_rate1
        ,case when init_gmv2 = 0 then 0 else qe2 / init_gmv2 end subsidy_rate2
        ,case when init_gmv3 = 0 then 0 else qe3 / init_gmv3 end subsidy_rate3
        ,case when init_gmv4 = 0 then 0 else qe4 / init_gmv4 end subsidy_rate4
        ,case when init_gmv5 = 0 then 0 else qe5 / init_gmv5 end subsidy_rate5
        ,case when init_gmv6 = 0 then 0 else qe6 / init_gmv6 end subsidy_rate6
        ,case when init_gmv7 = 0 then 0 else qe7 / init_gmv7 end subsidy_rate7
        ,case when init_gmv30 = 0 then 0 else qe30 / init_gmv30 end subsidy_rate30
        ,case when init_gmv60 = 0 then 0 else qe60 / init_gmv60 end subsidy_rate60
        ,case when init_gmv90 = 0 then 0 else qe90 / init_gmv90 end subsidy_rate90
        ,case when init_gmv180 = 0 then 0 else qe180 / init_gmv180 end subsidy_rate180
        --- 单间夜补贴
        ,case when room_night0 = 0 then 0 else qe0 / room_night0 end subsidy_per_room_night0
        ,case when room_night1 = 0 then 0 else qe1 / room_night1 end subsidy_per_room_night1
        ,case when room_night2 = 0 then 0 else qe2 / room_night2 end subsidy_per_room_night2
        ,case when room_night3 = 0 then 0 else qe3 / room_night3 end subsidy_per_room_night3
        ,case when room_night4 = 0 then 0 else qe4 / room_night4 end subsidy_per_room_night4
        ,case when room_night5 = 0 then 0 else qe5 / room_night5 end subsidy_per_room_night5
        ,case when room_night6 = 0 then 0 else qe6 / room_night6 end subsidy_per_room_night6
        ,case when room_night7 = 0 then 0 else qe7 / room_night7 end subsidy_per_room_night7
        ,case when room_night30 = 0 then 0 else qe30 / room_night30 end subsidy_per_room_night30
        ,case when room_night60 = 0 then 0 else qe60 / room_night60 end subsidy_per_room_night60
        ,case when room_night90 = 0 then 0 else qe90 / room_night90 end subsidy_per_room_night90
        ,case when room_night180 = 0 then 0 else qe180 / room_night180 end subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        -- 离店佣金
        ,ldyj0, ldyj1, ldyj2, ldyj3, ldyj4, ldyj5, ldyj6, ldyj7, ldyj30, ldyj60, ldyj90, ldyj180
from  (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,'小红书宽口径' as  channel
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0   then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1  then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2  then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3  then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4  then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5  then final_commission_after end) yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6  then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7  then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then final_commission_after end) yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then final_commission_after end) yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then final_commission_after end) yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then final_commission_after end) yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then init_gmv end) init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then init_gmv end) init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then init_gmv end) init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then init_gmv end) init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then init_gmv end) init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then init_gmv end) init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then init_gmv end) init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then init_gmv end) init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then init_gmv end) init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then coupon_substract_summary end) qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then coupon_substract_summary end) qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then coupon_substract_summary end) qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then coupon_substract_summary end) qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then coupon_substract_summary end) qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then coupon_substract_summary end) qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then coupon_substract_summary end) qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then coupon_substract_summary end) qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then coupon_substract_summary end) qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then coupon_substract_summary end) qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then coupon_substract_summary end) qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then order_no end) order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then order_no end) order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then order_no end) order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then order_no end) order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then order_no end) order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then order_no end) order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then order_no end) order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then order_no end) order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then order_no end) order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) order_cnt180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) ldyj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) ldyj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) ldyj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) ldyj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) ldyj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) ldyj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) ldyj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) ldyj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then ldyj end) ldyj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then ldyj end) ldyj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then ldyj end) ldyj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) ldyj180
    from red_wide_info t1 
    left join q_order t2 on t1.user_name=t2.user_name and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.mdd)
) t1 

union all
--- 小红书窄口径渠道
select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,t1.channel
        ,uv
        --- ARPU数据
        ,yj0 / uv   ARPU0
        ,yj1 / uv   ARPU1
        ,yj2 / uv   ARPU2
        ,yj3 / uv   ARPU3
        ,yj4 / uv   ARPU4
        ,yj5 / uv   ARPU5
        ,yj6 / uv   ARPU6
        ,yj7 / uv   ARPU7
        ,yj30 / uv  ARPU30
        ,yj60 / uv  ARPU60
        ,yj90 / uv  ARPU90
        ,yj180 / uv ARPU180
        --- 单用户订单数据
        ,order_cnt0 / uv order_cnt_per_user0
        ,order_cnt1 / uv order_cnt_per_user1
        ,order_cnt2 / uv order_cnt_per_user2
        ,order_cnt3 / uv order_cnt_per_user3
        ,order_cnt4 / uv order_cnt_per_user4
        ,order_cnt5 / uv order_cnt_per_user5
        ,order_cnt6 / uv order_cnt_per_user6
        ,order_cnt7 / uv order_cnt_per_user7
        ,order_cnt30 / uv order_cnt_per_user30
        ,order_cnt60 / uv order_cnt_per_user60
        ,order_cnt90 / uv order_cnt_per_user90
        ,order_cnt180 / uv order_cnt_per_user180
        --- 单订单间夜数
        ,room_night0 / order_cnt0 room_night_per_order0
        ,room_night1 / order_cnt1 room_night_per_order1
        ,room_night2 / order_cnt2 room_night_per_order2
        ,room_night3 / order_cnt3 room_night_per_order3
        ,room_night4 / order_cnt4 room_night_per_order4
        ,room_night5 / order_cnt5 room_night_per_order5
        ,room_night6 / order_cnt6 room_night_per_order6
        ,room_night7 / order_cnt7 room_night_per_order7
        ,room_night30 / order_cnt30 room_night_per_order30
        ,room_night60 / order_cnt60 room_night_per_order60
        ,room_night90 / order_cnt90 room_night_per_order90
        ,room_night180 / order_cnt180 room_night_per_order180
        --- ADR
        ,init_gmv0 / room_night0 adr0
        ,init_gmv1 / room_night1 adr1
        ,init_gmv2 / room_night2 adr2   
        ,init_gmv3 / room_night3 adr3
        ,init_gmv4 / room_night4 adr4
        ,init_gmv5 / room_night5 adr5
        ,init_gmv6 / room_night6 adr6
        ,init_gmv7 / room_night7 adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv60 / room_night60 adr60
        ,init_gmv90 / room_night90 adr90
        ,init_gmv180 / room_night180 adr180
        --- 佣金率
        ,case when init_gmv0 = 0 then 0 else yj0 / init_gmv0 end commission_rate0
        ,case when init_gmv1 = 0 then 0 else yj1 / init_gmv1 end commission_rate1
        ,case when init_gmv2 = 0 then 0 else yj2 / init_gmv2 end commission_rate2
        ,case when init_gmv3 = 0 then 0 else yj3 / init_gmv3 end commission_rate3
        ,case when init_gmv4 = 0 then 0 else yj4 / init_gmv4 end commission_rate4
        ,case when init_gmv5 = 0 then 0 else yj5 / init_gmv5 end commission_rate5
        ,case when init_gmv6 = 0 then 0 else yj6 / init_gmv6 end commission_rate6
        ,case when init_gmv7 = 0 then 0 else yj7 / init_gmv7 end commission_rate7
        ,case when init_gmv30 = 0 then 0 else yj30 / init_gmv30 end commission_rate30
        ,case when init_gmv60 = 0 then 0 else yj60 / init_gmv60 end commission_rate60
        ,case when init_gmv90 = 0 then 0 else yj90 / init_gmv90 end commission_rate90
        ,case when init_gmv180 = 0 then 0 else yj180 / init_gmv180 end commission_rate180
        --- 补贴率
        ,case when init_gmv0 = 0 then 0 else qe0 / init_gmv0 end subsidy_rate0
        ,case when init_gmv1 = 0 then 0 else qe1 / init_gmv1 end subsidy_rate1
        ,case when init_gmv2 = 0 then 0 else qe2 / init_gmv2 end subsidy_rate2
        ,case when init_gmv3 = 0 then 0 else qe3 / init_gmv3 end subsidy_rate3
        ,case when init_gmv4 = 0 then 0 else qe4 / init_gmv4 end subsidy_rate4
        ,case when init_gmv5 = 0 then 0 else qe5 / init_gmv5 end subsidy_rate5
        ,case when init_gmv6 = 0 then 0 else qe6 / init_gmv6 end subsidy_rate6
        ,case when init_gmv7 = 0 then 0 else qe7 / init_gmv7 end subsidy_rate7
        ,case when init_gmv30 = 0 then 0 else qe30 / init_gmv30 end subsidy_rate30
        ,case when init_gmv60 = 0 then 0 else qe60 / init_gmv60 end subsidy_rate60
        ,case when init_gmv90 = 0 then 0 else qe90 / init_gmv90 end subsidy_rate90
        ,case when init_gmv180 = 0 then 0 else qe180 / init_gmv180 end subsidy_rate180
        --- 单间夜补贴
        ,case when room_night0 = 0 then 0 else qe0 / room_night0 end subsidy_per_room_night0
        ,case when room_night1 = 0 then 0 else qe1 / room_night1 end subsidy_per_room_night1
        ,case when room_night2 = 0 then 0 else qe2 / room_night2 end subsidy_per_room_night2
        ,case when room_night3 = 0 then 0 else qe3 / room_night3 end subsidy_per_room_night3
        ,case when room_night4 = 0 then 0 else qe4 / room_night4 end subsidy_per_room_night4
        ,case when room_night5 = 0 then 0 else qe5 / room_night5 end subsidy_per_room_night5
        ,case when room_night6 = 0 then 0 else qe6 / room_night6 end subsidy_per_room_night6
        ,case when room_night7 = 0 then 0 else qe7 / room_night7 end subsidy_per_room_night7
        ,case when room_night30 = 0 then 0 else qe30 / room_night30 end subsidy_per_room_night30
        ,case when room_night60 = 0 then 0 else qe60 / room_night60 end subsidy_per_room_night60
        ,case when room_night90 = 0 then 0 else qe90 / room_night90 end subsidy_per_room_night90
        ,case when room_night180 = 0 then 0 else qe180 / room_night180 end subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        -- 离店佣金
        ,ldyj0, ldyj1, ldyj2, ldyj3, ldyj4, ldyj5, ldyj6, ldyj7, ldyj30, ldyj60, ldyj90, ldyj180
from  (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,'小红书窄口径' as  channel
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0   then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1  then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2  then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3  then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4  then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5  then final_commission_after end) yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6  then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7  then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then final_commission_after end) yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then final_commission_after end) yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then final_commission_after end) yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then final_commission_after end) yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then init_gmv end) init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then init_gmv end) init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then init_gmv end) init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then init_gmv end) init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then init_gmv end) init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then init_gmv end) init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then init_gmv end) init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then init_gmv end) init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then init_gmv end) init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then coupon_substract_summary end) qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then coupon_substract_summary end) qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then coupon_substract_summary end) qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then coupon_substract_summary end) qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then coupon_substract_summary end) qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then coupon_substract_summary end) qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then coupon_substract_summary end) qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then coupon_substract_summary end) qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then coupon_substract_summary end) qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then coupon_substract_summary end) qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then coupon_substract_summary end) qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then order_no end) order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then order_no end) order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then order_no end) order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then order_no end) order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then order_no end) order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then order_no end) order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then order_no end) order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then order_no end) order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then order_no end) order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) order_cnt180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) ldyj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) ldyj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) ldyj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) ldyj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) ldyj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) ldyj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) ldyj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) ldyj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then ldyj end) ldyj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then ldyj end) ldyj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then ldyj end) ldyj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) ldyj180
    from red_narrow_info t1 
    left join q_order t2 on t1.user_name=t2.user_name and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.mdd)
) t1 

union all
--- 短视频宽口径渠道
select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,t1.channel
        ,uv
        --- ARPU数据
        ,yj0 / uv   ARPU0
        ,yj1 / uv   ARPU1
        ,yj2 / uv   ARPU2
        ,yj3 / uv   ARPU3
        ,yj4 / uv   ARPU4
        ,yj5 / uv   ARPU5
        ,yj6 / uv   ARPU6
        ,yj7 / uv   ARPU7
        ,yj30 / uv  ARPU30
        ,yj60 / uv  ARPU60
        ,yj90 / uv  ARPU90
        ,yj180 / uv ARPU180
        --- 单用户订单数据
        ,order_cnt0 / uv order_cnt_per_user0
        ,order_cnt1 / uv order_cnt_per_user1
        ,order_cnt2 / uv order_cnt_per_user2
        ,order_cnt3 / uv order_cnt_per_user3
        ,order_cnt4 / uv order_cnt_per_user4
        ,order_cnt5 / uv order_cnt_per_user5
        ,order_cnt6 / uv order_cnt_per_user6
        ,order_cnt7 / uv order_cnt_per_user7
        ,order_cnt30 / uv order_cnt_per_user30
        ,order_cnt60 / uv order_cnt_per_user60
        ,order_cnt90 / uv order_cnt_per_user90
        ,order_cnt180 / uv order_cnt_per_user180
        --- 单订单间夜数
        ,room_night0 / order_cnt0 room_night_per_order0
        ,room_night1 / order_cnt1 room_night_per_order1
        ,room_night2 / order_cnt2 room_night_per_order2
        ,room_night3 / order_cnt3 room_night_per_order3
        ,room_night4 / order_cnt4 room_night_per_order4
        ,room_night5 / order_cnt5 room_night_per_order5
        ,room_night6 / order_cnt6 room_night_per_order6
        ,room_night7 / order_cnt7 room_night_per_order7
        ,room_night30 / order_cnt30 room_night_per_order30
        ,room_night60 / order_cnt60 room_night_per_order60
        ,room_night90 / order_cnt90 room_night_per_order90
        ,room_night180 / order_cnt180 room_night_per_order180
        --- ADR
        ,init_gmv0 / room_night0 adr0
        ,init_gmv1 / room_night1 adr1
        ,init_gmv2 / room_night2 adr2   
        ,init_gmv3 / room_night3 adr3
        ,init_gmv4 / room_night4 adr4
        ,init_gmv5 / room_night5 adr5
        ,init_gmv6 / room_night6 adr6
        ,init_gmv7 / room_night7 adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv60 / room_night60 adr60
        ,init_gmv90 / room_night90 adr90
        ,init_gmv180 / room_night180 adr180
        --- 佣金率
        ,case when init_gmv0 = 0 then 0 else yj0 / init_gmv0 end commission_rate0
        ,case when init_gmv1 = 0 then 0 else yj1 / init_gmv1 end commission_rate1
        ,case when init_gmv2 = 0 then 0 else yj2 / init_gmv2 end commission_rate2
        ,case when init_gmv3 = 0 then 0 else yj3 / init_gmv3 end commission_rate3
        ,case when init_gmv4 = 0 then 0 else yj4 / init_gmv4 end commission_rate4
        ,case when init_gmv5 = 0 then 0 else yj5 / init_gmv5 end commission_rate5
        ,case when init_gmv6 = 0 then 0 else yj6 / init_gmv6 end commission_rate6
        ,case when init_gmv7 = 0 then 0 else yj7 / init_gmv7 end commission_rate7
        ,case when init_gmv30 = 0 then 0 else yj30 / init_gmv30 end commission_rate30
        ,case when init_gmv60 = 0 then 0 else yj60 / init_gmv60 end commission_rate60
        ,case when init_gmv90 = 0 then 0 else yj90 / init_gmv90 end commission_rate90
        ,case when init_gmv180 = 0 then 0 else yj180 / init_gmv180 end commission_rate180
        --- 补贴率
        ,case when init_gmv0 = 0 then 0 else qe0 / init_gmv0 end subsidy_rate0
        ,case when init_gmv1 = 0 then 0 else qe1 / init_gmv1 end subsidy_rate1
        ,case when init_gmv2 = 0 then 0 else qe2 / init_gmv2 end subsidy_rate2
        ,case when init_gmv3 = 0 then 0 else qe3 / init_gmv3 end subsidy_rate3
        ,case when init_gmv4 = 0 then 0 else qe4 / init_gmv4 end subsidy_rate4
        ,case when init_gmv5 = 0 then 0 else qe5 / init_gmv5 end subsidy_rate5
        ,case when init_gmv6 = 0 then 0 else qe6 / init_gmv6 end subsidy_rate6
        ,case when init_gmv7 = 0 then 0 else qe7 / init_gmv7 end subsidy_rate7
        ,case when init_gmv30 = 0 then 0 else qe30 / init_gmv30 end subsidy_rate30
        ,case when init_gmv60 = 0 then 0 else qe60 / init_gmv60 end subsidy_rate60
        ,case when init_gmv90 = 0 then 0 else qe90 / init_gmv90 end subsidy_rate90
        ,case when init_gmv180 = 0 then 0 else qe180 / init_gmv180 end subsidy_rate180
        --- 单间夜补贴
        ,case when room_night0 = 0 then 0 else qe0 / room_night0 end subsidy_per_room_night0
        ,case when room_night1 = 0 then 0 else qe1 / room_night1 end subsidy_per_room_night1
        ,case when room_night2 = 0 then 0 else qe2 / room_night2 end subsidy_per_room_night2
        ,case when room_night3 = 0 then 0 else qe3 / room_night3 end subsidy_per_room_night3
        ,case when room_night4 = 0 then 0 else qe4 / room_night4 end subsidy_per_room_night4
        ,case when room_night5 = 0 then 0 else qe5 / room_night5 end subsidy_per_room_night5
        ,case when room_night6 = 0 then 0 else qe6 / room_night6 end subsidy_per_room_night6
        ,case when room_night7 = 0 then 0 else qe7 / room_night7 end subsidy_per_room_night7
        ,case when room_night30 = 0 then 0 else qe30 / room_night30 end subsidy_per_room_night30
        ,case when room_night60 = 0 then 0 else qe60 / room_night60 end subsidy_per_room_night60
        ,case when room_night90 = 0 then 0 else qe90 / room_night90 end subsidy_per_room_night90
        ,case when room_night180 = 0 then 0 else qe180 / room_night180 end subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        -- 离店佣金
        ,ldyj0, ldyj1, ldyj2, ldyj3, ldyj4, ldyj5, ldyj6, ldyj7, ldyj30, ldyj60, ldyj90, ldyj180
from  (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,'短视频宽口径' as  channel
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0   then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1  then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2  then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3  then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4  then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5  then final_commission_after end) yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6  then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7  then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then final_commission_after end) yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then final_commission_after end) yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then final_commission_after end) yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then final_commission_after end) yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then init_gmv end) init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then init_gmv end) init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then init_gmv end) init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then init_gmv end) init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then init_gmv end) init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then init_gmv end) init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then init_gmv end) init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then init_gmv end) init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then init_gmv end) init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then coupon_substract_summary end) qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then coupon_substract_summary end) qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then coupon_substract_summary end) qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then coupon_substract_summary end) qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then coupon_substract_summary end) qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then coupon_substract_summary end) qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then coupon_substract_summary end) qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then coupon_substract_summary end) qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then coupon_substract_summary end) qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then coupon_substract_summary end) qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then coupon_substract_summary end) qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then order_no end) order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then order_no end) order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then order_no end) order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then order_no end) order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then order_no end) order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then order_no end) order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then order_no end) order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then order_no end) order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then order_no end) order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) order_cnt180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) ldyj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) ldyj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) ldyj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) ldyj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) ldyj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) ldyj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) ldyj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) ldyj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then ldyj end) ldyj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then ldyj end) ldyj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then ldyj end) ldyj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) ldyj180
    from video_wide_info t1 
    left join q_order t2 on t1.user_name=t2.user_name and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.mdd)
) t1 
union all
--- 短视频窄口径渠道
select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,t1.channel
        ,uv
        --- ARPU数据
        ,yj0 / uv   ARPU0
        ,yj1 / uv   ARPU1
        ,yj2 / uv   ARPU2
        ,yj3 / uv   ARPU3
        ,yj4 / uv   ARPU4
        ,yj5 / uv   ARPU5
        ,yj6 / uv   ARPU6
        ,yj7 / uv   ARPU7
        ,yj30 / uv  ARPU30
        ,yj60 / uv  ARPU60
        ,yj90 / uv  ARPU90
        ,yj180 / uv ARPU180
        --- 单用户订单数据
        ,order_cnt0 / uv order_cnt_per_user0
        ,order_cnt1 / uv order_cnt_per_user1
        ,order_cnt2 / uv order_cnt_per_user2
        ,order_cnt3 / uv order_cnt_per_user3
        ,order_cnt4 / uv order_cnt_per_user4
        ,order_cnt5 / uv order_cnt_per_user5
        ,order_cnt6 / uv order_cnt_per_user6
        ,order_cnt7 / uv order_cnt_per_user7
        ,order_cnt30 / uv order_cnt_per_user30
        ,order_cnt60 / uv order_cnt_per_user60
        ,order_cnt90 / uv order_cnt_per_user90
        ,order_cnt180 / uv order_cnt_per_user180
        --- 单订单间夜数
        ,room_night0 / order_cnt0 room_night_per_order0
        ,room_night1 / order_cnt1 room_night_per_order1
        ,room_night2 / order_cnt2 room_night_per_order2
        ,room_night3 / order_cnt3 room_night_per_order3
        ,room_night4 / order_cnt4 room_night_per_order4
        ,room_night5 / order_cnt5 room_night_per_order5
        ,room_night6 / order_cnt6 room_night_per_order6
        ,room_night7 / order_cnt7 room_night_per_order7
        ,room_night30 / order_cnt30 room_night_per_order30
        ,room_night60 / order_cnt60 room_night_per_order60
        ,room_night90 / order_cnt90 room_night_per_order90
        ,room_night180 / order_cnt180 room_night_per_order180
        --- ADR
        ,init_gmv0 / room_night0 adr0
        ,init_gmv1 / room_night1 adr1
        ,init_gmv2 / room_night2 adr2   
        ,init_gmv3 / room_night3 adr3
        ,init_gmv4 / room_night4 adr4
        ,init_gmv5 / room_night5 adr5
        ,init_gmv6 / room_night6 adr6
        ,init_gmv7 / room_night7 adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv60 / room_night60 adr60
        ,init_gmv90 / room_night90 adr90
        ,init_gmv180 / room_night180 adr180
        --- 佣金率
        ,case when init_gmv0 = 0 then 0 else yj0 / init_gmv0 end commission_rate0
        ,case when init_gmv1 = 0 then 0 else yj1 / init_gmv1 end commission_rate1
        ,case when init_gmv2 = 0 then 0 else yj2 / init_gmv2 end commission_rate2
        ,case when init_gmv3 = 0 then 0 else yj3 / init_gmv3 end commission_rate3
        ,case when init_gmv4 = 0 then 0 else yj4 / init_gmv4 end commission_rate4
        ,case when init_gmv5 = 0 then 0 else yj5 / init_gmv5 end commission_rate5
        ,case when init_gmv6 = 0 then 0 else yj6 / init_gmv6 end commission_rate6
        ,case when init_gmv7 = 0 then 0 else yj7 / init_gmv7 end commission_rate7
        ,case when init_gmv30 = 0 then 0 else yj30 / init_gmv30 end commission_rate30
        ,case when init_gmv60 = 0 then 0 else yj60 / init_gmv60 end commission_rate60
        ,case when init_gmv90 = 0 then 0 else yj90 / init_gmv90 end commission_rate90
        ,case when init_gmv180 = 0 then 0 else yj180 / init_gmv180 end commission_rate180
        --- 补贴率
        ,case when init_gmv0 = 0 then 0 else qe0 / init_gmv0 end subsidy_rate0
        ,case when init_gmv1 = 0 then 0 else qe1 / init_gmv1 end subsidy_rate1
        ,case when init_gmv2 = 0 then 0 else qe2 / init_gmv2 end subsidy_rate2
        ,case when init_gmv3 = 0 then 0 else qe3 / init_gmv3 end subsidy_rate3
        ,case when init_gmv4 = 0 then 0 else qe4 / init_gmv4 end subsidy_rate4
        ,case when init_gmv5 = 0 then 0 else qe5 / init_gmv5 end subsidy_rate5
        ,case when init_gmv6 = 0 then 0 else qe6 / init_gmv6 end subsidy_rate6
        ,case when init_gmv7 = 0 then 0 else qe7 / init_gmv7 end subsidy_rate7
        ,case when init_gmv30 = 0 then 0 else qe30 / init_gmv30 end subsidy_rate30
        ,case when init_gmv60 = 0 then 0 else qe60 / init_gmv60 end subsidy_rate60
        ,case when init_gmv90 = 0 then 0 else qe90 / init_gmv90 end subsidy_rate90
        ,case when init_gmv180 = 0 then 0 else qe180 / init_gmv180 end subsidy_rate180
        --- 单间夜补贴
        ,case when room_night0 = 0 then 0 else qe0 / room_night0 end subsidy_per_room_night0
        ,case when room_night1 = 0 then 0 else qe1 / room_night1 end subsidy_per_room_night1
        ,case when room_night2 = 0 then 0 else qe2 / room_night2 end subsidy_per_room_night2
        ,case when room_night3 = 0 then 0 else qe3 / room_night3 end subsidy_per_room_night3
        ,case when room_night4 = 0 then 0 else qe4 / room_night4 end subsidy_per_room_night4
        ,case when room_night5 = 0 then 0 else qe5 / room_night5 end subsidy_per_room_night5
        ,case when room_night6 = 0 then 0 else qe6 / room_night6 end subsidy_per_room_night6
        ,case when room_night7 = 0 then 0 else qe7 / room_night7 end subsidy_per_room_night7
        ,case when room_night30 = 0 then 0 else qe30 / room_night30 end subsidy_per_room_night30
        ,case when room_night60 = 0 then 0 else qe60 / room_night60 end subsidy_per_room_night60
        ,case when room_night90 = 0 then 0 else qe90 / room_night90 end subsidy_per_room_night90
        ,case when room_night180 = 0 then 0 else qe180 / room_night180 end subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        -- 离店佣金
        ,ldyj0, ldyj1, ldyj2, ldyj3, ldyj4, ldyj5, ldyj6, ldyj7, ldyj30, ldyj60, ldyj90, ldyj180
from  (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,'短视频窄口径' as  channel
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0   then final_commission_after end) yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1  then final_commission_after end) yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2  then final_commission_after end) yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3  then final_commission_after end) yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4  then final_commission_after end) yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5  then final_commission_after end) yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6  then final_commission_after end) yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7  then final_commission_after end) yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then final_commission_after end) yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then final_commission_after end) yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then final_commission_after end) yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then final_commission_after end) yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then init_gmv end) init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then init_gmv end) init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then init_gmv end) init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then init_gmv end) init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then init_gmv end) init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then init_gmv end) init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then init_gmv end) init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then init_gmv end) init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then init_gmv end) init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then coupon_substract_summary end) qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then coupon_substract_summary end) qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then coupon_substract_summary end) qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then coupon_substract_summary end) qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then coupon_substract_summary end) qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then coupon_substract_summary end) qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then coupon_substract_summary end) qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then coupon_substract_summary end) qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then coupon_substract_summary end) qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then coupon_substract_summary end) qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then coupon_substract_summary end) qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then order_no end) order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then order_no end) order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then order_no end) order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then order_no end) order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then order_no end) order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then order_no end) order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then order_no end) order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then order_no end) order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then order_no end) order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) order_cnt180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) ldyj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) ldyj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) ldyj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) ldyj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) ldyj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) ldyj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) ldyj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) ldyj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then ldyj end) ldyj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then ldyj end) ldyj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then ldyj end) ldyj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) ldyj180
    from video_narrow_info t1 
    left join q_order t2 on t1.user_name=t2.user_name and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.mdd)
) t1 
order by order_date, user_type, mdd, channel
;


--- 1、Q分新老x目的地x渠道ARPU数据定时任务调度脚本
-- 1. 开启动态分区配置
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

with user_type as (-----新老客判定
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by user_id
)
,q_order as (---- Q订单明细表 (抓取近181天数据用于计算最大180天LTV)
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        -- 注意：日常调度只回刷过去181天的数据，确保覆盖到最大180天的LTV计算需求
        and order_date >= '${zdt.addDay(-181).format("yyyy-MM-dd")}' 
        and order_date <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
)
,channel_info as (---渠道信息表 --- 抓取近181天用户的渠道信息用于后续order_info表的渠道维度分析
    select dt,user_name,channel 
    from ihotel_default.dwd_flow_ug_channel_di 
    where dt >= '${zdt.addDay(-181).format("yyyy-MM-dd")}'  and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    group by dt,user_name,channel
)
,order_info as (--- 大盘生单人群分新老分目的地分渠道
    select t1.order_date
        ,t1.user_name
        ,t1.user_type
        ,t1.mdd
        ,t2.channel
    from q_order t1
    left join channel_info t2 on t1.user_name = t2.user_name and t1.order_date = t2.dt
    group by t1.order_date, t1.user_name, t1.user_type, t1.mdd, t2.channel
)

-- 2. 数据插入，动态分区 dt (对应 order_date) 放置于最后
INSERT OVERWRITE TABLE ads_ihotel_user_ltv_di PARTITION (dt)
-- 最终输出层
select 
         t1.user_type
        ,t1.mdd
        ,t1.channel
        ,t1.uv
        
        --- ARPU数据 (添加 nullif 保护防止除0异常)
        ,coalesce(yj0 / nullif(uv, 0), 0)   as ARPU0
        ,coalesce(yj1 / nullif(uv, 0), 0)   as ARPU1
        ,coalesce(yj2 / nullif(uv, 0), 0)   as ARPU2
        ,coalesce(yj3 / nullif(uv, 0), 0)   as ARPU3
        ,coalesce(yj4 / nullif(uv, 0), 0)   as ARPU4
        ,coalesce(yj5 / nullif(uv, 0), 0)   as ARPU5
        ,coalesce(yj6 / nullif(uv, 0), 0)   as ARPU6
        ,coalesce(yj7 / nullif(uv, 0), 0)   as ARPU7
        ,coalesce(yj30 / nullif(uv, 0), 0)  as ARPU30
        ,coalesce(yj60 / nullif(uv, 0), 0)  as ARPU60
        ,coalesce(yj90 / nullif(uv, 0), 0)  as ARPU90
        ,coalesce(yj180 / nullif(uv, 0), 0) as ARPU180
        
        --- 单用户订单数据
        ,coalesce(order_cnt0 / nullif(uv, 0), 0)   as order_cnt_per_user0
        ,coalesce(order_cnt1 / nullif(uv, 0), 0)   as order_cnt_per_user1
        ,coalesce(order_cnt2 / nullif(uv, 0), 0)   as order_cnt_per_user2
        ,coalesce(order_cnt3 / nullif(uv, 0), 0)   as order_cnt_per_user3
        ,coalesce(order_cnt4 / nullif(uv, 0), 0)   as order_cnt_per_user4
        ,coalesce(order_cnt5 / nullif(uv, 0), 0)   as order_cnt_per_user5
        ,coalesce(order_cnt6 / nullif(uv, 0), 0)   as order_cnt_per_user6
        ,coalesce(order_cnt7 / nullif(uv, 0), 0)   as order_cnt_per_user7
        ,coalesce(order_cnt30 / nullif(uv, 0), 0)  as order_cnt_per_user30
        ,coalesce(order_cnt60 / nullif(uv, 0), 0)  as order_cnt_per_user60
        ,coalesce(order_cnt90 / nullif(uv, 0), 0)  as order_cnt_per_user90
        ,coalesce(order_cnt180 / nullif(uv, 0), 0) as order_cnt_per_user180
        
        --- 单订单间夜数
        ,coalesce(room_night0 / nullif(order_cnt0, 0), 0)     as room_night_per_order0
        ,coalesce(room_night1 / nullif(order_cnt1, 0), 0)     as room_night_per_order1
        ,coalesce(room_night2 / nullif(order_cnt2, 0), 0)     as room_night_per_order2
        ,coalesce(room_night3 / nullif(order_cnt3, 0), 0)     as room_night_per_order3
        ,coalesce(room_night4 / nullif(order_cnt4, 0), 0)     as room_night_per_order4
        ,coalesce(room_night5 / nullif(order_cnt5, 0), 0)     as room_night_per_order5
        ,coalesce(room_night6 / nullif(order_cnt6, 0), 0)     as room_night_per_order6
        ,coalesce(room_night7 / nullif(order_cnt7, 0), 0)     as room_night_per_order7
        ,coalesce(room_night30 / nullif(order_cnt30, 0), 0)   as room_night_per_order30
        ,coalesce(room_night60 / nullif(order_cnt60, 0), 0)   as room_night_per_order60
        ,coalesce(room_night90 / nullif(order_cnt90, 0), 0)   as room_night_per_order90
        ,coalesce(room_night180 / nullif(order_cnt180, 0), 0) as room_night_per_order180
        
        --- ADR
        ,coalesce(init_gmv0 / nullif(room_night0, 0), 0)     as adr0
        ,coalesce(init_gmv1 / nullif(room_night1, 0), 0)     as adr1
        ,coalesce(init_gmv2 / nullif(room_night2, 0), 0)     as adr2   
        ,coalesce(init_gmv3 / nullif(room_night3, 0), 0)     as adr3
        ,coalesce(init_gmv4 / nullif(room_night4, 0), 0)     as adr4
        ,coalesce(init_gmv5 / nullif(room_night5, 0), 0)     as adr5
        ,coalesce(init_gmv6 / nullif(room_night6, 0), 0)     as adr6
        ,coalesce(init_gmv7 / nullif(room_night7, 0), 0)     as adr7
        ,coalesce(init_gmv30 / nullif(room_night30, 0), 0)   as adr30
        ,coalesce(init_gmv60 / nullif(room_night60, 0), 0)   as adr60
        ,coalesce(init_gmv90 / nullif(room_night90, 0), 0)   as adr90
        ,coalesce(init_gmv180 / nullif(room_night180, 0), 0) as adr180
        
        --- 佣金率 (简化为安全除法函数)
        ,coalesce(yj0 / nullif(init_gmv0, 0), 0)     as commission_rate0
        ,coalesce(yj1 / nullif(init_gmv1, 0), 0)     as commission_rate1
        ,coalesce(yj2 / nullif(init_gmv2, 0), 0)     as commission_rate2
        ,coalesce(yj3 / nullif(init_gmv3, 0), 0)     as commission_rate3
        ,coalesce(yj4 / nullif(init_gmv4, 0), 0)     as commission_rate4
        ,coalesce(yj5 / nullif(init_gmv5, 0), 0)     as commission_rate5
        ,coalesce(yj6 / nullif(init_gmv6, 0), 0)     as commission_rate6
        ,coalesce(yj7 / nullif(init_gmv7, 0), 0)     as commission_rate7
        ,coalesce(yj30 / nullif(init_gmv30, 0), 0)   as commission_rate30
        ,coalesce(yj60 / nullif(init_gmv60, 0), 0)   as commission_rate60
        ,coalesce(yj90 / nullif(init_gmv90, 0), 0)   as commission_rate90
        ,coalesce(yj180 / nullif(init_gmv180, 0), 0) as commission_rate180
        
        --- 补贴率
        ,coalesce(qe0 / nullif(init_gmv0, 0), 0)     as subsidy_rate0
        ,coalesce(qe1 / nullif(init_gmv1, 0), 0)     as subsidy_rate1
        ,coalesce(qe2 / nullif(init_gmv2, 0), 0)     as subsidy_rate2
        ,coalesce(qe3 / nullif(init_gmv3, 0), 0)     as subsidy_rate3
        ,coalesce(qe4 / nullif(init_gmv4, 0), 0)     as subsidy_rate4
        ,coalesce(qe5 / nullif(init_gmv5, 0), 0)     as subsidy_rate5
        ,coalesce(qe6 / nullif(init_gmv6, 0), 0)     as subsidy_rate6
        ,coalesce(qe7 / nullif(init_gmv7, 0), 0)     as subsidy_rate7
        ,coalesce(qe30 / nullif(init_gmv30, 0), 0)   as subsidy_rate30
        ,coalesce(qe60 / nullif(init_gmv60, 0), 0)   as subsidy_rate60
        ,coalesce(qe90 / nullif(init_gmv90, 0), 0)   as subsidy_rate90
        ,coalesce(qe180 / nullif(init_gmv180, 0), 0) as subsidy_rate180
        
        --- 单间夜补贴
        ,coalesce(qe0 / nullif(room_night0, 0), 0)     as subsidy_per_room_night0
        ,coalesce(qe1 / nullif(room_night1, 0), 0)     as subsidy_per_room_night1
        ,coalesce(qe2 / nullif(room_night2, 0), 0)     as subsidy_per_room_night2
        ,coalesce(qe3 / nullif(room_night3, 0), 0)     as subsidy_per_room_night3
        ,coalesce(qe4 / nullif(room_night4, 0), 0)     as subsidy_per_room_night4
        ,coalesce(qe5 / nullif(room_night5, 0), 0)     as subsidy_per_room_night5
        ,coalesce(qe6 / nullif(room_night6, 0), 0)     as subsidy_per_room_night6
        ,coalesce(qe7 / nullif(room_night7, 0), 0)     as subsidy_per_room_night7
        ,coalesce(qe30 / nullif(room_night30, 0), 0)   as subsidy_per_room_night30
        ,coalesce(qe60 / nullif(room_night60, 0), 0)   as subsidy_per_room_night60
        ,coalesce(qe90 / nullif(room_night90, 0), 0)   as subsidy_per_room_night90
        ,coalesce(qe180 / nullif(room_night180, 0), 0) as subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        -- 离店佣金
        ,ldyj0, ldyj1, ldyj2, ldyj3, ldyj4, ldyj5, ldyj6, ldyj7, ldyj30, ldyj60, ldyj90, ldyj180
        
        -- 动态分区字段放在最后
        ,t1.order_date as dt
from  (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,count(distinct t1.user_name) as uv 
        
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0   then final_commission_after end) as yj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1  then final_commission_after end) as yj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2  then final_commission_after end) as yj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3  then final_commission_after end) as yj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4  then final_commission_after end) as yj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5  then final_commission_after end) as yj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6  then final_commission_after end) as yj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7  then final_commission_after end) as yj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30   then final_commission_after end) as yj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60   then final_commission_after end) as yj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90   then final_commission_after end) as yj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180  then final_commission_after end) as yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then room_night end) as room_night0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then room_night end) as room_night1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then room_night end) as room_night2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then room_night end) as room_night3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then room_night end) as room_night4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then room_night end) as room_night5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then room_night end) as room_night6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) as room_night7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) as room_night30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then room_night end) as room_night60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then room_night end) as room_night90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) as room_night180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then init_gmv end) as init_gmv0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then init_gmv end) as init_gmv1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then init_gmv end) as init_gmv2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then init_gmv end) as init_gmv3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then init_gmv end) as init_gmv4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then init_gmv end) as init_gmv5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then init_gmv end) as init_gmv6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) as init_gmv7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) as init_gmv30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then init_gmv end) as init_gmv60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then init_gmv end) as init_gmv90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) as init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then coupon_substract_summary end) as qe0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then coupon_substract_summary end) as qe1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then coupon_substract_summary end) as qe2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then coupon_substract_summary end) as qe3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then coupon_substract_summary end) as qe4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then coupon_substract_summary end) as qe5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then coupon_substract_summary end) as qe6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then coupon_substract_summary end) as qe7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then coupon_substract_summary end) as qe30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then coupon_substract_summary end) as qe60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then coupon_substract_summary end) as qe90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then coupon_substract_summary end) as qe180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0    then t2.order_no end) as order_cnt0
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 1   then t2.order_no end) as order_cnt1
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 2   then t2.order_no end) as order_cnt2
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 3   then t2.order_no end) as order_cnt3
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 4   then t2.order_no end) as order_cnt4
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 5   then t2.order_no end) as order_cnt5
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 6   then t2.order_no end) as order_cnt6
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then t2.order_no end) as order_cnt7
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then t2.order_no end) as order_cnt30
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 60  then t2.order_no end) as order_cnt60
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 90  then t2.order_no end) as order_cnt90
        ,count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then t2.order_no end) as order_cnt180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) as ldyj0
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) as ldyj1
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) as ldyj2
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) as ldyj3
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) as ldyj4
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) as ldyj5
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) as ldyj6
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) as ldyj7
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 30  then ldyj end) as ldyj30
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 60  then ldyj end) as ldyj60
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 90  then ldyj end) as ldyj90
        ,sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) as ldyj180
    from order_info t1 
    left join q_order t2 on t1.user_name=t2.user_name and t2.order_date >= t1.order_date
    group by t1.order_date, cube(t1.user_type, t1.channel, t1.mdd)
) t1 
;


--- 2、C分新老x目的地ARPU数据定时任务调度脚本
-- 1. 开启动态分区配置
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

with c_user_type as(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by user_id, ubt_user_id
)
,c_order as (  --- c订单明细 (抓取近181天数据)
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                 when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                 when c.area in ('欧洲','亚太','美洲') then c.area
                 else '其他' end as mdd
            ,case when u.min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end as user_type
            ,o.user_id, o.order_no, o.room_fee, o.comission
            ,cast(extend_info['room_night'] as double) as room_night
            ,get_json_object(orig_discount_detail, '$.detail[1].amount') as cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join (
        select distinct order_no as order_no_oc
            ,orig_discount_detail
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da_patch
        where dt = date_sub(current_date, 1)
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            and terminal_channel_type = 'app'
            and order_status <> 'C'
            and substr(order_date,1,10) >= '${zdt.addDay(-181).format("yyyy-MM-dd")}' 
            and substr(order_date,1,10) <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    ) oc on o.order_no = oc.order_no_oc
    where o.dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and order_status <> 'C'
      and substr(order_date,1,10) >= '${zdt.addDay(-181).format("yyyy-MM-dd")}'
      and substr(order_date,1,10) <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
)
,order_info as (--- C侧大盘生单人群分新老分目的地
    select dt
        ,user_id
        ,user_type
        ,mdd
    from c_order
    group by dt, user_id, user_type, mdd
)

-- 2. 数据插入，动态分区 dt 放置于最后
INSERT OVERWRITE TABLE ihotel_default.ads_ihotel_c_user_ltv_di PARTITION (dt)

select 
         t1.user_type
        ,t1.mdd
        ,t1.uv
        
        --- ARPU数据
        ,coalesce(yj0 / nullif(uv, 0), 0)   as ARPU0
        ,coalesce(yj1 / nullif(uv, 0), 0)   as ARPU1
        ,coalesce(yj2 / nullif(uv, 0), 0)   as ARPU2
        ,coalesce(yj3 / nullif(uv, 0), 0)   as ARPU3
        ,coalesce(yj4 / nullif(uv, 0), 0)   as ARPU4
        ,coalesce(yj5 / nullif(uv, 0), 0)   as ARPU5
        ,coalesce(yj6 / nullif(uv, 0), 0)   as ARPU6
        ,coalesce(yj7 / nullif(uv, 0), 0)   as ARPU7
        ,coalesce(yj30 / nullif(uv, 0), 0)  as ARPU30
        ,coalesce(yj60 / nullif(uv, 0), 0)  as ARPU60
        ,coalesce(yj90 / nullif(uv, 0), 0)  as ARPU90
        ,coalesce(yj180 / nullif(uv, 0), 0) as ARPU180
        
        --- 单用户订单数据
        ,coalesce(order_cnt0 / nullif(uv, 0), 0)   as order_cnt_per_user0
        ,coalesce(order_cnt1 / nullif(uv, 0), 0)   as order_cnt_per_user1
        ,coalesce(order_cnt2 / nullif(uv, 0), 0)   as order_cnt_per_user2
        ,coalesce(order_cnt3 / nullif(uv, 0), 0)   as order_cnt_per_user3
        ,coalesce(order_cnt4 / nullif(uv, 0), 0)   as order_cnt_per_user4
        ,coalesce(order_cnt5 / nullif(uv, 0), 0)   as order_cnt_per_user5
        ,coalesce(order_cnt6 / nullif(uv, 0), 0)   as order_cnt_per_user6
        ,coalesce(order_cnt7 / nullif(uv, 0), 0)   as order_cnt_per_user7
        ,coalesce(order_cnt30 / nullif(uv, 0), 0)  as order_cnt_per_user30
        ,coalesce(order_cnt60 / nullif(uv, 0), 0)  as order_cnt_per_user60
        ,coalesce(order_cnt90 / nullif(uv, 0), 0)  as order_cnt_per_user90
        ,coalesce(order_cnt180 / nullif(uv, 0), 0) as order_cnt_per_user180
        
        --- 单订单间夜数
        ,coalesce(room_night0 / nullif(order_cnt0, 0), 0)     as room_night_per_order0
        ,coalesce(room_night1 / nullif(order_cnt1, 0), 0)     as room_night_per_order1
        ,coalesce(room_night2 / nullif(order_cnt2, 0), 0)     as room_night_per_order2
        ,coalesce(room_night3 / nullif(order_cnt3, 0), 0)     as room_night_per_order3
        ,coalesce(room_night4 / nullif(order_cnt4, 0), 0)     as room_night_per_order4
        ,coalesce(room_night5 / nullif(order_cnt5, 0), 0)     as room_night_per_order5
        ,coalesce(room_night6 / nullif(order_cnt6, 0), 0)     as room_night_per_order6
        ,coalesce(room_night7 / nullif(order_cnt7, 0), 0)     as room_night_per_order7
        ,coalesce(room_night30 / nullif(order_cnt30, 0), 0)   as room_night_per_order30
        ,coalesce(room_night60 / nullif(order_cnt60, 0), 0)   as room_night_per_order60
        ,coalesce(room_night90 / nullif(order_cnt90, 0), 0)   as room_night_per_order90
        ,coalesce(room_night180 / nullif(order_cnt180, 0), 0) as room_night_per_order180
        
        --- ADR
        ,coalesce(init_gmv0 / nullif(room_night0, 0), 0)     as adr0
        ,coalesce(init_gmv1 / nullif(room_night1, 0), 0)     as adr1
        ,coalesce(init_gmv2 / nullif(room_night2, 0), 0)     as adr2   
        ,coalesce(init_gmv3 / nullif(room_night3, 0), 0)     as adr3
        ,coalesce(init_gmv4 / nullif(room_night4, 0), 0)     as adr4
        ,coalesce(init_gmv5 / nullif(room_night5, 0), 0)     as adr5
        ,coalesce(init_gmv6 / nullif(room_night6, 0), 0)     as adr6
        ,coalesce(init_gmv7 / nullif(room_night7, 0), 0)     as adr7
        ,coalesce(init_gmv30 / nullif(room_night30, 0), 0)   as adr30
        ,coalesce(init_gmv60 / nullif(room_night60, 0), 0)   as adr60
        ,coalesce(init_gmv90 / nullif(room_night90, 0), 0)   as adr90
        ,coalesce(init_gmv180 / nullif(room_night180, 0), 0) as adr180
        
        --- 佣金率
        ,coalesce(yj0 / nullif(init_gmv0, 0), 0)     as commission_rate0
        ,coalesce(yj1 / nullif(init_gmv1, 0), 0)     as commission_rate1
        ,coalesce(yj2 / nullif(init_gmv2, 0), 0)     as commission_rate2
        ,coalesce(yj3 / nullif(init_gmv3, 0), 0)     as commission_rate3
        ,coalesce(yj4 / nullif(init_gmv4, 0), 0)     as commission_rate4
        ,coalesce(yj5 / nullif(init_gmv5, 0), 0)     as commission_rate5
        ,coalesce(yj6 / nullif(init_gmv6, 0), 0)     as commission_rate6
        ,coalesce(yj7 / nullif(init_gmv7, 0), 0)     as commission_rate7
        ,coalesce(yj30 / nullif(init_gmv30, 0), 0)   as commission_rate30
        ,coalesce(yj60 / nullif(init_gmv60, 0), 0)   as commission_rate60
        ,coalesce(yj90 / nullif(init_gmv90, 0), 0)   as commission_rate90
        ,coalesce(yj180 / nullif(init_gmv180, 0), 0) as commission_rate180
        
        --- 补贴率
        ,coalesce(qe0 / nullif(init_gmv0, 0), 0)     as subsidy_rate0
        ,coalesce(qe1 / nullif(init_gmv1, 0), 0)     as subsidy_rate1
        ,coalesce(qe2 / nullif(init_gmv2, 0), 0)     as subsidy_rate2
        ,coalesce(qe3 / nullif(init_gmv3, 0), 0)     as subsidy_rate3
        ,coalesce(qe4 / nullif(init_gmv4, 0), 0)     as subsidy_rate4
        ,coalesce(qe5 / nullif(init_gmv5, 0), 0)     as subsidy_rate5
        ,coalesce(qe6 / nullif(init_gmv6, 0), 0)     as subsidy_rate6
        ,coalesce(qe7 / nullif(init_gmv7, 0), 0)     as subsidy_rate7
        ,coalesce(qe30 / nullif(init_gmv30, 0), 0)   as subsidy_rate30
        ,coalesce(qe60 / nullif(init_gmv60, 0), 0)   as subsidy_rate60
        ,coalesce(qe90 / nullif(init_gmv90, 0), 0)   as subsidy_rate90
        ,coalesce(qe180 / nullif(init_gmv180, 0), 0) as subsidy_rate180
        
        --- 单间夜补贴
        ,coalesce(qe0 / nullif(room_night0, 0), 0)     as subsidy_per_room_night0
        ,coalesce(qe1 / nullif(room_night1, 0), 0)     as subsidy_per_room_night1
        ,coalesce(qe2 / nullif(room_night2, 0), 0)     as subsidy_per_room_night2
        ,coalesce(qe3 / nullif(room_night3, 0), 0)     as subsidy_per_room_night3
        ,coalesce(qe4 / nullif(room_night4, 0), 0)     as subsidy_per_room_night4
        ,coalesce(qe5 / nullif(room_night5, 0), 0)     as subsidy_per_room_night5
        ,coalesce(qe6 / nullif(room_night6, 0), 0)     as subsidy_per_room_night6
        ,coalesce(qe7 / nullif(room_night7, 0), 0)     as subsidy_per_room_night7
        ,coalesce(qe30 / nullif(room_night30, 0), 0)   as subsidy_per_room_night30
        ,coalesce(qe60 / nullif(room_night60, 0), 0)   as subsidy_per_room_night60
        ,coalesce(qe90 / nullif(room_night90, 0), 0)   as subsidy_per_room_night90
        ,coalesce(qe180 / nullif(room_night180, 0), 0) as subsidy_per_room_night180

        ,yj0, yj1, yj2, yj3, yj4, yj5, yj6, yj7, yj30, yj60, yj90, yj180
        ,order_cnt0, order_cnt1, order_cnt2, order_cnt3, order_cnt4, order_cnt5, order_cnt6, order_cnt7, order_cnt30, order_cnt60, order_cnt90, order_cnt180
        ,room_night0, room_night1, room_night2, room_night3, room_night4, room_night5, room_night6, room_night7, room_night30, room_night60, room_night90, room_night180
        ,init_gmv0, init_gmv1, init_gmv2, init_gmv3, init_gmv4, init_gmv5, init_gmv6, init_gmv7, init_gmv30, init_gmv60, init_gmv90, init_gmv180
        ,qe0, qe1, qe2, qe3, qe4, qe5, qe6, qe7, qe30, qe60, qe90, qe180
        
        -- 分区日期字段
        ,t1.dt as dt
from  (
    select t1.dt
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,count(distinct t1.user_id) as uv
        
        ,sum(case when datediff(t2.dt, t1.dt) = 0     then comission end) as yj0
        ,sum(case when datediff(t2.dt, t1.dt) <= 1    then comission end) as yj1
        ,sum(case when datediff(t2.dt, t1.dt) <= 2    then comission end) as yj2
        ,sum(case when datediff(t2.dt, t1.dt) <= 3    then comission end) as yj3
        ,sum(case when datediff(t2.dt, t1.dt) <= 4    then comission end) as yj4
        ,sum(case when datediff(t2.dt, t1.dt) <= 5    then comission end) as yj5
        ,sum(case when datediff(t2.dt, t1.dt) <= 6    then comission end) as yj6
        ,sum(case when datediff(t2.dt, t1.dt) <= 7    then comission end) as yj7
        ,sum(case when datediff(t2.dt, t1.dt) <= 30   then comission end) as yj30
        ,sum(case when datediff(t2.dt, t1.dt) <= 60   then comission end) as yj60
        ,sum(case when datediff(t2.dt, t1.dt) <= 90   then comission end) as yj90
        ,sum(case when datediff(t2.dt, t1.dt) <= 180  then comission end) as yj180

        ,sum(case when datediff(t2.dt, t1.dt) = 0    then room_night end) as room_night0
        ,sum(case when datediff(t2.dt, t1.dt) <= 1   then room_night end) as room_night1
        ,sum(case when datediff(t2.dt, t1.dt) <= 2   then room_night end) as room_night2
        ,sum(case when datediff(t2.dt, t1.dt) <= 3   then room_night end) as room_night3
        ,sum(case when datediff(t2.dt, t1.dt) <= 4   then room_night end) as room_night4
        ,sum(case when datediff(t2.dt, t1.dt) <= 5   then room_night end) as room_night5
        ,sum(case when datediff(t2.dt, t1.dt) <= 6   then room_night end) as room_night6
        ,sum(case when datediff(t2.dt, t1.dt) <= 7   then room_night end) as room_night7
        ,sum(case when datediff(t2.dt, t1.dt) <= 30  then room_night end) as room_night30
        ,sum(case when datediff(t2.dt, t1.dt) <= 60  then room_night end) as room_night60
        ,sum(case when datediff(t2.dt, t1.dt) <= 90  then room_night end) as room_night90
        ,sum(case when datediff(t2.dt, t1.dt) <= 180 then room_night end) as room_night180

        ,sum(case when datediff(t2.dt, t1.dt) = 0    then room_fee end) as init_gmv0
        ,sum(case when datediff(t2.dt, t1.dt) <= 1   then room_fee end) as init_gmv1
        ,sum(case when datediff(t2.dt, t1.dt) <= 2   then room_fee end) as init_gmv2
        ,sum(case when datediff(t2.dt, t1.dt) <= 3   then room_fee end) as init_gmv3
        ,sum(case when datediff(t2.dt, t1.dt) <= 4   then room_fee end) as init_gmv4
        ,sum(case when datediff(t2.dt, t1.dt) <= 5   then room_fee end) as init_gmv5
        ,sum(case when datediff(t2.dt, t1.dt) <= 6   then room_fee end) as init_gmv6
        ,sum(case when datediff(t2.dt, t1.dt) <= 7   then room_fee end) as init_gmv7
        ,sum(case when datediff(t2.dt, t1.dt) <= 30  then room_fee end) as init_gmv30
        ,sum(case when datediff(t2.dt, t1.dt) <= 60  then room_fee end) as init_gmv60
        ,sum(case when datediff(t2.dt, t1.dt) <= 90  then room_fee end) as init_gmv90
        ,sum(case when datediff(t2.dt, t1.dt) <= 180 then room_fee end) as init_gmv180

        ,sum(case when datediff(t2.dt, t1.dt) = 0    then cqe end) as qe0
        ,sum(case when datediff(t2.dt, t1.dt) <= 1   then cqe end) as qe1
        ,sum(case when datediff(t2.dt, t1.dt) <= 2   then cqe end) as qe2
        ,sum(case when datediff(t2.dt, t1.dt) <= 3   then cqe end) as qe3
        ,sum(case when datediff(t2.dt, t1.dt) <= 4   then cqe end) as qe4
        ,sum(case when datediff(t2.dt, t1.dt) <= 5   then cqe end) as qe5
        ,sum(case when datediff(t2.dt, t1.dt) <= 6   then cqe end) as qe6
        ,sum(case when datediff(t2.dt, t1.dt) <= 7   then cqe end) as qe7
        ,sum(case when datediff(t2.dt, t1.dt) <= 30  then cqe end) as qe30
        ,sum(case when datediff(t2.dt, t1.dt) <= 60  then cqe end) as qe60
        ,sum(case when datediff(t2.dt, t1.dt) <= 90  then cqe end) as qe90
        ,sum(case when datediff(t2.dt, t1.dt) <= 180 then cqe end) as qe180

        ,count(distinct case when datediff(t2.dt, t1.dt) = 0    then t2.order_no end) as order_cnt0
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 1   then t2.order_no end) as order_cnt1
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 2   then t2.order_no end) as order_cnt2
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 3   then t2.order_no end) as order_cnt3
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 4   then t2.order_no end) as order_cnt4
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 5   then t2.order_no end) as order_cnt5
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 6   then t2.order_no end) as order_cnt6
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 7   then t2.order_no end) as order_cnt7
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 30  then t2.order_no end) as order_cnt30
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 60  then t2.order_no end) as order_cnt60
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 90  then t2.order_no end) as order_cnt90
        ,count(distinct case when datediff(t2.dt, t1.dt) <= 180 then t2.order_no end) as order_cnt180
    from order_info t1 
    left join c_order t2 on t1.user_id = t2.user_id and t2.dt >= t1.dt
    group by t1.dt, cube(t1.user_type, t1.mdd)
) t1 
;



---- 数据集sql
with monthly_base as (
    --- 第一步：按月汇总所有的底层绝对值（用于计算真实的月度加权比率）
    select substr(dt, 1, 7) as mth
          ,mdd
          ,user_type
          ,channel
          ,max(dt) as max_dt  -- 提取该月内最晚的一天，作为计算成熟度的“木桶短板”
          ,sum(uv) as uv
          
          -- 聚合 0~7, 30, 90, 180 的基础佣金
          ,sum(yj0) as yj0, sum(yj1) as yj1, sum(yj2) as yj2, sum(yj3) as yj3, sum(yj4) as yj4, sum(yj5) as yj5, sum(yj6) as yj6, sum(yj7) as yj7, sum(yj30) as yj30, sum(yj90) as yj90, sum(yj180) as yj180
          -- 聚合基础订单量
          ,sum(order_cnt0) as order_cnt0, sum(order_cnt1) as order_cnt1, sum(order_cnt2) as order_cnt2, sum(order_cnt3) as order_cnt3, sum(order_cnt4) as order_cnt4, sum(order_cnt5) as order_cnt5, sum(order_cnt6) as order_cnt6, sum(order_cnt7) as order_cnt7, sum(order_cnt30) as order_cnt30, sum(order_cnt90) as order_cnt90, sum(order_cnt180) as order_cnt180
          -- 聚合基础间夜量
          ,sum(room_night0) as room_night0, sum(room_night1) as room_night1, sum(room_night2) as room_night2, sum(room_night3) as room_night3, sum(room_night4) as room_night4, sum(room_night5) as room_night5, sum(room_night6) as room_night6, sum(room_night7) as room_night7, sum(room_night30) as room_night30, sum(room_night90) as room_night90, sum(room_night180) as room_night180
          -- 聚合基础GMV
          ,sum(init_gmv0) as init_gmv0, sum(init_gmv1) as init_gmv1, sum(init_gmv2) as init_gmv2, sum(init_gmv3) as init_gmv3, sum(init_gmv4) as init_gmv4, sum(init_gmv5) as init_gmv5, sum(init_gmv6) as init_gmv6, sum(init_gmv7) as init_gmv7, sum(init_gmv30) as init_gmv30, sum(init_gmv90) as init_gmv90, sum(init_gmv180) as init_gmv180
    from ihotel_default.ads_ihotel_user_ltv_di
    group by substr(dt, 1, 7), mdd, user_type, channel
)

select mth as "月份"
      ,mdd as "目的地"
      ,user_type as "新老客"
      ,channel as "渠道"
      ,uv as "下单用户数"

      --- 1. ARPU (当月总佣金 / 当月总UV)
      ,case when datediff(current_date, max_dt) <= 0   then null else yj0 / nullif(uv, 0) end as "ARPU_0日"
      ,case when datediff(current_date, max_dt) <= 1   then null else yj1 / nullif(uv, 0) end as "ARPU_1日"
      ,case when datediff(current_date, max_dt) <= 2   then null else yj2 / nullif(uv, 0) end as "ARPU_2日"
      ,case when datediff(current_date, max_dt) <= 3   then null else yj3 / nullif(uv, 0) end as "ARPU_3日"
      ,case when datediff(current_date, max_dt) <= 4   then null else yj4 / nullif(uv, 0) end as "ARPU_4日"
      ,case when datediff(current_date, max_dt) <= 5   then null else yj5 / nullif(uv, 0) end as "ARPU_5日"
      ,case when datediff(current_date, max_dt) <= 6   then null else yj6 / nullif(uv, 0) end as "ARPU_6日"
      ,case when datediff(current_date, max_dt) <= 7   then null else yj7 / nullif(uv, 0) end as "ARPU_7日"
      ,case when datediff(current_date, max_dt) <= 30  then null else yj30 / nullif(uv, 0) end as "ARPU_30日"
      ,case when datediff(current_date, max_dt) <= 90  then null else yj90 / nullif(uv, 0) end as "ARPU_90日"
      ,case when datediff(current_date, max_dt) <= 180 then null else yj180 / nullif(uv, 0) end as "ARPU_180日"

      --- 2. 单用户订单数 (当月总单量 / 当月总UV)
      ,case when datediff(current_date, max_dt) <= 0   then null else order_cnt0 / nullif(uv, 0) end as "单用户订单_0日"
      ,case when datediff(current_date, max_dt) <= 1   then null else order_cnt1 / nullif(uv, 0) end as "单用户订单_1日"
      ,case when datediff(current_date, max_dt) <= 2   then null else order_cnt2 / nullif(uv, 0) end as "单用户订单_2日"
      ,case when datediff(current_date, max_dt) <= 3   then null else order_cnt3 / nullif(uv, 0) end as "单用户订单_3日"
      ,case when datediff(current_date, max_dt) <= 4   then null else order_cnt4 / nullif(uv, 0) end as "单用户订单_4日"
      ,case when datediff(current_date, max_dt) <= 5   then null else order_cnt5 / nullif(uv, 0) end as "单用户订单_5日"
      ,case when datediff(current_date, max_dt) <= 6   then null else order_cnt6 / nullif(uv, 0) end as "单用户订单_6日"
      ,case when datediff(current_date, max_dt) <= 7   then null else order_cnt7 / nullif(uv, 0) end as "单用户订单_7日"
      ,case when datediff(current_date, max_dt) <= 30  then null else order_cnt30 / nullif(uv, 0) end as "单用户订单_30日"
      ,case when datediff(current_date, max_dt) <= 90  then null else order_cnt90 / nullif(uv, 0) end as "单用户订单_90日"
      ,case when datediff(current_date, max_dt) <= 180 then null else order_cnt180 / nullif(uv, 0) end as "单用户订单_180日"

      --- 3. 单订单间夜数 (当月总间夜 / 当月总单量)
      ,case when datediff(current_date, max_dt) <= 0   then null else room_night0 / nullif(order_cnt0, 0) end as "单订单间夜_0日"
      ,case when datediff(current_date, max_dt) <= 1   then null else room_night1 / nullif(order_cnt1, 0) end as "单订单间夜_1日"
      ,case when datediff(current_date, max_dt) <= 2   then null else room_night2 / nullif(order_cnt2, 0) end as "单订单间夜_2日"
      ,case when datediff(current_date, max_dt) <= 3   then null else room_night3 / nullif(order_cnt3, 0) end as "单订单间夜_3日"
      ,case when datediff(current_date, max_dt) <= 4   then null else room_night4 / nullif(order_cnt4, 0) end as "单订单间夜_4日"
      ,case when datediff(current_date, max_dt) <= 5   then null else room_night5 / nullif(order_cnt5, 0) end as "单订单间夜_5日"
      ,case when datediff(current_date, max_dt) <= 6   then null else room_night6 / nullif(order_cnt6, 0) end as "单订单间夜_6日"
      ,case when datediff(current_date, max_dt) <= 7   then null else room_night7 / nullif(order_cnt7, 0) end as "单订单间夜_7日"
      ,case when datediff(current_date, max_dt) <= 30  then null else room_night30 / nullif(order_cnt30, 0) end as "单订单间夜_30日"
      ,case when datediff(current_date, max_dt) <= 90  then null else room_night90 / nullif(order_cnt90, 0) end as "单订单间夜_90日"
      ,case when datediff(current_date, max_dt) <= 180 then null else room_night180 / nullif(order_cnt180, 0) end as "单订单间夜_180日"

      --- 4. ADR (当月总GMV / 当月总间夜)
      ,case when datediff(current_date, max_dt) <= 0   then null else init_gmv0 / nullif(room_night0, 0) end as "ADR_0日"
      ,case when datediff(current_date, max_dt) <= 1   then null else init_gmv1 / nullif(room_night1, 0) end as "ADR_1日"
      ,case when datediff(current_date, max_dt) <= 2   then null else init_gmv2 / nullif(room_night2, 0) end as "ADR_2日"
      ,case when datediff(current_date, max_dt) <= 3   then null else init_gmv3 / nullif(room_night3, 0) end as "ADR_3日"
      ,case when datediff(current_date, max_dt) <= 4   then null else init_gmv4 / nullif(room_night4, 0) end as "ADR_4日"
      ,case when datediff(current_date, max_dt) <= 5   then null else init_gmv5 / nullif(room_night5, 0) end as "ADR_5日"
      ,case when datediff(current_date, max_dt) <= 6   then null else init_gmv6 / nullif(room_night6, 0) end as "ADR_6日"
      ,case when datediff(current_date, max_dt) <= 7   then null else init_gmv7 / nullif(room_night7, 0) end as "ADR_7日"
      ,case when datediff(current_date, max_dt) <= 30  then null else init_gmv30 / nullif(room_night30, 0) end as "ADR_30日"
      ,case when datediff(current_date, max_dt) <= 90  then null else init_gmv90 / nullif(room_night90, 0) end as "ADR_90日"
      ,case when datediff(current_date, max_dt) <= 180 then null else init_gmv180 / nullif(room_night180, 0) end as "ADR_180日"

      --- 5. 佣金率 (当月总佣金 / 当月总GMV)
      ,case when datediff(current_date, max_dt) <= 0   then null else yj0 / nullif(init_gmv0, 0) end as "佣金率_0日"
      ,case when datediff(current_date, max_dt) <= 1   then null else yj1 / nullif(init_gmv1, 0) end as "佣金率_1日"
      ,case when datediff(current_date, max_dt) <= 2   then null else yj2 / nullif(init_gmv2, 0) end as "佣金率_2日"
      ,case when datediff(current_date, max_dt) <= 3   then null else yj3 / nullif(init_gmv3, 0) end as "佣金率_3日"
      ,case when datediff(current_date, max_dt) <= 4   then null else yj4 / nullif(init_gmv4, 0) end as "佣金率_4日"
      ,case when datediff(current_date, max_dt) <= 5   then null else yj5 / nullif(init_gmv5, 0) end as "佣金率_5日"
      ,case when datediff(current_date, max_dt) <= 6   then null else yj6 / nullif(init_gmv6, 0) end as "佣金率_6日"
      ,case when datediff(current_date, max_dt) <= 7   then null else yj7 / nullif(init_gmv7, 0) end as "佣金率_7日"
      ,case when datediff(current_date, max_dt) <= 30  then null else yj30 / nullif(init_gmv30, 0) end as "佣金率_30日"
      ,case when datediff(current_date, max_dt) <= 90  then null else yj90 / nullif(init_gmv90, 0) end as "佣金率_90日"
      ,case when datediff(current_date, max_dt) <= 180 then null else yj180 / nullif(init_gmv180, 0) end as "佣金率_180日"

from monthly_base
order by "月份"  
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when user_type = '老客' then 3 else 4 end asc
      ,case when mdd = 'ALL' then 0 else 1 end asc
      ,mdd asc
      ,case when channel = 'ALL' then 0 else 1 end asc
      ,channel asc
;