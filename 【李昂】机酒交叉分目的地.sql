-- T15交叉率出境+目的地-new
with inter_flight as (
    select  a1.dt
            ,a1.user_name
            ,a1.s_arrcountryname
            ,a1.flight_type
            ,a1.min_pay_time
    from (
        select distinct
                t1.dt
                ,t1.user_name
                ,t1.s_arrcountryname
                ,t1.flight_type
                ,t1.min_pay_time
        from (
            select  substr(create_time, 1, 10) as dt
                    ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
                          when s_arrcityname in ('香港','澳门') then s_arrcityname
                          when e.area in ('欧洲','亚太','美洲') then e.area
                          else '其他'
                     end as s_arrcountryname
                    ,case when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内'
                            and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '1-出境'
                          when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                            and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '2-海外飞海外'
                          when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                            and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内' then '3-入境'
                          else '5-其他'
                     end as flight_type
                    ,o.o_qunarusername as user_name
                    ,flight_type_detail
                    ,biz_order_no
                    ,min(substr(create_time, 1, 10)) as min_pay_time
            from f_fuwu.dw_fact_inter_order_wide o
            -- left join user_type b on o.o_qunarusername = b.user_name
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
                on o.s_arrcountryname = e.country_name
            where dt >= '2026-01-01'
                and substr(create_time, 1, 10) >= '2026-01-01'
                and substr(create_time, 1, 10) <= date_sub(current_date, 16) -- 当天及往前推15天内的机票用户T-14~T
                and ticket_time is not null
                and refund_complete_time is null -- 已出票未退款
                and platform <> 'fenxiao' -- 去分销
                and (s_arrcountryname != '中国' or s_depcountryname != '中国')
            group by 1,2,3,4
        ) t1
    ) a1
)
,inter_hotel as (
    select  order_date as dt
            ,order_no
            ,user_name
            ,room_night as order_quantity
            ,order_time
            ,case when a.province_name in ('澳门','香港') then a.province_name
                  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                  when e.area in ('欧洲','亚太','美洲') then e.area
                  else '其他'
             end as mdd
            ,sum(
                case when batch_series like '%23base_ZK_728810%'
                        or batch_series like '%23extra_ZK_ce6f99%'
                     then final_commission_after
                        + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0)
                        + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0)
                     else final_commission_after
                end
             ) as actual_amount
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
        on a.country_name = e.country_name
    where dt = '%(DATE)s'
        and (a.province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        -- and terminal_channel_type in ('app')
        and (terminal_channel_type in ('app','www','touch') or user_tracking_data['inner_channel'] = 'smart_app')
        and is_valid = '1'
        and order_status not in ('CANCELLED','REJECTED')
        and substr(order_time, 1, 10) between '2026-01-01' and date_sub(current_date, 1)
        and trim(user_name) != ''
    group by 1,2,3,4,5,6
)
,active_data as (
    select  dt
            ,user_id
            ,user_name
            ,case when a.province_name in ('澳门','香港') then a.province_name
                  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                  when e.area in ('欧洲','亚太','美洲') then e.area
                  else '其他'
             end as mdd
            ,max(dt) as max_action_time
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt between '2026-01-01' and date_sub(current_date, 1)
        and business_type = 'hotel'
        and (a.province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
    group by 1,2,3,4
)

select distinct
        t1.dt as `日期`
        ,t1.s_arrcountryname
        ,t1.flight_type
        ,t1.user_type as `用户类型`
        ,t1.total_user_cnt as `国际机票下单用户(境外目的地)`
        ,t2.active_user_cnt as `T15国际酒店浏览用户`
        ,t1.cross_user_cnt as `T15国际酒店下单用户`
        ,t1.cross_order_cnt as `T15国际酒店订单量`
        ,t1.cross_order_quantity as `T15国际酒店间夜量`
        ,t1.cross_income as `T15国际酒店收益`
from (
    select  a.dt
            ,'整体' as user_type
            ,a.s_arrcountryname
            ,a.flight_type
            ,count(distinct a.user_name) as total_user_cnt
            ,count(distinct case when b.dt between a.dt and date_add(a.dt, 15) and b.order_time >= a.min_pay_time then b.user_name end) as cross_user_cnt
            ,count(distinct case when b.dt between a.dt and date_add(a.dt, 15) and b.order_time >= a.min_pay_time then b.order_no end) as cross_order_cnt
            ,sum(case when b.dt between a.dt and date_add(a.dt, 15) and b.order_time >= a.min_pay_time then b.order_quantity end) as cross_order_quantity
            ,sum(case when b.dt between a.dt and date_add(a.dt, 15) and b.order_time >= a.min_pay_time then b.actual_amount end) as cross_income
    from inter_flight a
    left join inter_hotel b
        on a.user_name = b.user_name
    where a.flight_type = '1-出境'
    group by 1,2,3,4
) t1
left join (
    select  a.dt
            ,a.s_arrcountryname
            ,a.flight_type
            ,'整体' as user_type
            ,count(distinct case when c.dt between a.dt and date_add(a.dt, 15) and c.max_action_time >= a.min_pay_time then c.user_name end) as active_user_cnt
    from inter_flight a
    left join active_data c
        on a.user_name = c.user_name
    where a.flight_type = '1-出境'
    group by 1,2,3,4
) t2
    on t1.dt = t2.dt
    and t1.user_type = t2.user_type
    and t1.s_arrcountryname = t2.s_arrcountryname
    and t1.flight_type = t2.flight_type
order by `日期` desc
;


--- 1、机票分目的地订单
select dt,s_arrcountryname,flight_type,flight_type_detail,order_count
        ,sum(order_count) over(partition by dt,flight_type) as total_order_count
from (
    select dt,s_arrcountryname,flight_type,flight_type_detail
            ,count(distinct biz_order_no) as order_count
    from (
        select  substr(create_time, 1, 10) as dt
                ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
                        when s_arrcityname in ('香港','澳门') then s_arrcityname
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他'
                    end as s_arrcountryname
                ,case when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内'
                        and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '1-出境'
                        when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                        and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '2-海外飞海外'
                        when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                        and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内' then '3-入境'
                        else '5-其他'
                    end as flight_type
                ,o.o_qunarusername as user_name
                ,flight_type_detail
                ,biz_order_no
        from f_fuwu.dw_fact_inter_order_wide o
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on o.s_arrcountryname = e.country_name
        where dt >= '2026-01-01'
            and substr(create_time, 1, 10) >= '2026-01-01'
            and substr(create_time, 1, 10) <= date_sub(current_date, 1) -- 当天及往前推15天内的机票用户T-14~T
            and ticket_time is not null
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao' -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
        group by 1,2,3,4,5,6
    ) t1
    group by 1,2,3,4
) t
order by 1 desc,4 desc
;


--- 2、国酒分目的地流量和订单占比
with uv as (----分日去重活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night         
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

select t1.dt,t1.mdd,t1.uv,t2.order_count,sum(t1.uv) over(partition by t1.dt) uv_sum,sum(t2.order_count) over(partition by t1.dt) order_count_sum
from (
    select dt,mdd,count(user_name) uv
    from uv
    group by 1,2 
)t1 
left join (
    select order_date
            ,mdd
            ,count(distinct order_no) order_count
    from q_order_app
    group by 1,2
) t2 on t1.dt = t2.order_date and t1.mdd = t2.mdd
order by 1 desc,3 desc
;