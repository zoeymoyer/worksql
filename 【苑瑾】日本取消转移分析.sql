WITH base AS (--- 日本订单
    SELECT
        order_date,
        order_no,
        order_status,
        first_cancelled_time,
        CASE
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                AND substr(first_cancelled_time, 1, 10) >= '2025-11-14'
                THEN '11.14之后'
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                AND substr(first_cancelled_time, 1, 10) < '2025-11-14'
                THEN '11.14之前'
            ELSE '未取消'
            END AS date_group
        ,user_id
    FROM mdw_order_v3_international a   -- 海外订单表
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name != '中国')
      AND terminal_channel_type IN ('www','app','touch')
      AND is_valid = '1'
      AND order_date >= '2025-10-18'     -- 近一个月预订
      AND order_date <= date_sub(current_date, 1)
      AND order_no <> '103576132435'
      AND a.country_name = '日本'
)
,q_order as (
    SELECT
        order_date,
        order_no,
        order_status,
        first_cancelled_time,
        CASE
            WHEN order_status IN ('CANCELLED', 'REJECTED')
                THEN '取消'
            ELSE '未取消'
            END AS date_group
        ,user_id
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
    FROM mdw_order_v3_international a   -- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    WHERE dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name != '中国')
      AND terminal_channel_type IN ('www','app','touch')
      AND is_valid = '1'
      AND order_date >= '2025-10-18'     -- 近一个月预订
      AND order_date <= date_sub(current_date, 1)
      AND order_no <> '103576132435'
)

select * from (
    select t1.date_group
        ,count(distinct t1.user_id) "取消订单UV"
        ,count(distinct t2.user_id) "取消订单用户再次下单UV"
        ,count(distinct case when t2.order_status not in ('CANCELLED', 'REJECTED') then t2.user_id end) "取消订单用户再次下单且未取消订单UV"
    from (  ---- 日本取消单用户
        select order_date
            ,user_id
            ,order_no
            ,date_group
        from base
        group by 1,2,3,4
    ) t1 
    left join q_order t2 on t1.user_id = t2.user_id  and t2.order_date > t1.order_date
    group by 1
) t1 left join (
    select t1.date_group,mdd
        ,count(distinct t1.user_id) "取消订单UV"
        ,count(distinct t2.user_id) "取消订单用户再次下单UV"
        ,count(distinct case when t2.order_status not in ('CANCELLED', 'REJECTED') then t2.user_id end) "取消订单用户再次下单且未取消订单UV"
    from (  ---- 日本取消单用户
        select order_date
            ,user_id
            ,order_no
            ,date_group
        from base
        group by 1,2,3,4
    ) t1 
    left join q_order t2 on t1.user_id = t2.user_id  and t2.order_date > t1.order_date
    group by 1,2
)t2 on t1.date_group=t2.date_group

;


--- 日本T0取消率
with user_type as (-----新老客
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

,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,count(distinct order_no) order_no_q
            ,count(distinct case when (first_cancelled_time is null or date(first_cancelled_time) > order_date)  
                          and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                          and (refund_time is null or date(refund_time) > order_date)
                    then order_no end) no_t0_cancel_order_no_q
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01'  and order_date <= date_sub(current_date, 1)
        and a.country_name = '日本'
        and order_no <> '103576132435'
    group by 1
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
        --    ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
        --        when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
        --        when c.area in ('欧洲','亚太','美洲') then c.area
        --        else '其他' end as mdd
        --     ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,count(distinct order_no) order_no_c
            ,count(distinct case when (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
                    then order_no end) no_t0_cancel_order_no_c
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and substr(order_date,1,10) >= '2025-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
      and extend_info['COUNTRY'] = '日本'
    group by 1
)


select t1.order_date
      ,order_no_q
      ,no_t0_cancel_order_no_q
      ,1 - no_t0_cancel_order_no_q / order_no_q  cancel_rate_q
      ,order_no_c
      ,no_t0_cancel_order_no_c
      ,1 - no_t0_cancel_order_no_c / order_no_c cancel_rate_c
from q_app_order t1
left join c_order t2
on t1.order_date=t2.dt
order by 1
;

--- 3 机票维度
select t1.dt,"取消订单UV-all","再次下单UV-all","目的地","取消订单UV","再次下单UV"
from (
    select date_group
          ,count(distinct t1.o_qunarusername) "取消订单UV-all"
          ,count(distinct t2.o_qunarusername) "再次下单UV-all"
    from (
        select to_date(create_time)    as create_date
                ,o_qunarusername
                ,CASE
                    WHEN substr(refund_complete_time, 1, 10) >= '2025-11-14'
                        THEN '11.14之后'
                    WHEN substr(refund_complete_time, 1, 10) < '2025-11-14'
                        THEN '11.14之前'
                    ELSE '其他'
                END AS date_group
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2025-10-18'   and dt <= date_sub(current_date, 1)
            --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
            and refund_complete_time is not null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
            and s_arrcountryname = '日本'
    )t1 
    left join (
        select to_date(create_time)    as create_date
            ,s_arrcountryname
            ,o_qunarusername
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2025-10-18'  and dt <= date_sub(current_date, 1)
            --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) t2 on t1.o_qunarusername=t2.o_qunarusername and t2.create_date > t1.create_date
    group by 1
)t1
left join (
    select date_group
          ,mdd "目的地"
          ,count(distinct t1.o_qunarusername) "取消订单UV"
          ,count(distinct t2.o_qunarusername) "再次下单UV"
    from (
        select to_date(create_time)    as create_date
                ,o_qunarusername
                ,CASE
                    WHEN substr(refund_complete_time, 1, 10) >= '2025-11-14'
                        THEN '11.14之后'
                    WHEN substr(refund_complete_time, 1, 10) < '2025-11-14'
                        THEN '11.14之前'
                    ELSE '其他'
                END AS date_group
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= '2025-10-18'   and dt <= date_sub(current_date, 1)
            --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
            and refund_complete_time is not null 
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
            and s_arrcountryname = '日本'
    )t1 
    left join (
        select to_date(create_time)    as create_date
            ,s_arrcountryname
            ,o_qunarusername
            ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
            when s_arrcityname in ('香港','澳门') then s_arrcityname
            when e.area in ('欧洲','亚太','美洲') then e.area
            else '其他' end  as mdd
        from f_fuwu.dw_fact_inter_order_wide a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.s_arrcountryname = e.country_name 
        where dt >= '2025-10-18'  and dt <= date_sub(current_date, 1)
            --and substr(create_time, 1, 10) >= '2025-08-01'  -- 生单时间
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    ) t2 on t1.o_qunarusername=t2.o_qunarusername and t2.create_date > t1.create_date
    group by 1,2
) t2  on t1.dt=t2.dt
;
