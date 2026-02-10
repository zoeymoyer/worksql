with uv as ----分日去重活跃用户   去除台湾
(
    select  dt as dates
            ,case when province_name in ('澳门','香港') then province_name else city_name end as city_name
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     where dt >= date_sub(current_date, 3)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (----订单明细表包含取消  最近3天去除台湾的订单明细
    select  distinct a.user_id
            ,a.user_name
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,3) and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
)
,user_jc as --机票
(  --- 订单到达省份不是台湾且订单到达国家是中国的使用订单出发城市，取最新订单到达时间
    select create_date,o_qunarusername,o_arrcityname
    from (
        select  to_date(create_time) as create_date
                ,o_qunarusername
                ,case  when o_arrcountryname = '中国' then o_depcityname else  o_arrcityname end o_arrcityname
                ,row_number() over(partition by o_qunarusername order by create_time desc) rn
        from f_fuwu.dw_fact_inter_order_wide
        where dt >= date_sub(current_date, 3) and dt <= date_sub(current_date,1)
            and substr(create_time, 1, 10) >= '2025-01-01'  -- 生单时间
            and ticket_time is not null      -- 出票完成时间
            and refund_complete_time is null -- 已出票未退款
            and platform <> 'fenxiao'        -- 去分销
            and (s_arrcountryname != '中国' or s_depcountryname != '中国')
            and o_arrprovincename != '台湾'  -- 去台湾
    )a where rn = 1 and o_arrcityname != 'unknown'
)
,user_view as (  --- 近3天浏览国酒未下单人群，按照时间降序取末次城市
    select dates,city_name,user_name ,1 num
    from (
        select t1.dates
                ,t1.city_name
                ,t1.user_name
                ,count(1) pv
                ,row_number() over(partition by t1.user_name order by t1.dates desc) rn
        from uv t1 
        left join q_order t2 on t1.user_name=t2.user_name
        where t2.user_name is null
        group by 1,2,3
    )a where rn = 1
)
,user_jo as (  --- 近3天机票订单用户在国酒未下单人群
    select create_date,o_arrcityname,o_qunarusername,0 num
    from user_jc t1 
    left join  q_order t2 on t1.o_qunarusername=t2.user_name
    where t2.user_name is null
) 
,notification_switch as (
    select user_name as key
           ,notification_switch as value 
    from pp_pub.dim_touch_username_switch_da  
    where dt = date_sub(current_date,1)
    and user_name is not null and user_name not in('',' ')
    and  notification_switch = '1' 
    group by 1,2
)


select user_name,city_name 
from (
        select user_name
            ,city_name
        from (
            select user_name
                    ,city_name
                    ,num
                    ,row_number() over(partition by user_name order by num) cnt
            from (
                select * 
                from user_view
                union all 
                select * 
                from user_jo
            )a
        )a where cnt = 1
) t1 left join notification_switch t2 on t1.user_name=t2.key
where t2.key is not null   --- 有push
;