with ouser_info as (  --- 24.02.15~24.07.15之间有过订单的用户 1023675 人
    select user_id,country_name,order_no as order_id
    from (
        select user_id,country_name,order_no,row_number() over(partition by user_id order by order_time desc) rn
        from mdw_order_v3_international   --- 海外订单表
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and order_status not in ('CANCELLED', 'REJECTED')
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid = '1'
            and order_date >= '2024-02-15' and order_date <= '2024-07-15'
            and order_no <> '103576132435'
    ) where rn = 1
)
,q_order as (----22.07.15~24.07.15 所有用户的总GMV和总订单量
    select a.user_id
           ,sum(init_gmv) gmv
           ,count(distinct order_no) order_no
           ,max(order_date) last_order_date
    from mdw_order_v3_international a 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2022-07-15' and order_date <= '2024-07-15'
        and order_no <> '103576132435'
    group by 1
)
,users_data as (
    select t1.user_id
        ,gmv
        ,order_no
        ,last_order_date
        ,country_name,order_id 
        ,1 keys
    from ouser_info t1 
    left join q_order t2 on t1.user_id=t2.user_id
    where t2.user_id is not null
)
,q8_data as (--- 计算gmv和订单量的80分位数，分别为8282.578125, 5.0
    select PERCENTILE_APPROX(gmv,0.8) q8_gmv
            ,PERCENTILE_APPROX(order_no,0.8) q8_order_no
            ,1 keys
    from users_data
)
,user_info as ( --- 高LTV和高复购标签用户
    select t1.user_id,gmv,order_no,last_order_date,q8_gmv,q8_order_no,country_name,order_id 
        ,case when  gmv >= q8_gmv and order_no >= q8_order_no  then '双高'
                when  gmv >= q8_gmv   then '高LTV'
                when  order_no >= q8_order_no  then '高复购'
                else '双非' end user_type
    from users_data t1 
    left join q8_data t2 on t1.keys=t2.keys
)
,uv as ---- 流量
(
    select  dt 
            ,a.user_id
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= '2024-02-15'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2
)
,order_all as ---- 订单
(
    select a.user_id
           ,order_date
    from mdw_order_v3_international a 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2024-02-15' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by 1,2
)
--- 验证用户ID 135308616
,data_info as (
    select t1.user_id,gmv,order_no,last_order_date,user_type,country_name,order_id 
            ,min_dt,max_dt,is_act,min_order_date,max_order_date,is_ord
    from (
        select user_id,gmv,order_no,last_order_date,user_type,country_name,order_id
            ,min(dt) min_dt,max(dt) max_dt,max(is_act)is_act
        from ( ---- 判断是否活跃
            select t1.user_id,gmv,order_no,last_order_date
                    ,user_type,country_name,order_id
                    ,t2.dt
                    ,case when t2.user_id is not null then 'Y' else 'N' end is_act
            from user_info t1 
            left join uv t2 on t1.user_id=t2.user_id 
            and t2.dt<=add_months(last_order_date,11) and t2.dt > last_order_date
        ) group by 1,2,3,4,5,6,7
    ) t1 left join ( ---- 判断是否流失
        select user_id
            ,min(order_date) min_order_date,max(order_date) max_order_date,max(is_ord)is_ord
        from (
            select t1.user_id
                    ,t2.order_date
                    ,case when t2.user_id is not null then 'Y' else 'N' end is_ord
            from user_info t1 
            left join order_all t2 on t1.user_id=t2.user_id 
            and t2.order_date<=add_months(last_order_date,11) and t2.order_date > last_order_date
        ) group by 1
    )t2 on t1.user_id=t2.user_id
)
,gd as (  ---- 工单
    select order_no 
    from fuwu.dwd_complaint_close_di
    where dt >= '2024-02-15'
    group by 1
)
,c365old as (   ---- 20250715老客
    select dt,key
    from ihotel_default.ads_user_oldc_uid_di
    where dt = '20250715'
         and value=1
    group by 1,2
)
-- select user_type
--         ,count(distinct user_id) uv
--         ,count(distinct case when is_ord = 'N' then user_id end) loss_uv
--         ,count(distinct case when is_ord = 'N' then user_id end) / count(distinct user_id) loss_rate
--         ,count(distinct case when is_ord = 'N' and is_act = 'Y' then user_id end) have_act_loss_uv
--         ,count(distinct case when is_ord = 'N' and is_act = 'N' then user_id end) no_act_loss_uv
-- from data_info
-- group by 1

-- select count(distinct t1.user_id) uv
--         ,count(distinct case when t2.order_no is not null then t1.user_id end) have_gd_uv
--         ,count(distinct case when t2.order_no is not null then t1.user_id end) /  count(distinct t1.user_id) have_gd_rate
-- from data_info t1 
-- left join gd t2 on t1.order_id=t2.order_no
-- where is_ord = 'N'

select count(distinct t1.user_id) uv
        ,count(distinct case when t2.key is not null then t1.user_id end) c365old_uv
        ,count(distinct case when t2.key is not null then t1.user_id end) /  count(distinct t1.user_id) c365old_uv_rate
from data_info t1 
left join c365old t2 on t1.user_id=t2.key
where is_ord = 'N'
;



