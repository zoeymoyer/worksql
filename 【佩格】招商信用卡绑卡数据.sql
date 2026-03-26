--- 绑定生单用户占比21%、绑定且使用招商信用卡支付用户占比37.8%，GMV占比36.6%
with cmb_bound_users as (
    --- 1. 提取绑卡用户池，并获取其最早的绑卡时间
    select uid as user_id
          ,min(substr(create_date, 1, 10)) as bound_date -- 取最早绑卡日期 (yyyy-MM-dd)
    from hotel.edw_qunar_pay_user_card
    where d = date_sub(current_date, 1)
        and unbind_date is null
        and bank_card_type = 1 
        and bank_name like '%招商银行%'
    group by 1
)
, pay_info as (
    --- 2. 提取支付明细，按订单号聚合判定是否用了招行信用卡(更新较慢t+2)
    select orderid as order_no
          ,max(case when brand_name = '招商银行(信用卡)' then 1 else 0 end) as is_cmb_pay
    from pp_pub.dwd__qunar_selfpayord_inter_hotel_di 
    where d >= date_sub(current_date, 30)  
      and d <= date_sub(current_date, 1)
      and paymenttype = '信用卡'
    group by 1
)
, base_data as (
    --- 3. 提取大盘订单，并与绑卡用户、支付明细交汇判定
    select o.order_date
          ,o.user_id
          ,o.order_no
          ,o.init_gmv
          
          -- 判定1：该用户下单时是否已经绑了招行卡 (订单日期 >= 绑卡日期)
          ,case when u.user_id is not null and o.order_date >= u.bound_date then 1 else 0 end as is_bound_user  
          -- 判定2：该笔订单最终是否使用了招行信用卡支付
          ,coalesce(p.is_cmb_pay, 0) as is_cmb_pay
    from default.mdw_order_v3_international o 
    left join cmb_bound_users u on o.user_id = u.user_id
    left join pay_info p on o.order_no = p.order_no
    where o.dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (o.province_name in ('台湾','澳门','香港') or o.country_name !='中国') 
        and o.terminal_channel_type in ('www','app','touch')
        and o.order_status not in ('CANCELLED', 'REJECTED')
        and o.is_valid = '1'
        and o.order_no <> '103576132435'
        and o.order_date >= cast(date_sub(current_date, 30) as string) 
        and o.order_date <= cast(date_sub(current_date, 1) as string)
)

--- 4. 最终聚合输出
select order_date
    ,count(distinct user_id) as `大盘下单用户数`
    ,count(distinct case when is_bound_user = 1 then user_id end) as `绑招行卡用户数`
    ,count(distinct case when is_bound_user = 1 and is_cmb_pay = 1 then user_id end) `实际用招行卡支付用户数`
    -- 【核心指标 1】绑卡用户占比
    ,concat(round(count(distinct case when is_bound_user = 1 then user_id end) / count(distinct user_id) * 100, 2), '%') as `绑卡用户占比`
    ,concat(round(count(distinct case when is_bound_user = 1 and is_cmb_pay = 1 then user_id end) / count(distinct case when is_bound_user = 1 then user_id end) * 100, 2), '%') as `实际用招行卡支付用户占比`
    
    -- 这波绑卡用户贡献的GMV切分
    ,sum(case when is_bound_user = 1 then init_gmv else 0 end) as `绑卡用户总GMV`
    ,sum(case when is_bound_user = 1 and is_cmb_pay = 1 then init_gmv else 0 end) as `实际用招行卡支付GMV`
    ,sum(case when is_bound_user = 1 and is_cmb_pay = 0 then init_gmv else 0 end) as `未用招行卡支付GMV`
    
    -- 【核心指标 2】绑了卡且实际用招行卡支付的GMV占比
    ,concat(round(
        sum(case when is_bound_user = 1 and is_cmb_pay = 1 then init_gmv else 0 end) 
        / nullif(sum(case when is_bound_user = 1 then init_gmv else 0 end), 0) * 100
     , 2), '%') as `实际用卡支付GMV占比`

    -- 【核心指标 3】绑了卡但没用招行卡支付的GMV占比
    ,concat(round(
        sum(case when is_bound_user = 1 and is_cmb_pay = 0 then init_gmv else 0 end) 
        / nullif(sum(case when is_bound_user = 1 then init_gmv else 0 end), 0) * 100
     , 2), '%') as `未用卡支付GMV占比`

from base_data
group by 1
;