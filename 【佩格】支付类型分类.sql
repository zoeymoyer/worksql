with q_order as (
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            -- ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,order_no,init_gmv,room_night
            ,case when ext_flag_map['pay_after_stay_flag']='true' then '后付订单' 
                    else '非后付订单' end is_pay_after --- 是否后付订单
            ,case when ext_flag_map['installment_num_list'] >= 1 and ext_flag_map['installment_num_list'] !='null' 
                    then '分期' else '非分期' end is_installment  --- 是否分期订单
            ,case when pay_type = 'CASH' then '现付' else '其他' end  is_pay_go
            ,ext_flag_map['pay_after_stay_flag'] pay_after_stay_flag   --- 后付
            ,ext_flag_map['sub_auth_type'] sub_auth_type  --- 后付  7 拿去花后付
            ,ext_flag_map['pay_method_name_list'] pay_method_name_list -- 分期信用支付和信用卡
            ,ext_flag_map['installment_num_list'] installmentNumList   --- 分期次数
            ,pay_type  --- 支付类型
            ,case when order_date between date_sub(current_date, 8) and date_sub(current_date, 2) then '本周' else '上周' end week_type
    from default.mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 2)
        and order_no <> '103576132435'
)
,pay_ment as(
    select d,orderid,post_flag
            ,brand_name
            ,paymenttype
            ,case when paymenttype = '微信' then '微信' 
                when paymenttype = '支付宝' then '支付宝'  
                when paymenttype in  ('信用卡','储蓄卡') then '银行卡'  
                when paymenttype =  '余额' then '余额支付'  
                when paymenttype =  '信用支付' then '拿去花支付'  
                else '其他' end  paytype
    from pp_pub.dwd__qunar_selfpayord_inter_hotel_di 
    where d >= date_sub(current_date, 15)
            and d <= date_sub(current_date, 2)
)
,order_info as (
    select order_date,order_no,init_gmv
            ,is_pay_after    --- 是否后付
            ,is_installment  --- 是否分期
            ,is_pay_go       --- 是否现付
            ,pay_after_stay_flag --- true 后付
            ,sub_auth_type       --- pay_after_stay_flag=true && sub_auth_type = 7 拿去花后付 pay_after_stay_flag=true && sub_auth_type != 7 标准后付
            ,pay_method_name_list --- 筛选分期类型， pay_method_name_list='信用卡' '信用支付'
            ,installmentNumList   --- 分期次数
            ,paytype,paymenttype   --- 支付方式分层
            ,pay_type
            ,pay_type_new as pay_type_second
            ,case when pay_type_new = '信用担保'  then '现付'
                when pay_type_new in ('拿去花后付','标准后付') then '后付'
                when pay_type_new in ('信用卡分期','拿去花分期') then '分期'
                else '预付' end pay_type_first
    from (
        select order_date,order_no,init_gmv
            ,is_pay_after    --- 是否后付
            ,is_installment  --- 是否分期
            ,is_pay_go       --- 是否现付
            ,pay_after_stay_flag --- true 后付
            ,sub_auth_type       --- pay_after_stay_flag=true && sub_auth_type = 7 拿去花后付 pay_after_stay_flag=true && sub_auth_type != 7 标准后付
            ,pay_method_name_list --- 筛选分期类型， pay_method_name_list='信用卡' '信用支付'
            ,installmentNumList   --- 分期次数
            ,paytype,paymenttype   --- 支付方式分层
            ,pay_type
            ,case when pay_type = '其他' and paytype = '微信' then '微信' 
                    when pay_type = '其他' and paytype = '支付宝' then '支付宝' 
                    when pay_type = '其他' and paytype = '银行卡' then '银行卡' 
                    when pay_type = '其他' and paytype = '余额支付' then '余额支付' 
                    when pay_type = '其他' and paytype = '拿去花支付' then '拿去花支付' 
                    when pay_type = '其他' and paytype = '其他' then '其他' 
                    else pay_type end pay_type_new
        from (--- 分期和后付算两次
            select order_date,order_no,init_gmv,is_pay_after,is_installment,is_pay_go,pay_after_stay_flag,sub_auth_type,pay_method_name_list,installmentNumList
                ,case when is_pay_go = '现付' then '信用担保' 
                        when is_pay_after='后付订单' and sub_auth_type = 7 then '拿去花后付'
                        when is_pay_after='后付订单' and sub_auth_type != 7 then '标准后付'
                        when is_installment='分期' and pay_method_name_list rlike '信用卡'  then '信用卡分期'
                        when is_installment='分期' and pay_method_name_list rlike '信用支付'  then '拿去花分期'
                        else  '其他' end  pay_type
                ,brand_name,paymenttype,paytype
            from q_order t1
            left join pay_ment t2 on t1.order_date=t2.d  and t1.order_no=t2.orderid
            union all
            select order_date,order_no,init_gmv,is_pay_after,is_installment,is_pay_go,pay_after_stay_flag,sub_auth_type,pay_method_name_list,installmentNumList
                ,case  when is_installment='分期' and pay_method_name_list rlike '信用卡'  then '信用卡分期'
                    when is_installment='分期' and pay_method_name_list rlike '信用支付'  then '拿去花分期'
                    end  pay_type
                ,brand_name,paymenttype,paytype
            from q_order t1
            left join pay_ment t2 on t1.order_date=t2.d  and t1.order_no=t2.orderid
            where is_installment='分期' and is_pay_after='后付订单'
        ) t
    ) t  
)

select t1.order_date,pay_type_first,pay_type_second,order_no,init_gmv,order_no_all,init_gmv_all,order_no/order_no_all order_no_rate,init_gmv/ init_gmv_all init_gmv_rate
from (
    select order_date,pay_type_first,pay_type_second,count(distinct order_no) order_no,sum(init_gmv) init_gmv
    from order_info t
    group by 1,2,3
) t1 
left join (
    select order_date,count(distinct order_no) order_no_all,sum(init_gmv)init_gmv_all
    from q_order 
    group by 1
)t2 on t1.order_date=t2.order_date
;

select t1.order_date,pay_type_first,order_no,init_gmv,order_no_all,init_gmv_all,order_no/order_no_all order_no_rate,init_gmv/ init_gmv_all init_gmv_rate
from (
    select order_date,pay_type_first,count(distinct order_no) order_no,sum(init_gmv) init_gmv
    from (select order_date,pay_type_first,order_no,init_gmv from order_info group by 1,2,3,4) t group by 1,2
) t1 
left join (
    select order_date,count(distinct order_no) order_no_all,sum(init_gmv)init_gmv_all
    from q_order 
    group by 1
)t2 on t1.order_date=t2.order_date
;


---- sql1看板报表，周日均一级分类
,data_info as (
    select week_type,pay_type_first
        ,round(sum(order_no) / count(1)) `订单量` 
        ,round(sum(init_gmv) / count(1)) `GMV` 
        ,round(sum(order_no_rate) / count(1) * 100, 1) `订单量占比`   --- 保留1位小数
        ,round(sum(init_gmv_rate) / count(1)* 100, 1) `GMV占比`      --- 保留1位小数
    from (
        select t1.order_date,pay_type_first,order_no,init_gmv,order_no_all,init_gmv_all,order_no/order_no_all order_no_rate,init_gmv/ init_gmv_all init_gmv_rate
                ,case when t1.order_date between date_sub(current_date, 8) and date_sub(current_date, 2) then '本周' else '上周' end week_type
        from (
            select order_date,pay_type_first,count(distinct order_no) order_no,sum(init_gmv) init_gmv
            from (select order_date,pay_type_first,order_no,init_gmv from order_info group by 1,2,3,4) t group by 1,2
        ) t1 
        left join ( --- 整体用于算占比
            select order_date
                ,count(distinct order_no) order_no_all
                ,sum(init_gmv)init_gmv_all
            from q_order 
            group by 1
        )t2 on t1.order_date=t2.order_date
    )t group by 1,2
)
select t1.pay_type_first
        ,t1.`订单量`  
        ,t1.`GMV` 
        ,concat(t1.`订单量占比` , '%') `订单量占比` 
        ,concat(t1.`GMV占比` , '%') `GMV占比` 
        
        ,concat(round((t1.`订单量占比` / t2.`订单量占比` - 1)  * 100, 1), '%') `订单量占比WoW`
        ,concat(round((t1.`GMV占比` / t2.`GMV占比` - 1)  * 100, 1), '%') `GMV占比WoW`

from (
    select pay_type_first
        ,`订单量`  
        ,`GMV` 
        ,`订单量占比` 
        ,`GMV占比` 
    from data_info where week_type = '本周'
) t1 left join (
    select pay_type_first
        ,`订单量`  
        ,`GMV` 
        ,`订单量占比` 
        ,`GMV占比` 
    from data_info where week_type = '上周' 
) t2 on t1.pay_type_first=t2.pay_type_first
order by case when pay_type_first='现付' then 1
              when pay_type_first='后付' then 2
              when pay_type_first='分期' then 3
              when pay_type_first='预付' then 4
         end 
;
 

---- sql2看板报表，周日均一级分类
,data_info as (
    select week_type,pay_type_first,pay_type_second
        ,round(sum(order_no) / count(1)) `订单量` 
        ,round(sum(init_gmv) / count(1)) `GMV` 
        ,round(sum(order_no_rate) / count(1) * 100, 1) `订单量占比`   --- 保留1位小数
        ,round(sum(init_gmv_rate) / count(1)* 100, 1) `GMV占比`      --- 保留1位小数
    from (
        select t1.order_date,pay_type_first,pay_type_second,order_no,init_gmv,order_no_all,init_gmv_all,order_no/order_no_all order_no_rate,init_gmv/ init_gmv_all init_gmv_rate
                ,case when t1.order_date between date_sub(current_date, 8) and date_sub(current_date, 2) then '本周' else '上周' end week_type
        from (
            select order_date,pay_type_first,pay_type_second,count(distinct order_no) order_no,sum(init_gmv) init_gmv
            from order_info t group by 1,2,3
        ) t1 
        left join ( --- 整体用于算占比
            select order_date
                ,count(distinct order_no) order_no_all
                ,sum(init_gmv)init_gmv_all
            from q_order 
            group by 1
        )t2 on t1.order_date=t2.order_date
    )t group by 1,2,3
)

select t1.pay_type_first,t2.pay_type_second
        ,t1.`订单量`  
        ,t1.`GMV` 
        ,concat(t1.`订单量占比` , '%') `订单量占比` 
        ,concat(t1.`GMV占比` , '%') `GMV占比` 
        
        ,concat(round((t1.`订单量占比` / t2.`订单量占比` - 1)  * 100, 1), '%') `订单量占比WoW`
        ,concat(round((t1.`GMV占比` / t2.`GMV占比` - 1)  * 100, 1), '%') `GMV占比WoW`

from (
    select pay_type_first,pay_type_second
        ,`订单量`  
        ,`GMV` 
        ,`订单量占比` 
        ,`GMV占比` 
    from data_info where week_type = '本周'
) t1 left join (
    select pay_type_first,pay_type_second
        ,`订单量`  
        ,`GMV` 
        ,`订单量占比` 
        ,`GMV占比` 
    from data_info where week_type = '上周' 
) t2 on t1.pay_type_first=t2.pay_type_first and t1.pay_type_second=t2.pay_type_second
;



