--CQ取消率数据
with user_type as(  --- 用于判定Q新老客
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
,c_user_type as (   --- 用于判定c新老客
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

,q_app_order as (--- Q订单 APP
    select order_date
           ,substr(checkout_date,1,10) checkout_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,a.user_id,order_no,room_night
           ,order_status  
           ,pay_type   --- 支付状态，预付和现付
           ,product_order_refund_type   --- 预付后的退款规则 NO_CANCEL不可取消
           ,product_order_cancel_type   --- 预付后的退款规则 NO_CANCEL不可取消
           --- 是否非当日取消拒单  Y为当日非取消非拒单订单
           ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                            or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                            or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
           ---取消订单是否在取消挽留功能周期内
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) and date(first_cancelled_time) between '2025-12-23' and date_sub(current_date, 1) then 'Y' else 'N' end is_wl_feature  
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and checkout_date between '2024-01-01' and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)

,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee
            ,order_status
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when substr(o.extend_info['LastCancelTime'],1,10) = substr(order_date,1,10) then 'Y' else 'N' end is_no_cancle
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2024-01-01' and date_sub(current_date, 1) -- 退房日期范围
)
,overlap_user_q as  (  --- 搬单订单逻辑：若A订单与B订单入离时间存在重叠，判断为搬单订单
    select distinct order_no
          , user_id
          , checkin_date
          , checkout_date_next
          , checkout_date
          , checkout_date_last
          , case when checkout_date_next <= checkout_date then 1 
               when checkout_date_last >= checkout_date then 1 
               else 0 end as overlap_ord
    from (
        select distinct user_id
            , order_no
            , checkin_date
            , checkout_date
            , lead(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_next
            , lag(checkout_date,1,null) over(partition by user_id order by checkin_date asc) as checkout_date_last
        from mdw_order_v3_international 
        where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
            -- and terminal_channel_type in ('www','app','touch')
            and terminal_channel_type = 'app'
            -- and order_status not in ('CANCELLED','REJECTED')
            and is_valid = '1'
            and checkout_date between '2024-01-01' and '%(FORMAT_DATE)s'
            and order_no <> '103576132435'
    ) z
)
,overlap_user_c as  (  --- 搬单订单逻辑：若A订单与B订单入离时间存在重叠，判断为搬单订单
    select distinct order_no
          , user_id
          , checkin_date
          , checkout_date_next
          , checkout_date
          , checkout_date_last
          , case when checkout_date_next <= checkout_date then 1 
               when checkout_date_last >= checkout_date then 1 
               else 0 end as overlap_ord
    from (
        select distinct user_id
            , order_no
            , substr(o.checkin_date, 1, 10) checkin_date
            , substr(o.checkout_date, 1, 10) checkout_date
            , lead(substr(o.checkout_date, 1, 10),1,null) over(partition by user_id order by substr(o.checkin_date, 1, 10) asc) as checkout_date_next
            , lag(substr(o.checkout_date, 1, 10),1,null) over(partition by user_id order by substr(o.checkin_date, 1, 10) asc) as checkout_date_last
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
        WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
            AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
            AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
            AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
            AND o.terminal_channel_type = 'app'
            AND substr(o.checkout_date, 1, 10) between '2024-01-01' and date_sub(current_date, 1) -- 退房日期范围
    ) z
)

select t1.checkout_date,t1.mdd,t1.user_type
      ,`Q订单`,`Q未取消订单-当日`,`Q未取消订单-总共`,`Q已离店订单`,`Q取消订单-当日`
      ,`C订单`,`C未取消订单-当日`,`C已离店订单`


      ,`Q现付订单` ,`Q现付订单` / `Q订单` `Q现付订单占比`

      ,`Q不可取消订单`,`Q不可取消订单` / `Q订单` `Q不可取消订单占比`
      ,`C不可取消订单`,`C不可取消订单` / `C订单` `C不可取消订单占比`
      ,`Q订单(搬单)`,`Q订单(搬单)` / `Q订单` `Q搬单订单占比`
      ,`C订单(搬单)`,`C订单(搬单)` / `C订单` `C搬单订单占比`
      
      ,`Q取消订单-当日(不可取消订单)` / `Q未取消订单-当日(不可取消订单)` as `Q非当日取消率(不可取消订单)`
      ,`Q取消订单-当日(可取消订单)` / `Q未取消订单-当日(可取消订单)` as `Q非当日取消率(可取消订单)`
      ,`Q取消订单-当日(搬单)` / `Q未取消订单-当日(搬单)` as `Q非当日取消率(搬单)`
      ,`Q取消订单-当日` / `Q未取消订单-当日` as `Q非当日取消率`

      ,1- `C已离店订单` / `C未取消订单-当日` as  `C非当日取消率`
      ,1- `C已离店订单(不可取消订单)` / `C未取消订单-当日(不可取消订单)` as  `C非当日取消率(不可取消订单)`
      ,1- `C已离店订单(可取消订单)` / `C未取消订单-当日(可取消订单)` as  `C非当日取消率(可取消订单)`
      ,1- `C已离店订单(搬单)` / `C未取消订单-当日(搬单)` as  `C非当日取消率(搬单)`
   
from (--- Q离店取消率
    select checkout_date
          ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
          ,count(distinct order_no) as `Q订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共`
          ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单`
          ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as `Q取消订单-当日`

          ,count(distinct case when pay_type = 'CASH' then order_no end) as `Q现付订单`

          ,count(distinct case when product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL' then order_no end) as `Q不可取消订单`
          ,count(distinct case when product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL' and is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日(不可取消订单)`
          ,count(distinct case when product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL' and order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共(不可取消订单)`
          ,count(distinct case when product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL' and order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单(不可取消订单)`
          ,count(distinct case when product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL' and is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日(不可取消订单)`

          ,count(distinct case when product_order_refund_type != 'NO_CANCEL'  or product_order_cancel_type <> 'NO_CANCEL' then order_no end) as `Q可取消订单`
          ,count(distinct case when product_order_refund_type != 'NO_CANCEL'  or product_order_cancel_type <> 'NO_CANCEL' and is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日(可取消订单)`
          ,count(distinct case when product_order_refund_type != 'NO_CANCEL'  or product_order_cancel_type <> 'NO_CANCEL' and order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共(可取消订单)`
          ,count(distinct case when product_order_refund_type != 'NO_CANCEL'  or product_order_cancel_type <> 'NO_CANCEL' and order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单(可取消订单)`
          ,count(distinct case when product_order_refund_type != 'NO_CANCEL'  or product_order_cancel_type <> 'NO_CANCEL' and is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日(可取消订单)`
          
          ,count(distinct case when orderno is not null then order_no end) as `Q订单(搬单)`
          ,count(distinct case when is_not_cancel_d0 = 'Y' and orderno is not null then order_no end) as `Q未取消订单-当日(搬单)`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') and orderno is not null then order_no end) as `Q未取消订单-总共(搬单)`
          ,count(distinct case when order_status in ('CHECKED_OUT') and orderno is not null then order_no end) as `Q已离店订单(搬单)`
          ,count(distinct case when is_cancel_d0 = 'Y' and orderno is not null then order_no end) as     `Q取消订单-当日(搬单)`
    from q_app_order t1
    left join (
        select order_no orderno
        from overlap_user_q
        where overlap_ord = 1
        group by 1
    ) t2 on t1.order_no=t2.orderno
    group by checkout_date,cube(user_type, mdd)
) t1 
left join (--- C 
    select checkout_date
          ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`

          ,count(distinct case when is_no_cancle='Y' then order_no end) as `C不可取消订单`
          ,count(distinct case when is_no_cancle='Y' and is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日(不可取消订单)`
          ,count(distinct case when is_no_cancle='Y' and order_status <> 'C' then order_no end) as     `C已离店订单(不可取消订单)`

          ,count(distinct case when is_no_cancle='N' then order_no end) as `C可取消订单`
          ,count(distinct case when is_no_cancle='N' and is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日(可取消订单)`
          ,count(distinct case when is_no_cancle='N' and order_status <> 'C' then order_no end) as     `C已离店订单(可取消订单)`

          ,count(distinct case when orderno is not null then order_no end) as `C订单(搬单)`
          ,count(distinct case when is_not_cancel_d0 = 'Y' and orderno is not null then order_no end) as `C未取消订单-当日(搬单)`
          ,count(distinct case when order_status <> 'C'  and orderno is not null then order_no end) as `C已离店订单(搬单)`
    from c_order t1
    left join (
        select order_no orderno
        from overlap_user_c
        where overlap_ord = 1
        group by 1
    ) t2 on t1.order_no=t2.orderno
    group by checkout_date,cube(user_type, mdd)
) t2 on t1.checkout_date=t2.checkout_date and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by 1
;


---- 2、取消订单后30日重订占比Q
with user_type as(
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
,q_app_order as (--- Q订单 APP
    select order_date
           ,substr(checkout_date,1,10) checkout_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,a.user_id,order_no,room_night
           ,order_status
           ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
           ,first_cancelled_time
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        --and checkout_date between '2024-01-01' and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)


select checkout_date
      ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
      ,count(distinct t1.order_no) `Q取消订单-当日`
      ,count(distinct t1.user_id) `Q取消订单UV-当日`
      ,count(distinct t2.order_no) `取消30日内再次下单订单量`
      ,count(distinct t2.user_id) `取消30日内再次下单UV`
      
      ,count(distinct case when t1.mdd=t2.mdd then t2.order_no end) `取消30日内再次下单订单量（同目的地）`
      ,count(distinct case when t1.mdd=t2.mdd then t2.user_id end) `取消30日内再次下单UV（同目的地）`
from (--- 取消订单
    select first_cancelled_time,user_id,order_no,checkout_date,mdd
    from q_app_order 
    where is_cancel_d0 = 'Y'
       and checkout_date between '2024-01-01' and date_sub(current_date, 1)
)t1 left join (
    select order_date,user_id,order_no,mdd
    from q_app_order
    where order_status not in ('CANCELLED', 'REJECTED') 
) t2 on t1.user_id=t2.user_id and datediff(t2.order_date,t1.first_cancelled_time) between 1 and 30
group by 1,cube(t1.mdd)
order by 1
;


---- 3、取消订单后30日重订占比C
with c_user_type as (   --- 用于判定c新老客
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
,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when  substr(o.extend_info['CANCEL_TIME'], 1, 10) = substr(o.order_date, 1, 10) then 'Y' else 'N' end is_cancel_d0 --- 当日是否取消拒单
            ,order_status
            ,substr(o.extend_info['CANCEL_TIME'], 1, 10) cancel_time
            ,count(1) over(partition by substr(order_date,1,10),o.user_id) book_order_cnt   --- 预定口径下订单数量
            ,count(1) over(partition by substr(o.checkout_date, 1, 10),o.user_id) checkout_order_cnt --- 离店口径下订单数量
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        -- AND substr(o.checkout_date, 1, 10) between '2024-01-01' and date_sub(current_date, 1) -- 退房日期范围
)


select checkout_date,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
      ,count(distinct t1.order_no) `Q取消订单-当日`
      ,count(distinct t1.user_id) `Q取消订单UV-当日`
      ,count(distinct t2.order_no) `Q取消30日内再次下单订单量`
      ,count(distinct t2.user_id) `Q取消30日内再次下单UV`
      
      ,count(distinct case when t1.mdd=t2.mdd then t2.order_no end) `取消30日内再次下单订单量（同目的地）`
      ,count(distinct case when t1.mdd=t2.mdd then t2.user_id end) `取消30日内再次下单UV（同目的地）`
from (--- 取消订单
    select cancel_time,user_id,order_no,checkout_date,mdd
    from c_order 
    where is_cancel_d0 = 'N' and order_status = 'C'
       and checkout_date between '2024-01-01' and date_sub(current_date, 1)
)t1 left join (
    select order_date,user_id,order_no,mdd
    from c_order
) t2 on t1.user_id=t2.user_id and datediff(t2.order_date,t1.cancel_time) between 1 and 30
group by 1,cube(t1.mdd)
order by 1
;


--- 4、代理商货源维度
with data as (
    select order_date as `日期`
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case 
                    when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'DC'
                    when  qta_supplier_id in ('1615667','800000164') then 'C2Q'
                    when wrapper_id in ('hca908oh60s','hca908oh60t') then 'ABE'
                    when wrapper_id in ('hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s') then 'ABE'
                    when wrapper_id in ('hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'ABE'
                    else '代理' 
            end as `渠道`
            ,case when wrapper_id in ('hca1erb900o','hca1f62c70i','hca10eg3k6k','hca1fceu50i','hca1fc1250i','hca1fcv840j','hca1fck230i','hca123i850l','hca1fbra40i','hca1fel540i','hca1erb900m','hca1fdkh10i','hca1fbr920j','hca1f71a00j','hca1faud10j','hca10eq7a8i','hca1fbsn50i','hca10ep6l8j','hca1f71a00i','hca1f7nc00j','hca1fe4100k','hca1f95n50j','hca1em5m10n','hca1eg3k60n','hca1fc8h40i','hca1fe4100j','hca1fdfs80i','hca1fd8p70i','hca1625f80l','hca1fbr920i','hca1fbr920l','hca1fbr920m','hca1fevc00i','hca1f4om90i','hca908hc00p','hca1fdbn20i','hca1fe4050i','hca1fd4750i','hca1fcek50i','hca1eg3k60o','hca10175k6m')
                then '日本未下线二手代理'
                when wrapper_id in ('hca1fd8p70j','hca1fbsl70i','hca1ff1380j','hca1ffd230j','hca1fbl100i','hca1fd8q30i','hca1fbb170k','hca1f95n50i','hca1fevc00k','hca1fc0s20l','hca1ffk860i','hca1fbvm20i','hca1fes530i','hca1fep130i','hca1erb900k','hca1fc2e70k','hca1f91980i','hca1fdt920i','hca1fc6v80j','hca1fes670i','hca11u2s60m','hca1fcoi60j','hca1fbud40k','hca1ff0h80i','hca1fep110i','hca1f87v40i','hca1erb900n','hca10du058k','hca1fc6v80i','hca1fcoi60i','hca1fe4100i','hca1fe0b10i','hca1fevb80i','hca1fc2e70i','hca1f9f800i','hca1f62c70j','hca1fepr10i','hca1fevb80k','hca1fc6v50i','hca1fevc00j','hca1ffhj60i','hca1fcoi60k','hca1faro30i','hca1fepr10j','hca1ffhh60i','hca1fa7u40j','hca1du0580n','hca1fbb650i','hca1fd8m50i','hca1fden60i','hca2000210r','hca1faro30n','hca1fevb80j','hca1fbb170i','hca2000021p','hca1fa7u40i','hca1fdda60l','hca1fd4l40i','hca1fedv40i','hca1fdfn80i','hca1fd7b40i','hca1e3t010i','hca1fev680i','hca1ffik10i','hca1fctj50i','hca1f4o900i','hca1fc5d90j','hca1fdul60i','hca1fbud40i','hca1feo980j','hca1ff2k30i','hca1fd8i80i','hca1fff750i','hca10dueb7l','hca1fe5800i','hca1ffp400i','hca10e9bc3j','hca1feg300i','hca11vh820o','hca1fc0800i','hca123i850k','hca1ffk770i','hca1fe4o20i','hca1feo980i','hca1fbsl70j','hca1fdec40i'
                ) then '日本下线二手代理' 
                else '非二手代理' end is_second_agen
            ,hotel_seq,a.order_no,a.user_id,checkout_date
            ,order_status
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
            ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join
        (select order_no 
            , max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
        ) b 
    on a.order_no = b.order_no
    -- C关联信息表-用于提供供应商信息
    left join
        (select distinct partner_order_no
            , extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
        ) c
    on b.purchase_order_no = c.partner_order_no
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and a.order_no <> '103576132435'
            and checkout_date between '2025-01-01' and date_sub(current_date, 1)
)



select checkout_date
       ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
       ,if(grouping(`渠道`)=1,'ALL', `渠道`) as  `渠道`
       ,count(distinct order_no) as `Q订单`
       ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
       ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
       ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
       ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
       ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
from data
group by checkout_date,cube(`渠道`, mdd)
order by 1
;


SELECT 
    substr(o.checkout_date, 1, 10) AS checkout_date
   ,COUNT(DISTINCT o.order_no) AS `C订单`
   ,COUNT(DISTINCT CASE 
        WHEN o.extend_info['CANCEL_TIME'] IS NULL 
            OR o.extend_info['CANCEL_TIME'] = 'NULL' 
            OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) 
        THEN o.order_no 
    END) AS `C未取消订单-当日`
    ,COUNT(DISTINCT CASE WHEN o.order_status <> 'C' THEN o.order_no END) AS `C已离店订单-总共`
    ,(1 - COUNT(DISTINCT CASE WHEN o.order_status <> 'C' THEN o.order_no END)/COUNT(DISTINCT CASE WHEN o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) THEN o.order_no END)) as `C取消率（不含当日取消）`
FROM ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
WHERE 
    o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
    AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
    AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
    AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
    AND o.terminal_channel_type = 'app'
    AND substr(o.checkout_date, 1, 10) between date_sub(current_date, 390) and date_sub(current_date, 1) -- 退房日期范围
GROUP BY 1


select substr(checkout_date,1,10) as checkout_date
    , count(distinct order_no) as `Q订单`
    , count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end)) as `Q未取消订单-当日`
    , count (distinct (case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) or (refund_time is not null and date(refund_time) > order_date) then order_no end)) as `Q取消订单-当日`
    , count (distinct (case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) and date(first_cancelled_time) between '2025-12-23' and date_sub(current_date, 1) then order_no end)) as `Q取消订单在取消挽留功能周期内`
    , count (distinct (case when order_status = 'CHECKED_OUT' then order_no end)) as `Q已离店订单-总共`
    , count (distinct (case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) or (refund_time is not null and date(refund_time) > order_date) then order_no end))
    /count (distinct (case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) then order_no end)) as `Q取消率(不含当日取消）`
    ,count(distinct case when order_status = 'REJECTED' then order_no end) `拒单`
    ,count(distinct case when pay_type = 'CASH' then order_no end) `现付订单`
from mdw_order_v3_international a
where dt = '%(DATE)s'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    and terminal_channel_type = 'app'
    and is_valid = '1'
    and checkout_date between date_sub(current_date, 390) and date_sub(current_date, 1)
    and a.country_name = '日本'
    and a.order_no <> '103576132435'
group by 1
;

---- 6、支付价产品力
select substr(check_out,1,10) as check_out 
    ,mdd  
    ,count(distinct case when  pay_price_compare_result is not null then id end) as num
    ,count(distinct case when  pay_price_compare_result='Qlose'  then id end) as lose_num
    ,count(distinct case when  pay_price_compare_result='Qlose'  then id end)/count(distinct case when  pay_price_compare_result is not null then id end) as pay_price_lose_rate_sq2   -- 支付价lose率
    ,count(distinct case when  pay_price_compare_result='Qbeat'  then id end) as beat_num
    ,count(distinct case when  pay_price_compare_result='Qbeat'  then id end)/count(distinct case when  pay_price_compare_result is not null then id end) as pay_price_beat_rate_sq2  
from (
    select order_no
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and a.country_name = '日本'
        and a.order_no <> '103576132435'
    group by 1
)t1 left join 
(
    select check_out
        ,a.uniq_id
        ,a.crawl_time  -- 抓取时间 
        ,a.id
        ,a.pay_price_compare_result    
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        --,case when concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type 
        ,qunar_price_info['orderNum'] as orderNum
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt >= '20250501' and dt <= '%(DATE)s'
        and business_type = 'intl_crawl_cq_api_order'  --- 流量视角（intl_crawl_cq_api_userview）、抓取（intl_crawl_cq_spa）、生单（intl_crawl_cq_api_order）
        and compare_type = 'SIMILAR_PRODUCT_LOWEST'  ---  物理房型维度（PHYSICAL_ROOM_TYPE_LOWEST） 同质化维度（SIMILAR_PRODUCT_LOWEST）
        and room_type_cover = 'Qmeet'  
        and ctrip_room_status = 'true' 
        and qunar_room_status = 'true'    
        and qunar_price_info['order_product_similar_lowest']="1"    
)t2 on t1.order_no=t2.orderNum
group by 1,2
;





--Q不可取消占比
select substr(checkout_date,1,10) as checkout_date
    , count(distinct order_no) as `Q订单`
    , count (distinct (case when product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL' then order_no end)) as `不可取消订单量`
    ,count (distinct (case when product_order_refund_type <> 'NO_CANCEL' or product_order_cancel_type <> 'NO_CANCEL' then order_no end)) as `可取消订单量`
from mdw_order_v3_international a
where dt = '%(DATE)s'
    --and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    and a.country_name ='日本'
    and terminal_channel_type = 'app'
    and is_valid = '1'
    and checkout_date between '2025-01-01' and '2025-12-31'
    and a.order_no <> '103576132435'
group by 1
;





--CQ取消率数据
with user_type as(  --- 用于判定Q新老客
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
,c_user_type as (   --- 用于判定c新老客
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
,q_app_order as (--- Q订单 APP
    select order_date
           ,substr(checkout_date,1,10) checkout_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,a.user_id,order_no,room_night
           ,order_status  
           ,pay_type   --- 支付状态，预付和现付
           ,product_order_refund_type   --- 预付后的退款规则 NO_CANCEL不可取消
           ,product_order_cancel_type   --- 预付后的退款规则 NO_CANCEL不可取消
           --- 是否非当日取消拒单  Y为当日非取消非拒单订单
           ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                            or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                            or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and checkout_date between '2025-01-01' and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)

,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee
            ,order_status
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when substr(o.extend_info['LastCancelTime'],1,10) = substr(order_date,1,10) then 'Y' else 'N' end is_no_cancle
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
)

select t1.checkout_date,t1.is_jp,t1.user_type
      ,`Q订单`,`Q未取消订单-当日`,`Q未取消订单-总共`,`Q已离店订单`,`Q取消订单-当日`
      ,`C订单`,`C未取消订单-当日`,`C已离店订单`

      ,`Q取消订单-当日` / `Q未取消订单-当日` as `Q非当日取消率`
      ,1- `C已离店订单` / `C未取消订单-当日` as  `C非当日取消率`
      ,(`Q取消订单-当日` / `Q未取消订单-当日`) / (1- `C已离店订单` / `C未取消订单-当日`) as  `取消率QC`
from (--- Q离店取消率
    select checkout_date
          ,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp
          ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
          ,count(distinct order_no) as `Q订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共`
          ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单`
          ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as `Q取消订单-当日`
    from q_app_order t1
    group by checkout_date,cube(user_type, is_jp)
) t1 
left join (--- C 
    select checkout_date
          ,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp
          ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
    from c_order t1
    group by checkout_date,cube(user_type, is_jp)
) t2 on t1.checkout_date=t2.checkout_date and t1.is_jp=t2.is_jp and t1.user_type=t2.user_type
order by 1
;



select order_date as `日期`

            ,case when wrapper_id in ('hca1erb900o','hca1f62c70i','hca10eg3k6k','hca1fceu50i','hca1fc1250i','hca1fcv840j','hca1fck230i','hca123i850l','hca1fbra40i','hca1fel540i','hca1erb900m','hca1fdkh10i','hca1fbr920j','hca1f71a00j','hca1faud10j','hca10eq7a8i','hca1fbsn50i','hca10ep6l8j','hca1f71a00i','hca1f7nc00j','hca1fe4100k','hca1f95n50j','hca1em5m10n','hca1eg3k60n','hca1fc8h40i','hca1fe4100j','hca1fdfs80i','hca1fd8p70i','hca1625f80l','hca1fbr920i','hca1fbr920l','hca1fbr920m','hca1fevc00i','hca1f4om90i','hca908hc00p','hca1fdbn20i','hca1fe4050i','hca1fd4750i','hca1fcek50i','hca1eg3k60o','hca10175k6m')
                then '日本未下线二手代理'
                when wrapper_id in ('hca1fd8p70j','hca1fbsl70i','hca1ff1380j','hca1ffd230j','hca1fbl100i','hca1fd8q30i','hca1fbb170k','hca1f95n50i','hca1fevc00k','hca1fc0s20l','hca1ffk860i','hca1fbvm20i','hca1fes530i','hca1fep130i','hca1erb900k','hca1fc2e70k','hca1f91980i','hca1fdt920i','hca1fc6v80j','hca1fes670i','hca11u2s60m','hca1fcoi60j','hca1fbud40k','hca1ff0h80i','hca1fep110i','hca1f87v40i','hca1erb900n','hca10du058k','hca1fc6v80i','hca1fcoi60i','hca1fe4100i','hca1fe0b10i','hca1fevb80i','hca1fc2e70i','hca1f9f800i','hca1f62c70j','hca1fepr10i','hca1fevb80k','hca1fc6v50i','hca1fevc00j','hca1ffhj60i','hca1fcoi60k','hca1faro30i','hca1fepr10j','hca1ffhh60i','hca1fa7u40j','hca1du0580n','hca1fbb650i','hca1fd8m50i','hca1fden60i','hca2000210r','hca1faro30n','hca1fevb80j','hca1fbb170i','hca2000021p','hca1fa7u40i','hca1fdda60l','hca1fd4l40i','hca1fedv40i','hca1fdfn80i','hca1fd7b40i','hca1e3t010i','hca1fev680i','hca1ffik10i','hca1fctj50i','hca1f4o900i','hca1fc5d90j','hca1fdul60i','hca1fbud40i','hca1feo980j','hca1ff2k30i','hca1fd8i80i','hca1fff750i','hca10dueb7l','hca1fe5800i','hca1ffp400i','hca10e9bc3j','hca1feg300i','hca11vh820o','hca1fc0800i','hca123i850k','hca1ffk770i','hca1fe4o20i','hca1feo980i','hca1fbsl70j','hca1fdec40i'
                ) then '日本下线二手代理' 
                else '非二手代理' end is_second_agen
            ,hotel_seq,a.order_no,a.user_id,checkout_date
            ,order_status
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
            ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    
    on b.purchase_order_no = c.partner_order_no
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and a.order_no <> '103576132435'
            and checkout_date between '2025-01-01' and date_sub(current_date, 1)
;



--CQ取消率数据
with user_type as(  --- 用于判定Q新老客
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
,c_user_type as (   --- 用于判定c新老客
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
,q_app_order as (--- Q订单 APP
    select order_date
           ,substr(checkout_date,1,10) checkout_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,a.user_id,order_no,room_night
           ,order_status  
           ,pay_type   --- 支付状态，预付和现付
           ,product_order_refund_type   --- 预付后的退款规则 NO_CANCEL不可取消
           ,product_order_cancel_type   --- 预付后的退款规则 NO_CANCEL不可取消
           --- 是否非当日取消拒单  Y为当日非取消非拒单订单
           ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                            or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                            or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
           ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and checkout_date between '2025-01-01' and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)

,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee
            ,order_status
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
)

select t1.checkout_date,t1.is_jp
      ,`Q订单`,`Q未取消订单-当日`,`Q未取消订单-总共`,`Q已离店订单`,`Q取消订单-当日`,`Q不可取消订单`,`Q不可取消订单` / `Q订单` `Q不可取消订单占比`
      ,`C订单`,`C未取消订单-当日`,`C已离店订单`,`C不可取消订单`, `C取消订单`,`C不可取消订单` / `C订单` `C不可取消订单占比`

      ,`Q取消订单-当日` / `Q未取消订单-当日` as `Q非当日取消率`
      ,1- `C已离店订单` / `C未取消订单-当日` as  `C非当日取消率`
      ,`Q取消订单-当日` / `Q取消订单-当日ALL` as `Q取消订单-当日占比`
      ,`Q不可取消订单` / `Q不可取消订单ALL` as `Q不可取消订单占比`
      ,`C不可取消订单` / `C不可取消订单ALL` as `C不可取消订单占比`
      ,`C取消订单` / `C取消订单ALL` as `C取消订单占比`
      ,(`Q取消订单-当日` / `Q未取消订单-当日`) / (1- `C已离店订单` / `C未取消订单-当日` ) `取消率QC`
from (--- Q离店取消率
    select checkout_date
          ,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp
          ,count(distinct order_no) as `Q订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共`
          ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单`
          ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as `Q取消订单-当日`
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as `Q不可取消订单`
    from q_app_order t1
    group by checkout_date,cube(is_jp)
) t1 
left join (--- C 
    select checkout_date
          ,if(grouping(is_jp)=1,'ALL', is_jp) as  is_jp
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
    from c_order t1
    group by checkout_date,cube(is_jp)
) t2 on t1.checkout_date=t2.checkout_date and t1.is_jp=t2.is_jp
left join (--- Q离店取消率-ALL
    select checkout_date
          ,count(distinct order_no) as `Q订单ALL`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日ALL`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共ALL`
          ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单ALL`
          ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as `Q取消订单-当日ALL`
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as `Q不可取消订单ALL`
    from q_app_order t1
    group by checkout_date
) t3 on t1.checkout_date=t3.checkout_date
left join (--- C离店取消率-ALL
    select checkout_date
          ,count(distinct order_no) as `C订单ALL`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日ALL`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单ALL`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单ALL`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单ALL`
    from c_order t1
    group by checkout_date
) t4 on t1.checkout_date=t4.checkout_date
order by 1
;



--CQ取消率数据
with user_type as(  --- 用于判定Q新老客
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
,c_user_type as (   --- 用于判定c新老客
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
,q_app_order as (--- Q订单 APP
    select order_date
           ,substr(checkout_date,1,10) checkout_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,a.user_id,order_no,room_night
           ,order_status  
           ,pay_type   --- 支付状态，预付和现付
           ,product_order_refund_type   --- 预付后的退款规则 NO_CANCEL不可取消
           ,product_order_cancel_type   --- 预付后的退款规则 NO_CANCEL不可取消
           --- 是否非当日取消拒单  Y为当日非取消非拒单订单
           ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                            or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                            or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
           ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and checkout_date between '2025-01-01' and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)

,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee
            ,order_status
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
)

select t1.checkout_date,t1.mdd
      ,`Q订单`,`Q未取消订单-当日`,`Q未取消订单-总共`,`Q已离店订单`,`Q取消订单-当日`,`Q不可取消订单`,`Q不可取消订单` / `Q订单` `Q不可取消订单占比`
      ,`C订单`,`C未取消订单-当日`,`C已离店订单`,`C不可取消订单`, `C取消订单`,`C不可取消订单` / `C订单` `C不可取消订单占比`

      ,`Q取消订单-当日` / `Q未取消订单-当日` as `Q非当日取消率`
      ,1- `C已离店订单` / `C未取消订单-当日` as  `C非当日取消率`
      ,`Q取消订单-当日` / `Q取消订单-当日ALL` as `Q取消订单-当日占比`
      ,`Q不可取消订单` / `Q不可取消订单ALL` as `Q不可取消订单占比`
      ,`C不可取消订单` / `C不可取消订单ALL` as `C不可取消订单占比`
      ,`C取消订单` / `C取消订单ALL` as `C取消订单占比`
      ,(`Q取消订单-当日` / `Q未取消订单-当日`) / (1- `C已离店订单` / `C未取消订单-当日` ) `取消率QC`
from (--- Q离店取消率
    select checkout_date
          ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,count(distinct order_no) as `Q订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共`
          ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单`
          ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as `Q取消订单-当日`
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as `Q不可取消订单`
    from q_app_order t1
    group by checkout_date,cube(mdd)
) t1 
left join (--- C 
    select checkout_date
          ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
    from c_order t1
    group by checkout_date,cube(mdd)
) t2 on t1.checkout_date=t2.checkout_date and t1.mdd=t2.mdd
left join (--- Q离店取消率-ALL
    select checkout_date
          ,count(distinct order_no) as `Q订单ALL`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日ALL`
          ,count(distinct case when order_status not in ('CANCELLED','REJECTED') then order_no end) as `Q未取消订单-总共ALL`
          ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单ALL`
          ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as `Q取消订单-当日ALL`
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as `Q不可取消订单ALL`
    from q_app_order t1
    group by checkout_date
) t3 on t1.checkout_date=t3.checkout_date
left join (--- C离店取消率-ALL
    select checkout_date
          ,count(distinct order_no) as `C订单ALL`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日ALL`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单ALL`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单ALL`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单ALL`
    from c_order t1
    group by checkout_date
) t4 on t1.checkout_date=t4.checkout_date
order by 1
;