set hive.auto.convert.join = false;
set hive.optimize.correlation = false;
set hive.support.concurrency = false;

-- 1
with data_t_7_bese as ( -- 基础数据：活跃用户关联活动页访问（7天内）
    select a.`日期`
          ,a.user_id
          ,a.user_name
          ,uv.page_cid
          ,uv.page_title
    from (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a
    left join ( -- 活动页访问数据（多源合并）
        select distinct substr(log_time, 1, 10) as log_date
                      ,page_cid
                      ,page_title
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,activity_id
                      ,t1.code_name as page_title
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,page as activity_id
                      ,t1.code_name as page_title
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,page as activity_id
                      ,case when page = '37833' then '国酒&门票五一大促' end as page_title
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 30)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv on uv.user_name = a.user_name
    where datediff(a.`日期`, uv.log_date) between 0 and 7
        and uv.user_name is not null
        and uv.page_cid is not null
)
,data_t_7 as (
    select page_cid as `活动ID`
          ,page_title as `活动名称`
          ,`日期`
          ,`大盘贡献UV(t-7)`
          ,nvl(`有效单UV`, 0) as `有效单UV`
          ,concat(round(nvl(`有效单UV` / `大盘贡献UV(t-7)` * 100, 0), 2), '%') as `U2O`
          ,nvl(`有效订单量`, 0) as `有效订单量`
          ,concat(round(nvl(`有效订单量` / `大盘贡献UV(t-7)` * 100, 0), 2), '%') as `CR`
          ,nvl(`有效间夜量`, 0) as `有效间夜量`
          ,nvl(round(`有效佣金额`, 0), 0) as `有效佣金额`
          -- 新客
          ,nvl(`活动页新客UV(t-7)`, 0) as `活动页新客UV(t-7)`
          ,concat(round(nvl(`活动页新客UV(t-7)` / `大盘贡献UV(t-7)` * 100, 0), 2), '%') as `活动页新客UV占比`
          ,nvl(`下单新客UV`, 0) as `下单新客UV`
          ,concat(round(nvl(`新客有效订单量` / `活动页新客UV(t-7)` * 100, 0), 2), '%') as `新客U2O`
          ,nvl(`新客有效单UV`, 0) as `新客有效单UV`
          ,nvl(`新客有效订单量`, 0) as `新客有效订单量`
          ,nvl(`新客有效间夜量`, 0) as `新客有效间夜量`
          ,nvl(round(`新客有效佣金额`, 0), 0) as `新客有效佣金额`
    from (
        select a.`日期`
              ,a.page_cid
              ,a.page_title
              -- 总量
              ,count(distinct a.user_name) as `大盘贡献UV(t-7)`
              ,count(distinct orders.user_name) as `有效单UV`
              ,count(distinct orders.order_no) as `有效订单量`
              ,sum(orders.room_night) as `有效间夜量`
              ,sum(orders.`初始返后佣金`) as `有效佣金额`
              -- 新客
              ,count(distinct case when (first_order_date is null or first_order_date >= a.`日期`) then a.user_name else null end) as `活动页新客UV(t-7)`
              ,count(distinct case when (first_order_date is null or first_order_date >= orders.order_date) then orders.user_name else null end) as `下单新客UV`
              ,count(distinct case when (first_order_date is null or first_order_date >= orders.order_date) then orders.user_name end) as `新客有效单UV`
              ,count(distinct case when (first_order_date is null or first_order_date >= orders.order_date) then orders.order_no end) as `新客有效订单量`
              ,sum(case when (first_order_date is null or first_order_date >= orders.order_date) then orders.room_night end) as `新客有效间夜量`
              ,sum(case when (first_order_date is null or first_order_date >= orders.order_date) then orders.`初始返后佣金` end) as `新客有效佣金额`
        from data_t_7_bese a
        left join (
            select order_date
                  ,terminal_channel_type
                  ,user_id
                  ,user_name
                  ,batch_series
                  ,coupon_id
                  ,country_name
                  ,city_name
                  ,order_no
                  ,order_status
                  ,checkin_date
                  ,checkout_date
                  ,room_night
                  ,init_room_fee
                  ,coupon_substract
                  ,init_payamount_price
                  ,init_gmv
                  ,init_commission_after
                  ,case when batch_series in ('MacaoDisco_ZK_5e27de', '2night_ZK_952825', '3night_ZK_ad8c83') then (init_commission_after + nvl(coupon_substract, 0))
                        when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0))
                        else init_commission_after
                   end as `初始返后佣金`
            from mdw_order_v3_international
            where dt = '%(DATE)s'
                and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                and terminal_channel_type in ('www', 'app', 'touch')
                and is_valid = '1'
                and order_date between date_sub(current_date, 14) and '%(FORMAT_DATE)s'
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date)   -- 非当日取消订单
                and (first_rejected_time is null or date(first_rejected_time) > order_date)    -- 非当日取消订单
                and (refund_time is null or date(refund_time) > order_date)                    -- 非当日取消订单
        ) orders on a.user_name = orders.user_name and a.`日期` = orders.order_date
        left join (
            select user_name
                  ,min(order_date) as first_order_date
            from mdw_order_v3_international
            where dt = '%(DATE)s'
                and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                and terminal_channel_type in ('www', 'app', 'touch')
                and is_valid = '1'
                and order_status not in ('CANCELLED', 'REJECTED')
            group by 1
        ) new on a.user_name = new.user_name
        group by a.`日期`, a.page_cid, a.page_title
    ) ord
    order by `活动ID`, `日期` desc
)

select distinct t.page_cid as `活动ID`
                ,t.page_title as `活动名称`
                ,t.log_date as `日期`
                ,t.`活动页UV`
                ,t.`大盘贡献UV`
                ,concat(round(nvl(t.`大盘贡献UV` / t.`活动页UV` * 100, 0), 2), '%') as `活动uv渗透率`
                ,b.`大盘贡献UV(t-7)`
                ,nvl(b.`有效单UV`, 0) as `有效单UV`
                ,nvl(b.`U2O`, 0) as `U2O`
                ,nvl(b.`有效订单量`, 0) as `有效订单量`
                ,nvl(b.`CR`, 0) as `CR`
                ,nvl(b.`有效间夜量`, 0) as `有效间夜量`
                ,nvl(b.`有效佣金额`, 0) as `有效佣金额`
                -- 新客
                ,b.`活动页新客UV(t-7)`
                ,t.`活动页新客UV`
                ,nvl(b.`活动页新客UV占比`, 0) as `活动页新客UV占比`
                ,nvl(b.`新客U2O`, 0) as `新客U2O`
                ,nvl(b.`新客有效单UV`, 0) as `新客有效单UV`
                ,nvl(b.`新客有效订单量`, 0) as `新客有效订单量`
                ,nvl(b.`新客有效间夜量`, 0) as `新客有效间夜量`
                ,nvl(b.`新客有效佣金额`, 0) as `新客有效佣金额`
from (
    select uv.log_date
          ,uv.page_cid
          ,uv.page_title
          -- 总量
          ,count(distinct uv.user_name) as `活动页UV`
          ,count(distinct case when (new.first_order_date is null or new.first_order_date >= uv.log_date) then uv.user_name else null end) as `活动页新客UV`
          ,count(distinct case when a.user_name is not null then uv.user_name else null end) as `大盘贡献UV`
    from (
        select distinct substr(log_time, 1, 10) as log_date
                      ,page_cid
                      ,page_title
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,activity_id
                      ,t1.code_name as page_title
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,page as activity_id
                      ,t1.code_name as page_title
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,page as activity_id
                      ,case when page = '37833' then '国酒&门票五一大促' end as page_title
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 30)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv
    left join (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a on uv.user_name = a.user_name and uv.log_date = a.`日期`
    left join (
        select user_name
              ,min(order_date) as first_order_date
        from mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and is_valid = '1'
            and order_status not in ('CANCELLED', 'REJECTED')
        group by 1
    ) new on a.user_name = new.user_name
    where uv.page_cid is not null
    group by uv.log_date, uv.page_cid, uv.page_title
) t
left join data_t_7 b on t.page_cid = b.`活动ID` and t.page_title = b.`活动名称` and t.log_date = b.`日期`
order by `活动ID`, `日期` desc
;



set hive.auto.convert.join = false;
set hive.optimize.correlation = false;
set hive.support.concurrency = false;

-- sql2
with data_t_7 as (
    select page_cid as `活动ID`
          ,page_title as `活动名称`
          ,bd_source as `投放渠道`
          ,`日期`
          ,`大盘贡献UV(t-7)`
          ,nvl(`有效单UV`, 0) as `有效单UV`
          ,concat(round(nvl(`有效单UV` / `大盘贡献UV(t-7)` * 100, 0), 2), '%') as `U2O`
          ,nvl(`有效订单量`, 0) as `有效订单量`
          ,concat(round(nvl(`有效订单量` / `大盘贡献UV(t-7)` * 100, 0), 2), '%') as `CR`
          ,nvl(`有效间夜量`, 0) as `有效间夜量`
          ,nvl(round(`有效佣金额`, 0), 0) as `有效佣金额`
          -- 新客
          ,nvl(`活动页新客UV(t-7)`, 0) as `活动页新客UV(t-7)`
          ,concat(round(nvl(`活动页新客UV(t-7)` / `大盘贡献UV(t-7)` * 100, 0), 2), '%') as `活动页新客UV占比`
          ,nvl(`下单新客UV`, 0) as `下单新客UV`
          ,concat(round(nvl(`新客有效订单量` / `活动页新客UV(t-7)` * 100, 0), 2), '%') as `新客U2O`
          ,nvl(`新客有效单UV`, 0) as `新客有效单UV`
          ,nvl(`新客有效订单量`, 0) as `新客有效订单量`
          ,nvl(`新客有效间夜量`, 0) as `新客有效间夜量`
          ,nvl(round(`新客有效佣金额`, 0), 0) as `新客有效佣金额`
    from (
        select a.`日期`
              ,uv.page_cid
              ,bd_source
              ,page_title
              -- total
              ,count(distinct a.user_name) as `大盘贡献UV(t-7)`
              ,count(distinct orders.user_name) as `有效单UV`
              ,count(distinct orders.order_no) as `有效订单量`
              ,sum(orders.room_night) as `有效间夜量`
              ,sum(orders.`初始返后佣金`) as `有效佣金额`
              -- 新客
              ,count(distinct case when (first_order_date is null or first_order_date >= a.`日期`) then a.user_name else null end) as `活动页新客UV(t-7)`
              ,count(distinct case when (first_order_date is null or first_order_date >= orders.order_date) then orders.user_name else null end) as `下单新客UV`
              ,count(distinct case when (first_order_date is null or first_order_date >= orders.order_date) then orders.user_name end) as `新客有效单UV`
              ,count(distinct case when (first_order_date is null or first_order_date >= orders.order_date) then orders.order_no end) as `新客有效订单量`
              ,sum(case when (first_order_date is null or first_order_date >= orders.order_date) then orders.room_night end) as `新客有效间夜量`
              ,sum(case when (first_order_date is null or first_order_date >= orders.order_date) then orders.`初始返后佣金` end) as `新客有效佣金额`
        from (
            select distinct dt as `日期`
                          ,user_id
                          ,user_name
            from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
            where dt > date_sub(current_date, 15)
                and dt <= date_sub(current_date, 1)
                and business_type = 'hotel'
                and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
                and (search_pv + detail_pv + booking_pv + order_pv) > 0
                and user_name is not null
                and user_name not in ('null', 'NULL', '', ' ')
                and user_id is not null
                and user_id not in ('null', 'NULL', '', ' ')
        ) a
        left join (
            select distinct substr(log_time, 1, 10) as log_date
                          ,page_cid
                          ,page_title
                          ,bd_source
                          ,user_name
            from hotel.dwd_flow_qav_htl_qmark_di a
            inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
            where dt >= date_sub(current_date, 30)
                and dt <= date_sub(current_date, 1)
                and substr(log_time, 1, 10) >= date_sub(current_date, 30)
                and substr(log_time, 1, 10) <= date_sub(current_date, 1)
                and page_url like '%/shark/active%'
                and user_name not like '0000%'
            union
            select distinct dt
                          ,activity_id
                          ,t1.code_name as page_title
                          ,'公共活动页' as bd_source
                          ,user_name
            from marketdatagroup.dwd_market_activity_dt t
            inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
            where dt >= date_sub(current_date, 30)
                and dt <= '%(FORMAT_DATE)s'
            union
            select distinct dt
                          ,page as activity_id
                          ,t1.code_name as page_title
                          ,t1.code_name as bd_source
                          ,username
            from flight.dwd_flow_inter_activity_all_di t
            inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
            where dt >= date_sub(current_date, 30)
                and dt <= '%(FORMAT_DATE)s'
                and username not like '0000%'
            union
            select distinct dt
                          ,page as activity_id
                          ,case when page = '37833' then '国酒&门票五一大促' end as page_title
                          ,case when page = '37833' then '国酒&门票五一大促' end as bd_source
                          ,user_name
            from pp_pub.dwd_flow_qav_www_page_di a
            where dt >= date_sub(current_date, 30)
                and page = '37833'
                and user_name not in ('', ' ', 'null', 'NULL')
        ) uv on a.user_name = uv.user_name
        left join (
            select order_date
                  ,terminal_channel_type
                  ,user_id
                  ,user_name
                  ,batch_series
                  ,coupon_id
                  ,country_name
                  ,city_name
                  ,order_no
                  ,order_status
                  ,checkin_date
                  ,checkout_date
                  ,room_night
                  ,init_room_fee
                  ,coupon_substract
                  ,init_payamount_price
                  ,init_gmv
                  ,init_commission_after
                  ,case when batch_series in ('MacaoDisco_ZK_5e27de', '2night_ZK_952825', '3night_ZK_ad8c83') then (init_commission_after + nvl(coupon_substract, 0))
                        when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0))
                        else init_commission_after
                   end as `初始返后佣金`
            from mdw_order_v3_international
            where dt = '%(DATE)s'
                and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                and terminal_channel_type in ('www', 'app', 'touch')
                and is_valid = '1'
                and order_date between date_sub(current_date, 14) and '%(FORMAT_DATE)s'
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date)   -- 非当日取消订单
                and (first_rejected_time is null or date(first_rejected_time) > order_date)    -- 非当日取消订单
                and (refund_time is null or date(refund_time) > order_date)                    -- 非当日取消订单
        ) orders on a.user_name = orders.user_name and a.`日期` = orders.order_date
        left join (
            select user_name
                  ,min(order_date) as first_order_date
            from mdw_order_v3_international
            where dt = '%(DATE)s'
                and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                and terminal_channel_type in ('www', 'app', 'touch')
                and is_valid = '1'
                and order_status not in ('CANCELLED', 'REJECTED')
            group by 1
        ) new on a.user_name = new.user_name
        where uv.page_cid is not null
            and bd_source is not null
            and datediff(a.`日期`, uv.log_date) between 0 and 7
            and uv.user_name is not null
        group by a.`日期`, uv.page_cid, bd_source, page_title
    ) ord
    order by `活动ID`, `投放渠道`, `日期` desc
)

select ord.page_cid as `活动ID`
      ,ord.page_title as `活动名称`
      ,ord.bd_source as `投放渠道`
      ,ord.log_date as `日期`
      ,nvl(ord.`活动页UV`, 0) as `活动页UV`
      ,nvl(ord.`大盘贡献UV`, 0) as `大盘贡献UV`
      ,concat(round(nvl(ord.`大盘贡献UV` / ord.`活动页UV` * 100, 0), 2), '%') as `活动uv渗透率`
      ,b.`大盘贡献UV(t-7)`
      ,nvl(b.`有效单UV`, 0) as `有效单UV`
      ,nvl(b.`U2O`, 0) as `U2O`
      ,nvl(b.`有效订单量`, 0) as `有效订单量`
      ,nvl(b.`CR`, 0) as `CR`
      ,nvl(b.`有效间夜量`, 0) as `有效间夜量`
      ,nvl(b.`有效佣金额`, 0) as `有效佣金额`
      -- 新客
      ,b.`活动页新客UV(t-7)`
      ,ord.`活动页新客UV`
      ,nvl(b.`活动页新客UV占比`, 0) as `活动页新客UV占比`
      ,nvl(b.`新客U2O`, 0) as `新客U2O`
      ,nvl(b.`新客有效单UV`, 0) as `新客有效单UV`
      ,nvl(b.`新客有效订单量`, 0) as `新客有效订单量`
      ,nvl(b.`新客有效间夜量`, 0) as `新客有效间夜量`
      ,nvl(b.`新客有效佣金额`, 0) as `新客有效佣金额`
from (
    select uv.log_date
          ,uv.page_cid
          ,uv.bd_source
          ,uv.page_title
          -- total
          ,count(distinct uv.user_name) as `活动页UV`
          ,count(distinct case when a.user_name is not null then uv.user_name else null end) as `大盘贡献UV`
          -- 新客
          ,count(distinct case when (new.first_order_date is null or new.first_order_date >= uv.log_date) then uv.user_name else null end) as `活动页新客UV`
    from (
        select distinct substr(log_time, 1, 10) as log_date
                      ,page_cid
                      ,page_title
                      ,bd_source
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 14)
            and dt <= date_sub(current_date, 1)
            and substr(log_time, 1, 10) >= date_sub(current_date, 30)
            and substr(log_time, 1, 10) <= date_sub(current_date, 1)
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,activity_id
                      ,t1.code_name as page_title
                      ,'公共活动页' as bd_source
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,page as activity_id
                      ,t1.code_name as page_title
                      ,t1.code_name as bd_source
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,page as activity_id
                      ,case when page = '37833' then '国酒&门票五一大促' end as page_title
                      ,case when page = '37833' then '国酒&门票五一大促' end as bd_source
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 30)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv
    left join (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a on uv.user_name = a.user_name and uv.log_date = a.`日期`
    left join (
        select user_name
              ,min(order_date) as first_order_date
        from mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and is_valid = '1'
            and order_status not in ('CANCELLED', 'REJECTED')
        group by 1
    ) new on uv.user_name = new.user_name
    where uv.page_cid is not null
        and uv.bd_source is not null
    group by uv.log_date, uv.page_cid, uv.bd_source, uv.page_title
) ord
left join data_t_7 b on ord.page_cid = b.`活动ID`
    and ord.page_title = b.`活动名称`
    and ord.bd_source = b.`投放渠道`
    and ord.log_date = b.`日期`
order by `活动ID`, `投放渠道`, `日期` desc
;




set hive.auto.convert.join = false;
set hive.optimize.correlation = false;
set hive.support.concurrency = false;

-- sql3
with data_t_7 as (
    select a.`日期` as log_date
          ,count(distinct a.user_name) as `大盘贡献UV(t-7)`
          ,count(distinct orders.order_no) as `订单量`
          ,sum(orders.room_night) as `间夜量`
    from (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a
    left join (
        select distinct substr(log_time, 1, 10) as log_date
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
            and substr(log_time, 1, 10) >= date_sub(current_date, 30)
            and substr(log_time, 1, 10) <= date_sub(current_date, 1)
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 30)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv on a.user_name = uv.user_name
    left join (
        select order_date
              ,terminal_channel_type
              ,user_id
              ,user_name
              ,batch_series
              ,coupon_id
              ,country_name
              ,city_name
              ,order_no
              ,order_status
              ,checkin_date
              ,checkout_date
              ,room_night
              ,init_room_fee
              ,coupon_substract
              ,init_payamount_price
              ,init_gmv
              ,init_commission_after
              ,case when batch_series in ('MacaoDisco_ZK_5e27de', '2night_ZK_952825', '3night_ZK_ad8c83') then (init_commission_after + nvl(coupon_substract, 0))
                    when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0))
                    else init_commission_after
               end as `初始返后佣金`
        from mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and is_valid = '1'
            and order_date >= date_sub(current_date, 14)
            and order_date <= date_sub(current_date, 1)
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
            and (first_rejected_time is null or date(first_rejected_time) > order_date)
            and (refund_time is null or date(refund_time) > order_date)
    ) orders on a.user_id = orders.user_id and a.`日期` = orders.order_date
    where datediff(a.`日期`, uv.log_date) between 0 and 7
        and uv.user_name is not null
    group by a.`日期`
)
,orders as (
    select order_date as `日期`
          ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + nvl(ext_plat_certificate, 0))
                    else init_commission_after + nvl(ext_plat_certificate, 0)
               end) as `Q_佣金`
          ,sum(room_night) as `Q_间夜量`
    from mdw_order_v3_international a
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type in ('www', 'app', 'touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and order_date >= date_sub(current_date, 15)
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
    group by order_date
)

select ord.log_date as `日期`
      ,nvl(ord.`活动UV`, 0) as `活动UV`
      ,nvl(`大盘贡献UV`, 0) as `大盘贡献UV`
      ,concat(round(nvl(`大盘贡献UV` / ord.`活动UV` * 100, 0), 2), '%') as `活动uv渗透率`
      ,concat(round(nvl(`大盘贡献UV` / `大盘UV` * 100, 0), 2), '%') as `大盘流量占比`
      ,c.`大盘贡献UV(t-7)`
      ,concat(round(nvl(c.`大盘贡献UV(t-7)` / `大盘UV` * 100, 0), 2), '%') as `大盘流量占比(t-7)`
      ,nvl(c.`订单量`, 0) as `订单量(t-7)`
      ,concat(round(nvl(c.`订单量` / c.`大盘贡献UV(t-7)` * 100, 0), 2), '%') as `CR(t-7)`
      ,nvl(c.`间夜量`, 0) as `间夜量(t-7)`
      ,concat(round(nvl(c.`间夜量` / e.`Q_间夜量` * 100, 0), 2), '%') as `大盘间夜占比(t-7)`
from (
    select uv.log_date
          ,count(distinct uv.user_name) as `活动UV`
          ,count(distinct case when a.user_name is not null then uv.user_name else null end) as `大盘贡献UV`
    from (
        select distinct substr(log_time, 1, 10) as log_date
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 14)
            and dt <= date_sub(current_date, 1)
            and substr(log_time, 1, 10) >= date_sub(current_date, 30)
            and substr(log_time, 1, 10) <= date_sub(current_date, 1)
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 14)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv
    left join (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a on uv.user_name = a.user_name and uv.log_date = a.`日期`
    group by uv.log_date
) ord
left join (
    select dt as `日期`
          ,count(distinct a.user_id) as `大盘UV`
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt > date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and user_name is not null
        and user_name not in ('null', 'NULL', '', ' ')
        and user_id is not null
        and user_id not in ('null', 'NULL', '', ' ')
    group by dt
) duv on ord.log_date = duv.`日期`
left join data_t_7 c on ord.log_date = c.log_date
left join orders e on ord.log_date = e.`日期`
order by `日期` desc
;



set hive.auto.convert.join = false;
set hive.optimize.correlation = false;
set hive.support.concurrency = false;

-- sql4
with data_t_7 as (
    select a.`日期` as log_date
          ,count(distinct a.user_name) as `大盘贡献新客UV(t-7)`
          ,count(distinct orders.order_no) as `新客订单量`
          ,sum(orders.room_night) as `新客间夜量`
    from (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a
    left join (
        select distinct substr(log_time, 1, 10) as log_date
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 30)
            and dt <= date_sub(current_date, 1)
            and substr(log_time, 1, 10) >= date_sub(current_date, 30)
            and substr(log_time, 1, 10) <= date_sub(current_date, 1)
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 30)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 30)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv on a.user_name = uv.user_name
    left join (
        select order_date
              ,terminal_channel_type
              ,user_id
              ,user_name
              ,batch_series
              ,coupon_id
              ,country_name
              ,city_name
              ,order_no
              ,order_status
              ,checkin_date
              ,checkout_date
              ,room_night
              ,init_room_fee
              ,coupon_substract
              ,init_payamount_price
              ,init_gmv
              ,init_commission_after
              ,case when batch_series in ('MacaoDisco_ZK_5e27de', '2night_ZK_952825', '3night_ZK_ad8c83') then (init_commission_after + nvl(coupon_substract, 0))
                    when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0))
                    else init_commission_after
               end as `初始返后佣金`
        from mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and is_valid = '1'
            and order_date >= date_sub(current_date, 14)
            and order_date <= date_sub(current_date, 1)
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
            and (first_rejected_time is null or date(first_rejected_time) > order_date)
            and (refund_time is null or date(refund_time) > order_date)
    ) orders on a.user_id = orders.user_id and a.`日期` = orders.order_date
    left join (
        select user_id
              ,min(order_date) as first_order_date
        from mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and is_valid = '1'
            and order_status not in ('CANCELLED', 'REJECTED')
        group by 1
    ) new on a.user_id = new.user_id
    where (first_order_date is null or first_order_date >= uv.log_date)
        and datediff(a.`日期`, uv.log_date) between 0 and 7
        and uv.user_name is not null
    group by a.`日期`
)
,q_user_type as (
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,orders as (
    select order_date as `日期`
          ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after + nvl(split(coupon_info['23base_ZK_728810'], '_')[1], 0) + nvl(split(coupon_info['23extra_ZK_ce6f99'], '_')[1], 0) + nvl(ext_plat_certificate, 0))
                    else init_commission_after + nvl(ext_plat_certificate, 0)
               end) as `Q_佣金`
          ,sum(room_night) as `Q_间夜量`
    from mdw_order_v3_international a
    left join q_user_type b on a.user_id = b.user_id
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        -- and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
        and terminal_channel_type in ('www', 'app', 'touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and order_date >= date_sub(current_date, 15)
        and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and a.order_date = b.min_order_date
    group by order_date
)

select ord.log_date as `日期`
      ,nvl(ord.`活动新客UV`, 0) as `活动新客UV`
      ,nvl(`大盘贡献新客UV`, 0) as `大盘贡献新客UV`
      ,concat(round(nvl(`大盘贡献新客UV` / ord.`活动新客UV` * 100, 0), 2), '%') as `活动新客uv渗透率`
      ,concat(round(nvl(`大盘贡献新客UV` / `大盘UV` * 100, 0), 2), '%') as `大盘流量占比`
      ,c.`大盘贡献新客UV(t-7)`
      ,concat(round(nvl(c.`大盘贡献新客UV(t-7)` / `大盘UV` * 100, 0), 2), '%') as `大盘流量占比(t-7)`
      --,concat(round(nvl(`大盘贡献新客UV`/`大盘UV`*100,0),2),'%') as `活动新客uv渗透率`
      ,nvl(`新客订单量`, 0) as `新客订单量(t-7)`
      ,concat(round(nvl(`新客订单量` / c.`大盘贡献新客UV(t-7)` * 100, 0), 2), '%') as `新客CR(t-7)`
      ,nvl(`新客间夜量`, 0) as `新客间夜量(t-7)`
      ,concat(round(nvl(`新客间夜量` / e.`Q_间夜量` * 100, 0), 2), '%') as `大盘间夜占比(t-7)`
from (
    select uv.log_date
          ,count(distinct uv.user_name) as `活动新客UV`
          ,count(distinct case when a.user_name is not null then uv.user_name else null end) as `大盘贡献新客UV`
    from (
        select distinct substr(log_time, 1, 10) as log_date
                      ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on a.page_cid = t1.code and t1.type = 'page'
        where dt >= date_sub(current_date, 14)
            and dt <= date_sub(current_date, 1)
            and substr(log_time, 1, 10) >= date_sub(current_date, 30)
            and substr(log_time, 1, 10) <= date_sub(current_date, 1)
            and page_url like '%/shark/active%'
            and user_name not like '0000%'
        union
        select distinct dt
                      ,user_name
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.activity_id = t1.code and t1.type = 'public'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
        union
        select distinct dt
                      ,username
        from flight.dwd_flow_inter_activity_all_di t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1 on t.page = t1.code and t1.type = 'flight'
        where dt >= date_sub(current_date, 14)
            and dt <= '%(FORMAT_DATE)s'
            and username not like '0000%'
        union
        select distinct dt
                      ,user_name
        from pp_pub.dwd_flow_qav_www_page_di a
        where dt >= date_sub(current_date, 14)
            and page = '37833'
            and user_name not in ('', ' ', 'null', 'NULL')
    ) uv
    left join (
        select distinct dt as `日期`
                      ,user_id
                      ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and user_id is not null
            and user_id not in ('null', 'NULL', '', ' ')
    ) a on uv.user_name = a.user_name and uv.log_date = a.`日期`
    left join (
        select user_id
              ,min(order_date) as first_order_date
        from mdw_order_v3_international
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and is_valid = '1'
            and order_status not in ('CANCELLED', 'REJECTED')
        group by 1
    ) new on a.user_id = new.user_id
    where first_order_date is null or first_order_date >= uv.log_date
    group by uv.log_date
) ord
left join (
    select `日期`
          ,count(distinct user_id) as `大盘UV`
    from (
        select dt as `日期`
              ,a.user_id
              ,new.first_order_date
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        left join (
            select user_id
                  ,min(order_date) as first_order_date
            from mdw_order_v3_international
            where dt = '%(DATE)s'
                and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                and terminal_channel_type in ('www', 'app', 'touch')
                and is_valid = '1'
                and order_status not in ('CANCELLED', 'REJECTED')
            group by 1
        ) new on a.user_id = new.user_id
        where dt > date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and user_name is not null
            and user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null
            and a.user_id not in ('null', 'NULL', '', ' ')
    ) new_uv
    where first_order_date is null or first_order_date >= `日期`
    group by `日期`
) duv on ord.log_date = duv.`日期`
left join data_t_7 c on ord.log_date = c.log_date
left join orders e on ord.log_date = e.`日期`
order by `日期` desc
;