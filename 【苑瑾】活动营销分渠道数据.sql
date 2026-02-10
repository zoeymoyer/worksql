
-- sql2
with data_t_7 as (
    select page_cid as `活动ID` 
        ,page_title as `活动名称`
        ,bd_source as `投放渠道` 
        ,`日期`
        ,`大盘贡献UV(t-7)`
        ,nvl(`有效单UV`,0) as `有效单UV` 
        ,concat(round(nvl(`有效单UV`/`大盘贡献UV(t-7)`*100,0),2),'%') as `U2O`
        ,nvl(`有效订单量`,0) as `有效订单量`
        ,concat(round(nvl(`有效订单量`/`大盘贡献UV(t-7)`*100,0),2),'%') as `CR`
        ,nvl(`有效间夜量`,0) as `有效间夜量` 
        ,nvl(round(`有效佣金额`,0),0) as `有效佣金额`
        -- 新客
        ,nvl(`活动页新客UV(t-7)`,0) as `活动页新客UV(t-7)` 
        ,concat(round(nvl(`活动页新客UV(t-7)`/`大盘贡献UV(t-7)`*100,0),2),'%') as `活动页新客UV占比`
        ,nvl(`下单新客UV`,0) as `下单新客UV` 
        ,concat(round(nvl(`新客有效订单量`/`活动页新客UV(t-7)`*100,0),2),'%') as `新客U2O`
        ,nvl(`新客有效单UV`,0) as `新客有效单UV` 
        ,nvl(`新客有效订单量`,0) as `新客有效订单量` 
        ,nvl(`新客有效间夜量`,0) as `新客有效间夜量` 
        ,nvl(round(`新客有效佣金额`,0),0) as `新客有效佣金额`
    from(
        select a.`日期` ,uv.page_cid ,bd_source,page_title
            -- total
            ,count(distinct a.user_name) as `大盘贡献UV(t-7)`
            ,count(distinct orders.user_name ) as `有效单UV`
            ,count(distinct orders.order_no ) as `有效订单量`
            ,sum(orders.room_night ) as `有效间夜量`
            ,sum(orders.`初始返后佣金`) as `有效佣金额`
            -- 新客
            ,count(distinct case when (first_order_date is null or first_order_date>=a.`日期`) then a.user_name else null end) as `活动页新客UV(t-7)`
            ,count(distinct case when (first_order_date is null or first_order_date>=orders.order_date) then orders.user_name else null end) as `下单新客UV`
            ,count(distinct case when (first_order_date is null or first_order_date>=orders.order_date)  then orders.user_name end) as `新客有效单UV`
            ,count(distinct case when (first_order_date is null or first_order_date>=orders.order_date)  then orders.order_no end) as `新客有效订单量`
            ,sum(case when (first_order_date is null or first_order_date>=orders.order_date)  then orders.room_night end) as `新客有效间夜量`
            ,sum(case when (first_order_date is null or first_order_date>=orders.order_date)  then orders.`初始返后佣金` end) as `新客有效佣金额`
        from(
            select distinct
                dt as `日期`
                ,user_id
                ,user_name
            from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
            where   dt > date_sub(current_date, 60) and dt<= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
            and (search_pv + detail_pv + booking_pv + order_pv)>0
            and user_name is not null and user_name not in ('null','NULL','',' ')
            and user_id is not null and user_id not in ('null','NULL','',' ')
        )a 
        left join (
            select distinct 
                substr(log_time,1,10) as log_date 
                ,page_cid
                ,page_title
                ,bd_source
                ,user_name
            from hotel.dwd_flow_qav_htl_qmark_di a
            inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
            on a.page_cid = t1.code and t1.type = 'page'
            where dt>=date_sub(current_date, 60) and dt<=date_sub(current_date, 1)
                and substr(log_time,1,10)>=date_sub(current_date, 60) and substr(log_time,1,10)<=date_sub(current_date, 1)
                and page_url like '%/shark/active%'
                and user_name not like'0000%'
            union
            select distinct dt
                    ,activity_id
                    ,t1.code_name as page_title
                    ,'公共活动页' as bd_source
                    ,user_name 
            from marketdatagroup.dwd_market_activity_dt t
            inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
            on t.activity_id = t1.code and t1.type = 'public'
            where dt>=date_sub(current_date, 60) and dt<=date_sub(current_date, 1)
            union 
            select distinct dt,
                page as activity_id,
                t1.code_name as page_title,
                t1.code_name as bd_source,
                username 
            from flight.dwd_flow_inter_activity_all_di t 
            inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
            on t.page = t1.code and t1.type = 'flight'
            where dt>=date_sub(current_date, 60) and dt<=date_sub(current_date, 1)
            and username not like'0000%'
        )uv on a.user_name=uv.user_name
        left join  (
            select order_date ,terminal_channel_type ,user_id ,user_name ,batch_series ,coupon_id ,country_name ,city_name
                ,order_no ,order_status
                ,checkin_date ,checkout_date ,room_night
                ,init_room_fee ,coupon_substract ,init_payamount_price ,init_gmv ,init_commission_after
                ,case when  batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') then (init_commission_after+nvl(coupon_substract,0))
                when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0))
                else init_commission_after end as `初始返后佣金`
            from mdw_order_v3_international
            where dt='%(DATE)s'
                and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
                and terminal_channel_type in ('www','app','touch') and is_valid='1'
                and order_date between date_sub(current_date, 60) and date_sub(current_date, 1)
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date)  -- 非当日取消订单
                and (first_rejected_time is null or date(first_rejected_time) > order_date)  -- 非当日取消订单
                and (refund_time is null or date(refund_time) > order_date) -- 非当日取消订单
        )orders on a.user_name=orders.user_name and a.`日期`=orders.order_date
        left join(
            select user_name ,min(order_date) as first_order_date
            from mdw_order_v3_international
            where dt='%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
            and terminal_channel_type in ('www','app','touch') and is_valid='1'
            and order_status not in ('CANCELLED','REJECTED')
            group by 1
        )new on a.user_name=new.user_name
        where uv.page_cid is not null and bd_source is not null and datediff(a.`日期`,uv.log_date) = 0 and uv.user_name is not null
        group by 1,2,3,4
    )ord
    order by `活动ID`,`投放渠道`,`日期` desc
)


select page_cid as `活动ID` 
      ,page_title as `活动名称`
      ,bd_source as `投放渠道` 
      ,log_date as `日期`
      ,nvl(`活动页UV`,0) as `活动页UV` 
      ,nvl(`大盘贡献UV`,0) as `大盘贡献UV`
    --   ,concat(round(nvl(`大盘贡献UV`/`活动页UV`*100,0),2),'%') `活动uv渗透率`
      ,b.`大盘贡献UV(t-7)`
      ,nvl(b.`有效单UV`,0) as `有效单UV` 
    --   ,nvl(b.`U2O`,0) as `U2O`
      ,nvl(b.`有效订单量`,0) as `有效订单量`
      ,nvl(b.`CR`,0) as `CR`
      ,nvl(b.`有效间夜量`,0) as `有效间夜量` 
      ,nvl(b.`有效佣金额`,0) as `有效佣金额`
      -- 新客
      ,b.`活动页新客UV(t-7)` 
      ,`活动页新客UV` 
    --   ,nvl(b.`活动页新客UV占比`,0) as `活动页新客UV占比`
    --   ,nvl(b.`新客U2O`,0)  as `新客U2O`
      ,nvl(b.`新客有效单UV`,0) as `新客有效单UV` 
      ,nvl(b.`新客有效订单量`,0) as `新客有效订单量` 
      ,nvl(b.`新客有效间夜量`,0) as `新客有效间夜量` 
      ,nvl(b.`新客有效佣金额`,0) as `新客有效佣金额`
from(
    select log_date ,uv.page_cid ,bd_source,page_title
        -- total
        ,count(distinct uv.user_name) as `活动页UV`
        ,count(distinct case when a.user_name is not null then uv.user_name else null end ) as `大盘贡献UV`
        -- 新客
        ,count(distinct case when (first_order_date is null or first_order_date>=uv.log_date) then uv.user_name else null end) as `活动页新客UV`
    from(
        select distinct substr(log_time,1,10) as log_date 
            ,page_cid
            ,page_title
            ,bd_source
            ,user_name
        from hotel.dwd_flow_qav_htl_qmark_di a
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on a.page_cid = t1.code and t1.type = 'page'
        where dt>=date_sub(current_date, 60) and dt<=date_sub(current_date, 1)
            and substr(log_time,1,10)>=date_sub(current_date, 60) and substr(log_time,1,10)<=date_sub(current_date, 1)
            and page_url like '%/shark/active%'
            and user_name not like'0000%'
        union
        select distinct dt
            ,activity_id
            ,t1.code_name as page_title
            ,'公共活动页' as bd_source
            ,user_name 
        from marketdatagroup.dwd_market_activity_dt t
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.activity_id = t1.code and t1.type = 'public'
        where dt>=date_sub(current_date, 60) and dt<=date_sub(current_date, 1)
        union 
        select distinct dt,
            page as activity_id,
            t1.code_name as page_title,
            t1.code_name as bd_source,
            username 
        from flight.dwd_flow_inter_activity_all_di t 
        inner join temp.temp_xuejing_lu_user_active_code_yxhd_forever t1
        on t.page = t1.code and t1.type = 'flight'
        where dt>=date_sub(current_date, 60) and dt<=date_sub(current_date, 1)
           and username not like'0000%'
      )uv
    left join(
        select 
            distinct
            dt as `日期`
            ,user_id
            ,user_name
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        where  dt> date_sub(current_date, 60) and dt<= date_sub(current_date, 1)
          and business_type = 'hotel'
          and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
          and (search_pv + detail_pv + booking_pv + order_pv)>0
          and user_name is not null and user_name not in ('null','NULL','',' ')
          and user_id is not null and user_id not in ('null','NULL','',' ')
      )a on uv.user_name=a.user_name and uv.log_date =a.`日期`
  	left join(
        select user_name ,min(order_date) as first_order_date
        from mdw_order_v3_international
        where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
        group by 1
      )new on uv.user_name=new.user_name
    where uv.page_cid is not null and bd_source is not null
    group by 1,2,3,4
)ord
left join data_t_7 b on ord.page_cid=b.`活动ID` and ord.page_title = b.`活动名称` and ord.bd_source = b.`投放渠道` and ord.log_date=b.`日期`
order by `活动ID`,`投放渠道`,`日期` desc
;