with user_type as
(
    select user_id ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and terminal_channel_type in ('www','app','touch') and is_valid='1'
    and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,init_uv as
(
    select dt as dates 
            ,user_name
            ,a.user_id
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type 
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join user_type b on a.user_id=b.user_id
    where dt >=  '2024-01-01' and dt <= '%(FORMAT_DATE)s'
    and business_type = 'hotel'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and (search_pv+detail_pv+booking_pv+order_pv)>0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,red as (
    select distinct flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2023-11-01' and '%(FORMAT_DATE)s'
    --and business_type = 'hotel-inter'  --宽口径不需要这个
    and query_platform = 'redbook'
)
,post_amount as
(
    select `投放日期`                                            
         ,sum(cost)/100 as `投放金额` 
         ,sum(cost)*1.000 / 100 *(1-avg(cost_rate))/1.06 as `实际消耗金额`
         ,count(distinct case when cost <> 0 then note_id else null end) as `帖子在线量`              
         ,sum(view_count) as `帖子曝光量`                 
         ,sum(valid_click_count) as `帖子点击量`                      
    from(
        select distinct `投放日期`, a.note_id , cost, ad_name, view_count, valid_click_count,cost_rate
        from(
            select cost_dt as `投放日期` , note_id, cost, ad_name, view_count ,valid_click_count,cost_rate
            from pp_pub.dwd_redbook_spotlight_creative_cost_info_da
            where dt = '%(FORMAT_DATE)s'                                                 --全量快照,取最新一天T-1数据
            and cost_dt>='2024-01-01' and cost_dt<=date_sub(current_date,1)                                
        )a
        --关联国酒帖子
        join( 
            select distinct note_id           
            from pp_pub.dwd_redbook_notes_detail_nd
            where dt = '%(FORMAT_DATE)s'                                                --全量快照,取最新一天T-1数据
            and query is not null
            and note_id is not null
            and note_busi = 'hotel-inter'
        )b on a.note_id = b.note_id
    )cost_distinct
    group by 1
)
,order_a as
(
    select order_date ,user_id,order_no ,room_night ,init_gmv ,
    case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                else init_commission_after+nvl(ext_plat_certificate,0) end as final_commission_after
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and terminal_channel_type in ('www','app','touch') and is_valid='1'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
    and (first_rejected_time is null or date(first_rejected_time) > order_date)
    and (refund_time is null or date(refund_time) > order_date)
    and order_date>='2024-09-01' and order_date<=date_sub(current_date,1)
    and order_no <> '103576132435'
)
,platform_new as (--- 判定平台新
    select  dt,
            user_pk,
            user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-01-01'
        and dict_type = 'pncl_wl_username'
    group by 1,2,3
)
,uv as ( --- uv分维度
    select t1.dates,t1.user_id,t1.user_name,t1.user_type
        ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
        end as user_type1
    from init_uv t1 
    left join platform_new t2  on t1.dates=t2.dt and t1.user_name=t2.user_pk
)
,nu_all as (
    select dates,count(distinct user_name) nu_all
    from uv
    where user_type = '新客'
    group by 1
)


select t1.dates
      ,`引流新客UV`
      ,`引流平台新客UV`
      ,nu_all
      ,`引流新客UV` /  nu_all `新客占比`
      ,`投放金额` 
      ,`实际消耗金额` 
      ,`帖子在线量` 
      ,`帖子曝光量` 
      ,`帖子点击量` 
from (
    select uv.dates 
            ,count(distinct case when user_type = '新客' then uv.user_id end) as `引流新客UV`
            ,count(distinct case when user_type1 = '平台新业务新' then uv.user_id end) as `引流平台新客UV`
    from
    (
        select distinct uv.dates,uv.user_id,user_type,user_type1
        from uv 
        left join red r on uv.user_name = r.user_name
        where r.dt >= date_sub(dates, 7) and r.dt <= uv.dates and r.user_name is not null
    ) uv
    left join order_a ord on uv.user_id = ord.user_id
    and uv.dates = ord.order_date
    group by 1
)t1
left join nu_all t2 on t1.dates=t2.dates
left join post_amount t3 on t1.dates=t3.`投放日期`
order by 1
;  
