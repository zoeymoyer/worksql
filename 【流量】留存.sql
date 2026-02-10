with user_type -----用户首单日
as (
        select user_id
               ,min(order_date) as min_order_date
               ,count(distinct order_no) history_orders
               ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
        from mdw_order_v3_international   --- 海外订单表
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and order_status not in ('CANCELLED', 'REJECTED')
            and is_valid = '1'
        group by 1
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)

select t1.dt
      ,count(distinct t1.user_id) uv
      ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  uv1
      ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  uv7
      ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  uv14
      ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  uv30
      ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  uv60
      ,case when max(datediff(t8.dt,t1.dt))>= 90 then count(distinct t8.user_id) else null end  uv90
      ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
      ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  / count(distinct t1.user_id) re1
      ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  / count(distinct t1.user_id) re7
      ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  / count(distinct t1.user_id) re14
      ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  / count(distinct t1.user_id) re30
      ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  / count(distinct t1.user_id) re60
      ,case when max(datediff(t8.dt,t1.dt))>= 90 then count(distinct t8.user_id) else null end  / count(distinct t1.user_id) re90
      ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180
from  uv t1
left join uv t2 on t1.user_id=t2.user_id and datediff(t2.dt,t1.dt) = 1
left join uv t3 on t1.user_id=t3.user_id and datediff(t3.dt,t1.dt) = 7
left join uv t4 on t1.user_id=t4.user_id and datediff(t4.dt,t1.dt) = 14
left join uv t5 on t1.user_id=t5.user_id and datediff(t5.dt,t1.dt) = 30
left join uv t6 on t1.user_id=t6.user_id and datediff(t6.dt,t1.dt) = 60
left join uv t8 on t1.user_id=t8.user_id and datediff(t8.dt,t1.dt) = 90
left join uv t7 on t1.user_id=t7.user_id and datediff(t7.dt,t1.dt) = 180
group by 1
order by t1.dt 
;



select sum(order_no) / count(1)
from (
    select o.user_id,count(distinct order_no) order_no
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
        and terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(order_date,1,10) >= '2025-01-01'
        and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1
)
