--- 1、国酒流量留存
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
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-06-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,c_user_type as
(   --- 用于判定c新老客
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
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid user_id
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-06-01' and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,q_uv_data as (
    select t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct t1.user_id) uv
        ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  uv1
        ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  uv7
        -- ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  uv14
        -- ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  uv30
        -- ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  uv60
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
        ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  / count(distinct t1.user_id) re1
        ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  / count(distinct t1.user_id) re7
        -- ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  / count(distinct t1.user_id) re14
        -- ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  / count(distinct t1.user_id) re30
        -- ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  / count(distinct t1.user_id) re60
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180

        ,case when max(datediff(t8.dt,t1.dt))>= 1 then count(distinct t8.user_id) else null end  uv1_total
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  uv7_total
        -- ,case when max(datediff(t10.dt,t1.dt))>= 14 then count(distinct t10.user_id) else null end  uv14_total
        -- ,case when max(datediff(t11.dt,t1.dt))>= 30 then count(distinct t11.user_id) else null end  uv30_total
        -- ,case when max(datediff(t12.dt,t1.dt))>= 60 then count(distinct t12.user_id) else null end  uv60_total
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
        ,case when max(datediff(t8.dt,t1.dt))>= 1 then count(distinct t8.user_id) else null end  / count(distinct t1.user_id) re1_total
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  / count(distinct t1.user_id) re7_total
        -- ,case when max(datediff(t10.dt,t1.dt))>= 14 then count(distinct t10.user_id) else null end  / count(distinct t1.user_id) re14_total
        -- ,case when max(datediff(t11.dt,t1.dt))>= 30 then count(distinct t11.user_id) else null end  / count(distinct t1.user_id) re30_total
        -- ,case when max(datediff(t12.dt,t1.dt))>= 60 then count(distinct t12.user_id) else null end  / count(distinct t1.user_id) re60_total
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180
    from  uv t1
    left join uv t2 on t1.user_id=t2.user_id and datediff(t2.dt,t1.dt) = 1
    left join uv t3 on t1.user_id=t3.user_id and datediff(t3.dt,t1.dt) = 7
    -- left join uv t4 on t1.user_id=t4.user_id and datediff(t4.dt,t1.dt) = 14
    -- left join uv t5 on t1.user_id=t5.user_id and datediff(t5.dt,t1.dt) = 30
    -- left join uv t6 on t1.user_id=t6.user_id and datediff(t6.dt,t1.dt) = 60
    -- left join uv t7 on t1.user_id=t7.user_id and datediff(t7.dt,t1.dt) = 180
    left join uv t8  on t1.user_id=t8.user_id  and datediff(t8.dt,t1.dt)  between 1 and 1 
    left join uv t9  on t1.user_id=t9.user_id  and datediff(t9.dt,t1.dt)  between 1 and 7
    -- left join uv t10 on t1.user_id=t10.user_id and datediff(t10.dt,t1.dt) between 0 and 14
    -- left join uv t11 on t1.user_id=t11.user_id and datediff(t11.dt,t1.dt) between 0 and 30
    -- left join uv t12 on t1.user_id=t12.user_id and datediff(t12.dt,t1.dt) between 0 and 60
    group by t1.dt,cube(t1.mdd,t1.user_type)
)

,c_uv_data as (
    select t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct t1.user_id) c_uv
        ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  c_uv1
        ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  c_uv7
        -- ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  c_uv14
        -- ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  c_uv30
        -- ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  c_uv60
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
        ,case when max(datediff(t2.dt,t1.dt))>= 1 then count(distinct t2.user_id) else null end  / count(distinct t1.user_id) c_re1
        ,case when max(datediff(t3.dt,t1.dt))>= 7 then count(distinct t3.user_id) else null end  / count(distinct t1.user_id) c_re7
        -- ,case when max(datediff(t4.dt,t1.dt))>= 14 then count(distinct t4.user_id) else null end  / count(distinct t1.user_id) c_re14
        -- ,case when max(datediff(t5.dt,t1.dt))>= 30 then count(distinct t5.user_id) else null end  / count(distinct t1.user_id) c_re30
        -- ,case when max(datediff(t6.dt,t1.dt))>= 60 then count(distinct t6.user_id) else null end  / count(distinct t1.user_id) c_re60
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180

        ,case when max(datediff(t8.dt,t1.dt))>= 1 then count(distinct t8.user_id) else null end  c_uv1_total
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  c_uv7_total
        -- ,case when max(datediff(t10.dt,t1.dt))>= 14 then count(distinct t10.user_id) else null end  c_uv14_total
        -- ,case when max(datediff(t11.dt,t1.dt))>= 30 then count(distinct t11.user_id) else null end  c_uv30_total
        -- ,case when max(datediff(t12.dt,t1.dt))>= 60 then count(distinct t12.user_id) else null end  c_uv60_total
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  uv180
        ,case when max(datediff(t8.dt,t1.dt))>= 1 then count(distinct t8.user_id) else null end  / count(distinct t1.user_id) c_re1_total
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  / count(distinct t1.user_id) c_re7_total
        -- ,case when max(datediff(t10.dt,t1.dt))>= 14 then count(distinct t10.user_id) else null end  / count(distinct t1.user_id) c_re14_total
        -- ,case when max(datediff(t11.dt,t1.dt))>= 30 then count(distinct t11.user_id) else null end  / count(distinct t1.user_id) c_re30_total
        -- ,case when max(datediff(t12.dt,t1.dt))>= 60 then count(distinct t12.user_id) else null end  / count(distinct t1.user_id) c_re60_total
        --   ,case when max(datediff(t7.dt,t1.dt))>= 180 then count(distinct t7.user_id) else null end  / count(distinct t1.user_id) re180
    from  c_uv t1
    left join c_uv t2 on t1.user_id=t2.user_id and datediff(t2.dt,t1.dt) = 1
    left join c_uv t3 on t1.user_id=t3.user_id and datediff(t3.dt,t1.dt) = 7
    -- left join c_uv t4 on t1.user_id=t4.user_id and datediff(t4.dt,t1.dt) = 14
    -- left join c_uv t5 on t1.user_id=t5.user_id and datediff(t5.dt,t1.dt) = 30
    -- left join c_uv t6 on t1.user_id=t6.user_id and datediff(t6.dt,t1.dt) = 60
    -- left join c_uv t7 on t1.user_id=t7.user_id and datediff(t7.dt,t1.dt) = 180
    left join c_uv t8  on t1.user_id=t8.user_id  and datediff(t8.dt,t1.dt)  between 1 and 1 
    left join c_uv t9  on t1.user_id=t9.user_id  and datediff(t9.dt,t1.dt)  between 1 and 7
    -- left join c_uv t10 on t1.user_id=t10.user_id and datediff(t10.dt,t1.dt) between 0 and 14
    -- left join c_uv t11 on t1.user_id=t11.user_id and datediff(t11.dt,t1.dt) between 0 and 30
    -- left join c_uv t12 on t1.user_id=t12.user_id and datediff(t12.dt,t1.dt) between 0 and 60
    group by t1.dt,cube(t1.mdd,t1.user_type)
)


select t1.dt,t1.mdd,t1.user_type
       ,uv,c_uv,uv/c_uv uv_qc
       ,re1,c_re1,re1/c_re1 re1_qc
       ,re7,c_re7,re7/c_re7 re7_qc
    --    ,re14,c_re14,re14/c_re14 re14_qc
    --    ,re30,c_re30,re30/c_re30 re30_qc
       ,re7_total,c_re7_total,re7_total/c_re7_total  re7_total_qc
    --    ,re14_total,c_re14_total,re14_total/c_re14_total re14_total_qc
    --    ,re30_total,c_re30_total,re30_total/c_re30_total re30_total_qc
from q_uv_data t1 
left join c_uv_data t2 on t1.dt=t2.dt and t1.user_type=t2.user_type and t1.mdd=t2.mdd
order by t1.dt ,case when mdd = '香港'  then 1
           when mdd = '澳门'  then 2
           when mdd = '泰国'  then 3
           when mdd = '日本'  then 4
           when mdd = '韩国'  then 5
           when mdd = '马来西亚'  then 6
           when mdd = '新加坡'  then 7
           when mdd = '美国'  then 8
           when mdd = '印度尼西亚'  then 9
           when mdd = '俄罗斯'  then 10
           when mdd = '欧洲'  then 11
           when mdd = '亚太'  then 12
           when mdd = '美洲'  then 13
           when mdd = '其他'  then 14
           when mdd = 'ALL'  then 0
      end asc
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when  user_type = '老客' then 3 end asc
;



--- 2、活跃度 DAU/WAU
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
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,c_user_type as
(   --- 用于判定c新老客
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
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid user_id
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-05-01' and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,date_range as (   ---- 生成要看的时间序列
    -- select date_sub(current_date, n) startdate
    -- from (
    --     SELECT explode(sequence(1, 88)) as n
    -- )
    select startdate
    from temp.temp_dim_dates_0601_0105
)

--- 分用户类型wau、dau
select t1.startdate,t1.user_type,Q_WAU,C_WAU,dau,c_dau
from (---Q_WAU
    select t1.startdate,user_type
        ,count(distinct t2.user_id) Q_WAU
    from date_range t1
    left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
    AND t2.dt <= t1.startdate 
    group by 1,2
) t1 
left join (---C_WAU
    select t1.startdate,user_type
        ,count(distinct t2.user_id) C_WAU
    from date_range t1
    left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
    AND t2.dt <= t1.startdate 
    group by 1,2
)t3 on t1.startdate=t3.startdate and t1.user_type=t3.user_type
left join (---DAU
    select t1.dt,t1.user_type,dau,c_dau
    from (
        select dt,user_type,count(distinct user_name) dau
        from uv
        group by 1,2
    )t1 left join (
        select dt,user_type,count(distinct user_id) c_dau
        from c_uv
        group by 1,2
    )t2 on t1.dt=t2.dt and t1.user_type=t2.user_type
)t2 on t1.startdate=t2.dt and t1.user_type=t2.user_type
order by 1
;
--- 分目的地wau、dau
select t1.startdate,t1.mdd,Q_WAU,C_WAU,dau,c_dau
from (---Q_WAU
    select t1.startdate,mdd
        ,count(distinct t2.user_id) Q_WAU
    from date_range t1
    left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
    AND t2.dt <= t1.startdate 
    group by 1,2
) t1 
left join (---C_WAU
    select t1.startdate,mdd
        ,count(distinct t2.user_id) C_WAU
    from date_range t1
    left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
    AND t2.dt <= t1.startdate 
    group by 1,2
)t3 on t1.startdate=t3.startdate and t1.mdd=t3.mdd
left join (---DAU
    select t1.dt,t1.mdd,dau,c_dau
    from (
        select dt,mdd,count(distinct user_name) dau
        from uv
        group by 1,2
    )t1 left join (
        select dt,mdd,count(distinct user_id) c_dau
        from c_uv
        group by 1,2
    )t2 on t1.dt=t2.dt and t1.mdd=t2.mdd
)t2 on t1.startdate=t2.dt and t1.mdd=t2.mdd
order by 1
;


--- 3、活跃频次分布
with user_type -----用户首单日
as (
        select user_id
               ,min(order_date) as min_order_date
               ,count(distinct order_no) history_orders
               ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end) yj_all
        from default.mdw_order_v3_international   --- 海外订单表
        where dt = '%(DATE)s'
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and terminal_channel_type in ('www', 'app', 'touch')
            and order_status not in ('CANCELLED', 'REJECTED')
            and is_valid = '1'
        group by 1
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2025-05-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,c_user_type as
(   --- 用于判定c新老客
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
,c_uv as
(   --- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid user_id
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= '2025-05-01' and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,date_range as (   ---- 生成要看的时间序列
    -- select date_sub(current_date, n) startdate
    -- from (
    --     SELECT explode(sequence(1, 219)) as n
    -- )
    select startdate
    from temp.temp_dim_dates_0601_0105
)
---- 整体
select t1.startdate,t1.act_days,q_uv,c_uv,'ALL' user_type,'ALL' mdd
from (
    select startdate,act_days,count(distinct user_id) q_uv
    from (
        select t1.startdate,user_id
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2
    )t1 group by 1,2
)t1 left join (
    select startdate,act_days,count(distinct user_id) c_uv
    from (
        select t1.startdate,user_id
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2
    )t1 group by 1,2   
)t2 on t1.startdate=t2.startdate and t1.act_days=t2.act_days
order by 1,2
;

--- 分新老
select t1.startdate,t1.act_days,q_uv,c_uv,t1.user_type,'ALL' mdd
from (
    select startdate,act_days,user_type,count(distinct user_id) q_uv
    from (
        select t1.startdate,user_id,user_type
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3
    )t1 group by 1,2,3
)t1 left join (
    select startdate,act_days,user_type,count(distinct user_id) c_uv
    from (
        select t1.startdate,user_id,user_type
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3
    )t1 group by 1,2,3 
)t2 on t1.startdate=t2.startdate and t1.act_days=t2.act_days and t1.user_type=t2.user_type
order by 1,2
;

--- 分目的地
select t1.startdate,t1.act_days,q_uv,c_uv,'ALL' user_type, t1.mdd
from (
    select startdate,act_days,mdd,count(distinct user_id) q_uv
    from (
        select t1.startdate,user_id,mdd
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3
    )t1 group by 1,2,3
)t1 left join (
    select startdate,act_days,mdd,count(distinct user_id) c_uv
    from (
        select t1.startdate,user_id,mdd
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3
    )t1 group by 1,2,3 
)t2 on t1.startdate=t2.startdate and t1.act_days=t2.act_days and t1.mdd=t2.mdd
order by 1,2
;

