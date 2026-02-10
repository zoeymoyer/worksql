--- 1、留存
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
,q_data as (
    select t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct t1.user_id) uv
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  uv7_total
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  / count(distinct t1.user_id) re7_total
    from  uv t1
    left join uv t9  on t1.user_id=t9.user_id  and datediff(t9.dt,t1.dt)  between 1 and 7
    group by t1.dt,cube(t1.mdd,t1.user_type)
)
,c_data as (
    select t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct t1.user_id) c_uv
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  cuv7_total
        ,case when max(datediff(t9.dt,t1.dt))>= 7 then count(distinct t9.user_id) else null end  / count(distinct t1.user_id) cre7_total
    from  c_uv t1
    left join c_uv t9  on t1.user_id=t9.user_id  and datediff(t9.dt,t1.dt)  between 1 and 7
    group by t1.dt,cube(t1.mdd,t1.user_type)   
)

select t1.dt,t1.mdd,t1.user_type,uv,uv7_total,re7_total,c_uv,cuv7_total,cre7_total
from q_data t1
left join c_data t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type

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
    --     SELECT explode(sequence(1, 222)) as n
    -- )
    select startdate
    from temp.temp_dim_dates_0601_0108
)
--- 分新老目的地
select t1.startdate,t1.act_days,q_uv,c_uv,t1.user_type, t1.mdd
from (
    select startdate,act_days,user_type,mdd,count(distinct user_id) q_uv
    from (
        select t1.startdate,user_id,mdd,user_type
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3,4
    )t1 group by 1,2,3,4
)t1 left join (
    select startdate,act_days,user_type,mdd,count(distinct user_id) c_uv
    from (
        select t1.startdate,user_id,mdd,user_type
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3,4
    )t1 group by 1,2,3,4 
)t2 on t1.startdate=t2.startdate and t1.act_days=t2.act_days and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by 1,2
;
--- 分新老目的地平均活跃频次
select t1.startdate,q_uv,c_uv,t1.user_type, t1.mdd
from (
    select startdate,user_type,mdd,count(distinct user_id) q_uv
    from (
        select t1.startdate,user_id,mdd,user_type
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3,4
    )t1 group by 1,2,3
)t1 left join (
    select startdate,user_type,mdd,count(distinct user_id) c_uv
    from (
        select t1.startdate,user_id,mdd,user_type
            ,count(distinct t2.dt) act_days
        from date_range t1
        left join c_uv t2 ON t2.dt >= date_sub(t1.startdate, 6)  --- 取最近6天数据
        AND t2.dt <= t1.startdate 
        group by 1,2,3,4
    )t1 group by 1,2,3
)t2 on t1.startdate=t2.startdate  and t1.mdd=t2.mdd and t1.user_type=t2.user_type
order by 1,2
;
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





---- 4、增量近7日首访
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
,q_info as (--- 增量用户

        select t1.dt,t1.user_id,t1.user_type,t1.mdd
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking7   --- 是否增量用户
            ,case when t3.user_id is not null then 'N' else 'Y' end is_bulking14   --- 是否增量用户
        from uv t1 
        left join uv t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
  		left join uv t3 on t1.user_id=t3.user_id
        and t3.dt < t1.dt and datediff(t1.dt,t3.dt) <= 14
        where t1.dt >= '2025-06-01'
        group by 1,2,3,4,5,6
)
,c_info as (--- 增量用户

        select t1.dt,t1.user_id,t1.user_type,t1.mdd
            ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking7   --- 是否增量用户
            ,case when t3.user_id is not null then 'N' else 'Y' end is_bulking14   --- 是否增量用户
        from c_uv t1 
        left join c_uv t2 on t1.user_id=t2.user_id
        and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
  		left join c_uv t3 on t1.user_id=t3.user_id
        and t3.dt < t1.dt and datediff(t1.dt,t3.dt) <= 14
        where t1.dt >= '2025-06-01'
        group by 1,2,3,4,5,6
)
,q_data as (
    select t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct t1.user_id) uv
        ,count(distinct case when is_bulking7='Y' then t1.user_id end) uv_7d
        ,count(distinct case when is_bulking14='Y' then t1.user_id end) uv_14d
    from  q_info t1
    group by t1.dt,cube(t1.mdd,t1.user_type)
)
,c_data as (
    select t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,count(distinct t1.user_id) c_uv
        ,count(distinct case when is_bulking7='Y' then t1.user_id end) cuv_7d
        ,count(distinct case when is_bulking14='Y' then t1.user_id end) cuv_14d
    from  c_info t1
    group by t1.dt,cube(t1.mdd,t1.user_type)
)

select t1.dt,t1.mdd,t1.user_type,uv,uv_7d,uv_14d,c_uv,cuv_7d,cuv_14d
from q_data t1
left join c_data t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
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