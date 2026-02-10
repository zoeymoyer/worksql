with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
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
    where dt >= '2025-12-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5 
)
,q_uv as (
    select dt,mdd,user_type,user_id,user_name,max(action_time)action_time
    from (
        select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id,user_name,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join user_type b on a.user_id = b.user_id 
        where dt >='2025-12-01'
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and action_entrance_map['fromforlog'] in ('4104','4106') 
        group by 1,2,3,4,5,6

        union all

        select dt
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id,user_name,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time 
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        left join user_type b on a.user_id = b.user_id 
        where dt >= '2025-12-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and action_entrance_map['fromforlog'] in ('4104','4106')
        group by 1,2,3,4,5,6 
    )t group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,order_time
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-12-01'
        and order_no <> '103576132435'
)

select t1.dt
      ,uv    --- 引流UV
      ,nuv   --- 新客
      ,uv / all_uv uv_rate  --- 引流占比
      ,nuv / all_nuv nuv_rate  --- 新客占比
      ----- t7
      ,order_no    --- 订单量
      ,order_no_nu     --- 新客订单量
      ,order_no / order_no_all  order_no_rate  --- 订单量占比
      ,order_no_nu / order_no_nu_all order_no_nu_rate  --- 新客订单量占比

      ,room_night  --- 间夜量
      ,room_night_nu   --- 新客间夜量
      ,order_no_conpon  -- 用券订单量
      ,order_no_conpon_nu  -- 新客用券订单量
      ,order_no/uv cr
      ,order_no_nu/nuv    ncr --- 新客cr
      ,order_uv     --- 生单uv
      ,order_uv_nu
      ,yj      ---- 佣金
      ,yj_nu
      ,gmv    
      ,gmv_nu
      ,qe     --- 券额
      ,qe_nu

      ---- t0
      ,order_no_t0
      ,order_uv_t0
      ,room_night_t0
      ,yj_t0
      ,gmv_t0
      ,qe_t0
      ,order_no_conpon_t0

      ,order_no_nu_t0
      ,order_uv_nu_t0
      ,room_night_nu_t0
      ,yj_nu_t0
      ,gmv_nu_t0
      ,qe_nu_t0
      ,order_no_conpon_nu_t0

      ,all_uv
      ,all_nuv
      ,order_no_all
      ,order_uv_all
      ,room_night_all
      ,yj_all
      ,gmv_all
      ,order_no_conpon_all

      ,order_no_nu_all
      ,order_uv_nu_all
      ,room_night_nu_all
      ,yj_nu_all
      ,gmv_nu_all
      ,order_no_conpon_nu_all
      
from (
    select dt
          ,count(user_id) uv
          ,count(case when user_type = '新客' then user_id end) nuv
    from q_uv
    group by 1
)t1 
left join (--- T7
    select order_date
          ,count(distinct t1.order_no) order_no
          ,count(distinct t1.user_id) order_uv
          ,sum(t1.room_night) room_night
          ,sum(final_commission_after) yj
          ,sum(init_gmv) gmv
          ,sum(coupon_substract_summary) qe
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon

          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu
          ,count(distinct case when user_type = '新客' then t1.user_id end) order_uv_nu
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu
          ,sum(case when user_type = '新客' then t1.final_commission_after end) yj_nu
          ,sum(case when user_type = '新客' then t1.init_gmv end) gmv_nu
          ,sum(case when user_type = '新客' then t1.coupon_substract_summary end) qe_nu
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu
    from q_app_order t1 
    left join (
        select dt,user_id
        from q_uv 
        group by 1,2
    )t2 on t1.user_id=t2.user_id
      where t2.user_id is not null and t2.dt >= date_sub(t1.order_date, 7) and t2.dt <= t1.order_date
    group by 1
)t2 on t1.dt=t2.order_date
left join (--- T0
    select order_date
          ,count(distinct t1.order_no) order_no_t0
          ,count(distinct t1.user_id) order_uv_t0
          ,sum(t1.room_night) room_night_t0
          ,sum(t1.final_commission_after) yj_t0
          ,sum(t1.init_gmv) gmv_t0
          ,sum(t1.coupon_substract_summary) qe_t0
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon_t0

          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu_t0
          ,count(distinct case when user_type = '新客' then t1.user_id end) order_uv_nu_t0
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu_t0
          ,sum(case when user_type = '新客' then t1.final_commission_after end) yj_nu_t0
          ,sum(case when user_type = '新客' then t1.init_gmv end) gmv_nu_t0
          ,sum(case when user_type = '新客' then t1.coupon_substract_summary end) qe_nu_t0
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu_t0
    from q_app_order t1 
    left join (
        select dt,user_id,action_time
        from q_uv 
        group by 1,2,3
    )t2 on t1.user_id=t2.user_id and t1.order_date=t2.dt and t1.order_time > t2.action_time
    where t2.user_id is not null
    group by 1
)t3 on t1.dt=t3.order_date
left join (---- 大盘DAU
    select dt
          ,count(user_id) all_uv
          ,count(case when user_type = '新客' then user_id end) all_nuv
    from uv
    group by 1
)t4 on t1.dt=t4.dt
left join (--- 大盘订单 APP端
    select order_date
          ,count(distinct t1.order_no) order_no_all
          ,count(distinct t1.user_id) order_uv_all
          ,sum(t1.room_night) room_night_all
          ,sum(t1.final_commission_after) yj_all
          ,sum(t1.init_gmv) gmv_all
          ,sum(t1.coupon_substract_summary) qe_all
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon_all

          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu_all
          ,count(distinct case when user_type = '新客' then t1.user_id end) order_uv_nu_all
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu_all
          ,sum(case when user_type = '新客' then t1.final_commission_after end) yj_nu_all
          ,sum(case when user_type = '新客' then t1.init_gmv end) gmv_nu_all
          ,sum(case when user_type = '新客' then t1.coupon_substract_summary end) qe_nu_all
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu_all
    from q_app_order t1 
    group by 1
) t5 on t1.dt=t5.order_date
order by 1
;