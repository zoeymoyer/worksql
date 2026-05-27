with daily_active as (-- 1. 抽取每日活跃用户的底层明细（先做日级别去重，提升后续裂变性能）
    select dt
         ,a.user_id
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e  on a.country_name = e.country_name
    where dt >= date_sub(current_date, 60) 
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null 
        and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null 
        and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2
),
dau_calc as (-- 2. 常规 DAU 计算
    select dt
         ,count(distinct user_id) as dau
    from daily_active
    group by dt
),
rolling_base as (-- 3. 日期向后裂变：利用 posexplode 将一条记录向后复制 30 天 ，pos 从 0 开始，最大到 29
    select date_add(dt, pos) as target_dt 
         ,user_id
         ,pos
    from daily_active
    lateral view posexplode(split(space(29), '')) t as pos, val
),
rolling_calc as (-- 4. 计算目标日期下的滚动 WAU 和 滚动 MAU
    select target_dt as dt
         ,count(distinct case when pos <= 6 then user_id end) as rolling_7d_wau
         ,count(distinct case when pos <= 29 then user_id end) as rolling_30d_mau
    from rolling_base
    where target_dt <= date_sub(current_date, 1) -- 截断超出昨天的未来日期
        and target_dt >= date_sub(current_date, 30) -- 保证前置数据充足，只输出准确的近30天结果
    group by target_dt
)
-- 5. 组装最终结果
select d.dt as `日期`
     ,d.dau as `DAU`
     ,r.rolling_7d_wau as `滚动7天WAU`
     ,r.rolling_30d_mau as `滚动30天MAU`
     ,dau / r.rolling_7d_wau as `DAU/WAU`
     ,dau / r.rolling_30d_mau as `DAU/MAU`
from dau_calc d
left join rolling_calc r on d.dt = r.dt
where r.rolling_30d_mau is not null
order by d.dt desc;