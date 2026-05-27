
-- 1、预定口径
with holiday_definition as (
    select 'Q1' as holiday_name, '2025-01-01' as s, '2025-03-31' as e, '2025' as y_flag union all
    select 'Q1',  '2026-01-01', '2026-03-31', '2026' union all
    select '春节', '2025-01-28', '2025-02-04', '2025' union all
    select '春节', '2026-02-15', '2026-02-23', '2026' union all
    select '清明', '2025-04-04', '2025-04-06', '2025' union all
    select '清明', '2026-04-04', '2026-04-06', '2026' union all
    select '五一', '2025-05-01', '2025-05-05', '2025' union all
    select '五一', '2026-05-01', '2026-05-05', '2026'
)
,base_data as (-- 1. 基础数据提取及法定节假日打标
    select dt
          ,mdd
          ,user_type
          ,h.holiday_name
          ,h.y_flag as year_flag
          ,q_room_night
          -- 比率及QC指标 (用于求算数平均)
          ,qc_rn_rate            -- 间夜QC
          ,qc_traffic_rate       -- 流量QC
          ,qc_cr                 -- 转化QC
          ,qc_avg_rn             -- 单间夜QC
          ,qc_revenue            -- 收益QC
          ,qc_adr                -- ADR_QC
          ,qc_take_rate_diff     -- 收益率QC差
          ,qc_subsidy_rate_diff  -- 券补贴率QC差
    from ihotel_default.ads_intl_hotel_qc_monitor_di t
    inner join holiday_definition h on t.dt >= h.s and t.dt <= h.e
    where dt >= '2025-01-01' 
      and dt <= '2026-05-05'
)
,agg_data as (-- 2. 分周期、年份、维度的日均及算数平均汇总
    select  holiday_name
          ,year_flag
          ,mdd
          ,user_type
          
          -- 日均间夜量 = 周期内间夜总量 / 天数
          ,sum(q_room_night) / count(distinct dt) as avg_q_rn
          
          -- QC类指标直接求算数平均 (AVG)
          ,avg(qc_rn_rate)           as avg_qc_rn_rate
          ,avg(qc_traffic_rate)      as avg_qc_traffic_rate
          ,avg(qc_cr)                as avg_qc_cr
          ,avg(qc_avg_rn)            as avg_qc_avg_rn
          ,avg(qc_revenue)           as avg_qc_revenue
          ,avg(qc_adr)               as avg_qc_adr
          ,avg(qc_take_rate_diff)    as avg_qc_take_rate_diff
          ,avg(qc_subsidy_rate_diff) as avg_qc_subsidy_rate_diff
    from base_data
    where holiday_name is not null
    group by 1, 2, 3, 4
)
-- 3. 自关联对比25与26年，输出大盘对比
select 
    curr.holiday_name            as "节日/周期"
    ,curr.mdd                    as "目的地"
    ,curr.user_type              as "用户类型"
    
    -- 量级指标及同环比
    ,curr.avg_q_rn               as "26年_日均间夜量"
    ,prev.avg_q_rn               as "25年_日均间夜量"
    ,concat(round((curr.avg_q_rn / prev.avg_q_rn - 1) * 100, 1), '%') as "日均间夜量_YoY"
    
    -- 当期核心指标展示 (算数平均结果)
    ,curr.avg_qc_rn_rate         as "26年_间夜QC"
    ,curr.avg_qc_traffic_rate    as "26年_流量QC"
    ,curr.avg_qc_cr              as "26年_转化QC"
    ,curr.avg_qc_avg_rn          as "26年_单间夜QC"
    ,curr.avg_qc_revenue         as "26年_收益QC"
    ,curr.avg_qc_adr             as "26年_ADR_QC"
    ,curr.avg_qc_take_rate_diff  as "26年_收益率QC差"
    ,curr.avg_qc_subsidy_rate_diff as "26年_券补贴率QC差"
    
    -- YoY 差值对比 (直接相减，体现百分点 pp 波动)
    ,round((curr.avg_qc_rn_rate - prev.avg_qc_rn_rate) * 100, 2)                                 as "间夜QC_YoY_pp"
    ,round((curr.avg_qc_traffic_rate - prev.avg_qc_traffic_rate) * 100, 2)                       as "流量QC_YoY_pp"
    ,round((curr.avg_qc_cr - prev.avg_qc_cr) * 100, 2)                                           as "转化QC_YoY_pp"
    ,round((curr.avg_qc_avg_rn - prev.avg_qc_avg_rn) * 100, 2)                                   as "单间夜QC_YoY_pp"
    ,round((curr.avg_qc_revenue - prev.avg_qc_revenue) * 100, 2)                                 as "收益QC_YoY_pp"
    ,round((curr.avg_qc_adr - prev.avg_qc_adr) * 100, 2)                                         as "ADR_QC_YoY_pp"
    ,round((curr.avg_qc_take_rate_diff - prev.avg_qc_take_rate_diff) * 100, 2)                   as "收益率QC差_YoY_pp"
    ,round((curr.avg_qc_subsidy_rate_diff - prev.avg_qc_subsidy_rate_diff) * 100, 2)             as "券补贴率QC差_YoY_pp"

from agg_data curr
left join agg_data prev
  on curr.holiday_name = prev.holiday_name
 and curr.mdd = prev.mdd
 and curr.user_type = prev.user_type
 and prev.year_flag = '2025'
where curr.year_flag = '2026'
order by 
    case curr.holiday_name 
        when 'Q1' then 1 
        when '春节' then 2 
        when '清明' then 3 
        when '五一' then 4 
        else 5 
    end,
    curr.mdd,
    curr.user_type;


-- 2、离店口径
with holiday_definition as (
    select 'Q1' as holiday_name, '2025-01-01' as s, '2025-03-31' as e, '2025' as y_flag union all
    select 'Q1',  '2026-01-01', '2026-03-31', '2026' union all
    select '春节', '2025-01-28', '2025-02-04', '2025' union all
    select '春节', '2026-02-15', '2026-02-23', '2026' union all
    select '清明', '2025-04-04', '2025-04-06', '2025' union all
    select '清明', '2026-04-04', '2026-04-06', '2026' union all
    select '五一', '2025-05-01', '2025-05-05', '2025' union all
    select '五一', '2026-05-01', '2026-05-05', '2026'
)
,base_data as (-- 1. 基础数据提取及法定节假日打标
    select dt
          ,mdd
          ,user_type
          ,h.holiday_name
          ,h.y_flag as year_flag
          ,q_room_night
          -- 比率及QC指标 (用于求算数平均)
          ,qc_rn_rate            -- 间夜QC
          ,qc_avg_rn             -- 单间夜QC
          ,qc_revenue            -- 收益QC
          ,qc_adr                -- ADR_QC
          ,qc_take_rate_diff     -- 收益率QC差
          ,qc_subsidy_rate_diff  -- 券补贴率QC差
    from ihotel_default.ads_ihotel_qc_checkout_metrics_di t
    inner join holiday_definition h on t.dt >= h.s and t.dt <= h.e
    where dt >= '2025-01-01' 
      and dt <= '2026-05-05'
)
,agg_data as (-- 2. 分周期、年份、维度的日均及算数平均汇总
    select  holiday_name
          ,year_flag
          ,mdd
          ,user_type
          
          -- 日均间夜量 = 周期内间夜总量 / 天数
          ,sum(q_room_night) / count(distinct dt) as avg_q_rn
          
          -- QC类指标直接求算数平均 (AVG)
          ,avg(qc_rn_rate)           as avg_qc_rn_rate
          ,avg(qc_avg_rn)            as avg_qc_avg_rn
          ,avg(qc_revenue)           as avg_qc_revenue
          ,avg(qc_adr)               as avg_qc_adr
          ,avg(qc_take_rate_diff)    as avg_qc_take_rate_diff
          ,avg(qc_subsidy_rate_diff) as avg_qc_subsidy_rate_diff
    from base_data
    where holiday_name is not null
    group by 1, 2, 3, 4
)
-- 3. 自关联对比25与26年，输出大盘对比
select 
    curr.holiday_name            as "节日/周期"
    ,curr.mdd                    as "目的地"
    ,curr.user_type              as "用户类型"
    
    -- 量级指标及同环比
    ,curr.avg_q_rn               as "26年_日均间夜量"
    ,prev.avg_q_rn               as "25年_日均间夜量"
    ,concat(round((curr.avg_q_rn / prev.avg_q_rn - 1) * 100, 1), '%') as "日均间夜量_YoY"
    
    -- 当期核心指标展示 (算数平均结果)
    ,curr.avg_qc_rn_rate         as "26年_间夜QC"
    ,curr.avg_qc_avg_rn          as "26年_单间夜QC"
    ,curr.avg_qc_revenue         as "26年_收益QC"
    ,curr.avg_qc_adr             as "26年_ADR_QC"
    ,curr.avg_qc_take_rate_diff  as "26年_收益率QC差"
    ,curr.avg_qc_subsidy_rate_diff as "26年_券补贴率QC差"
    
    -- YoY 差值对比 (直接相减，体现百分点 pp 波动)
    ,round((curr.avg_qc_rn_rate - prev.avg_qc_rn_rate) * 100, 2)                                 as "间夜QC_YoY_pp"
    ,round((curr.avg_qc_avg_rn - prev.avg_qc_avg_rn) * 100, 2)                                   as "单间夜QC_YoY_pp"
    ,round((curr.avg_qc_revenue - prev.avg_qc_revenue) * 100, 2)                                 as "收益QC_YoY_pp"
    ,round((curr.avg_qc_adr - prev.avg_qc_adr) * 100, 2)                                         as "ADR_QC_YoY_pp"
    ,round((curr.avg_qc_take_rate_diff - prev.avg_qc_take_rate_diff) * 100, 2)                   as "收益率QC差_YoY_pp"
    ,round((curr.avg_qc_subsidy_rate_diff - prev.avg_qc_subsidy_rate_diff) * 100, 2)             as "券补贴率QC差_YoY_pp"

from agg_data curr
left join agg_data prev
    on curr.holiday_name = prev.holiday_name
    and curr.mdd = prev.mdd
    and curr.user_type = prev.user_type
    and prev.year_flag = '2025'
where curr.year_flag = '2026'
order by 
    case curr.holiday_name 
        when 'Q1' then 1 
        when '春节' then 2 
        when '清明' then 3 
        when '五一' then 4 
        else 5 
    end,
    curr.mdd,
    curr.user_type;





select dt 
      ,mdd 
      ,user_type 
      ,q_room_night as "Q_间夜量"
      ,q_order_cnt as "Q_订单量"
      ,q_gmv as "Q_GMV"
      ,q_commission as "Q_佣金"
      ,q_order_user_cnt as "Q_下单用户"
      ,q_take_rate as "Q_收益率"
      ,q_adr as "Q_ADR"
      ,qc_rn_rate as "间夜QC"
      ,qc_order_cnt as "订单量QC"
      ,qc_revenue as "收益QC"
      ,q_order_user_cnt_app / c_order_user_cnt as "生单用户QC"
      ,q_room_night_app as "Q_间夜量_app"
      ,q_gmv_app as "Q_GMV_app"
      ,q_commission_app as "Q_佣金_app"
      ,q_order_cnt_app as "Q_订单量_app"
      ,q_order_user_cnt_app as "Q_下单用户_app"
      ,q_take_rate_app as "Q_收益率_app"
      ,q_adr_app as "Q_ADR_app"
      ,c_room_night as "C_间夜量"
      ,c_gmv as "C_GMV"
      ,c_commission as "C_佣金"
      ,c_order_cnt as "C_订单量"
      ,c_order_user_cnt as "C_下单用户"
      ,c_take_rate as "C_收益率"
      ,c_adr as "C_ADR"
      ,q_subsidy_rate as "Q_券补贴率"
      ,q_subsidy_rate_app as "Q_券补贴率_app"
      ,c_subsidy_rate as "C_券补贴率"
      ,qc_subsidy_rate_diff as "券补贴率QC差"
      ,q_coupon_amount as "Q_券额"
      ,q_coupon_amount_app as "Q_券额_app"
      ,c_coupon_amount as "C_券额"
      
from ihotel_default.ads_ihotel_qc_checkout_metrics_di where dt >= '2025-01-01'
order by dt desc 
      ,case when mdd = '香港'  then 1 
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
      ,case when user_type = 'ALL'  then 1 
            when user_type = '新客'  then 2 
            when user_type = '老客'  then 3 
       end asc
;