
with base_data as (-- 1. 提取核心指标基础表
    select 
          dt
        ,mdd
        ,user_type
        ,qc_rn_rate      as rn_qc        -- 间夜QC
        ,qc_traffic_rate as traffic_qc   -- 流量QC
        ,qc_cr           as cr_qc        -- 转化QC
        ,qc_avg_rn       as avg_rn_qc    -- 单间夜QC
        ,qc_take_rate_diff               -- 收益率QC差
        ,qc_subsidy_rate_diff            -- 券补贴率QC差
    from ihotel_default.ads_intl_hotel_qc_monitor_di
    where dt >= '2025-01-01'
)
,lag_data as (-- 2. 利用窗口函数获取 T-1, T-7, T-364 的对比值
    select 
          *
        -- 昨日数据 (T-1)
        ,lag(rn_qc, 1)      over(partition by mdd, user_type order by dt) as rn_qc_t1
        ,lag(traffic_qc, 1) over(partition by mdd, user_type order by dt) as traffic_qc_t1
        ,lag(cr_qc, 1)      over(partition by mdd, user_type order by dt) as cr_qc_t1
        ,lag(avg_rn_qc, 1)  over(partition by mdd, user_type order by dt) as avg_rn_qc_t1
        -- 上周同期 (T-7)
        ,lag(rn_qc, 7)      over(partition by mdd, user_type order by dt) as rn_qc_t7
        ,lag(traffic_qc, 7) over(partition by mdd, user_type order by dt) as traffic_qc_t7
        ,lag(cr_qc, 7)      over(partition by mdd, user_type order by dt) as cr_qc_t7
        ,lag(avg_rn_qc, 7)  over(partition by mdd, user_type order by dt) as avg_rn_qc_t7
        -- 去年同期 (T-364, 对应星期拉齐)
        ,lag(rn_qc, 364)      over(partition by mdd, user_type order by dt) as rn_qc_y1
        ,lag(traffic_qc, 364) over(partition by mdd, user_type order by dt) as traffic_qc_y1
        ,lag(cr_qc, 364)      over(partition by mdd, user_type order by dt) as cr_qc_y1
        ,lag(avg_rn_qc, 364)  over(partition by mdd, user_type order by dt) as avg_rn_qc_y1
    from base_data
)
select 
      dt
    ,(dayofweek(dt) + 5) % 7 + 1 as "星期"
    ,mdd "目的地"
    ,user_type  "用户类型"
    
    -- A. 核心指标当前值
    ,rn_qc as "间夜QC"
    ,traffic_qc as "流量QC"
    ,cr_qc as "转化QC"
    ,avg_rn_qc as "单间夜QC"
    ,qc_take_rate_diff as "收益率QC差"
    ,qc_subsidy_rate_diff as "券补贴率QC差"

    -----------------------------------------------------------------------
    -- B. 日环比波动 (DoD) & 贡献拆解(贡献值之和 = 间夜QC_DoD_pp)
    -----------------------------------------------------------------------
    ,(rn_qc - rn_qc_t1) * 100 as "间夜QC_DoD_pp"
    ,(traffic_qc - traffic_qc_t1) * 100 as "流量QC_DoD_pp"
    ,(cr_qc - cr_qc_t1) * 100 as "转化QC_DoD_pp"
    ,(avg_rn_qc - avg_rn_qc_t1) * 100 as "单间夜QC_DoD_pp"
      -- 1. 流量贡献 = (昨日流量变化) * 昨日转化 * 昨日单间夜
    ,round((traffic_qc - traffic_qc_t1) * cr_qc_t1 * avg_rn_qc_t1 * 100, 4) as "DoD贡献_流量QC_pp"
      -- 2. 转化贡献 = 今日流量 * (今日转化变化) * 昨日单间夜
    ,round(traffic_qc * (cr_qc - cr_qc_t1) * avg_rn_qc_t1 * 100, 4) as "DoD贡献_转化QC_pp"
      -- 3. 单间夜贡献 = 今日流量 * 今日转化 * (今日单间夜变化)
    ,round(traffic_qc * cr_qc * (avg_rn_qc - avg_rn_qc_t1) * 100, 4) as "DoD贡献_单间夜QC_pp"

    -----------------------------------------------------------------------
    -- C. 周环比波动 (WoW) & 贡献拆解(贡献值之和 = 间夜QC_DoD_pp)
    -----------------------------------------------------------------------
    ,(rn_qc - rn_qc_t7) * 100 as "间夜QC_WoW_pp"
    ,(traffic_qc - traffic_qc_t7) * 100 as "流量QC_WoW_pp"
    ,(cr_qc - cr_qc_t7) * 100 as "转化QC_WoW_pp"
    ,(avg_rn_qc - avg_rn_qc_t7) * 100 as "单间夜QC_WoW_pp"
    ,round((traffic_qc - traffic_qc_t7) * cr_qc_t7 * avg_rn_qc_t7 * 100, 4) as "WoW贡献_流量QC_pp"
    ,round(traffic_qc * (cr_qc - cr_qc_t7) * avg_rn_qc_t7 * 100, 4) as "WoW贡献_转化QC_pp"
    ,round(traffic_qc * cr_qc * (avg_rn_qc - avg_rn_qc_t7) * 100, 4) as "WoW贡献_单间夜QC_pp"

    -----------------------------------------------------------------------
    -- D. 年同比波动 (YoY) & 贡献拆解(贡献值之和 = 间夜QC_DoD_pp)
    -----------------------------------------------------------------------
    ,(rn_qc - rn_qc_y1) * 100 as "间夜QC_YoY_pp"
    ,(traffic_qc - traffic_qc_y1) * 100 as "流量QC_YoY_pp"
    ,(cr_qc - cr_qc_y1) * 100 as "转化QC_YoY_pp"
    ,(avg_rn_qc - avg_rn_qc_y1) * 100 as "单间夜QC_YoY_pp"
    ,round((traffic_qc - traffic_qc_y1) * cr_qc_y1 * avg_rn_qc_y1 * 100, 4) as "YoY贡献_流量QC_pp"
    ,round(traffic_qc * (cr_qc - cr_qc_y1) * avg_rn_qc_y1 * 100, 4) as "YoY贡献_转化QC_pp"
    ,round(traffic_qc * cr_qc * (avg_rn_qc - avg_rn_qc_y1) * 100, 4) as "YoY贡献_单间夜QC_pp" 
from lag_data
where dt >= '2026-01-01'
order by dt desc
    ,case when "目的地" = '香港'  then 1 
            when "目的地" = '澳门'  then 2 
            when "目的地" = '泰国'  then 3 
            when "目的地" = '日本'  then 4 
            when "目的地" = '韩国'  then 5 
            when "目的地" = '马来西亚'  then 6 
            when "目的地" = '新加坡'  then 7 
            when "目的地" = '美国'  then 8 
            when "目的地" = '印度尼西亚'  then 9 
            when "目的地" = '俄罗斯'  then 10 
            when "目的地" = '欧洲'  then 11 
            when "目的地" = '亚太'  then 12 
            when "目的地" = '美洲'  then 13 
            when "目的地" = '其他'  then 14 
            when "目的地" = 'ALL'  then 0 
       end asc
    ,case when "用户类型" = 'ALL'  then 1 
            when "用户类型" = '新客'  then 2 
            when "用户类型" = '老客'  then 3 
    end asc
;