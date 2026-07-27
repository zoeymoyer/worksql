

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
        
        ,uv                              -- Q DAU  
        ,c_uv                            -- C DAU 
        ,q_room_night_app                -- Q 间夜量 
        ,c_room_night                    -- C 间夜量 
        ,q_commission_app                -- Q 收益 
        ,c_commission                    -- C 收益 
        ,q_cr_app                        -- Q CR转化率 
        ,c_cr                            -- C CR转化率 
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
  		,lag(uv, 1) over(partition by mdd, user_type order by dt) as uv_t1
        ,lag(c_uv, 1) over(partition by mdd, user_type order by dt) as c_uv_t1
        ,lag(q_room_night_app, 1) over(partition by mdd, user_type order by dt) as q_room_night_app_t1
        ,lag(c_room_night, 1) over(partition by mdd, user_type order by dt) as c_room_night_t1
        ,lag(q_commission_app, 1) over(partition by mdd, user_type order by dt) as q_commission_app_t1
        ,lag(c_commission, 1) over(partition by mdd, user_type order by dt) as c_commission_t1
        ,lag(q_cr_app, 1) over(partition by mdd, user_type order by dt) as q_cr_app_t1
        ,lag(c_cr, 1) over(partition by mdd, user_type order by dt) as c_cr_t1
  
        -- 上周同期 (T-7)
        ,lag(rn_qc, 7)      over(partition by mdd, user_type order by dt) as rn_qc_t7
        ,lag(traffic_qc, 7) over(partition by mdd, user_type order by dt) as traffic_qc_t7
        ,lag(cr_qc, 7)      over(partition by mdd, user_type order by dt) as cr_qc_t7
        ,lag(avg_rn_qc, 7)  over(partition by mdd, user_type order by dt) as avg_rn_qc_t7
  		
  		,lag(uv, 7) over(partition by mdd, user_type order by dt) as uv_t7
        ,lag(c_uv, 7) over(partition by mdd, user_type order by dt) as c_uv_t7
        ,lag(q_room_night_app, 7) over(partition by mdd, user_type order by dt) as q_room_night_app_t7
        ,lag(c_room_night, 7) over(partition by mdd, user_type order by dt) as c_room_night_t7
        ,lag(q_commission_app, 7) over(partition by mdd, user_type order by dt) as q_commission_app_t7
        ,lag(c_commission, 7) over(partition by mdd, user_type order by dt) as c_commission_t7
        ,lag(q_cr_app, 7) over(partition by mdd, user_type order by dt) as q_cr_app_t7
        ,lag(c_cr, 7) over(partition by mdd, user_type order by dt) as c_cr_t7
        -- 去年同期 (T-364, 对应星期拉齐)
        ,lag(rn_qc, 364)      over(partition by mdd, user_type order by dt) as rn_qc_y1
        ,lag(traffic_qc, 364) over(partition by mdd, user_type order by dt) as traffic_qc_y1
        ,lag(cr_qc, 364)      over(partition by mdd, user_type order by dt) as cr_qc_y1
        ,lag(avg_rn_qc, 364)  over(partition by mdd, user_type order by dt) as avg_rn_qc_y1
  
  		,lag(uv, 364) over(partition by mdd, user_type order by dt) as uv_y1
        ,lag(c_uv, 364) over(partition by mdd, user_type order by dt) as c_uv_y1
        ,lag(q_room_night_app, 364) over(partition by mdd, user_type order by dt) as q_room_night_app_y1
        ,lag(c_room_night, 364) over(partition by mdd, user_type order by dt) as c_room_night_y1
        ,lag(q_commission_app, 364) over(partition by mdd, user_type order by dt) as q_commission_app_y1
        ,lag(c_commission, 364) over(partition by mdd, user_type order by dt) as c_commission_y1
        ,lag(q_cr_app, 364) over(partition by mdd, user_type order by dt) as q_cr_app_y1
        ,lag(c_cr, 364) over(partition by mdd, user_type order by dt) as c_cr_y1
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
    
    ,uv as "Q_DAU"
    ,c_uv as "C_DAU"
    ,q_room_night_app as "Q间夜量"
    ,c_room_night as "C间夜量"
    ,q_commission_app as "Q收益"
    ,c_commission as "C收益"
    ,q_cr_app as "Q_CR"
    ,c_cr as "C_CR"

    -----------------------------------------------------------------------
    -- B. 日环比波动 (DoD) & 贡献拆解(贡献值之和 = 间夜QC_DoD_pp)
    -----------------------------------------------------------------------
    ,(rn_qc - rn_qc_t1) * 100 as "间夜QC_DoD_pp"
    ,(traffic_qc - traffic_qc_t1) * 100 as "流量QC_DoD_pp"
    ,(cr_qc - cr_qc_t1) * 100 as "转化QC_DoD_pp"
    ,(avg_rn_qc - avg_rn_qc_t1) * 100 as "单间夜QC_DoD_pp"

    ,(uv / uv_t1 - 1) * 100 as "Q_DAU_DoD_pp"
    ,(c_uv / c_uv_t1 - 1) * 100 as "C_DAU_DoD_pp"
    ,(q_room_night_app / q_room_night_app_t1 - 1) * 100 as "Q间夜量_DoD_pp"
    ,(c_room_night / c_room_night_t1 - 1) * 100 as "C间夜量_DoD_pp"
    ,(q_commission_app / q_commission_app_t1 - 1) * 100 as "Q收益_DoD_pp"
    ,(c_commission / c_commission_t1 - 1) * 100 as "C收益_DoD_pp"
    ,(q_cr_app - q_cr_app_t1) * 100 as "Q_CR_DoD_pp"
    ,(c_cr - c_cr_t1) * 100 as "C_CR_DoD_pp"

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

    ,(uv / uv_t7 - 1) * 100 as "Q_DAU_WoW_pp"
    ,(c_uv / c_uv_t7 - 1) * 100 as "C_DAU_WoW_pp"
    ,(q_room_night_app / q_room_night_app_t7 - 1) * 100 as "Q间夜量_WoW_pp"
    ,(c_room_night / c_room_night_t7 - 1) * 100 as "C间夜量_WoW_pp"
    ,(q_commission_app / q_commission_app_t7 - 1) * 100 as "Q收益_WoW_pp"
    ,(c_commission / c_commission_t7 - 1) * 100 as "C收益_WoW_pp"
    ,(q_cr_app - q_cr_app_t7) * 100 as "Q_CR_WoW_pp"
    ,(c_cr - c_cr_t7) * 100 as "C_CR_WoW_pp"

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

    ,(uv / uv_y1 - 1) * 100 as "Q_DAU_YoY_pp"
    ,(c_uv / c_uv_y1 - 1) * 100 as "C_DAU_YoY_pp"
    ,(q_room_night_app / q_room_night_app_y1 - 1) * 100 as "Q间夜量_YoY_pp"
    ,(c_room_night / c_room_night_y1 - 1) * 100 as "C间夜量_YoY_pp"
    ,(q_commission_app / q_commission_app_y1 - 1) * 100 as "Q收益_YoY_pp"
    ,(c_commission / c_commission_y1 - 1) * 100 as "C收益_YoY_pp"
    ,(q_cr_app - q_cr_app_y1) * 100 as "Q_CR_YoY_pp"
    ,(c_cr - c_cr_y1) * 100 as "C_CR_YoY_pp"

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



with base_data as (
    --- 1. 提取核心指标基础表
    select 
        dt
        ,mdd
        ,user_type
        ,qc_rn_rate             as rn_qc           -- 间夜QC
        ,qc_traffic_rate        as traffic_qc      -- 流量QC
        ,qc_cr                  as cr_qc           -- 转化QC
        ,qc_avg_rn              as avg_rn_qc       -- 单间夜QC
        ,qc_take_rate_diff      as qc_take_rate_diff -- 收益率QC差
        ,qc_subsidy_rate_diff   as qc_subsidy_rate_diff -- 券补贴率QC差
        ,uv                     as q_dau           -- Q DAU
        ,c_uv                   as c_dau           -- C DAU
        ,q_room_night_app       as q_rn            -- Q 间夜量
        ,c_room_night           as c_rn            -- C 间夜量
        ,q_commission_app       as q_comm          -- Q 收益
        ,c_commission           as c_comm          -- C 收益
        ,q_cr_app               as q_cr            -- Q CR转化率
        ,c_cr                   as c_cr            -- C CR转化率
    from ihotel_default.ads_intl_hotel_qc_monitor_di
    where dt >= '2025-01-01'
)
,lag_data as (
    --- 2. 利用窗口函数获取 T-1, T-7, T-364 的对比值
    select 
        *
        --- 昨日数据 (T-1)
        ,lag(rn_qc, 1)      over(partition by mdd, user_type order by dt) as rn_qc_t1
        ,lag(traffic_qc, 1) over(partition by mdd, user_type order by dt) as traffic_qc_t1
        ,lag(cr_qc, 1)      over(partition by mdd, user_type order by dt) as cr_qc_t1
        ,lag(avg_rn_qc, 1)  over(partition by mdd, user_type order by dt) as avg_rn_qc_t1
        ,lag(q_dau, 1)      over(partition by mdd, user_type order by dt) as q_dau_t1
        ,lag(c_dau, 1)      over(partition by mdd, user_type order by dt) as c_dau_t1
        ,lag(q_rn, 1)       over(partition by mdd, user_type order by dt) as q_rn_t1
        ,lag(c_rn, 1)       over(partition by mdd, user_type order by dt) as c_rn_t1
        ,lag(q_comm, 1)     over(partition by mdd, user_type order by dt) as q_comm_t1
        ,lag(c_comm, 1)     over(partition by mdd, user_type order by dt) as c_comm_t1
        ,lag(q_cr, 1)       over(partition by mdd, user_type order by dt) as q_cr_t1
        ,lag(c_cr, 1)       over(partition by mdd, user_type order by dt) as c_cr_t1
        --- 上周同期 (T-7)
        ,lag(rn_qc, 7)      over(partition by mdd, user_type order by dt) as rn_qc_t7
        ,lag(traffic_qc, 7) over(partition by mdd, user_type order by dt) as traffic_qc_t7
        ,lag(cr_qc, 7)      over(partition by mdd, user_type order by dt) as cr_qc_t7
        ,lag(avg_rn_qc, 7)  over(partition by mdd, user_type order by dt) as avg_rn_qc_t7
        ,lag(q_dau, 7)      over(partition by mdd, user_type order by dt) as q_dau_t7
        ,lag(c_dau, 7)      over(partition by mdd, user_type order by dt) as c_dau_t7
        ,lag(q_rn, 7)       over(partition by mdd, user_type order by dt) as q_rn_t7
        ,lag(c_rn, 7)       over(partition by mdd, user_type order by dt) as c_rn_t7
        ,lag(q_comm, 7)     over(partition by mdd, user_type order by dt) as q_comm_t7
        ,lag(c_comm, 7)     over(partition by mdd, user_type order by dt) as c_comm_t7
        ,lag(q_cr, 7)       over(partition by mdd, user_type order by dt) as q_cr_t7
        ,lag(c_cr, 7)       over(partition by mdd, user_type order by dt) as c_cr_t7
        --- 去年同期 (T-364)
        ,lag(rn_qc, 364)    over(partition by mdd, user_type order by dt) as rn_qc_y1
        ,lag(traffic_qc, 364) over(partition by mdd, user_type order by dt) as traffic_qc_y1
        ,lag(cr_qc, 364)    over(partition by mdd, user_type order by dt) as cr_qc_y1
        ,lag(avg_rn_qc, 364)  over(partition by mdd, user_type order by dt) as avg_rn_qc_y1
        ,lag(q_dau, 364)    over(partition by mdd, user_type order by dt) as q_dau_y1
        ,lag(c_dau, 364)    over(partition by mdd, user_type order by dt) as c_dau_y1
        ,lag(q_rn, 364)     over(partition by mdd, user_type order by dt) as q_rn_y1
        ,lag(c_rn, 364)     over(partition by mdd, user_type order by dt) as c_rn_y1
        ,lag(q_comm, 364)   over(partition by mdd, user_type order by dt) as q_comm_y1
        ,lag(c_comm, 364)   over(partition by mdd, user_type order by dt) as c_comm_y1
        ,lag(q_cr, 364)     over(partition by mdd, user_type order by dt) as q_cr_y1
        ,lag(c_cr, 364)     over(partition by mdd, user_type order by dt) as c_cr_y1
    from base_data
)
select 
    dt
    ,(dayofweek(dt) + 5) % 7 + 1                                               as "星期"
    ,mdd                                                                       as "目的地"
    ,user_type                                                                 as "用户类型"
    --- A. 核心指标当前值
    ,rn_qc                                                                     as "间夜QC"
    ,traffic_qc                                                                as "流量QC"
    ,cr_qc                                                                     as "转化QC"
    ,avg_rn_qc                                                                 as "单间夜QC"
    ,qc_take_rate_diff                                                         as "收益率QC差"
    ,qc_subsidy_rate_diff                                                      as "券补贴率QC差"
    ,q_dau                                                                     as "Q_DAU"
    ,c_dau                                                                     as "C_DAU"
    ,q_rn                                                                      as "Q间夜量"
    ,c_rn                                                                      as "C间夜量"
    ,q_comm                                                                    as "Q收益"
    ,c_comm                                                                    as "C收益"
    ,q_cr                                                                      as "Q_CR"
    ,c_cr                                                                      as "C_CR"
    --- B. 日环比波动 (DoD) & 贡献拆解
    ,(rn_qc - rn_qc_t1) * 100                                                  as "间夜QC_DoD_pp"
    ,(traffic_qc - traffic_qc_t1) * 100                                        as "流量QC_DoD_pp"
    ,(cr_qc - cr_qc_t1) * 100                                                  as "转化QC_DoD_pp"
    ,(avg_rn_qc - avg_rn_qc_t1) * 100                                          as "单间夜QC_DoD_pp"
    ,(q_dau / nullif(q_dau_t1, 0) - 1) * 100                                   as "Q_DAU_DoD_pp"
    ,(c_dau / nullif(c_dau_t1, 0) - 1) * 100                                   as "C_DAU_DoD_pp"
    ,(q_rn / nullif(q_rn_t1, 0) - 1) * 100                                     as "Q间夜量_DoD_pp"
    ,(c_rn / nullif(c_rn_t1, 0) - 1) * 100                                     as "C间夜量_DoD_pp"
    ,(q_comm / nullif(q_comm_t1, 0) - 1) * 100                                 as "Q收益_DoD_pp"
    ,(c_comm / nullif(c_comm_t1, 0) - 1) * 100                                 as "C收益_DoD_pp"
    ,(q_cr - q_cr_t1) * 100                                                    as "Q_CR_DoD_pp"
    ,(c_cr - c_cr_t1) * 100                                                    as "C_CR_DoD_pp"
    ,round((traffic_qc - traffic_qc_t1) * cr_qc_t1 * avg_rn_qc_t1 * 100, 4)    as "DoD贡献_流量QC_pp"
    ,round(traffic_qc * (cr_qc - cr_qc_t1) * avg_rn_qc_t1 * 100, 4)            as "DoD贡献_转化QC_pp"
    ,round(traffic_qc * cr_qc * (avg_rn_qc - avg_rn_qc_t1) * 100, 4)           as "DoD贡献_单间夜QC_pp"
    --- C. 周环比波动 (WoW) & 贡献拆解
    ,(rn_qc - rn_qc_t7) * 100                                                  as "间夜QC_WoW_pp"
    ,(traffic_qc - traffic_qc_t7) * 100                                        as "流量QC_WoW_pp"
    ,(cr_qc - cr_qc_t7) * 100                                                  as "转化QC_WoW_pp"
    ,(avg_rn_qc - avg_rn_qc_t7) * 100                                          as "单间夜QC_WoW_pp"
    ,(q_dau / nullif(q_dau_t7, 0) - 1) * 100                                   as "Q_DAU_WoW_pp"
    ,(c_dau / nullif(c_dau_t7, 0) - 1) * 100                                   as "C_DAU_WoW_pp"
    ,(q_rn / nullif(q_rn_t7, 0) - 1) * 100                                     as "Q间夜量_WoW_pp"
    ,(c_rn / nullif(c_rn_t7, 0) - 1) * 100                                     as "C间夜量_WoW_pp"
    ,(q_comm / nullif(q_comm_t7, 0) - 1) * 100                                 as "Q收益_WoW_pp"
    ,(c_comm / nullif(c_comm_t7, 0) - 1) * 100                                 as "C收益_WoW_pp"
    ,(q_cr - q_cr_t7) * 100                                                    as "Q_CR_WoW_pp"
    ,(c_cr - c_cr_t7) * 100                                                    as "C_CR_WoW_pp"
    ,round((traffic_qc - traffic_qc_t7) * cr_qc_t7 * avg_rn_qc_t7 * 100, 4)    as "WoW贡献_流量QC_pp"
    ,round(traffic_qc * (cr_qc - cr_qc_t7) * avg_rn_qc_t7 * 100, 4)            as "WoW贡献_转化QC_pp"
    ,round(traffic_qc * cr_qc * (avg_rn_qc - avg_rn_qc_t7) * 100, 4)           as "WoW贡献_单间夜QC_pp"
    --- D. 年同比波动 (YoY) & 贡献拆解
    ,(rn_qc - rn_qc_y1) * 100                                                  as "间夜QC_YoY_pp"
    ,(traffic_qc - traffic_qc_y1) * 100                                        as "流量QC_YoY_pp"
    ,(cr_qc - cr_qc_y1) * 100                                                  as "转化QC_YoY_pp"
    ,(avg_rn_qc - avg_rn_qc_y1) * 100                                          as "单间夜QC_YoY_pp"
    ,(q_dau / nullif(q_dau_y1, 0) - 1) * 100                                   as "Q_DAU_YoY_pp"
    ,(c_dau / nullif(c_dau_y1, 0) - 1) * 100                                   as "C_DAU_YoY_pp"
    ,(q_rn / nullif(q_rn_y1, 0) - 1) * 100                                     as "Q间夜量_YoY_pp"
    ,(c_rn / nullif(c_rn_y1, 0) - 1) * 100                                     as "C间夜量_YoY_pp"
    ,(q_comm / nullif(q_comm_y1, 0) - 1) * 100                                 as "Q收益_YoY_pp"
    ,(c_comm / nullif(c_comm_y1, 0) - 1) * 100                                 as "C收益_YoY_pp"
    ,(q_cr - q_cr_y1) * 100                                                    as "Q_CR_YoY_pp"
    ,(c_cr - c_cr_y1) * 100                                                    as "C_CR_YoY_pp"
    ,round((traffic_qc - traffic_qc_y1) * cr_qc_y1 * avg_rn_qc_y1 * 100, 4)    as "YoY贡献_流量QC_pp"
    ,round(traffic_qc * (cr_qc - cr_qc_y1) * avg_rn_qc_y1 * 100, 4)            as "YoY贡献_转化QC_pp"
    ,round(traffic_qc * cr_qc * (avg_rn_qc - avg_rn_qc_y1) * 100, 4)           as "YoY贡献_单间夜QC_pp"
from lag_data
where dt >= '2026-01-01'
order by dt desc
    ,case when "目的地" = '香港'     then 1 
          when "目的地" = '澳门'     then 2 
          when "目的地" = '泰国'     then 3 
          when "目的地" = '日本'     then 4 
          when "目的地" = '韩国'     then 5 
          when "目的地" = '马来西亚' then 6 
          when "目的地" = '新加坡'   then 7 
          when "目的地" = '美国'     then 8 
          when "目的地" = '印度尼西亚' then 9 
          when "目的地" = '俄罗斯'   then 10 
          when "目的地" = '欧洲'     then 11 
          when "目的地" = '亚太'     then 12 
          when "目的地" = '美洲'     then 13 
          when "目的地" = '其他'     then 14 
          when "目的地" = 'ALL'      then 0 
     end asc
    ,case when "用户类型" = 'ALL'  then 1 
          when "用户类型" = '新客' then 2 
          when "用户类型" = '老客' then 3 
     end asc;