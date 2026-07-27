
with base_data as (--- KOS投流明细
    select t1.dt
        ,t1.plan_id  -- 计划ID
        ,t1.note_id  -- 贴ID
        ,t1.creative_id -- 创意ID
        ,t1.post_type  -- 投放类型
        ,t1.note_url  -- 贴url
        ,t1.note_scene_mix  -- 贴类型
        ,t1.first_cost_dt  -- 首次有消耗日期
        ,t1.post_dt -- 发布日期
        ,coalesce(t1.cost_real, 0) as cost_real  --- 消耗
        ,coalesce(t1.new_user_last, 0) as new_user_last  -- 平台新
        ,coalesce(t1.hotel_inter_yw_new, 0) as hotel_inter_yw_new -- 业务新
        ,coalesce(t1.income, 0) as income  -- 收入
        ,t1.performance_group_2 
    from smm.ads_smm_redbook_touliu_creative_detail_hotel_inter_di t1
    where t1.dt >= date_sub(current_date, 30) and t.dt <= date_sub(current_date, 1)
        and t1.performance_group_2 = 'KOS'
        and t1.cost_real > 0
)
,plan_daily as (--- 先聚合到日期+计划粒度，避免计划下多个帖子导致重复
    select dt
        ,plan_id
        ,min(first_cost_dt) as first_cost_dt
        ,concat_ws(',', collect_set(note_scene_mix)) as note_scene_mix
        ,concat_ws(',', collect_set(note_url)) as note_url
        ,count(distinct note_id) note_cnt
        ,sum(cost_real) as cost_real
        ,sum(new_user_last) as new_user_last
        ,sum(hotel_inter_yw_new) as hotel_inter_yw_new
        ,sum(income) as income
    from base_data
    group by 1,2
)
,daily_stats as (
    --- 3. 计算日度统计
    select dt
        ,plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        ,cost_real
        ,new_user_last
        ,hotel_inter_yw_new
        ,income
    from plan_daily
)
,plan_window_metric as (--- 4. 使用窗口函数计算滚动 7 天和 30 天的动态值
    select dt 
        ,plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        -- 最近 7 天滚动汇总
        ,sum(cost_real) over(partition by plan_id order by dt rows between 6 preceding and current row) as `滚动7天_消耗`
        ,sum(new_user_last) over(partition by plan_id order by dt rows between 6 preceding and current row) as `滚动7天_平台新`
        ,sum(hotel_inter_yw_new) over(partition by plan_id order by dt rows between 6 preceding and current row) as `滚动7天_业务新`
        ,sum(income) over(partition by plan_id order by dt rows between 6 preceding and current row) as `滚动7天_收入`
        -- 最近 30 天滚动汇总
        ,sum(cost_real) over(partition by plan_id order by dt rows between 29 preceding and current row) as `滚动30天_消耗`
        ,sum(new_user_last) over(partition by plan_id order by dt rows between 29 preceding and current row) as `滚动30天_平台新`
        ,sum(hotel_inter_yw_new) over(partition by plan_id order by dt rows between 29 preceding and current row) as `滚动30天_业务新`
        ,sum(income) over(partition by plan_id order by dt rows between 29 preceding and current row) as `滚动30天_收入`
    from daily_stats
)
,plan_calc as (--- 计算每日计划指标
    select dt
        ,plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt

        ,`滚动7天_消耗` / 7  as `消耗T7`
        ,`滚动7天_平台新` / 7 as `平台新T7`
        ,`滚动7天_业务新` / 7 as `业务新T7`
        ,`滚动7天_收入` / 7 as `收入T7`
        ,`滚动7天_消耗` / nullif(`滚动7天_平台新`, 0) / 0.96 as `CAC_T7`
        ,`滚动30天_收入` / 30 as `收入T30`
        ,`滚动30天_平台新` / 30  as `平台新T30`
        ,`滚动30天_业务新` / 30  as `业务新T30`

        ,`滚动30天_收入` / nullif(`滚动30天_平台新`, 0) as `ARPU_T30`
        ,(`滚动30天_收入` / nullif(`滚动30天_平台新`, 0)) / nullif(`滚动7天_消耗` / nullif(`滚动7天_平台新`, 0) / 0.96, 0) as `ROI`
    from plan_window_metric
)
,plan_result as (--- 按每日动态指标生成10个分类
    select dt
        ,plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        ,`消耗T7`
        ,`平台新T7`
        ,`业务新T7`
        ,`收入T30`
        ,`平台新T30`
        ,`业务新T30`
        ,`CAC_T7`
        ,`ARPU_T30`
        ,`ROI`

        --- 标签
        ,case when first_cost_dt is null then '1.近7天无消耗'
              when datediff(date_sub(current_date, 1),first_cost_dt) <= 7 then '2.7天以内'
              else '3.7天以上'
         end as first_cost_type
        ,case when `平台新T7` > 0 then '1.有平台新' else '2.无平台新' end as platform_new_t7_flag
        ,case when `业务新T7` > 0 then '1.有业务新' else '2.无业务新' end as business_new_t7_flag
        ,case when `消耗T7` < 0 then '1.无消耗'
              when `消耗T7` > 0   and `消耗T7` <= 100 then '2.日均消耗(0,100]' 
              when `消耗T7` > 100 and `消耗T7` <= 200 then '3.日均消耗(100,200]' 
              when `消耗T7` > 200 and `消耗T7` <= 300 then '4.日均消耗(200,300]' 
              when `消耗T7` > 300 and `消耗T7` <= 400 then '5.日均消耗(300,400]' 
              when `消耗T7` > 400 and `消耗T7` <= 500 then '6.日均消耗(400,500]' 
              when `消耗T7` > 500 then '7.日均消耗>500'
            end as cost_t7_flag
        ,case when `收入T30` >= 0 then '1.有收入' else '2.无收入' end as income_t30_flag
        ,case when `ROI` >= 0.2 then '1.ROI≥0.2' else '2.ROI<0.2' end as roi_flag
        ,case when `CAC_T7` >= 200 then '1.CAC≥200' else '2.CAC<200' end as cac_t7_flag

        --- 分类
        ,case when datediff(date_sub(current_date, 1),first_cost_dt) <= 7 then '1.7天以内'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` = 0 and `业务新T7` = 0 then '2.T7+无平台新无业务新'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` = 0 and `业务新T7` > 0 and `ROI` >= 0.2 then '3.T7+无平台新有业务新ROI高'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` = 0 and `业务新T7` > 0 and `ROI` < 0.2 then '4.T7+无平台新有业务新ROI低'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `收入T30` < 0 then '5.T7+有平台新收入<0'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` >= 200 and `ROI` >= 0.2 then '6.T7+有平台新CAC高ROI高'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` >= 200 and `ROI` < 0.2 then '7.T7+有平台新CAC高ROI低'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` < 200 and `ROI` >= 0.2 then '8.T7+有平台新CAC低ROI高'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` < 200 and `ROI` < 0.2 then '9.T7+有平台新CAC低ROI低'
              else '10.其他' end as category
        --- 重点分类
        ,case when `平台新T7` = 0 and `业务新T7` > 0  then '1.无平台新有业务新'
              when `消耗T7` >= 200 and `消耗T7` < 500 then '2.日均消耗(200,500]'
              when `消耗T7` >= 500 and `CAC_T7` > 300 then '3.日均消耗≥500且CAC＞300'
              else '4.其他' end as focus_category
    from plan_calc
)

select dt as `日期`
    ,focus_category as `策略类型`
    ,category as `分类`
    ,plan_id as `计划ID`
    ,note_cnt as `贴数`
    ,note_scene_mix as `贴类型`
    -- ,post_type as `投放类型`
    ,note_url as `笔记URL`
    ,first_cost_dt as `首次消耗日期`
    
    ,`消耗T7` 
    ,`平台新T7`
    ,`业务新T7`
    ,`收入T30`
    ,`平台新T30`
    ,`业务新T30`
    ,`CAC_T7`
    ,`ARPU_T30`
    ,`ROI`
    ,first_cost_type,platform_new_t7_flag,business_new_t7_flag,cost_t7_flag,income_t30_flag,roi_flag,cac_t7_flag
from plan_result
order by dt desc
    ,`消耗T7`  desc
;




--- 交付sql
with base_data as (--- KOS投流明细
    select t1.dt
        ,t1.plan_id  -- 计划ID
        ,t1.note_id  -- 贴ID
        ,t1.creative_id -- 创意ID
        ,t1.post_type  -- 投放类型
        ,t1.note_url  -- 贴url
        ,t1.note_scene_mix  -- 贴类型
        ,t1.first_cost_dt  -- 首次有消耗日期
        ,t1.post_dt -- 发布日期
        ,coalesce(t1.cost_real, 0) as cost_real  --- 消耗
        ,coalesce(t1.new_user_last, 0) as new_user_last  -- 平台新
        ,coalesce(t1.hotel_inter_yw_new, 0) as hotel_inter_yw_new -- 业务新
        ,coalesce(t1.income, 0) as income  -- 收入
        ,t1.performance_group_2 
    from smm.ads_smm_redbook_touliu_creative_detail_hotel_inter_di t1
    where t1.dt >= date_sub(current_date, 30)
        and t1.dt <= date_sub(current_date, 1)
        and t1.performance_group_2 = 'KOS'
        and t1.cost_real > 0
)
,plan_daily as (--- 先聚合到日期+计划粒度，避免计划下多个帖子导致重复
    select plan_id
        ,min(first_cost_dt) as first_cost_dt
        ,concat_ws('|', collect_set(note_scene_mix)) as note_scene_mix
        ,concat_ws('|', collect_set(note_url)) as note_url
        ,count(distinct note_id) note_cnt
        ,sum(cost_real) as cost_real_30
        ,sum(new_user_last) as new_user_last_30
        ,sum(hotel_inter_yw_new) as hotel_inter_yw_new_30
        ,sum(income) as income_30
        ,sum(case when dt >= date_sub(current_date, 7) then cost_real else 0 end) as cost_real_7
        ,sum(case when dt >= date_sub(current_date, 7) then new_user_last else 0 end) as new_user_last_7
        ,sum(case when dt >= date_sub(current_date, 7) then hotel_inter_yw_new else 0 end) as hotel_inter_yw_new_7
        ,sum(case when dt >= date_sub(current_date, 7) then income else 0 end) as income_7
    from base_data
    group by 1
)
,plan_calc as (--- 计算每日计划指标
    select plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        ,cost_real_30
        ,new_user_last_30
        ,hotel_inter_yw_new_30
        ,income_30
        ,cost_real_7
        ,new_user_last_7
        ,hotel_inter_yw_new_7
        ,income_7
        ,cost_real_7 / 7 as "消耗T7"
        ,new_user_last_7 / 7 as "平台新T7"
        ,hotel_inter_yw_new_7 / 7 as "业务新T7"
        ,income_7 / 7 as "收入T7"
        ,cost_real_7 / nullif(new_user_last_7, 0) / 0.96 as "CAC_T7"
        ,income_30 / 30 as "收入T30"
        ,new_user_last_30 / 30 as "平台新T30"
        ,hotel_inter_yw_new_30 / 30 as "业务新T30"
        ,income_30 / nullif(new_user_last_30, 0) as "ARPU_T30"
        ,(income_30 / nullif(new_user_last_30, 0)) / nullif(cost_real_7 / nullif(new_user_last_7, 0) / 0.96, 0) as "ROI"
        ,income_30 / nullif(cost_real_7, 0) as "ROI_30_7"
    from plan_daily
)
,plan_result as (--- 按每日动态指标生成10个分类
    select plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        ,"消耗T7"
        ,"平台新T7"
        ,"业务新T7"
        ,"收入T30"
        ,"平台新T30"
        ,"业务新T30"
        ,"CAC_T7"
        ,"ARPU_T30"
        ,"ROI"
        ,"ROI_30_7"
        ,cost_real_30
        ,new_user_last_30
        ,hotel_inter_yw_new_30
        ,income_30
        ,cost_real_7
        ,new_user_last_7
        ,hotel_inter_yw_new_7
        ,income_7
        --- 标签
        ,case when first_cost_dt is null then '1.近7天无消耗'
              when datediff(date_sub(current_date, 1), first_cost_dt) <= 7 then '2.7天以内'
              else '3.7天以上' end as first_cost_type
        ,case when "平台新T7" > 0 then '1.有平台新' else '2.无平台新' end as platform_new_t7_flag
        ,case when "业务新T7" > 0 then '1.有业务新' else '2.无业务新' end as business_new_t7_flag
        ,case when "消耗T7" < 0 then '1.无消耗'
              when "消耗T7" > 0 and "消耗T7" <= 100 then '2.日均消耗(0,100]' 
              when "消耗T7" > 100 and "消耗T7" <= 200 then '3.日均消耗(100,200]' 
              when "消耗T7" > 200 and "消耗T7" <= 300 then '4.日均消耗(200,300]' 
              when "消耗T7" > 300 and "消耗T7" <= 400 then '5.日均消耗(300,400]' 
              when "消耗T7" > 400 and "消耗T7" <= 500 then '6.日均消耗(400,500]' 
              when "消耗T7" > 500 then '7.日均消耗>500'
         end as cost_t7_flag
        ,case when "收入T30" >= 0 then '1.有收入' else '2.无收入' end as income_t30_flag
        ,case when "ROI" >= 0.2 then '1.ROI≥0.2' else '2.ROI<0.2' end as roi_flag
        ,case when "CAC_T7" >= 200 then '1.CAC≥200' else '2.CAC<200' end as cac_t7_flag
        --- 分类
        ,case when datediff(date_sub(current_date, 1), first_cost_dt) <= 7 then '1.7天以内'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" = 0 and "业务新T7" = 0 then '2.T7+无平台新无业务新'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" = 0 and "业务新T7" > 0 and "ROI" >= 0.2 then '3.T7+无平台新有业务新ROI高'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" = 0 and "业务新T7" > 0 and "ROI" < 0.2 then '4.T7+无平台新有业务新ROI低'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" > 0 and "收入T30" < 0 then '5.T7+有平台新收入<0'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" > 0 and "CAC_T7" >= 200 and "ROI" >= 0.2 then '6.T7+有平台新CAC高ROI高'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" > 0 and "CAC_T7" >= 200 and "ROI" < 0.2 then '7.T7+有平台新CAC高ROI低'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" > 0 and "CAC_T7" < 200 and "ROI" >= 0.2 then '8.T7+有平台新CAC低ROI高'
              when datediff(date_sub(current_date, 1), first_cost_dt) > 7 and "平台新T7" > 0 and "CAC_T7" < 200 and "ROI" < 0.2 then '9.T7+有平台新CAC低ROI低'
              else '10.其他' end as category
        --- 重点分类
        ,case when "平台新T7" = 0 and "业务新T7" > 0 then '1.无平台新有业务新'
              when "消耗T7" >= 200 and "消耗T7" < 500 then '2.日均消耗(200,500]'
              when "消耗T7" >= 500 and "CAC_T7" > 300 then '3.日均消耗≥500且CAC＞300'
              else '4.其他' end as focus_category
    from plan_calc
)
select focus_category as "策略类型"
    ,category as "分类"
    ,plan_id as "计划ID"
    ,note_cnt as "贴数"
    ,note_scene_mix as "贴类型"
    -- ,post_type as "投放类型"
    ,note_url as "笔记URL"
    ,first_cost_dt as "首次消耗日期"
    ,"消耗T7"
    ,"平台新T7"
    ,"业务新T7"
    ,"收入T30"
    ,"平台新T30"
    ,"业务新T30"
    ,"CAC_T7"
    ,"ARPU_T30"
    ,"ROI"
    ,"ROI_30_7"
    ,first_cost_type
    ,platform_new_t7_flag
    ,business_new_t7_flag
    ,cost_t7_flag
    ,income_t30_flag
    ,roi_flag
    ,cac_t7_flag
    ,cost_real_30
    ,new_user_last_30
    ,hotel_inter_yw_new_30
    ,income_30
    ,cost_real_7
    ,new_user_last_7
    ,hotel_inter_yw_new_7
    ,income_7
from plan_result
order by "消耗T7" desc
;

with base_data as (--- KOS投流明细
    select t1.dt
        ,t1.plan_id  -- 计划ID
        ,t1.note_id  -- 贴ID
        ,t1.creative_id -- 创意ID
        ,t1.post_type  -- 投放类型
        ,t1.note_url  -- 贴url
        ,t1.note_scene_mix  -- 贴类型
        ,t1.first_cost_dt  -- 首次有消耗日期
        ,t1.post_dt -- 发布日期
        ,coalesce(t1.cost_real, 0) as cost_real  --- 消耗
        ,coalesce(t1.new_user_last, 0) as new_user_last  -- 平台新
        ,coalesce(t1.hotel_inter_yw_new, 0) as hotel_inter_yw_new -- 业务新
        ,coalesce(t1.income, 0) as income  -- 收入
        ,t1.performance_group_2 
    from smm.ads_smm_redbook_touliu_creative_detail_hotel_inter_di t1
    where t1.dt >= date_sub(current_date, 30) and t1.dt <= date_sub(current_date, 1)
        and t1.performance_group_2 = 'KOS'
        and t1.cost_real > 0
)
,plan_daily as (--- 先聚合到日期+计划粒度，避免计划下多个帖子导致重复
    select plan_id
        ,min(first_cost_dt) as first_cost_dt
        ,concat_ws('|', collect_set(note_scene_mix)) as note_scene_mix
        ,concat_ws('|', collect_set(note_url)) as note_url
        ,count(distinct note_id) note_cnt
        ,sum(cost_real) as cost_real_30
        ,sum(new_user_last) as new_user_last_30
        ,sum(hotel_inter_yw_new) as hotel_inter_yw_new_30
        ,sum(income) as income_30
        ,sum(case when dt >= date_sub(current_date, 7) then cost_real else 0 end) as cost_real_7
        ,sum(case when dt >= date_sub(current_date, 7) then new_user_last else 0 end) as new_user_last_7
        ,sum(case when dt >= date_sub(current_date, 7) then hotel_inter_yw_new else 0 end) as hotel_inter_yw_new_7
        ,sum(case when dt >= date_sub(current_date, 7) then income else 0 end) as income_7
    from base_data
    group by 1
)
,plan_calc as (--- 计算每日计划指标
    select plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        ,cost_real_30,new_user_last_30,hotel_inter_yw_new_30,income_30,cost_real_7,new_user_last_7,hotel_inter_yw_new_7,income_7

        ,cost_real_7 / 7  as `消耗T7`
        ,new_user_last_7 / 7 as `平台新T7`
        ,hotel_inter_yw_new_7 / 7 as `业务新T7`
        ,income_7 / 7 as `收入T7`
        ,cost_real_7 / nullif(new_user_last_7, 0) / 0.96 as `CAC_T7`
        ,income_30 / 30 as `收入T30`
        ,new_user_last_30 / 30  as `平台新T30`
        ,hotel_inter_yw_new_30 / 30  as `业务新T30`

        ,income_30 / nullif(new_user_last_30, 0) as `ARPU_T30`
        ,(income_30 / nullif(new_user_last_30, 0)) / nullif(cost_real_7 / nullif(new_user_last_7, 0) / 0.96, 0) as `ROI`
        ,income_30 / nullif(cost_real_7, 0) as `ROI_30_7`
    from plan_daily
)
,plan_result as (--- 按每日动态指标生成10个分类
    select plan_id
        ,first_cost_dt
        ,note_scene_mix
        ,note_url
        ,note_cnt
        ,`消耗T7`
        ,`平台新T7`
        ,`业务新T7`
        ,`收入T30`
        ,`平台新T30`
        ,`业务新T30`
        ,`CAC_T7`
        ,`ARPU_T30`
        ,`ROI`
        ,`ROI_30_7`
        ,cost_real_30,new_user_last_30,hotel_inter_yw_new_30,income_30,cost_real_7,new_user_last_7,hotel_inter_yw_new_7,income_7
        --- 标签
        ,case when first_cost_dt is null then '1.近7天无消耗'
              when datediff(date_sub(current_date, 1),first_cost_dt) <= 7 then '2.7天以内'
              else '3.7天以上'
         end as first_cost_type
        ,case when `平台新T7` > 0 then '1.有平台新' else '2.无平台新' end as platform_new_t7_flag
        ,case when `业务新T7` > 0 then '1.有业务新' else '2.无业务新' end as business_new_t7_flag
        ,case when `消耗T7` < 0 then '1.无消耗'
              when `消耗T7` > 0   and `消耗T7` <= 100 then '2.日均消耗(0,100]' 
              when `消耗T7` > 100 and `消耗T7` <= 200 then '3.日均消耗(100,200]' 
              when `消耗T7` > 200 and `消耗T7` <= 300 then '4.日均消耗(200,300]' 
              when `消耗T7` > 300 and `消耗T7` <= 400 then '5.日均消耗(300,400]' 
              when `消耗T7` > 400 and `消耗T7` <= 500 then '6.日均消耗(400,500]' 
              when `消耗T7` > 500 then '7.日均消耗>500'
            end as cost_t7_flag
        ,case when `收入T30` >= 0 then '1.有收入' else '2.无收入' end as income_t30_flag
        ,case when `ROI` >= 0.2 then '1.ROI≥0.2' else '2.ROI<0.2' end as roi_flag
        ,case when `CAC_T7` >= 200 then '1.CAC≥200' else '2.CAC<200' end as cac_t7_flag

        --- 分类
        ,case when datediff(date_sub(current_date, 1),first_cost_dt) <= 7 then '1.7天以内'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` = 0 and `业务新T7` = 0 then '2.T7+无平台新无业务新'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` = 0 and `业务新T7` > 0 and `ROI` >= 0.2 then '3.T7+无平台新有业务新ROI高'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` = 0 and `业务新T7` > 0 and `ROI` < 0.2 then '4.T7+无平台新有业务新ROI低'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `收入T30` < 0 then '5.T7+有平台新收入<0'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` >= 200 and `ROI` >= 0.2 then '6.T7+有平台新CAC高ROI高'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` >= 200 and `ROI` < 0.2 then '7.T7+有平台新CAC高ROI低'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` < 200 and `ROI` >= 0.2 then '8.T7+有平台新CAC低ROI高'
              when datediff(date_sub(current_date, 1),first_cost_dt) > 7 and `平台新T7` > 0 and `CAC_T7` < 200 and `ROI` < 0.2 then '9.T7+有平台新CAC低ROI低'
              else '10.其他' end as category
        --- 重点分类
        ,case when `平台新T7` = 0 and `业务新T7` > 0  then '1.无平台新有业务新'
              when `消耗T7` >= 200 and `消耗T7` < 500 then '2.日均消耗(200,500]'
              when `消耗T7` >= 500 and `CAC_T7` > 300 then '3.日均消耗≥500且CAC＞300'
              else '4.其他' end as focus_category
    from plan_calc
)

select focus_category as `策略类型`
    ,category as `分类`
    ,plan_id as `计划ID`
    ,note_cnt as `贴数`
    ,note_scene_mix as `贴类型`
    -- ,post_type as `投放类型`
    ,note_url as `笔记URL`
    ,first_cost_dt as `首次消耗日期`
    
    ,`消耗T7` 
    ,`平台新T7`
    ,`业务新T7`
    ,`收入T30`
    ,`平台新T30`
    ,`业务新T30`
    ,`CAC_T7`
    ,`ARPU_T30`
    ,`ROI`
    ,`ROI_30_7`
    ,first_cost_type,platform_new_t7_flag,business_new_t7_flag,cost_t7_flag,income_t30_flag,roi_flag,cac_t7_flag
    ,cost_real_30,new_user_last_30,hotel_inter_yw_new_30,income_30,cost_real_7,new_user_last_7,hotel_inter_yw_new_7,income_7
    
from plan_result

order by `消耗T7`  desc
;