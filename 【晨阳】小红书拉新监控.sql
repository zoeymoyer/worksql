-- 1) 日数据（全日期），波动=上周同期同比
with raw_data as (-- 日汇总底表（全量日期）
    select dt
        ,sum(cost_real)                                                                       as cost_real
        ,sum(flow_new_user)                                                                   as platform_new_uv
        ,sum(new_user_last)                                                                   as new_user_last
        ,sum(hotel_inter_yw_new)                                                              as biz_new_uv
        ,sum(valid_click_count)                                                               as read_uv
        ,sum(view_count)                                                                      as expo_uv
        ,sum(flow_user)                                                                       as flow_uv
        ,sum(order_user_last)                                                                 as order_uv
        ,sum(new_income)                                                                      as new_income
        ,sum(old_income)                                                                      as old_income
    from smm.ads_redbook_touliu_plan_detail_monitor_hotel_inter_di
    group by dt
)
,daily_metrics as (
    select dt
        ,cost_real                                                                            as cost_real
        ,(new_income + old_income) / nullif(cost_real, 0)                                     as roi
        ,cost_real / nullif(new_user_last * 0.96, 0)                                          as cac
        ,(new_income + old_income) / nullif(new_user_last, 0)                                 as arpu
        ,platform_new_uv                                                                      as platform_new_uv
        ,biz_new_uv                                                                           as biz_new_uv
        ,read_uv / nullif(expo_uv, 0)                                                         as ctr
        ,flow_uv / nullif(read_uv, 0)                                                         as read_flow_rate
        ,order_uv / nullif(flow_uv, 0)                                                        as flow_order_rate
        ,date_format(dt, 'u')                                                                 as day_of_week
        ,date_sub(dt, pmod(datediff(dt, '2024-01-05'), 7))                                    as week_start
        ,substr(dt, 1, 7)                                                                     as month_start
    from raw_data
)
select t1.dt                                                                                  as `日期`
    ,t1.day_of_week                                                                           as `星期`
    ,t1.cost_real                                                                             as `本期实际消耗`
    ,(t1.cost_real - t2.cost_real) / nullif(t2.cost_real, 0)                                  as `实际消耗周同比`
    ,t1.roi                                                                                   as `roi`
    ,(t1.roi - t2.roi) / nullif(t2.roi, 0)                                                    as `roi周同比`
    ,t1.cac                                                                                   as `cac`
    ,(t1.cac - t2.cac) / nullif(t2.cac, 0)                                                    as `cac周同比`
    ,t1.arpu                                                                                  as `arpu`
    ,(t1.arpu - t2.arpu) / nullif(t2.arpu, 0)                                                 as `arpu周同比`
    ,t1.platform_new_uv                                                                       as `平台新客uv`
    ,(t1.platform_new_uv - t2.platform_new_uv) / nullif(t2.platform_new_uv, 0)                as `平台新客周同比`
    ,t1.biz_new_uv                                                                            as `国际酒店业务新客`
    ,(t1.biz_new_uv - t2.biz_new_uv) / nullif(t2.biz_new_uv, 0)                               as `业务新周同比`
    ,t1.ctr                                                                                   as `ctr`
    ,t1.read_flow_rate                                                                        as `阅读引流比`
    ,t1.flow_order_rate                                                                       as `引流下单比`
from daily_metrics t1
left join daily_metrics t2 
    on t2.dt = date_sub(t1.dt, 7)
order by t1.dt desc;


-- 2) 周日均(周五~周四)，波动=周环比
with raw_data as (
    select dt
        ,sum(cost_real)                                                                       as cost_real
        ,sum(flow_new_user)                                                                   as platform_new_uv
        ,sum(new_user_last)                                                                   as new_user_last
        ,sum(hotel_inter_yw_new)                                                              as biz_new_uv
        ,sum(valid_click_count)                                                               as read_uv
        ,sum(view_count)                                                                      as expo_uv
        ,sum(flow_user)                                                                       as flow_uv
        ,sum(order_user_last)                                                                 as order_uv
        ,sum(new_income)                                                                      as new_income
        ,sum(old_income)                                                                      as old_income
    from smm.ads_redbook_touliu_plan_detail_monitor_hotel_inter_di
    group by dt
)
,daily_metrics as (
    select dt
        ,cost_real                                                                            as cost_real
        ,(new_income + old_income) / nullif(cost_real, 0)                                     as roi
        ,cost_real / nullif(new_user_last * 0.96, 0)                                          as cac
        ,(new_income + old_income) / nullif(new_user_last, 0)                                 as arpu
        ,platform_new_uv                                                                      as platform_new_uv
        ,biz_new_uv                                                                           as biz_new_uv
        ,read_uv / nullif(expo_uv, 0)                                                         as ctr
        ,flow_uv / nullif(read_uv, 0)                                                         as read_flow_rate
        ,order_uv / nullif(flow_uv, 0)                                                        as flow_order_rate
        ,date_sub(dt, pmod(datediff(dt, '2024-01-05'), 7))                                    as week_start
    from raw_data
)
,weekly_avg as (
    select week_start
        ,avg(cost_real)                                                                       as avg_cost_real
        ,avg(roi)                                                                             as avg_roi
        ,avg(cac)                                                                             as avg_cac
        ,avg(arpu)                                                                            as avg_arpu
        ,avg(platform_new_uv)                                                                 as avg_platform_new_uv
        ,avg(biz_new_uv)                                                                      as avg_biz_new_uv
        ,avg(ctr)                                                                             as avg_ctr
        ,avg(read_flow_rate)                                                                  as avg_read_flow_rate
        ,avg(flow_order_rate)                                                                 as avg_flow_order_rate
    from daily_metrics
    group by week_start
)
select substr(t1.week_start, 1, 4)                                                            as `年份`
    ,concat(date_format(t1.week_start, 'MMdd'), '~', date_format(date_add(t1.week_start, 6), 'MMdd')) as `自然周`
    ,t1.avg_cost_real                                                                         as `日均实际消耗`
    ,(t1.avg_cost_real - t2.avg_cost_real) / nullif(t2.avg_cost_real, 0)                      as `日均消耗周环比`
    ,t1.avg_roi                                                                               as `roi`
    ,(t1.avg_roi - t2.avg_roi) / nullif(t2.avg_roi, 0)                                        as `roi周环比`
    ,t1.avg_cac                                                                               as `cac`
    ,(t1.avg_cac - t2.avg_cac) / nullif(t2.avg_cac, 0)                                        as `cac周环比`
    ,t1.avg_arpu                                                                              as `arpu`
    ,(t1.avg_arpu - t2.avg_arpu) / nullif(t2.avg_arpu, 0)                                     as `arpu周环比`
    ,t1.avg_platform_new_uv                                                                   as `日均平台新客uv`
    ,(t1.avg_platform_new_uv - t2.avg_platform_new_uv) / nullif(t2.avg_platform_new_uv, 0)    as `日均平台新客周环比`
    ,t1.avg_biz_new_uv                                                                        as `日均业务新客`
    ,(t1.avg_biz_new_uv - t2.avg_biz_new_uv) / nullif(t2.avg_biz_new_uv, 0)                   as `日均业务新周环比`
    ,t1.avg_ctr                                                                               as `ctr`
    ,t1.avg_read_flow_rate                                                                    as `阅读引流比`
    ,t1.avg_flow_order_rate                                                                   as `引流下单比`
from weekly_avg t1
left join weekly_avg t2 
    on t2.week_start = date_sub(t1.week_start, 7)
order by t1.week_start desc;


-- 3) 月日均(自然月)，波动=月环比
with raw_data as (
    select dt
        ,sum(cost_real)                                                                       as cost_real
        ,sum(flow_new_user)                                                                   as platform_new_uv
        ,sum(new_user_last)                                                                   as new_user_last
        ,sum(hotel_inter_yw_new)                                                              as biz_new_uv
        ,sum(valid_click_count)                                                               as read_uv
        ,sum(view_count)                                                                      as expo_uv
        ,sum(flow_user)                                                                       as flow_uv
        ,sum(order_user_last)                                                                 as order_uv
        ,sum(new_income)                                                                      as new_income
        ,sum(old_income)                                                                      as old_income
    from smm.ads_redbook_touliu_plan_detail_monitor_hotel_inter_di
    group by dt
)
,daily_metrics as (
    select dt
        ,cost_real                                                                            as cost_real
        ,(new_income + old_income) / nullif(cost_real, 0)                                     as roi
        ,cost_real / nullif(new_user_last * 0.96, 0)                                          as cac
        ,(new_income + old_income) / nullif(new_user_last, 0)                                 as arpu
        ,platform_new_uv                                                                      as platform_new_uv
        ,biz_new_uv                                                                           as biz_new_uv
        ,read_uv / nullif(expo_uv, 0)                                                         as ctr
        ,flow_uv / nullif(read_uv, 0)                                                         as read_flow_rate
        ,order_uv / nullif(flow_uv, 0)                                                        as flow_order_rate
        ,substr(dt, 1, 7)                                                                     as month_start
    from raw_data
)
,monthly_avg as (
    select month_start
        ,avg(cost_real)                                                                       as avg_cost_real
        ,avg(roi)                                                                             as avg_roi
        ,avg(cac)                                                                             as avg_cac
        ,avg(arpu)                                                                            as avg_arpu
        ,avg(platform_new_uv)                                                                 as avg_platform_new_uv
        ,avg(biz_new_uv)                                                                      as avg_biz_new_uv
        ,avg(ctr)                                                                             as avg_ctr
        ,avg(read_flow_rate)                                                                  as avg_read_flow_rate
        ,avg(flow_order_rate)                                                                 as avg_flow_order_rate
    from daily_metrics
    group by month_start
)
select t1.month_start                                                                         as `自然月`
    ,t1.avg_cost_real                                                                         as `日均实际消耗`
    ,(t1.avg_cost_real - t2.avg_cost_real) / nullif(t2.avg_cost_real, 0)                      as `日均消耗月环比`
    ,t1.avg_roi                                                                               as `roi`
    ,(t1.avg_roi - t2.avg_roi) / nullif(t2.avg_roi, 0)                                        as `roi月环比`
    ,t1.avg_cac                                                                               as `cac`
    ,(t1.avg_cac - t2.avg_cac) / nullif(t2.avg_cac, 0)                                        as `cac月环比`
    ,t1.avg_arpu                                                                              as `arpu`
    ,(t1.avg_arpu - t2.avg_arpu) / nullif(t2.avg_arpu, 0)                                     as `arpu月环比`
    ,t1.avg_platform_new_uv                                                                   as `日均平台新客uv`
    ,(t1.avg_platform_new_uv - t2.avg_platform_new_uv) / nullif(t2.avg_platform_new_uv, 0)    as `日均平台新客月环比`
    ,t1.avg_biz_new_uv                                                                        as `日均业务新客`
    ,(t1.avg_biz_new_uv - t2.avg_biz_new_uv) / nullif(t2.avg_biz_new_uv, 0)                   as `日均业务新月环比`
    ,t1.avg_ctr                                                                               as `ctr`
    ,t1.avg_read_flow_rate                                                                    as `阅读引流比`
    ,t1.avg_flow_order_rate                                                                   as `引流下单比`
from monthly_avg t1
left join monthly_avg t2 
    on t2.month_start = substr(add_months(concat(t1.month_start, '-01'), -1), 1, 7)
order by t1.month_start desc;



-- 4) 计划维度日数据（全日期），波动=上周同期同比
with raw_data as (
    select dt
        ,plan_id
        ,datediff(date_sub(current_date,1), plan_create_dt)                                as plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,sum(cost_real)                                                                       as cost_real
        ,sum(flow_new_user)                                                                   as platform_new_uv
        ,sum(new_user_last)                                                                   as new_user_last
        ,sum(hotel_inter_yw_new)                                                              as biz_new_uv
        ,sum(valid_click_count)                                                               as read_uv
        ,sum(view_count)                                                                      as expo_uv
        ,sum(flow_user)                                                                       as flow_uv
        ,sum(order_user_last)                                                                 as order_uv
        ,sum(new_income)                                                                      as new_income
        ,sum(old_income)                                                                      as old_income
    from smm.ads_redbook_touliu_plan_detail_monitor_hotel_inter_di
    group by dt
        ,plan_id
        ,plan_create_dt
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
)
,daily_metrics as (
    select dt
        ,plan_id
        ,plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,cost_real                                                                            as cost_real
        ,(new_income + old_income) / nullif(cost_real, 0)                                     as roi
        ,cost_real / nullif(new_user_last * 0.96, 0)                                          as cac
        ,(new_income + old_income) / nullif(new_user_last, 0)                                 as arpu
        ,platform_new_uv                                                                      as platform_new_uv
        ,biz_new_uv                                                                           as biz_new_uv
        ,read_uv / nullif(expo_uv, 0)                                                         as ctr
        ,flow_uv / nullif(read_uv, 0)                                                         as read_flow_rate
        ,order_uv / nullif(flow_uv, 0)                                                        as flow_order_rate
        ,(dayofweek(dt) + 5) % 7 + 1                                                          as day_of_week
    from raw_data
)


select t1.dt                                                                                  as "日期"
    ,t1.day_of_week                                                                           as "星期"
    ,t1.plan_id                                                                               as "计划id"
    ,t1.plan_online_cycle                                                                     as "计划在线周期"
    ,t1.post_type                                                                             as "贴类型"
    ,t1.scene_second_v2_mix                                                                   as "二级场景"
    ,t1.postback_type                                                                         as "回传类型"
    ,t1.city                                                                                  as "城市"
    ,t1.cost_real                                                                             as "本期实际消耗"
    ,(t1.cost_real - t2.cost_real) / nullif(t2.cost_real, 0)                                  as "实际消耗周同比"
    ,t1.roi                                                                                   as "roi"
    ,(t1.roi - t2.roi) / nullif(t2.roi, 0)                                                    as "roi周同比"
    ,t1.cac                                                                                   as "cac"
    ,(t1.cac - t2.cac) / nullif(t2.cac, 0)                                                    as "cac周同比"
    ,t1.arpu                                                                                  as "arpu"
    ,(t1.arpu - t2.arpu) / nullif(t2.arpu, 0)                                                 as "arpu周同比"
    ,t1.platform_new_uv                                                                       as "平台新客uv"
    ,(t1.platform_new_uv - t2.platform_new_uv) / nullif(t2.platform_new_uv, 0)                as "平台新客周同比"
    ,t1.biz_new_uv                                                                            as "国际酒店业务新客"
    ,(t1.biz_new_uv - t2.biz_new_uv) / nullif(t2.biz_new_uv, 0)                               as "业务新周同比"
    ,t1.ctr                                                                                   as "ctr"
    ,t1.read_flow_rate                                                                        as "阅读引流比"
    ,t1.flow_order_rate                                                                       as "引流下单比"
from daily_metrics t1
left join daily_metrics t2 
    on t2.dt = date_sub(t1.dt, 7)
    and t1.plan_id = t2.plan_id
    and t1.post_type = t2.post_type
    and t1.scene_second_v2_mix = t2.scene_second_v2_mix
    and t1.postback_type = t2.postback_type
    and t1.city = t2.city
order by t1.dt desc, t1.cost_real  desc;

-- 5) 计划维度周日均(周五~周四)，波动=周环比
with raw_data as (
    select dt
        ,plan_id
        ,datediff(date_sub(current_date, 1), plan_create_dt)                                  as plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,sum(cost_real)                                                                       as cost_real
        ,sum(flow_new_user)                                                                   as platform_new_uv
        ,sum(new_user_last)                                                                   as new_user_last
        ,sum(hotel_inter_yw_new)                                                              as biz_new_uv
        ,sum(valid_click_count)                                                               as read_uv
        ,sum(view_count)                                                                      as expo_uv
        ,sum(flow_user)                                                                       as flow_uv
        ,sum(order_user_last)                                                                 as order_uv
        ,sum(new_income)                                                                      as new_income
        ,sum(old_income)                                                                      as old_income
    from smm.ads_redbook_touliu_plan_detail_monitor_hotel_inter_di
    group by 1,2,3,4,5,6,7
)
,daily_metrics as (
    select dt
        ,plan_id
        ,plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,cost_real                                                                            as cost_real
        ,(new_income + old_income) / nullif(cost_real, 0)                                     as roi
        ,cost_real / nullif(new_user_last * 0.96, 0)                                          as cac
        ,(new_income + old_income) / nullif(new_user_last, 0)                                 as arpu
        ,platform_new_uv                                                                      as platform_new_uv
        ,biz_new_uv                                                                           as biz_new_uv
        ,read_uv / nullif(expo_uv, 0)                                                         as ctr
        ,flow_uv / nullif(read_uv, 0)                                                         as read_flow_rate
        ,order_uv / nullif(flow_uv, 0)                                                        as flow_order_rate
        ,date_sub(dt, pmod(datediff(dt, '2024-01-05'), 7))                                    as week_start
    from raw_data
)
,weekly_avg as (
    select week_start
        ,plan_id
        ,max(plan_online_cycle)                                                               as plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,avg(cost_real)                                                                       as avg_cost_real
        ,avg(roi)                                                                             as avg_roi
        ,avg(cac)                                                                             as avg_cac
        ,avg(arpu)                                                                            as avg_arpu
        ,avg(platform_new_uv)                                                                 as avg_platform_new_uv
        ,avg(biz_new_uv)                                                                      as avg_biz_new_uv
        ,avg(ctr)                                                                             as avg_ctr
        ,avg(read_flow_rate)                                                                  as avg_read_flow_rate
        ,avg(flow_order_rate)                                                                 as avg_flow_order_rate
    from daily_metrics
    group by week_start
        ,plan_id
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
)
select substr(t1.week_start, 1, 4)                                                            as "年份"
    ,concat(date_format(t1.week_start, '%m%d'), '~', date_format(date_add(t1.week_start, 6), '%m%d')) as "自然周"
    ,t1.plan_id                                                                               as "计划id"
    ,t1.plan_online_cycle                                                                     as "计划在线周期"
    ,t1.post_type                                                                             as "贴类型"
    ,t1.scene_second_v2_mix                                                                   as "二级场景"
    ,t1.postback_type                                                                         as "回传类型"
    ,t1.city                                                                                  as "城市"
    ,t1.avg_cost_real                                                                         as "日均实际消耗"
    ,(t1.avg_cost_real - t2.avg_cost_real) / nullif(t2.avg_cost_real, 0)                      as "日均消耗周环比"
    ,t1.avg_roi                                                                               as "roi"
    ,(t1.avg_roi - t2.avg_roi) / nullif(t2.avg_roi, 0)                                        as "roi周环比"
    ,t1.avg_cac                                                                               as "cac"
    ,(t1.avg_cac - t2.avg_cac) / nullif(t2.avg_cac, 0)                                        as "cac周环比"
    ,t1.avg_arpu                                                                              as "arpu"
    ,(t1.avg_arpu - t2.avg_arpu) / nullif(t2.avg_arpu, 0)                                     as "arpu周环比"
    ,t1.avg_platform_new_uv                                                                   as "日均平台新客uv"
    ,(t1.avg_platform_new_uv - t2.avg_platform_new_uv) / nullif(t2.avg_platform_new_uv, 0)    as "日均平台新客周环比"
    ,t1.avg_biz_new_uv                                                                        as "日均业务新客"
    ,(t1.avg_biz_new_uv - t2.avg_biz_new_uv) / nullif(t2.avg_biz_new_uv, 0)                   as "日均业务新周环比"
    ,t1.avg_ctr                                                                               as "ctr"
    ,t1.avg_read_flow_rate                                                                    as "阅读引流比"
    ,t1.avg_flow_order_rate                                                                   as "引流下单比"
from weekly_avg t1
left join weekly_avg t2 
    on t2.week_start = date_sub(t1.week_start, 7)
    and t1.plan_id = t2.plan_id
    and t1.post_type = t2.post_type
    and t1.scene_second_v2_mix = t2.scene_second_v2_mix
    and t1.postback_type = t2.postback_type
    and t1.city = t2.city
order by t1.week_start desc, t1.avg_cost_real desc;


-- 6) 计划维度月日均(自然月)，波动=月环比
with raw_data as (
    select dt
        ,plan_id
        ,datediff(date_sub(current_date, 1), plan_create_dt)                                as plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,sum(cost_real)                                                                       as cost_real
        ,sum(flow_new_user)                                                                   as platform_new_uv
        ,sum(new_user_last)                                                                   as new_user_last
        ,sum(hotel_inter_yw_new)                                                              as biz_new_uv
        ,sum(valid_click_count)                                                               as read_uv
        ,sum(view_count)                                                                      as expo_uv
        ,sum(flow_user)                                                                       as flow_uv
        ,sum(order_user_last)                                                                 as order_uv
        ,sum(new_income)                                                                      as new_income
        ,sum(old_income)                                                                      as old_income
    from smm.ads_redbook_touliu_plan_detail_monitor_hotel_inter_di
    group by dt
        ,plan_id
        ,plan_create_dt
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
)
,daily_metrics as (
    select dt
        ,plan_id
        ,plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,cost_real                                                                            as cost_real
        ,(new_income + old_income) / nullif(cost_real, 0)                                     as roi
        ,cost_real / nullif(new_user_last * 0.96, 0)                                          as cac
        ,(new_income + old_income) / nullif(new_user_last, 0)                                 as arpu
        ,platform_new_uv                                                                      as platform_new_uv
        ,biz_new_uv                                                                           as biz_new_uv
        ,read_uv / nullif(expo_uv, 0)                                                         as ctr
        ,flow_uv / nullif(read_uv, 0)                                                         as read_flow_rate
        ,order_uv / nullif(flow_uv, 0)                                                        as flow_order_rate
        ,substr(dt, 1, 7)                                                                     as month_start
    from raw_data
)
,monthly_avg as (
    select month_start
        ,plan_id
        ,max(plan_online_cycle)                                                               as plan_online_cycle
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
        ,avg(cost_real)                                                                       as avg_cost_real
        ,avg(roi)                                                                             as avg_roi
        ,avg(cac)                                                                             as avg_cac
        ,avg(arpu)                                                                            as avg_arpu
        ,avg(platform_new_uv)                                                                 as avg_platform_new_uv
        ,avg(biz_new_uv)                                                                      as avg_biz_new_uv
        ,avg(ctr)                                                                             as avg_ctr
        ,avg(read_flow_rate)                                                                  as avg_read_flow_rate
        ,avg(flow_order_rate)                                                                 as avg_flow_order_rate
    from daily_metrics
    group by month_start
        ,plan_id
        ,post_type
        ,scene_second_v2_mix
        ,postback_type
        ,city
)
select t1.month_start                                                                         as "自然月"
    ,t1.plan_id                                                                               as "计划id"
    ,t1.plan_online_cycle                                                                     as "计划在线周期"
    ,t1.post_type                                                                             as "贴类型"
    ,t1.scene_second_v2_mix                                                                   as "二级场景"
    ,t1.postback_type                                                                         as "回传类型"
    ,t1.city                                                                                  as "城市"
    ,t1.avg_cost_real                                                                         as "日均实际消耗"
    ,(t1.avg_cost_real - t2.avg_cost_real) / nullif(t2.avg_cost_real, 0)                      as "日均消耗月环比"
    ,t1.avg_roi                                                                               as "roi"
    ,(t1.avg_roi - t2.avg_roi) / nullif(t2.avg_roi, 0)                                        as "roi月环比"
    ,t1.avg_cac                                                                               as "cac"
    ,(t1.avg_cac - t2.avg_cac) / nullif(t2.avg_cac, 0)                                        as "cac月环比"
    ,t1.avg_arpu                                                                              as "arpu"
    ,(t1.avg_arpu - t2.avg_arpu) / nullif(t2.avg_arpu, 0)                                     as "arpu月环比"
    ,t1.avg_platform_new_uv                                                                   as "日均平台新客uv"
    ,(t1.avg_platform_new_uv - t2.avg_platform_new_uv) / nullif(t2.avg_platform_new_uv, 0)    as "日均平台新客月环比"
    ,t1.avg_biz_new_uv                                                                        as "日均业务新客"
    ,(t1.avg_biz_new_uv - t2.avg_biz_new_uv) / nullif(t2.avg_biz_new_uv, 0)                   as "日均业务新月环比"
    ,t1.avg_ctr                                                                               as "ctr"
    ,t1.avg_read_flow_rate                                                                    as "阅读引流比"
    ,t1.avg_flow_order_rate                                                                   as "引流下单比"
from monthly_avg t1
left join monthly_avg t2 
    on t2.month_start = substr(add_months(concat(t1.month_start, '-01'), -1), 1, 7)
    and t1.plan_id = t2.plan_id
    and t1.post_type = t2.post_type
    and t1.scene_second_v2_mix = t2.scene_second_v2_mix
    and t1.postback_type = t2.postback_type
    and t1.city = t2.city
order by t1.month_start desc, t1.avg_cost_real desc;