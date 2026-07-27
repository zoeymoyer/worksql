with base as (
    select dt
        ,sum(cost_real) as cost_real  --- 实际消耗
        ,sum(income) as income   --- 收入
        ,sum(new_user_last) as new_user_last -- 平台新
        ,sum(lost_user_last) as lost_user_last --- 流失老
        ,sum(hotel_inter_yw_new) as hotel_inter_yw_new --- 业务新
        ,sum(view_count) as view_count -- 曝光
        ,sum(valid_click_count) as valid_click_count -- 点击
        ,sum(flow_user_t0) as flow_user_t0
        ,sum(order_uv_last_t0) as order_uv_last_t0
        ,sum(order_num_last_t0) as order_num_last_t0
    from smm.ads_smm_redbook_touliu_creative_detail_hotel_inter_di
    where is_open = 1
    group by 1
)
,metric_base as (
    select dt
        ,cost_real
        ,income / nullif(cost_real,0) as roi
        ,cost_real / nullif(new_user_last,0) / 0.96 as cac
        ,(income) / nullif(new_user_last,0) as arpu
        ,new_user_last
        ,hotel_inter_yw_new
        ,valid_click_count / nullif(view_count,0) as ctr
        ,flow_user_t0 / nullif(valid_click_count,0) as t0_read_rate
        ,order_uv_last_t0 / nullif(flow_user_t0,0) as t0_order_rate
        ,(dayofweek(dt) + 5) % 7 + 1                                                          as day_of_week
    from base
)
,metric_wow as (
    select dt
        ,day_of_week
        ,cost_real
        ,lag(cost_real,7) over(order by dt) as last_cost_real

        ,roi
        ,lag(roi,7) over(order by dt) as last_roi

        ,cac
        ,lag(cac,7) over(order by dt) as last_cac

        ,arpu
        ,lag(arpu,7) over(order by dt) as last_arpu

        ,new_user_last
        ,lag(new_user_last,7) over(order by dt) as last_new_user_last

        ,hotel_inter_yw_new
        ,lag(hotel_inter_yw_new,7) over(order by dt) as last_hotel_inter_yw_new

        ,ctr
        ,lag(ctr,7) over(order by dt) as last_ctr

        ,t0_read_rate
        ,lag(t0_read_rate,7) over(order by dt) as last_t0_read_rate

        ,t0_order_rate
        ,lag(t0_order_rate,7) over(order by dt) as last_t0_order_rate
    from metric_base
)

select dt as "日期"
    ,day_of_week as "星期"
    ,cost_real as "实际消耗"
    ,concat(round((cost_real / nullif(last_cost_real,0)-1)*100,2),'%') as "消耗周环比"

    ,round(roi,4) as "roi"
    ,concat(round((roi / nullif(last_roi,0)-1)*100,2),'%') as "roi周环比"

    ,round(cac,2) as "cac"
    ,concat(round((cac / nullif(last_cac,0)-1)*100,2),'%') as "cac周环比"

    ,round(arpu,2) as "arpu"
    ,concat(round((arpu / nullif(last_arpu,0)-1)*100,2),'%') as "arpu周环比"

    ,new_user_last as "平台新"
    ,concat(round((new_user_last / nullif(last_new_user_last,0)-1)*100,2),'%') as "平台新周环比"

    ,hotel_inter_yw_new as "业务新"
    ,concat(round((hotel_inter_yw_new / nullif(last_hotel_inter_yw_new,0)-1)*100,2),'%') as "业务新周环比"

    ,concat(round(ctr*100,2),'%') as "ctr"
    ,concat(round((ctr - last_ctr)*100,2),'pp') as "ctr周环比"

    ,concat(round(t0_read_rate*100,2),'%') as "t0阅读引流比"
    ,concat(round((t0_read_rate - last_t0_read_rate)*100,2),'pp') as "t0阅读引流比周环比"

    ,concat(round(t0_order_rate*100,2),'%') as "t0引流下单比"
    ,concat(round((t0_order_rate - last_t0_order_rate)*100,2),'pp') as "t0引流下单比周环比"

from metric_wow
order by dt desc
;