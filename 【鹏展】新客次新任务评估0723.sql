-- 1、实验评估
with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date
                ,user_info['orig_device_id'] as orig_device_id
                ,user_id
                ,user_name
                ,count(order_no) as order_nos_90
                ,sum(room_night) as room_nights_90
            from mdw_order_v3_international
            where dt = '%(DATE)s'
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2,3,4
        )a where room_nights_90>=15
    group by 1
)
,abtest AS (--- 实验明细
    select  a.dt,
            ab_version version,
            ab_exp_value AS user_id
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    where a.dt >= '2026-07-10'  and a.dt <= '2026-07-16'
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '260608_ho_gj_NewCustomerTask'
        and ab_exp_value not in (select user_id from biguser)  --- 去除大单用户
    group by 1,2,3
)
,user_first_order as(--- 获取首单日期用于判定绝对新客
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
,user_daily_orders as (--- 预聚合：每个用户每一天的订单量
    select user_id
            ,order_date
            ,count(distinct order_no) as daily_order_cnt
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1, 2
)
,uv as (--分日去重活跃用户
    select  a.dt 
            ,case when a.province_name in ('澳门','香港') then a.province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case  when a.dt <= b.min_order_date or b.min_order_date is null then '新客'
                   when coalesce(sum(o.daily_order_cnt), 0) <= 3 then '次新用户'
                   else '老客' 
             end as user_type  --- 动态聚合访问前的订单量进行判定
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_first_order b on a.user_id = b.user_id 
    left join user_daily_orders o on a.user_id = o.user_id and o.order_date < a.dt  --- 核心：只关联访问日期 dt 之前的订单
    where a.dt >= '2026-07-10'
        and a.dt <= '2026-07-16'
        and a.business_type = 'hotel'
        and (a.province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (a.search_pv + a.detail_pv + a.booking_pv + a.order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 
        a.dt
        ,case when a.province_name in ('澳门','香港') then a.province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end
        ,a.user_id
        ,a.user_name
        ,b.min_order_date
)
,q_order as (
    select a.order_date
        ,case  when a.order_date = b.min_order_date then '新客'
                when coalesce(sum(o.daily_order_cnt), 0) <= 3 then '次新用户'
                else '老客' 
             end as user_type
        ,a.order_no,a.user_id,a.user_name,room_night,init_gmv
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
        ,coalesce(sum(o.daily_order_cnt), 0) daily_order_cnt
    from default.mdw_order_v3_international a
    left join user_first_order b on a.user_id = b.user_id 
    left join user_daily_orders o on a.user_id = o.user_id and o.order_date < a.order_date  --- 核心：只累加当前下单日之前的订单量
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where  dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        -- and terminal_channel_type in ('www', 'app', 'touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
        and (refund_time is null or date(refund_time) > a.order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2026-06-15' and a.order_date <= date_sub(current_date, 1)
    group by a.order_date,a.order_no,a.user_id,a.user_name,b.min_order_date,room_night,init_gmv
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end
        ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end
)
,q_conpon_info as (
    select order_no
        ,sum(coupon_amount) as coupon_amount
        ,coalesce(sum(case when coupon_type = 0 then coupon_amount end), 0) as coupon_amount0
        ,coalesce(sum(case when coupon_type = 1 then coupon_amount end), 0) as coupon_amount1
        ,coalesce(sum(case when coupon_type = 2 then coupon_amount end), 0) as coupon_amount2
        ,coalesce(sum(case when coupon_type = 3 then coupon_amount end), 0) as coupon_amount3
        ,coalesce(sum(case when coupon_type = 4 then coupon_amount end), 0) as coupon_amount4
        ,coalesce(sum(case when coupon_type = 5 then coupon_amount end), 0) as coupon_amount5
        ,coalesce(sum(case when coupon_type = 6 then coupon_amount end), 0) as coupon_amount6
        ,coalesce(sum(case when coupon_type = 7 then coupon_amount end), 0) as coupon_amount7
        ,coalesce(sum(case when coupon_type = 8 then coupon_amount end), 0) as coupon_amount8
        ,coalesce(sum(case when coupon_type = 9 then coupon_amount end), 0) as coupon_amount9
        ,coalesce(sum(case when coupon_type = 10 then coupon_amount end), 0) as coupon_amount10
    from (
        select order_no
            ,coalesce(group_code, 0) as coupon_serie
            ,coalesce(cast(split(coupon_substract, ',')[pos] as int), 0) as coupon_amount
            ,coalesce(coupon_detail[group_code], 0) as coupon_type
        from (
            select coupon_detail
                ,batch_series
                ,coupon_substract
                ,coupon_substract_summary
                ,coalesce(cashbackmap['voucher_amount'], 0) as voucher_amount
                ,coalesce(cashbackmap['voucher_pack_price'], 0) as voucher_pack_price
                ,order_no
                ,order_date
                ,init_gmv
            from default.mdw_order_v3_international a
            where dt = '%(DATE)s'
                and order_date between date_add('%(FORMAT_DATE)s', -14) and '%(FORMAT_DATE)s'
                and order_date >= '2026-06-15'
                and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
                and terminal_channel_type = 'app'
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
                and (first_rejected_time is null or date(first_rejected_time) > order_date)
                and (refund_time is null or date(refund_time) > order_date)
                and is_valid = '1'
                and order_no <> '103576132435'
        ) q
        lateral view posexplode(split(batch_series, ',')) t as pos, group_code
    ) z
    group by 1
)
,ab_uv as (--- 流量层按实验聚合
    select  a.dt
            ,exp.version
            ,count( a.user_id) as uv
    from uv a
    inner join abtest exp on a.dt = exp.dt and a.user_id = exp.user_id  
    where a.user_type in ('新客', '次新用户')
    group by 1,2
)
,ab_order as (--- 订单层按实验聚合
    select  a.order_date dt
            ,exp.version
            ,count(distinct a.user_id) as order_uv
            ,count(distinct a.order_no) as order_cnt
            ,sum(a.room_night) as room_night
            ,sum(a.init_gmv) as init_gmv
            ,sum(a.final_commission_after) as final_commission_after
            ,sum(a.coupon_substract_summary) as coupon_substract_summary
            ,sum(coupon_amount) as coupon_amount  --"券补总额"
            ,sum(coupon_amount1 + coupon_amount2 + coupon_amount6) as coupon_amount_1_2_6  --"券补_竞争"
            ,sum(coupon_amount4 + coupon_amount5) as coupon_amount_4_5  --"券补_叠加"
    from q_order a
    inner join abtest exp on a.order_date = exp.dt and a.user_id = exp.user_id 
    left join q_conpon_info c on a.order_no = c.order_no
    where a.user_type in ('新客', '次新用户')
    group by 1,2
)

--- 结果汇总大盘输出层
select  coalesce(t1.dt, t2.dt) as "日期"
        ,coalesce(t1.version, t2.version) as "ab分组"
        ,sum(coalesce(t1.uv, 0)) as "新客次新UV"
        ,sum(coalesce(t2.order_uv, 0)) as "生单uv"
        ,sum(coalesce(t2.order_cnt, 0)) as "订单量"
        ,sum(coalesce(t2.room_night, 0)) as "间夜量"
        ,sum(coalesce(t2.init_gmv, 0)) as "GMV"
        ,sum(coalesce(t2.final_commission_after, 0)) as "佣金"
        ,sum(coalesce(t2.coupon_substract_summary, 0)) as "券额"
        ,sum(coalesce(t2.coupon_amount, 0)) as "券补总额"
        ,sum(coalesce(t2.coupon_amount_1_2_6, 0)) as "券补_竞争"
        ,sum(coalesce(t2.coupon_amount_4_5, 0)) as "券补_叠加"
 
        ,concat(round(coalesce(sum(t2.order_cnt)/nullif(sum(t1.uv), 0), 0) * 100, 2), '%') as "次新CR"
        ,concat(round(coalesce(sum(t2.coupon_substract_summary)/nullif(sum(t2.init_gmv), 0), 0) * 100, 2), '%') as "券补率"
        ,concat(round(coalesce(sum(t2.coupon_amount_1_2_6)/nullif(sum(t2.init_gmv), 0), 0) * 100, 2), '%') as "券补_竞争率"
        ,concat(round(coalesce(sum(t2.coupon_amount_4_5)/nullif(sum(t2.init_gmv), 0), 0) * 100, 2), '%') as "券补_叠加率"
        ,concat(round(coalesce(sum(t2.final_commission_after)/nullif(sum(t2.init_gmv), 0), 0) * 100, 2), '%') as "佣金率"
        ,concat(round(coalesce(sum(t2.room_night)/nullif(sum(t1.uv), 0), 0) * 100, 2), '%') as "单UV间夜"
        ,concat(round(coalesce(sum(t2.final_commission_after)/nullif(sum(t1.uv), 0), 0) * 100, 2), '%') as "单UV收益"
        ,concat(round(coalesce(sum(t2.coupon_substract_summary)/nullif(sum(t1.uv), 0), 0) * 100, 2), '%') as "单UV券补"

from ab_uv t1
full outer join ab_order t2 on t1.dt = t2.dt and t1.version = t2.version
group by coalesce(t1.dt, t2.dt), coalesce(t1.version, t2.version)
order by "日期" desc, "ab分组"
;


-- 2、实验组与对照组新客次新生单用户复购跃迁分析
with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date
                ,user_info['orig_device_id'] as orig_device_id
                ,user_id
                ,user_name
                ,count(order_no) as order_nos_90
                ,sum(room_night) as room_nights_90
            from mdw_order_v3_international
            where dt = '%(DATE)s'
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2,3,4
        )a where room_nights_90>=15
    group by 1
)
,abtest AS (--- 实验明细
    select  a.dt,
            ab_version version,
            ab_exp_value AS user_id
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    where a.dt >= '2026-07-10'  and a.dt <= '2026-07-16'
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '260608_ho_gj_NewCustomerTask'
        and ab_exp_value not in (select user_id from biguser)  --- 去除大单用户
    group by 1,2,3
)
,user_first_order as ( --- 获取首单日期用于判定绝对新客
    select user_id
        ,min(order_date) as min_order_date
    from default.mdw_order_v3_international --- 海外订单表
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name != '中国')
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
    group by 1
)
,user_daily_orders as ( --- 预聚合：每个用户每一天的订单量
    select user_id
        ,order_date
        ,count(distinct order_no) as daily_order_cnt
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name != '中国')
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
    group by 1,2
)
,q_order as ( --- 订单明细及新客次新判定
    select a.order_date
        ,a.order_time
        ,case when a.order_date = b.min_order_date then '新客'
              when coalesce(sum(o.daily_order_cnt), 0) <= 3 then '次新用户'
              else '老客'
         end as user_type
        ,a.order_no
        ,a.user_id
        ,a.user_name
        ,a.room_night
        ,a.init_gmv
        ,coalesce(sum(o.daily_order_cnt), 0) as daily_order_cnt
    from default.mdw_order_v3_international a
    left join user_first_order b on a.user_id = b.user_id
    left join user_daily_orders o on a.user_id = o.user_id and o.order_date < a.order_date
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        -- and terminal_channel_type in ('www','app','touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date)
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date)
        and (refund_time is null or date(refund_time) > a.order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2026-06-15'
        and a.order_date <= date_sub(current_date, 1)
    group by a.order_date
        ,a.order_time
        ,a.order_no
        ,a.user_id
        ,a.user_name
        ,b.min_order_date
        ,a.room_night
        ,a.init_gmv
)
,ab_order_user_detail as ( --- 实验周期内已生单的新客次新用户
    select a.order_date
        ,unix_timestamp(a.order_time) as order_time
        ,a.order_no
        ,a.user_id
        ,a.user_type
        ,exp.version
        ,case when exp.version in ('A','B','C') then '实验组'
              when exp.version in ('D','E') then '对照组'
         end as exp_group
        ,row_number() over(
            partition by a.user_id
            order by a.order_date,unix_timestamp(a.order_time),a.order_no
        ) as rn
    from q_order a
    inner join abtest exp on a.order_date = exp.dt and a.user_id = exp.user_id
    where a.user_type in ('新客','次新用户')
)
,target_user as ( --- 每个生单用户实验周期内第一笔订单
    select user_id
        ,version
        ,exp_group
        ,user_type
        ,order_date as first_order_date
        ,order_time as first_order_time
        ,order_no as first_order_no
    from ab_order_user_detail
    where rn = 1
)
,user_order_sequence as ( --- 从实验周期第一笔订单开始计算第1、2、3、4单
    select t1.user_id
        ,t2.version
        ,t2.exp_group
        ,t2.user_type
        ,t1.order_no
        ,t1.order_date
        ,unix_timestamp(t1.order_time) as order_time
        ,row_number() over(
            partition by t1.user_id
            order by t1.order_date,unix_timestamp(t1.order_time),t1.order_no
        ) as order_sequence
    from q_order t1
    inner join target_user t2 on t1.user_id = t2.user_id
    where unix_timestamp(t1.order_time) >= t2.first_order_time
)
,user_order_summary as ( --- 用户从实验订单起累计完成的订单数
    select user_id
        ,exp_group
        ,version
        --,user_type
        ,max(order_sequence) as order_cnt
    from user_order_sequence
    group by 1,2,3
)
,reach_node_summary as ( --- 达到各订单节点的累计用户数
    select exp_group
       -- ,user_type
        ,version
        ,'1单' as order_node
        ,1 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 1
    group by 1,2
    union all
    select exp_group
        --,user_type
        ,version
        ,'2单' as order_node
        ,2 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 2
    group by 1,2
    union all
    select exp_group
        --,user_type
        ,version
        ,'3单' as order_node
        ,3 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 3
    group by 1,2
    union all
    select exp_group
        --,user_type
        ,version
        ,'4单' as order_node
        ,4 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 4
    group by 1,2
)
,exact_order_summary as ( --- 用户最终订单数分布
    select exp_group
        ,version
        ,'1单' as order_node
        ,1 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt = 1
    group by 1,2
    union all
    select exp_group
        ,version
        ,'2单' as order_node
        ,2 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt = 2
    group by 1,2
    union all
    select exp_group
        ,version
        ,'3单' as order_node
        ,3 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt = 3
    group by 1,2
    union all
    select exp_group
        ,version
        ,'4单及以上' as order_node
        ,4 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 4
    group by 1,2
)
,user_order_interval_base as ( --- 获取相邻两单日期
    select user_id
        ,exp_group
        ,version
        ,order_sequence
        ,order_date
        ,lag(order_date, 1) over(
            partition by user_id
            order by order_sequence
        ) as last_order_date
    from user_order_sequence
)
,user_order_interval as ( --- 1-2、2-3、3-4单间隔
    select user_id
        ,exp_group
        ,version
        ,case when order_sequence = 2 then '1-2单'
              when order_sequence = 3 then '2-3单'
              when order_sequence = 4 then '3-4单'
         end as order_node
        ,order_sequence - 1 as node_order
        ,datediff(order_date, last_order_date) as interval_days
    from user_order_interval_base
    where order_sequence between 2 and 4
        and last_order_date is not null
)
,interval_summary as ( --- 分订单节点复购间隔
    select exp_group
        ,version
        ,order_node
        ,count(distinct user_id) as user_cnt
        ,count(*) as interval_cnt
        ,avg(interval_days) as avg_interval_days
        ,percentile_approx(interval_days, 0.5) as median_interval_days
        ,sum(case when interval_days = 0 then 1 else 0 end) as interval_0d_cnt
        ,sum(case when interval_days between 1 and 3 then 1 else 0 end) as interval_1_3d_cnt
        ,sum(case when interval_days between 4 and 7 then 1 else 0 end) as interval_4_7d_cnt
        ,sum(case when interval_days between 8 and 14 then 1 else 0 end) as interval_8_14d_cnt
        ,sum(case when interval_days between 15 and 30 then 1 else 0 end) as interval_15_30d_cnt
        ,sum(case when interval_days > 30 then 1 else 0 end) as interval_30d_plus_cnt
    from user_order_interval
    group by 1,2,3
    union all
    select exp_group
        ,version
        ,'整体' as order_node
        ,count(distinct user_id) as user_cnt
        ,count(*) as interval_cnt
        ,avg(interval_days) as avg_interval_days
        ,percentile_approx(interval_days, 0.5) as median_interval_days
        ,sum(case when interval_days = 0 then 1 else 0 end) as interval_0d_cnt
        ,sum(case when interval_days between 1 and 3 then 1 else 0 end) as interval_1_3d_cnt
        ,sum(case when interval_days between 4 and 7 then 1 else 0 end) as interval_4_7d_cnt
        ,sum(case when interval_days between 8 and 14 then 1 else 0 end) as interval_8_14d_cnt
        ,sum(case when interval_days between 15 and 30 then 1 else 0 end) as interval_15_30d_cnt
        ,sum(case when interval_days > 30 then 1 else 0 end) as interval_30d_plus_cnt
    from user_order_interval
    group by 1,2
)
,result as (
    select '达到订单节点用户数' as stat_type
        ,exp_group
        ,version
        ,order_node
        ,node_order
        ,user_cnt
        ,cast(null as bigint) as interval_cnt
        ,cast(null as double) as avg_interval_days
        ,cast(null as double) as median_interval_days
        ,cast(null as double) as interval_0d_rate
        ,cast(null as double) as interval_1_3d_rate
        ,cast(null as double) as interval_4_7d_rate
        ,cast(null as double) as interval_8_14d_rate
        ,cast(null as double) as interval_15_30d_rate
        ,cast(null as double) as interval_30d_plus_rate
    from reach_node_summary
    union all
    select '最终订单数分布' as stat_type
        ,exp_group
        ,version
        ,order_node
        ,node_order
        ,user_cnt
        ,cast(null as bigint) as interval_cnt
        ,cast(null as double) as avg_interval_days
        ,cast(null as double) as median_interval_days
        ,cast(null as double) as interval_0d_rate
        ,cast(null as double) as interval_1_3d_rate
        ,cast(null as double) as interval_4_7d_rate
        ,cast(null as double) as interval_8_14d_rate
        ,cast(null as double) as interval_15_30d_rate
        ,cast(null as double) as interval_30d_plus_rate
    from exact_order_summary
    union all
    select '相邻订单间隔分布' as stat_type
        ,exp_group
        ,version
        ,order_node
        ,node_order
        ,user_cnt
        ,interval_cnt
        ,round(avg_interval_days, 2) as avg_interval_days
        ,round(median_interval_days, 2) as median_interval_days
        ,interval_0d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_0d_rate
        ,interval_1_3d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_1_3d_rate
        ,interval_4_7d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_4_7d_rate
        ,interval_8_14d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_8_14d_rate
        ,interval_15_30d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_15_30d_rate
        ,interval_30d_plus_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_30d_plus_rate
    from interval_summary
)
select stat_type as "统计类型"
    ,exp_group as "实验类型"
    ,version as "实验版本"
    ,order_node as "订单节点"
    ,user_cnt as "用户数"
    ,interval_cnt as "间隔样本数"
    ,avg_interval_days as "平均间隔天数"
    ,median_interval_days as "中位间隔天数"
    ,interval_0d_rate as "当日复购占比"
    ,interval_1_3d_rate as "1至3天占比"
    ,interval_4_7d_rate as "4至7天占比"
    ,interval_8_14d_rate as "8至14天占比"
    ,interval_15_30d_rate as "15至30天占比"
    ,interval_30d_plus_rate as "30天以上占比"
from result
order by case when stat_type = '达到订单节点用户数' then 1
              when stat_type = '最终订单数分布' then 2
              when stat_type = '相邻订单间隔分布' then 3
         end
    ,case when exp_group = '实验组' then 1
          when exp_group = '对照组' then 2
     end

;


-- 2、实验组与对照组新客次新生单用户复购跃迁分析
with biguser as ( --- 新逻辑大单用户，15间夜以上
    select  user_id
    from(
            select
                order_date
                ,user_info['orig_device_id'] as orig_device_id
                ,user_id
                ,user_name
                ,count(order_no) as order_nos_90
                ,sum(room_night) as room_nights_90
            from mdw_order_v3_international
            where dt = '%(DATE)s'
              and (province_name in ('台湾','澳门','香港') or country_name != '中国')
              and terminal_channel_type = 'app'
              and is_valid = '1'
              and order_status not in ('CANCELLED','REJECTED')
              and order_date >= date_sub(current_date, 90)
              and order_date <= date_sub(current_date, 1)
            group by 1,2,3,4
        )a where room_nights_90>=15
    group by 1
)
,abtest AS (--- 实验明细
    select  a.dt,
            ab_version version,
            ab_exp_value AS user_id
    from ihotel_default.dw_ihotel_abtest_index_di_v2 a --user_id
    where a.dt >= '2026-07-10'  and a.dt <= '2026-07-16'
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '260608_ho_gj_NewCustomerTask'
        and ab_exp_value not in (select user_id from biguser)  --- 去除大单用户
    group by 1,2,3
)
,user_first_order as ( --- 获取首单日期用于判定绝对新客
    select user_id
        ,min(order_date) as min_order_date
    from default.mdw_order_v3_international --- 海外订单表
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name != '中国')
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
    group by 1
)
,user_daily_orders as ( --- 预聚合：每个用户每一天的订单量
    select user_id
        ,order_date
        ,count(distinct order_no) as daily_order_cnt
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or country_name != '中国')
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid = '1'
    group by 1,2
)
,q_order as ( --- 订单明细及新客次新判定
    select a.order_date
        ,a.order_time
        ,case when a.order_date = b.min_order_date then '新客'
              when coalesce(sum(o.daily_order_cnt), 0) <= 3 then '次新用户'
              else '老客'
         end as user_type
        ,a.order_no
        ,a.user_id
        ,a.user_name
        ,a.room_night
        ,a.init_gmv
        ,coalesce(sum(o.daily_order_cnt), 0) as daily_order_cnt
    from default.mdw_order_v3_international a
    left join user_first_order b on a.user_id = b.user_id
    left join user_daily_orders o on a.user_id = o.user_id and o.order_date < a.order_date
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
        -- and terminal_channel_type in ('www','app','touch')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date)
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date)
        and (refund_time is null or date(refund_time) > a.order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2026-06-15'
        and a.order_date <= date_sub(current_date, 1)
    group by a.order_date
        ,a.order_time
        ,a.order_no
        ,a.user_id
        ,a.user_name
        ,b.min_order_date
        ,a.room_night
        ,a.init_gmv
)
,ab_order_user_detail as ( --- 实验周期内已生单的新客次新用户
    select a.order_date
        ,unix_timestamp(a.order_time) as order_time
        ,a.order_no
        ,a.user_id
        ,a.user_type
        ,exp.version
        ,case when exp.version in ('A','B','C') then '实验组'
              when exp.version in ('D','E') then '对照组'
         end as exp_group
        ,row_number() over(
            partition by a.user_id
            order by a.order_date,unix_timestamp(a.order_time),a.order_no
        ) as rn
    from q_order a
    inner join abtest exp on a.order_date = exp.dt and a.user_id = exp.user_id
    where a.user_type in ('新客','次新用户')
)
,target_user as ( --- 每个生单用户实验周期内第一笔订单
    select user_id
        ,version
        ,exp_group
        ,user_type
        ,order_date as first_order_date
        ,order_time as first_order_time
        ,order_no as first_order_no
    from ab_order_user_detail
    where rn = 1
)
,user_order_sequence as ( --- 从实验周期第一笔订单开始计算第1、2、3、4单
    select t1.user_id
        ,t2.version
        ,t2.exp_group
        ,t2.user_type
        ,t1.order_no
        ,t1.order_date
        ,unix_timestamp(t1.order_time) as order_time
        ,row_number() over(
            partition by t1.user_id
            order by t1.order_date,unix_timestamp(t1.order_time),t1.order_no
        ) as order_sequence
    from q_order t1
    inner join target_user t2 on t1.user_id = t2.user_id
    where unix_timestamp(t1.order_time) >= t2.first_order_time
)
,user_order_summary as ( --- 用户从实验订单起累计完成的订单数
    select user_id
        ,exp_group
        ,version
        --,user_type
        ,max(order_sequence) as order_cnt
    from user_order_sequence
    group by 1,2,3
)
,reach_node_summary as ( --- 达到各订单节点的累计用户数
    select exp_group
       -- ,user_type
        ,version
        ,'1单' as order_node
        ,1 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 1
    group by 1,2
    union all
    select exp_group
        --,user_type
        ,version
        ,'2单' as order_node
        ,2 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 2
    group by 1,2
    union all
    select exp_group
        --,user_type
        ,version
        ,'3单' as order_node
        ,3 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 3
    group by 1,2
    union all
    select exp_group
        --,user_type
        ,version
        ,'4单' as order_node
        ,4 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 4
    group by 1,2
)
,exact_order_summary as ( --- 用户最终订单数分布
    select exp_group
        ,version
        ,'1单' as order_node
        ,1 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt = 1
    group by 1,2
    union all
    select exp_group
        ,version
        ,'2单' as order_node
        ,2 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt = 2
    group by 1,2
    union all
    select exp_group
        ,version
        ,'3单' as order_node
        ,3 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt = 3
    group by 1,2
    union all
    select exp_group
        ,version
        ,'4单及以上' as order_node
        ,4 as node_order
        ,count(distinct user_id) as user_cnt
    from user_order_summary
    where order_cnt >= 4
    group by 1,2
)
,user_order_interval_base as ( --- 获取相邻两单日期及时间
    select user_id
        ,exp_group
        ,version
        ,order_sequence
        ,order_date
        ,order_time
        ,lag(order_date, 1) over(
            partition by user_id
            order by order_sequence
        ) as last_order_date
        ,lag(order_time, 1) over(
            partition by user_id
            order by order_sequence
        ) as last_order_time
    from user_order_sequence
)
,user_order_interval as ( --- 1-2、2-3、3-4单间隔
    select user_id
        ,exp_group
        ,version
        ,case when order_sequence = 2 then '1-2单'
              when order_sequence = 3 then '2-3单'
              when order_sequence = 4 then '3-4单'
         end as order_node
        ,order_sequence - 1 as node_order
        ,datediff(order_date, last_order_date) as interval_days
        ,(order_time - last_order_time) * 1.0000 / 3600 as interval_hours
    from user_order_interval_base
    where order_sequence between 2 and 4
        and last_order_date is not null
        and last_order_time is not null
)
,interval_summary as ( --- 分订单节点复购间隔
    select exp_group
        ,version
        ,order_node
        ,node_order
        ,count(distinct user_id) as user_cnt
        ,count(*) as interval_cnt
        ,avg(interval_hours) as avg_interval_hours
        ,percentile_approx(interval_hours, 0.5) as median_interval_hours
        ,sum(case when interval_days = 0 then 1 else 0 end) as interval_0d_cnt
        ,sum(case when interval_days between 1 and 3 then 1 else 0 end) as interval_1_3d_cnt
        ,sum(case when interval_days between 4 and 7 then 1 else 0 end) as interval_4_7d_cnt
        ,sum(case when interval_days between 8 and 14 then 1 else 0 end) as interval_8_14d_cnt
        ,sum(case when interval_days between 15 and 30 then 1 else 0 end) as interval_15_30d_cnt
        ,sum(case when interval_days > 30 then 1 else 0 end) as interval_30d_plus_cnt
    from user_order_interval
    group by 1,2,3,4
    union all
    select exp_group
        ,version
        ,'整体' as order_node
        ,0 as node_order
        ,count(distinct user_id) as user_cnt
        ,count(*) as interval_cnt
        ,avg(interval_hours) as avg_interval_hours
        ,percentile_approx(interval_hours, 0.5) as median_interval_hours
        ,sum(case when interval_days = 0 then 1 else 0 end) as interval_0d_cnt
        ,sum(case when interval_days between 1 and 3 then 1 else 0 end) as interval_1_3d_cnt
        ,sum(case when interval_days between 4 and 7 then 1 else 0 end) as interval_4_7d_cnt
        ,sum(case when interval_days between 8 and 14 then 1 else 0 end) as interval_8_14d_cnt
        ,sum(case when interval_days between 15 and 30 then 1 else 0 end) as interval_15_30d_cnt
        ,sum(case when interval_days > 30 then 1 else 0 end) as interval_30d_plus_cnt
    from user_order_interval
    group by 1,2
)
,result as (
    select '达到订单节点用户数' as stat_type
        ,exp_group
        ,version
        ,order_node
        ,node_order
        ,user_cnt
        ,cast(null as bigint) as interval_cnt
        ,cast(null as double) as avg_interval_hours
        ,cast(null as double) as median_interval_hours
        ,cast(null as double) as interval_0d_rate
        ,cast(null as double) as interval_1_3d_rate
        ,cast(null as double) as interval_4_7d_rate
        ,cast(null as double) as interval_8_14d_rate
        ,cast(null as double) as interval_15_30d_rate
        ,cast(null as double) as interval_30d_plus_rate
    from reach_node_summary
    union all
    select '最终订单数分布' as stat_type
        ,exp_group
        ,version
        ,order_node
        ,node_order
        ,user_cnt
        ,cast(null as bigint) as interval_cnt
        ,cast(null as double) as avg_interval_hours
        ,cast(null as double) as median_interval_hours
        ,cast(null as double) as interval_0d_rate
        ,cast(null as double) as interval_1_3d_rate
        ,cast(null as double) as interval_4_7d_rate
        ,cast(null as double) as interval_8_14d_rate
        ,cast(null as double) as interval_15_30d_rate
        ,cast(null as double) as interval_30d_plus_rate
    from exact_order_summary
    union all
    select '相邻订单间隔分布' as stat_type
        ,exp_group
        ,version
        ,order_node
        ,node_order
        ,user_cnt
        ,interval_cnt
        ,round(avg_interval_hours, 2) as avg_interval_hours
        ,round(median_interval_hours, 2) as median_interval_hours
        ,interval_0d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_0d_rate
        ,interval_1_3d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_1_3d_rate
        ,interval_4_7d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_4_7d_rate
        ,interval_8_14d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_8_14d_rate
        ,interval_15_30d_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_15_30d_rate
        ,interval_30d_plus_cnt * 1.0000 / nullif(interval_cnt, 0) as interval_30d_plus_rate
    from interval_summary
)
select stat_type as "统计类型"
    ,exp_group as "实验类型"
    ,version as "实验版本"
    ,order_node as "订单节点"
    ,user_cnt as "用户数"
    ,interval_cnt as "间隔样本数"
    ,avg_interval_hours as "平均间隔小时"
    ,median_interval_hours as "中位间隔小时"
    ,interval_0d_rate as "当日复购占比"
    ,interval_1_3d_rate as "1至3天占比"
    ,interval_4_7d_rate as "4至7天占比"
    ,interval_8_14d_rate as "8至14天占比"
    ,interval_15_30d_rate as "15至30天占比"
    ,interval_30d_plus_rate as "30天以上占比"
from result
order by case when stat_type = '达到订单节点用户数' then 1
              when stat_type = '最终订单数分布' then 2
              when stat_type = '相邻订单间隔分布' then 3
         end
    ,case when exp_group = '实验组' then 1
          when exp_group = '对照组' then 2
     end
    ,version
    ,node_order
;