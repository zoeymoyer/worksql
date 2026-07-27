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
    where a.dt >= '2026-06-15'  and a.dt <= date_sub(current_date, 1)
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
    where a.dt >= '2026-06-15'
        and a.dt <= date_sub(current_date, 1)
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
,ab_uv as (--- 流量层按实验聚合
    select  a.dt
            ,exp.version
            ,count( a.user_id) as uv
            ,count( case when a.user_type in ('新客', '次新用户') then a.user_id end) as new_c_sub_uv
    from uv a
    inner join abtest exp on a.dt = exp.dt and a.user_id = exp.user_id  
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
            ,count(distinct case when a.user_type in ('次新用户', '新客') then a.user_id end) as new_c_sub_order_uv
            ,count(distinct case when a.user_type in ('次新用户', '新客') then a.order_no end) as new_c_sub_order_cnt
            ,sum(case when a.user_type in ('次新用户', '新客') then a.room_night end) as new_c_sub_room_night
            ,count(distinct case when daily_order_cnt > 1 then a.user_id end) as multi_order_uv
    from q_order a
    inner join abtest exp on a.order_date = exp.dt and a.user_id = exp.user_id 
    group by a.order_date, exp.version
)

--- 结果汇总大盘输出层
select  coalesce(t1.dt, t2.dt) as `日期`
        ,coalesce(t1.version, t2.version) as `ab分组`
        ,sum(coalesce(t1.uv, 0)) as `UV`
        ,sum(coalesce(t2.order_uv, 0)) as `生单uv`
        ,sum(coalesce(t2.order_cnt, 0)) as `订单量`
        ,sum(coalesce(t2.room_night, 0)) as `间夜量`
        ,sum(coalesce(t2.init_gmv, 0)) as `GMV`
        ,sum(coalesce(t2.final_commission_after, 0)) as `佣金`
        ,sum(coalesce(t2.coupon_substract_summary, 0)) as `券额`
        --- 新客及次新细分统计指标
        ,sum(coalesce(t1.new_c_sub_uv, 0)) as `新客次新UV`
        ,sum(coalesce(t2.new_c_sub_order_uv, 0)) as `新客次新生单uv`
        ,sum(coalesce(t2.new_c_sub_order_cnt, 0)) as `新客次新订单量`
        ,sum(coalesce(t2.new_c_sub_room_night, 0)) as `新客次新间夜量`
        ,sum(coalesce(t2.multi_order_uv, 0)) as `2单及2单以上uv`

        ,concat(round(coalesce(sum(t2.new_c_sub_order_cnt)/nullif(sum(t1.new_c_sub_uv), 0), 0) * 100, 2), '%') as `新客次新CR`
        ,concat(round(coalesce(sum(t2.coupon_substract_summary)/nullif(sum(t2.init_gmv), 0), 0) * 100, 2), '%') as `券补率`
        ,concat(round(coalesce(sum(t2.final_commission_after)/nullif(sum(t2.init_gmv), 0), 0) * 100, 2), '%') as `佣金率`
        ,concat(round(coalesce(sum(t2.order_cnt)/nullif(sum(t1.uv), 0), 0) * 100, 2), '%') as `大盘CR`
from ab_uv t1
full outer join ab_order t2 on t1.dt = t2.dt and t1.version = t2.version
group by coalesce(t1.dt, t2.dt), coalesce(t1.version, t2.version)
order by `日期` desc, `ab分组`
;


-- 2、过程数据-曝光
select dt,if(grouping(get_json_object(value,'$.ext.pageId'))=1,'ALL',get_json_object(value,'$.ext.pageId')) as pageId,count(distinct user_name) uv
from ihotel_default.dw_qav_hotel_track_info_di
where dt >= '20260615' and dt <= '20260624'
    and key in ('ihotel/Common/IdentityTip/show/NewCustomerTask'              --- H页曝光
                ,'ihotel/Common/IdentityTip/show/NewCustomerTask'     --- 搜索按钮点击 
        )
    and get_json_object(value,'$.ext.pageId') in ('home','list','detail','order')
group by 1,cube(get_json_object(value,'$.ext.pageId'))
order by 1,2
;

-- 3、过程数据-任务
select substr(task_in_at,1,10) as dt
    ,count(distinct user_id) as `任务报名人数`
    ,count(distinct case when get_json_object(sub_task_infos,'$.1.sub_task_status')='已领取' then user_id end) as `任务1完成人数`
    ,count(distinct case when get_json_object(sub_task_infos,'$.2.sub_task_status')='已领取' then user_id end) as `任务2完成人数`
    ,count(distinct case when get_json_object(sub_task_infos,'$.3.sub_task_status')='已领取' then user_id end) as `任务3完成人数`
    ,count(distinct case when get_json_object(sub_task_infos,'$.4.sub_task_status')='已领取' then user_id end) as `任务4完成人数`
from ihotel_default.dwd_ord_promotion_new_customer_da --- 全量表
where dt = date_sub(current_date,1)
group by 1
order by 1
;

-- 4、新客次新大盘数据
with user_first_order as(--- 获取首单日期用于判定绝对新客
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
    where a.dt >= '2026-06-15'
        and a.dt <= date_sub(current_date, 1)
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

,uv_info as (--- 流量层按实验聚合
    select  a.dt
            ,count( a.user_id) as uv
            ,count( case when a.user_type in ('新客') then a.user_id end) as new_uv
            ,count( case when a.user_type in ('次新用户') then a.user_id end) as new_c_sub_uv
            ,count( case when a.user_type in ('老客') then a.user_id end) as old_uv
    from uv a
    group by 1
)
,order_info as (--- 订单层按实验聚合
    select  a.order_date dt
            ,count(distinct a.order_no) as order_cnt
            ,count(distinct case when a.user_type in ('新客') then a.order_no end) as new_order_cnt
            ,count(distinct case when a.user_type in ('次新用户') then a.order_no end) as new_c_sub_order_cnt
            ,count(distinct case when a.user_type in ('老客') then a.order_no end) as old_order_cnt

            ,sum(a.room_night) as room_night
            ,sum(a.init_gmv) as init_gmv
            ,sum(a.final_commission_after) as final_commission_after
            ,sum(a.coupon_substract_summary) as coupon_substract_summary

            ,sum(case when a.user_type in ('新客') then a.room_night end) as new_room_night
            ,sum(case when a.user_type in ('新客') then a.init_gmv end) as new_init_gmv
            ,sum(case when a.user_type in ('新客') then a.final_commission_after end) as new_final_commission_after
            ,sum(case when a.user_type in ('新客') then a.coupon_substract_summary end) as new_coupon_substract_summary

            ,sum(case when a.user_type in ('次新用户') then a.room_night end) as new_c_sub_room_night
            ,sum(case when a.user_type in ('次新用户') then a.init_gmv end) as new_c_sub_init_gmv
            ,sum(case when a.user_type in ('次新用户') then a.final_commission_after end) as new_c_sub_final_commission_after
            ,sum(case when a.user_type in ('次新用户') then a.coupon_substract_summary end) as new_c_sub_coupon_substract_summary

            ,sum(case when a.user_type in ('老客') then a.room_night end) as old_room_night
            ,sum(case when a.user_type in ('老客') then a.init_gmv end) as old_init_gmv
            ,sum(case when a.user_type in ('老客') then a.final_commission_after end) as old_final_commission_after
            ,sum(case when a.user_type in ('老客') then a.coupon_substract_summary end) as old_coupon_substract_summary
    from q_order a
    group by 1
)

select t1.dt
    ,t1.uv
    ,t1.new_uv
    ,t1.new_c_sub_uv
    ,t1.old_uv
    ,t2.order_cnt
    ,t2.new_order_cnt
    ,t2.new_c_sub_order_cnt
    ,t2.old_order_cnt

    ,t2.room_night
    ,t2.init_gmv
    ,t2.final_commission_after
    ,t2.coupon_substract_summary

    ,t2.new_room_night
    ,t2.new_init_gmv
    ,t2.new_final_commission_after
    ,t2.new_coupon_substract_summary

    ,t2.new_c_sub_room_night
    ,t2.new_c_sub_init_gmv
    ,t2.new_c_sub_final_commission_after
    ,t2.new_c_sub_coupon_substract_summary

    ,t2.old_room_night
    ,t2.old_init_gmv
    ,t2.old_final_commission_after
    ,t2.old_coupon_substract_summary
from uv_info t1
left join order_info t2 on t1.dt = t2.dt
order by 1 desc
;


with user_first_order as (--- 获取首单日期用于判定绝对新客
    select  user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,user_history_cnt as (
    select  a.user_id
            ,a.order_date
            ,coalesce(sum(o.daily_order_cnt), 0) as before_order_cnt
    from (
        select user_id, order_date 
        from default.mdw_order_v3_international
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        group by user_id, order_date
    ) a
    left join (
        select user_id, order_date, count(distinct order_no) as daily_order_cnt
        from default.mdw_order_v3_international
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
            and order_status not in ('CANCELLED', 'REJECTED')
            and is_valid = '1'
        group by user_id, order_date
    ) o on a.user_id = o.user_id and o.order_date < a.order_date
    group by 1, 2
)

,q_order as (
    select  a.order_date
            ,case  when a.order_date = b.min_order_date then '新客' when h.before_order_cnt <= 3 then '次新用户'
                else '老客' 
             end as user_type
            ,a.order_no
            ,a.user_id
            ,a.init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                      then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                      else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0 else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,case when batch_series like '%xinkeRW96z_ZK_23857e%' then '新客券' 
                  when (batch_series like '%cxRWaward_MJ_00d2f6%' or batch_series like '%xinkeRW2_MJ_3141bb%' or batch_series like '%xinkeRW3_MJ_0a734e%') then '次新券' 
                  else '其他券' end as coupon_type
    from default.mdw_order_v3_international a
    left join user_first_order b on a.user_id = b.user_id 
    left join user_history_cnt h on a.user_id = h.user_id and a.order_date = h.order_date --- 完美1对1防爆关联
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and terminal_channel_type in ('app')
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
        and (refund_time is null or date(refund_time) > a.order_date)
        and is_valid = '1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2026-06-15' and a.order_date <= date_sub(current_date, 1)
)

select  order_date as order_date
        ,sum(init_gmv) as init_gmv
        ,sum(final_commission_after) as yj
        ,sum(coupon_substract_summary) as qe
        ,count(distinct order_no) as order_cnt
        ,count(distinct case when coupon_type = '新客券' then order_no end) as new_coupon_order_cnt
        ,count(distinct case when coupon_type = '次新券' then order_no end) as new_c_sub_coupon_order_cnt
        ,count(distinct case when coupon_type = '其他券' then order_no end) as other_coupon_order_cnt
        ,count(distinct user_id) as user_cnt
        ,count(distinct case when coupon_type = '新客券' then user_id end) as new_coupon_user_cnt
        ,count(distinct case when coupon_type = '次新券' then user_id end) as new_c_sub_coupon_user_cnt
        ,count(distinct case when coupon_type = '其他券' then user_id end) as other_coupon_user_cnt
from q_order
group by 1
order by 1 desc
;