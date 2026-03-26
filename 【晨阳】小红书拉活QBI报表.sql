--- 1、小红书拉活报表
with user_type as ( -----用户首单日
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
,uv_1 as (----分日去重活跃用户
    select  dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,platform_new as (--- 判定平台新
    select  dt,user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv as (-- 国酒流量
    select t1.dt,t1.user_id,user_name
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type_new
    from uv_1 t1 
    left join platform_new t2  on t1.dt=t2.dt and t1.user_name=t2.user_pk
    group by 1,2,3,4
)
,q_order1 as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
            ,terminal_channel_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        -- and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2026-02-01' and order_date <= date_sub(current_date, 1)
)
,q_order as (
    select t1.*
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type_new
    from q_order1 t1 
    left join platform_new t2  on t1.order_date=t2.dt and t1.user_name=t2.user_pk
)
-- ,red as(
--     select  flow_dt as dt,user_name
--     from pp_pub.dwd_redbook_global_flow_detail_di
--     where dt between '2025-12-01' and date_sub(current_date,1)
--     --and business_type = 'hotel-inter'  --宽口径不需要这个
--     and query_platform = 'redbook'
--     group by 1,2
-- )
-- ,user_xhs as (--- 宽口径小红书渠道
--     select  t1.dt
--           ,t1.user_id
--           ,t1.user_name
--           ,t1.user_type_new
--     from uv  t1
--     left join red t2 on t1.user_name = t2.user_name
--     where t2.dt >= date_sub(t1.dt, 7) and t2.dt <= t1.dt 
--     and t2.user_name is not null
--     group by 1,2,3,4
-- )
,xhs_lh as (--- 小红书拉活订单数据
    select t1.dt,t1.order_no,uid,username,income, case when business_name = '国际酒店' then '窄口径' end is_ihotel
    from pub.dwd_ord_order_media_lahuo_attribution_di t1
    where is_media_lahuo_kpi = 1 and order_type_class = 'hotel-inter'
    and dt >= '2026-02-01' and dt <= date_sub(current_date, 1)
)
,q_order_info as (--- 大盘预定订单数据
    select order_date,if(grouping(user_type_new)=1, 'ALL', user_type_new) user_type_new,sum(room_night) room_night,count(distinct order_no) order_no,sum(final_commission_after) yj
    from q_order where terminal_channel_type = 'app'
    group by 1,cube(user_type_new)
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
            ,count(distinct order_no) c_order_no
            ,sum(extend_info['room_night']) c_room_night
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-02-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1

)

select t1.dt,t1.user_type_new
        ,`拉活订单量`,`拉活增量订单量`
        ,`拉活增量订单量` / `拉活订单量` `增量订单占比`
        ,`拉活间夜量`, `拉活增量间夜量`
        ,`拉活订单量` / t2.order_no `订单量占比`
        ,`拉活间夜量` / t2.room_night `间夜量占比`
        ,`拉活间夜量` / t3.c_room_night `间夜QC贡献`
        ,`拉活增量间夜量` / t3.c_room_night `增量间夜QC贡献`
        ,t2.room_night `Q_间夜量`,t2.order_no `Q_订单量`
        ,t3.c_room_night `C_间夜量`,t3.c_order_no `C_订单量`

        ,`拉活订单量_窄口径`,`拉活增量订单量_窄口径`
        ,`拉活增量订单量_窄口径` / `拉活订单量_窄口径` `增量订单占比_窄口径`
        ,`拉活间夜量_窄口径`, `拉活增量间夜量_窄口径`
        ,`拉活订单量_窄口径` / t2.order_no `订单量占比`
        ,`拉活间夜量_窄口径` / t2.room_night `间夜量占比`
        ,`拉活间夜量_窄口径` / t3.c_room_night `间夜QC贡献`
        ,`拉活增量间夜量_窄口径` / t3.c_room_night `增量间夜QC贡献`
from (
    select t1.dt,if(grouping(user_type_new)=1, 'ALL', user_type_new) user_type_new
            ,count(distinct t1.order_no) `拉活订单量`
            ,count(distinct case when is_increment = 'Y' then t1.order_no end) `拉活增量订单量`
            ,sum(room_night)  `拉活间夜量`
            ,sum(case when is_increment = 'Y' then room_night end)  `拉活增量间夜量`
            ,count(distinct uid) `拉活生单uv`
            ,count(distinct case when is_increment = 'Y' then uid end) `拉活增量生单uv`

            ,count(distinct case when is_ihotel = '窄口径' then t1.order_no end) `拉活订单量_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t1.order_no end) `拉活增量订单量_窄口径`
            ,sum(case when is_ihotel = '窄口径' then room_night end) `拉活间夜量_窄口径`
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then room_night end) `拉活增量间夜量_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' then uid end)  `拉活生单uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then uid end)  `拉活增量生单uv_窄口径`
    from xhs_lh t1
    left join q_order t2 on t1.order_no=t2.order_no
    left join (--- 小红书拉活订单在过往3天小红书最早访问日期前7日内是否活跃
        select t1.dt,t1.username,min_act_date_14d,case when t2.user_name is null then 'Y' else 'N' end is_increment
        from (--- 小红书拉活订单在过往3天小红书最早访问日期
            select t1.dt,t1.username,min(t2.dt) min_act_date_14d
            from xhs_lh t1
            left join uv t2 on t1.username=t2.user_name and datediff(t1.dt,t2.dt) <= 3 and t1.dt >= t2.dt
            group by 1,2
        )t1 left join uv t2 on t1.username=t2.user_name and datediff(t1.min_act_date_14d,t2.dt) < 7 and t1.min_act_date_14d > t2.dt
        group by 1,2,3,4
    )t3 on t1.dt=t3.dt and t1.username=t3.username
    group by 1,cube(user_type_new)
)t1 left join q_order_info t2 on t1.dt=t2.order_date and t1.user_type_new=t2.user_type_new
left join c_order t3 on t1.dt=t3.dt
order by 1
;



--- 2、小红书拉活流量报表
with user_type as ( -----用户首单日
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
,uv_1 as (----分日去重活跃用户
    select  dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)
,platform_new as (--- 判定平台新
    select  dt,user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv as (-- 国酒流量
    select t1.dt,t1.user_id,user_name
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type_new
    from uv_1 t1 
    left join platform_new t2  on t1.dt=t2.dt and t1.user_name=t2.user_pk
    group by 1,2,3,4
)
,q_order1 as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
            ,terminal_channel_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        -- and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2026-02-01' and order_date <= date_sub(current_date, 1)
)
,q_order as (
    select t1.*
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type_new
    from q_order1 t1 
    left join platform_new t2  on t1.order_date=t2.dt and t1.user_name=t2.user_pk
)
,xhs_lh_flow as (--- 小红书拉活流量数据
    select dt,flow_user_name user_name,case when business_type='hotel-inter' then '窄口径' end is_ihotel
    from pp_pub.dwd_smm_v9_app_lahuo_flow_detail_di 
    where  dt >= '2026-02-01'
    group by 1,2,3
)
,xhs_lh as (
    select t1.dt,t1.user_name,user_type_new,is_ihotel
    from xhs_lh_flow t1 
    join uv t2 on t1.dt=t2.dt and t1.user_name = t2.user_name
    group by 1,2,3,4
)
,q_order_info as (--- 大盘预定订单数据
    select order_date,if(grouping(user_type_new)=1, 'ALL', user_type_new) user_type_new,sum(room_night) room_night,count(distinct order_no) order_no,sum(final_commission_after) yj
    from q_order where terminal_channel_type = 'app'
    group by 1,cube(user_type_new)
)
,c_order as (  --- c订单数据
    select substr(order_date,1,10) as dt
            ,count(distinct order_no) c_order_no
            ,sum(extend_info['room_night']) c_room_night
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-02-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1
)

select t1.dt,t1.user_type_new
        ,`拉活uv`,`增量拉活uv`
        ,`增量拉活uv` / `拉活uv` `增量拉活占比`
        ,`拉活订单量`,`拉活增量订单量`
        ,`拉活增量订单量` / `拉活订单量` `增量订单占比`
        ,`拉活间夜量`, `拉活增量间夜量`
        ,`拉活订单量` / t2.order_no `订单量占比`
        ,`拉活间夜量` / t2.room_night `间夜量占比`
        ,`拉活间夜量` / t3.c_room_night `间夜QC贡献`
        ,`拉活增量间夜量` / t3.c_room_night `增量间夜QC贡献`

        ,t2.room_night `Q_间夜量`,t2.order_no `Q_订单量`
        ,t3.c_room_night `C_间夜量`,t3.c_order_no `C_订单量`

        ,`拉活uv_窄口径`,`增量拉活uv_窄口径`
        ,`增量拉活uv_窄口径` / `拉活uv_窄口径` `增量拉活占比_窄口径`
        ,`拉活订单量_窄口径`,`拉活增量订单量_窄口径`
        ,`拉活增量订单量_窄口径` / `拉活订单量_窄口径` `增量订单占比_窄口径`
        ,`拉活间夜量_窄口径`, `拉活增量间夜量_窄口径`
        ,`拉活订单量_窄口径` / t2.order_no `订单量占比`
        ,`拉活间夜量_窄口径` / t2.room_night `间夜量占比`
        ,`拉活间夜量_窄口径` / t3.c_room_night `间夜QC贡献`
        ,`拉活增量间夜量_窄口径` / t3.c_room_night `增量间夜QC贡献`
from (
    select t1.dt,if(grouping(t1.user_type_new)=1, 'ALL', t1.user_type_new) user_type_new
            ,count(distinct t1.user_name) `拉活uv`
            ,count(distinct case when is_increment = 'Y' then t1.user_name end) `增量拉活uv`
            ,count(distinct t2.order_no) `拉活订单量`
            ,count(distinct case when is_increment = 'Y' then t2.order_no end) `拉活增量订单量`
            ,sum(room_night)  `拉活间夜量`
            ,sum(case when is_increment = 'Y' then room_night end)  `拉活增量间夜量`
            ,count(distinct t2.user_name) `拉活生单uv`
            ,count(distinct case when is_increment = 'Y' then t2.user_name end) `拉活增量生单uv`

            ,count(distinct case when is_ihotel = '窄口径' then t1.user_name end) `拉活uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t1.user_name end) `增量拉活uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' then t2.order_no end) `拉活订单量_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t2.order_no end) `拉活增量订单量_窄口径`
            ,sum(case when is_ihotel = '窄口径' then room_night end) `拉活间夜量_窄口径`
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then room_night end) `拉活增量间夜量_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' then t2.user_name end)  `拉活生单uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t2.user_name end)  `拉活增量生单uv_窄口径`
    from xhs_lh t1
    left join q_order t2 on t1.user_name=t2.user_name and t1.dt=t2.order_date
    left join (--- 小红书拉活流量在过往3天小红书最早访问日期前7日内是否活跃
        select t1.dt,t1.user_name,min_act_date_14d,case when t2.user_name is null then 'Y' else 'N' end is_increment
        from (--- 小红书拉活用户在过往3天小红书最早访问日期
            select t1.dt,t1.user_name,min(t2.dt) min_act_date_14d
            from xhs_lh t1
            left join uv t2 on t1.user_name=t2.user_name and datediff(t1.dt,t2.dt) <= 3 and t1.dt >= t2.dt
            group by 1,2
        )t1 left join uv t2 on t1.user_name=t2.user_name and datediff(t1.min_act_date_14d,t2.dt) < 7 and t1.min_act_date_14d > t2.dt
        group by 1,2,3,4
    )t3 on t1.dt=t3.dt and t1.user_name=t3.user_name
    group by 1,cube(t1.user_type_new)
)t1 left join q_order_info t2 on t1.dt=t2.order_date and t1.user_type_new=t2.user_type_new
left join c_order t3 on t1.dt=t3.dt
order by 1
;


--- 3、信息流拉活流量报表
with user_type as ( -----用户首单日
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
,uv_1 as (----分日去重活跃用户
    select  dt 
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,device_id
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-01-04'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,platform_new as (--- 判定平台新
    select  dt,user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2026-01-04' and dt <= date_sub(current_date, 1)
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv as (-- 国酒流量
    select t1.dt,t1.user_id,user_name
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type_new,device_id
    from uv_1 t1 
    left join platform_new t2  on t1.dt=t2.dt and t1.user_name=t2.user_pk
    group by 1,2,3,4,5
)
,q_order1 as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end final_commission_after
            ,terminal_channel_type
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        -- and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2026-01-04' and order_date <= date_sub(current_date, 1)
)
,q_order as (
    select t1.*
           ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type_new
    from q_order1 t1 
    left join platform_new t2  on t1.order_date=t2.dt and t1.user_name=t2.user_pk
)
,xxl_lh_flow as (--- 信息流拉活流量数据
    select dt,client_uid,case when account_id in ('73904399','73904400','75506778','75506762') then '窄口径' end is_ihotel
    from pub.dwd_flow_active_xxl_oneh_di
    where  dt >= '2026-01-04'
    group by 1,2,3
)
,xxl_lh as (
    select t1.dt,user_name,user_type_new,is_ihotel
    from xxl_lh_flow t1 
    join uv t2 on t1.dt=t2.dt and t1.client_uid = t2.device_id
    group by 1,2,3,4
)
,q_order_info as (--- 大盘预定订单数据
    select order_date,if(grouping(user_type_new)=1, 'ALL', user_type_new) user_type_new,sum(room_night) room_night,count(distinct order_no) order_no,sum(final_commission_after) yj
    from q_order where terminal_channel_type = 'app'
    group by 1,cube(user_type_new)
)
,c_order as (  --- c订单数据
    select substr(order_date,1,10) as dt
            ,count(distinct order_no) c_order_no
            ,sum(extend_info['room_night']) c_room_night
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-02-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
    group by 1
)

select t1.dt,t1.user_type_new
        ,`拉活uv`,`增量拉活uv`
        ,`增量拉活uv` / `拉活uv` `增量拉活占比`
        ,`拉活订单量`,`拉活增量订单量`
        ,`拉活增量订单量` / `拉活订单量` `增量订单占比`
        ,`拉活间夜量`, `拉活增量间夜量`
        ,`拉活订单量` / t2.order_no `订单量占比`
        ,`拉活间夜量` / t2.room_night `间夜量占比`
        ,`拉活间夜量` / t3.c_room_night `间夜QC贡献`
        ,`拉活增量间夜量` / t3.c_room_night `增量间夜QC贡献`

        ,t2.room_night `Q_间夜量`,t2.order_no `Q_订单量`
        ,t3.c_room_night `C_间夜量`,t3.c_order_no `C_订单量`

        ,`拉活uv_窄口径`,`增量拉活uv_窄口径`
        ,`增量拉活uv_窄口径` / `拉活uv_窄口径` `增量拉活占比_窄口径`
        ,`拉活订单量_窄口径`,`拉活增量订单量_窄口径`
        ,`拉活增量订单量_窄口径` / `拉活订单量_窄口径` `增量订单占比_窄口径`
        ,`拉活间夜量_窄口径`, `拉活增量间夜量_窄口径`
        ,`拉活订单量_窄口径` / t2.order_no `订单量占比`
        ,`拉活间夜量_窄口径` / t2.room_night `间夜量占比`
        ,`拉活间夜量_窄口径` / t3.c_room_night `间夜QC贡献`
        ,`拉活增量间夜量_窄口径` / t3.c_room_night `增量间夜QC贡献`
from (
    select t1.dt,if(grouping(t1.user_type_new)=1, 'ALL', t1.user_type_new) user_type_new
            ,count(distinct t1.user_name) `拉活uv`
            ,count(distinct case when is_increment = 'Y' then t1.user_name end) `增量拉活uv`
            ,count(distinct t2.order_no) `拉活订单量`
            ,count(distinct case when is_increment = 'Y' then t2.order_no end) `拉活增量订单量`
            ,sum(room_night)  `拉活间夜量`
            ,sum(case when is_increment = 'Y' then room_night end)  `拉活增量间夜量`
            ,count(distinct t2.user_name) `拉活生单uv`
            ,count(distinct case when is_increment = 'Y' then t2.user_name end) `拉活增量生单uv`

            ,count(distinct case when is_ihotel = '窄口径' then t1.user_name end) `拉活uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t1.user_name end) `增量拉活uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' then t2.order_no end) `拉活订单量_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t2.order_no end) `拉活增量订单量_窄口径`
            ,sum(case when is_ihotel = '窄口径' then room_night end) `拉活间夜量_窄口径`
            ,sum(case when is_ihotel = '窄口径' and is_increment = 'Y' then room_night end) `拉活增量间夜量_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' then t2.user_name end)  `拉活生单uv_窄口径`
            ,count(distinct case when is_ihotel = '窄口径' and is_increment = 'Y' then t2.user_name end)  `拉活增量生单uv_窄口径`
    from xxl_lh t1
    left join q_order t2 on t1.user_name=t2.user_name and t1.dt=t2.order_date
    left join (--- 小红书拉活流量在过往3天小红书最早访问日期前7日内是否活跃
        select t1.dt,t1.user_name,min_act_date_14d,case when t2.user_name is null then 'Y' else 'N' end is_increment
        from (--- 小红书拉活用户在过往3天小红书最早访问日期
            select t1.dt,t1.user_name,min(t2.dt) min_act_date_14d
            from xxl_lh t1
            left join uv t2 on t1.user_name=t2.user_name and datediff(t1.dt,t2.dt) <= 3 and t1.dt >= t2.dt
            group by 1,2
        )t1 left join uv t2 on t1.user_name=t2.user_name and datediff(t1.min_act_date_14d,t2.dt) < 7 and t1.min_act_date_14d > t2.dt
        group by 1,2,3,4
    )t3 on t1.dt=t3.dt and t1.user_name=t3.user_name
    group by 1,cube(t1.user_type_new)
)t1 left join q_order_info t2 on t1.dt=t2.order_date and t1.user_type_new=t2.user_type_new
left join c_order t3 on t1.dt=t3.dt
order by 1
;



with uv_1 as (----分日去重活跃用户
    select  dt 
            ,a.user_id
            ,a.user_name
            ,device_id
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4
)

select t1.dt,count(distinct user_name)
from uv_1 t1 left join pub.dwd_flow_active_xxl_oneh_di t2 on t1.dt=t2.dt and t1.device_id=t2.client_uid
where t2.dt is not null
group by 1
order by 1 desc;
