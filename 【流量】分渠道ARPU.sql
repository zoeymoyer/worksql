--- 1、分渠道ARPU
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,row_number() over(partition by order_date,a.user_id order by order_time) rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,first_order_info as (
        select  t1.order_date,t1.user_id,t1.user_type,channel,coupon_substract_summary,jf_amt
        from q_order t1 
        left join (select dt,user_id,channel from ihotel_default.dwd_flow_ug_channel_di group by 1,2,3)  t2 on t1.order_date=t2.dt and t1.user_id=t2.user_id 
        where t1.rn=1
        group by 1,2,3,4,5,6
)


select t1.order_date
        ,t1.user_type
        ,t1.channel
        ,uv
        ,qe + jf_amt qe   --- 当日首单券补
        ,yj0
        ,yj30
        ,yj180
        ,yj0 / uv    ARPU0
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,(qe + jf_amt) / uv cac
        ,yj0 / (qe + jf_amt)  ltv0_cac
        ,yj30 / (qe + jf_amt)  ltv30_cac
        ,yj180 / (qe + jf_amt)  ltv180_cac
        ,room_night0
        ,room_night30
        ,room_night180
from (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then room_night end) room_night0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) else null end room_night180

    from first_order_info t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.channel)
) t1 
left join (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.channel)=1,'ALL', t1.channel) as  channel
        ,sum(t1.coupon_substract_summary) qe
        ,sum(t1.jf_amt) jf_amt
    from first_order_info t1 
    group by t1.order_date,cube(t1.user_type,t1.channel)
)t2 on t1.order_date=t2.order_date and t1.user_type=t2.user_type and t1.channel=t2.channel

order by t1.order_date 
;

--- 2、分目的地ARPU
with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,row_number() over(partition by order_date,a.user_id order by order_time) rn
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and order_status not in ('CANCELLED','REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date,1)
)
,first_order_info as (
        select  t1.order_date,t1.user_id,t1.user_type,t1.mdd,coupon_substract_summary,jf_amt
        from q_order t1 
        where t1.rn=1
        group by 1,2,3,4,5,6
)


select t1.order_date
        ,t1.user_type
        ,t1.mdd
        ,uv
        ,qe + jf_amt qe   --- 当日首单券补
        ,yj0
        ,yj30
        ,yj180
        ,yj0 / uv    ARPU0
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,(qe + jf_amt) / uv cac
        ,yj0 / (qe + jf_amt)  ltv0_cac
        ,yj30 / (qe + jf_amt)  ltv30_cac
        ,yj180 / (qe + jf_amt)  ltv180_cac
        ,room_night0
        ,room_night30
        ,room_night180
from (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then room_night end) room_night0
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) else null end room_night180

    from first_order_info t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.mdd)
) t1 
left join (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,sum(t1.coupon_substract_summary) qe
        ,sum(t1.jf_amt) jf_amt
    from first_order_info t1 
    group by t1.order_date,cube(t1.user_type,t1.mdd)
)t2 on t1.order_date=t2.order_date and t1.user_type=t2.user_type and t1.mdd=t2.mdd

order by t1.order_date 
;


--- 分新老目的地
--- ARPU拆解，公式：单用户订单*单订单间夜*ADR*佣金率
with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消 
    select order_date
            -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,CAST(a.init_commission_after AS DOUBLE) + coalesce(CAST(a.ext_plat_certificate AS DOUBLE), 0.0) 
              + CASE WHEN (a.batch_series LIKE '%23base_ZK_728810%' OR a.batch_series LIKE '%23extra_ZK_ce6f99%')
                    THEN coalesce(CAST(split(a.coupon_info['23base_ZK_728810'],'_')[1] AS DOUBLE), 0.0)
                        + coalesce(CAST(split(a.coupon_info['23extra_ZK_ce6f99'],'_')[1] AS DOUBLE), 0.0)
                    ELSE 0.0
                END AS yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
                  else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,room_night,order_no,init_gmv
            ,row_number() over(partition by order_date,a.user_id order by order_time) rn
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date,1)
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from  pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-01-01' and dict_type = 'pncl_wl_username'
    group by 1,2
)

select order_date
        ,mdd,user_type
        ,uv
        ,yj0
        ,yj7
        ,yj30
        ,yj180
        ,init_gmv0
        ,init_gmv7
        ,init_gmv30
        ,init_gmv180
        ,room_night0
        ,room_night7
        ,room_night30
        ,room_night180
        ,order_no0
        ,order_no7
        ,order_no30
        ,order_no180

        ,yj0 / uv    ARPU0
        ,yj7 / uv    ARPU7
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
        ,order_no0   / uv single_order0
        ,order_no7   / uv single_order7
        ,order_no30  / uv single_order30
        ,order_no180 / uv single_order180
        ,room_night0 / order_no0   single_roomnight0
        ,room_night7 / order_no7  single_roomnight7
        ,room_night30 / order_no30  single_roomnight30
        ,room_night180 / order_no180  single_roomnight180
        ,init_gmv0 / room_night0  adr0
        ,init_gmv7 / room_night7  adr7
        ,init_gmv30 / room_night30 adr30
        ,init_gmv180 / room_night180 adr180
        ,yj0 / init_gmv0    yj_rate0
        ,yj7 / init_gmv7    yj_rate7
        ,yj30 / init_gmv30  yj_rate30
        ,yj180 / init_gmv180 yj_rate180
from (
    select t1.order_date
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then final_commission_after end) else null end yj7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then init_gmv end) init_gmv0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then init_gmv end) else null end init_gmv7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then init_gmv end) else null end init_gmv30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then init_gmv end) else null end init_gmv180

        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then room_night end) room_night0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then room_night end) else null end room_night7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then room_night end) else null end room_night30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then room_night end) else null end room_night180

        ,count(distinct case when datediff(t2.order_date, t1.order_date) = 0  then order_no end) order_no0
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then count(distinct case when datediff(t2.order_date, t1.order_date) <= 7   then order_no end) else null end order_no7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then count(distinct case when datediff(t2.order_date, t1.order_date) <= 30  then order_no end) else null end order_no30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then count(distinct case when datediff(t2.order_date, t1.order_date) <= 180 then order_no end) else null end order_no180
    from ( --- 当日首单
        select distinct t1.order_date,t1.user_id,t1.user_type,t1.mdd
            ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
        from q_order t1 
        left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk
        where t1.rn = 1  
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by t1.order_date,cube(t1.user_type,t1.mdd)
) 
order by order_date 
;