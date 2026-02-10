with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from mdw_order_v3_international a 
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
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2024-01-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)


select order_date
        ,user_type1
        ,uv
        ,yj0,byj0
        ,yj1,byj1
        ,yj2,byj2
        ,yj3,byj3
        ,yj4,byj4
        ,yj5,byj5
        ,yj6,byj6
        ,yj7,byj7
        ,yj30,byj30
        ,yj180,byj180
        ,yj0 / uv    ARPU0
        ,yj1 / uv    ARPU1
        ,yj2 / uv    ARPU2
        ,yj3 / uv    ARPU3
        ,yj4 / uv    ARPU4
        ,yj5 / uv    ARPU5
        ,yj6 / uv    ARPU6
        ,yj7 / uv    ARPU7
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
from (
    select t1.order_date,t1.user_type1
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=1   then sum(case when datediff(t2.order_date, t1.order_date) <= 1   then final_commission_after end) else null end yj1
        ,case when max(datediff(t2.order_date,t1.order_date))>=2   then sum(case when datediff(t2.order_date, t1.order_date) <= 2   then final_commission_after end) else null end yj2
        ,case when max(datediff(t2.order_date,t1.order_date))>=3   then sum(case when datediff(t2.order_date, t1.order_date) <= 3   then final_commission_after end) else null end yj3
        ,case when max(datediff(t2.order_date,t1.order_date))>=4   then sum(case when datediff(t2.order_date, t1.order_date) <= 4   then final_commission_after end) else null end yj4
        ,case when max(datediff(t2.order_date,t1.order_date))>=5   then sum(case when datediff(t2.order_date, t1.order_date) <= 5   then final_commission_after end) else null end yj5
        ,case when max(datediff(t2.order_date,t1.order_date))>=6   then sum(case when datediff(t2.order_date, t1.order_date) <= 6   then final_commission_after end) else null end yj6
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then final_commission_after end) else null end yj7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180
        ,case when max(datediff(t2.order_date,t1.order_date))>=1   then sum(case when datediff(t2.order_date, t1.order_date) = 0    then ldyj end) else null end  byj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=2   then sum(case when datediff(t2.order_date, t1.order_date) <= 1   then ldyj end) else null end  byj1
        ,case when max(datediff(t2.order_date,t1.order_date))>=3   then sum(case when datediff(t2.order_date, t1.order_date) <= 2   then ldyj end) else null end  byj2
        ,case when max(datediff(t2.order_date,t1.order_date))>=4   then sum(case when datediff(t2.order_date, t1.order_date) <= 3   then ldyj end) else null end  byj3
        ,case when max(datediff(t2.order_date,t1.order_date))>=5   then sum(case when datediff(t2.order_date, t1.order_date) <= 4   then ldyj end) else null end  byj4
        ,case when max(datediff(t2.order_date,t1.order_date))>=6   then sum(case when datediff(t2.order_date, t1.order_date) <= 5   then ldyj end) else null end  byj5
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 6   then ldyj end) else null end  byj6
        ,case when max(datediff(t2.order_date,t1.order_date))>=8   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then ldyj end) else null end  byj7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30 then ldyj end) else null end   byj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then ldyj end) else null end  byj180
    from (
        select distinct t1.order_date,t1.user_id,t1.user_type
            ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
        from q_order t1 
        left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk 
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1,2
) 
order by order_date 
;


---- 短视频窄口径
with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from mdw_order_v3_international a 
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
        and order_date >= '2025-04-01' and order_date <= date_sub(current_date,1)
)
,platform_new as (--- 判定平台新
    select  dt, user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= '2025-04-01' 
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-04-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from 
    (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-03-01' and '%(FORMAT_DATE)s' and coalesce(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
        t1.dt
        ,t1.query
        ,member_name
        ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >=  '2025-03-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹', '樊庆曦')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    where t3.query is null
)

,video_info as (
    select distinct uv.dt,uv.user_id
    from uv 
    left join video r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    and uv.user_type = '新客'
) 

,video_data as (
    select distinct uv.dt,uv.user_id
    from video_info uv
    join q_order r on uv.user_id = r.user_id and  uv.dt=r.order_date 
) 


select order_date
        ,uv
        ,yj0
        ,yj1
        ,yj2
        ,yj3
        ,yj4
        ,yj5
        ,yj6
        ,yj7
        ,yj30
        ,yj180
        ,yj0 / uv    ARPU0
        ,yj1 / uv    ARPU1
        ,yj2 / uv    ARPU2
        ,yj3 / uv    ARPU3
        ,yj4 / uv    ARPU4
        ,yj5 / uv    ARPU5
        ,yj6 / uv    ARPU6
        ,yj7 / uv    ARPU7
        ,yj30 / uv   ARPU30
        ,yj180 / uv  ARPU180
from (
    select t1.order_date
        ,count(distinct t1.user_id) uv
        ,sum(case when datediff(t2.order_date, t1.order_date) = 0  then final_commission_after end) yj0
        ,case when max(datediff(t2.order_date,t1.order_date))>=1   then sum(case when datediff(t2.order_date, t1.order_date) <= 1   then final_commission_after end) else null end yj1
        ,case when max(datediff(t2.order_date,t1.order_date))>=2   then sum(case when datediff(t2.order_date, t1.order_date) <= 2   then final_commission_after end) else null end yj2
        ,case when max(datediff(t2.order_date,t1.order_date))>=3   then sum(case when datediff(t2.order_date, t1.order_date) <= 3   then final_commission_after end) else null end yj3
        ,case when max(datediff(t2.order_date,t1.order_date))>=4   then sum(case when datediff(t2.order_date, t1.order_date) <= 4   then final_commission_after end) else null end yj4
        ,case when max(datediff(t2.order_date,t1.order_date))>=5   then sum(case when datediff(t2.order_date, t1.order_date) <= 5   then final_commission_after end) else null end yj5
        ,case when max(datediff(t2.order_date,t1.order_date))>=6   then sum(case when datediff(t2.order_date, t1.order_date) <= 6   then final_commission_after end) else null end yj6
        ,case when max(datediff(t2.order_date,t1.order_date))>=7   then sum(case when datediff(t2.order_date, t1.order_date) <= 7   then final_commission_after end) else null end yj7
        ,case when max(datediff(t2.order_date,t1.order_date))>=30  then sum(case when datediff(t2.order_date, t1.order_date) <= 30  then final_commission_after end) else null end yj30
        ,case when max(datediff(t2.order_date,t1.order_date))>=180 then sum(case when datediff(t2.order_date, t1.order_date) <= 180 then final_commission_after end) else null end yj180
    from (
        select t1.dt order_date,t1.user_id
        from video_data t1
    ) t1 
    left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
    group by 1
) 
order by order_date 
;



--- Q4短视频窄口径新客收益
with user_type as (-----新老客
    select user_id
            , min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
            else final_commission_after+coalesce(ext_plat_certificate,0) end as ldyj
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= '2025-10-01' and order_date <= '2026-01-31' 
)
,uv as ----分日去重活跃用户
(
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >= '2025-10-01'
       and dt <= '2026-01-31'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,video as   -- 换成短视频数据
(
    select distinct t1.dt,user_name,potential_new_flag,page
    from 
    (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-08-01' and '%(FORMAT_DATE)s' and coalesce(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
        t1.dt
        ,t1.query
        ,member_name
        ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin','vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >=  '2025-08-01'
        and member_name in ('郭锦芳','方霁雪', '朱贝贝', '李雪莹', '樊庆曦')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    where t3.query is null
)

,video_info as (
    select distinct uv.dt,uv.user_id
    from uv 
    left join video r on uv.user_name = r.user_name
    where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    and uv.user_type = '新客'
) 

,video_data as (
    select distinct uv.dt,uv.user_id
    from video_info uv
    join q_order r on uv.user_id = r.user_id and  uv.dt=r.order_date 
) 



select substr(t1.order_date,1,7) mth,substr(t2.order_date,1,7) mth
    ,count(distinct t1.user_id) uv
    ,sum(final_commission_after) yj
from (
    select t1.dt order_date,t1.user_id
    from video_data t1
) t1 
left join q_order t2 on t1.user_id=t2.user_id and t2.order_date >= t1.order_date
group by 1,2

;