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
,q_uv as (
        select distinct dt 
                ,case when fromforlog='4104' or fromforlog='4106' then '[4104,4106]' else fromforlog end fromforlog
                ,case when fromforlog='4104' or fromforlog='4106' then '二屏内容贴' 
                    when fromforlog='200000081' then '二屏商卡' 
                    when fromforlog='200000083' then '市场活动去使用' 
                    when fromforlog='200000105' then '天天领券任务' 
                    when fromforlog='200000121' then '答题领积分任务'  
                    when fromforlog='200000118' then '国酒活动去使用' 
                    when fromforlog='200000119' then '机票实时短信' 
                    when fromforlog='200000120' then '带参数push' 
                    when fromforlog='200000122' then '国酒大搜落地页商卡' 
                    when fromforlog='200000123' then '带参数短信' 
                    when fromforlog='671' then '大搜落地页-酒店tab' 
                    when fromforlog='96' then '大搜' 
                    when fromforlog='4626' then '我的页面弹窗（机酒用户）' 
                    when fromforlog='913' then 'App首页宫格-酒店频道-海外酒店tab' 
                    when fromforlog='914' then 'App首页-海外酒店' 
                    when fromforlog='4604' then '国际酒店H页快筛标签' 
                    when fromforlog='824' then '收藏跳转到酒店详情页'
                    else fromforlog
                end as fromforlog_type
                ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,a.user_id
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join user_type b on a.user_id = b.user_id 
        where dt >= '2025-01-01'
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
            and fromforlog in ('200000119','200000120','4104','4106','200000081','200000123','200000121','200000122','200000105','200000118','200000083','671','96','4626','913','914','4604','824') 
        )
,q_uv_all as (
        select distinct dt 
                ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,a.user_id
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join user_type b on a.user_id = b.user_id 
        where dt >= '2025-01-01'
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        )
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)


select t1.dt,t1.fromforlog,t1.fromforlog_type
      ,uv
      ,concat(round(order_no/uv *100, 2), '%') cr
      ,concat(round(order_no_conpon / order_no *100, 2), '%') order_no_conpon  -- 用券订单占比
      ,order_no    --- 订单量
      ,room_night  --- 间夜量
      ,nuv   --- 新客
      ,uv_all
      ,nuv_all
      ,concat(round(uv/uv_all *100, 2), '%') uvrate --- 流量占比
      ,concat(round(nuv/nuv_all *100, 2), '%') nuvrate --- 新客流量占比
      ,concat(round(nuv/uv *100, 2), '%') nrate --- 新客占比
      ,concat(round(order_no_nu/nuv *100, 2), '%') ncr --- 新客cr
      ,concat(round(order_no_conpon_nu/order_no_nu *100, 2), '%')  order_no_conpon_nu --- 新客用券订单占比
      ,order_no_nu     --- 新客订单量
      ,room_night_nu   --- 新客间夜量
from (
    select dt
          ,fromforlog
          ,fromforlog_type
          ,count(user_id) uv
          ,count(case when user_type = '新客' then user_id end) nuv
    from q_uv
    group by 1,2,3
)t1 left join (
    select order_date
          ,fromforlog
          ,fromforlog_type
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon
          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu
    from q_app_order t1 
    left join (
        select dt,fromforlog,fromforlog_type,user_id
        from q_uv 
        group by 1,2,3,4
    )t2 on t1.order_date=t2.dt and t1.user_id=t2.user_id
    where t2.user_id is not null
    group by 1,2,3
)t2 on t1.dt=t2.order_date and t1.fromforlog=t2.fromforlog
left join (
        select dt
           ,count(user_id) uv_all
           ,count(case when user_type = '新客' then user_id end) nuv_all
        from q_uv_all 
        group by 1
) t3 on t1.dt=t3.dt
order by  case when fromforlog = '913' then 1
               when fromforlog = '96' then 2
               when fromforlog = '914' then 3
               when fromforlog = '824' then 4
               when fromforlog = '671' then 5
               when fromforlog = '4626' then 6
               when fromforlog = '4604' then 7
               when fromforlog = '[4104,4106]' then 8
               when fromforlog = '200000083' then 9
               when fromforlog = '200000105' then 10
               when fromforlog = '200000123' then 11
               when fromforlog = '200000118' then 12
               when fromforlog = '200000081' then 13
               when fromforlog = '200000120' then 14
               when fromforlog = '200000119' then 15
               when fromforlog = '200000121' then 16
               when fromforlog = '200000122' then 17
          end asc
          ,t1.dt desc
;





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
,q_uv as (
        select distinct dt 
                ,case when fromforlog='4104' or fromforlog='4106' then '[4104,4106]' else fromforlog end fromforlog
                ,case when fromforlog='4104' or fromforlog='4106' then '二屏内容贴' 
                    when fromforlog='200000081' then '二屏商卡' 
                    when fromforlog='200000083' then '市场活动去使用' 
                    when fromforlog='200000105' then '天天领券任务' 
                    when fromforlog='200000121' then '答题领积分任务'  
                    when fromforlog='200000118' then '国酒活动去使用' 
                    when fromforlog='200000119' then '机票实时短信' 
                    when fromforlog='200000120' then '带参数push' 
                    when fromforlog='200000122' then '国酒大搜落地页商卡' 
                    when fromforlog='200000123' then '带参数短信' 
                    when fromforlog='671' then '大搜落地页-酒店tab' 
                    when fromforlog='96' then '大搜' 
                    when fromforlog='4626' then '我的页面弹窗（机酒用户）' 
                    when fromforlog='913' then 'App首页宫格-酒店频道-海外酒店tab' 
                    when fromforlog='914' then 'App首页-海外酒店' 
                    when fromforlog='4604' then '国际酒店H页快筛标签' 
                    when fromforlog='824' then '收藏跳转到酒店详情页' 
                    else fromforlog
                end as fromforlog_type
                ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,a.user_id
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join user_type b on a.user_id = b.user_id 
        where dt >= '2025-01-01'
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
            --and fromforlog in ('200000119','200000120','4104','4106','200000081','200000123','200000121','200000122','200000105','200000118','200000083','671','96','4626','913','914','4604','824') 
        )
,q_uv_all as (
        select distinct dt 
                ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,a.user_id
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        left join user_type b on a.user_id = b.user_id 
        where dt >= '2025-01-01'
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        )
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= 2025-01-01 and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)


select t1.dt,t1.fromforlog,t1.fromforlog_type,t1.mdd
      ,uv
      ,concat(round(order_no/uv *100, 2), '%') cr
      ,concat(round(order_no_conpon / order_no *100, 2), '%') order_no_conpon  -- 用券订单占比
      ,order_no    --- 订单量
      ,room_night  --- 间夜量
      ,uv_all
      ,nuv_all
      ,concat(round(uv/uv_all *100, 2), '%') uvrate --- 流量占比
      ,concat(round(nuv/nuv_all *100, 2), '%') nuvrate --- 新客流量占比
      ,concat(round(nuv/uv *100, 2), '%') nrate --- 新客占比
      ,concat(round(order_no_nu/nuv *100, 2), '%') ncr --- 新客cr
      ,concat(round(order_no_conpon_nu/order_no_nu *100, 2), '%')  order_no_conpon_nu --- 新客用券订单占比
      ,order_no_nu     --- 新客订单量
      ,room_night_nu   --- 新客间夜量
from (
    select dt
          ,fromforlog
          ,fromforlog_type
          ,mdd
          ,count(user_id) uv
          ,count(case when user_type = '新客' then user_id end) nuv
    from q_uv
    group by 1,2,3,4
)t1 left join (
    select order_date
          ,fromforlog
          ,fromforlog_type
          ,mdd
          ,count(distinct t1.order_no) order_no
          ,sum(t1.room_night) room_night
          ,count(distinct case when is_user_conpon = 'Y' then order_no else null end) order_no_conpon
          ,count(distinct case when user_type = '新客' then t1.order_no end) order_no_nu
          ,sum(case when user_type = '新客' then t1.room_night end) room_night_nu
          ,count(distinct case when user_type = '新客' and is_user_conpon = 'Y' then t1.order_no end) order_no_conpon_nu
    from q_app_order t1 
    left join (
        select dt,fromforlog,fromforlog_type,user_id
        from q_uv 
        group by 1,2,3,4
    )t2 on t1.order_date=t2.dt and t1.user_id=t2.user_id
    where t2.user_id is not null
    group by 1,2,3,4
)t2 on t1.dt=t2.order_date and t1.fromforlog=t2.fromforlog and t1.mdd=t2.mdd
left join (
        select dt
           ,count(user_id) uv_all
           ,count(case when user_type = '新客' then user_id end) nuv_all
        from q_uv_all 
        group by 1
) t3 on t1.dt=t3.dt
order by  case when fromforlog = '913' then 1
               when fromforlog = '96' then 2
               when fromforlog = '914' then 3
               when fromforlog = '824' then 4
               when fromforlog = '671' then 5
               when fromforlog = '4626' then 6
               when fromforlog = '4604' then 7
               when fromforlog = '[4104,4106]' then 8
               when fromforlog = '200000083' then 9
               when fromforlog = '200000105' then 10
               when fromforlog = '200000123' then 11
               when fromforlog = '200000118' then 12
               when fromforlog = '200000081' then 13
               when fromforlog = '200000120' then 14
               when fromforlog = '200000119' then 15
               when fromforlog = '200000121' then 16
               when fromforlog = '200000122' then 17
          end asc
          ,t1.dt desc
          ,t1.mdd desc
;