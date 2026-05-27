--- 1、D页首访BML次留7留
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----分日去重活跃用户
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
    where dt >= '2025-12-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,d_uv as (---- D页流量
    select a.dt 
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          , case when province_name in ('澳门','香港') then province_name 
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                when c.area in ('欧洲','亚太','美洲') then c.area
                else '其他' end as mdd
          ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
          ,a.user_id,hotel_seq,qtrace_id trace_id
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name
    left join user_type b on a.user_id = b.user_id 
    where a.dt >= '2026-01-01' and a.dt <= date_sub(current_date, 1)
      and a.business_type = 'hotel'
      and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6,7
)
,q_d_info as (--- D页 7日首访用户
    select t1.dt,t1.user_id,t1.user_type,t1.mdd,t1.new_mdd,t1.hotel_seq,t1.trace_id
        ,case when t2.user_id is not null then 'N' else 'Y' end is_bulking7   --- 是否增量用户
    from d_uv t1 
    left join d_uv t2 on t1.user_id=t2.user_id
    and t2.dt < t1.dt and datediff(t1.dt,t2.dt) <= 7
    group by 1,2,3,4,5,6,7,8
)
,q_compare_price as(-- QC比较数据
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
        ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
        ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
        ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result,hotel_seq,user_id
        ,qunar_price_info['traceId'] trace_id
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    where dt >= '20260101' and dt <= replace(date_sub(current_date, 1),'-','')
        and business_type = 'intl_crawl_cq_api_userview' --- 流量视角
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and room_type_cover = 'Qmeet'
        and ctrip_room_status = 'true' 
        and qunar_room_status = 'true'
)
,q_d_compare as (
    select t1.dt,t1.user_id,t1.user_type,t1.mdd,t1.new_mdd
            ,case when pay_price_compare_result = 'Qlose' then 'lose' when pay_price_compare_result = 'Qbeat' then 'beat' 
                when pay_price_compare_result not in ('Qlose','Qbeat') then 'meet' else '其他' end as pay_price_compare_result
    from (
        select *
        from q_d_info where is_bulking7 = 'Y'
    ) t1 
    left join q_compare_price t2 on t1.trace_id = t2.trace_id and t1.hotel_seq = t2.hotel_seq 
            and t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1,2,3,4,5,6
)

select t1.dt,t1.user_type,t1.mdd,t1.pay_price_compare_result
        
       ,count(distinct t1.user_id) as user_cnt
       ,count(distinct case when t2.user_id is not null then t1.user_id end) as user_cnt_1d
       ,count(distinct case when t3.user_id is not null then t1.user_id end) as user_cnt_7d
       ,count(distinct t2.user_id)  / count(distinct t1.user_id) as user_1d_rate
       ,count(distinct t3.user_id)  / count(distinct t1.user_id) as user_7d_rate
from q_d_compare t1
left join uv t2 on t1.user_id = t2.user_id and datediff(t2.dt,t1.dt) = 1
left join uv t3 on t1.user_id = t3.user_id and datediff(t3.dt,t1.dt) = 7
group by 1,2,3,4
order by 1,2,3,4

;


--- 2、D页流量BML次留7留
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----分日去重活跃用户
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
    where dt >= '2025-12-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,d_uv as (---- D页流量
    select a.dt 
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          , case when province_name in ('澳门','香港') then province_name 
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                when c.area in ('欧洲','亚太','美洲') then c.area
                else '其他' end as mdd
          ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
          ,a.user_id,hotel_seq,qtrace_id trace_id
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name
    left join user_type b on a.user_id = b.user_id 
    where a.dt >= '2026-01-01' and a.dt <= date_sub(current_date, 1)
      and a.business_type = 'hotel'
      and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6,7
)

,q_compare_price as(-- QC比较数据
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
        ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
        ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
        ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result,hotel_seq,user_id
        ,qunar_price_info['traceId'] trace_id
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    where dt >= '20260101' and dt <= replace(date_sub(current_date, 1),'-','')
        and business_type = 'intl_crawl_cq_api_userview' --- 流量视角
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and room_type_cover = 'Qmeet'
        and ctrip_room_status = 'true' 
        and qunar_room_status = 'true'
)
,q_d_compare as (
    select t1.dt,t1.user_id,t1.user_type,t1.mdd,t1.new_mdd
            ,case when pay_price_compare_result = 'Qlose' then 'lose' when pay_price_compare_result = 'Qbeat' then 'beat' 
                when pay_price_compare_result not in ('Qlose','Qbeat') then 'meet' else '其他' end as pay_price_compare_result
    from (
        select *
        from d_uv
    ) t1 
    left join q_compare_price t2 on t1.trace_id = t2.trace_id and t1.hotel_seq = t2.hotel_seq 
            and t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1,2,3,4,5,6
)

select t1.dt,t1.user_type,t1.mdd,t1.pay_price_compare_result
        
       ,count(distinct t1.user_id) as user_cnt
       ,count(distinct case when t2.user_id is not null then t1.user_id end) as user_cnt_1d
       ,count(distinct case when t3.user_id is not null then t1.user_id end) as user_cnt_7d
       ,count(distinct t2.user_id)  / count(distinct t1.user_id) as user_1d_rate
       ,count(distinct t3.user_id)  / count(distinct t1.user_id) as user_7d_rate
from q_d_compare t1
left join uv t2 on t1.user_id = t2.user_id and datediff(t2.dt,t1.dt) = 1
left join uv t3 on t1.user_id = t3.user_id and datediff(t3.dt,t1.dt) = 7
group by 1,2,3,4
order by 1,2,3,4
;


--- 3、D页流量BL次留7留
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (----分日去重活跃用户
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
    where dt >= '2025-12-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,d_uv as (---- D页流量
    select a.dt 
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          , case when province_name in ('澳门','香港') then province_name 
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                when c.area in ('欧洲','亚太','美洲') then c.area
                else '其他' end as mdd
          ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
          ,a.user_id,hotel_seq,qtrace_id trace_id
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name
    left join user_type b on a.user_id = b.user_id 
    where a.dt >= '2026-01-01' and a.dt <= date_sub(current_date, 1)
      and a.business_type = 'hotel'
      and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5,6,7
)

,q_compare_price as(-- QC比较数据
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
        ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
        ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
        ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result,hotel_seq,user_id
        ,qunar_price_info['traceId'] trace_id,qunar_physical_room_id
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    where dt >= '20260101' and dt <= replace(date_sub(current_date, 1),'-','')
        and business_type = 'intl_crawl_cq_api_userview_acc' --- 流量视角 主站 -- intl_crawl_cq_api_userview_acc 主站模拟券后价
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and room_type_cover = 'Qmeet'
        and ctrip_room_status = 'true' 
        and qunar_room_status = 'true'
)
,uv_loses as (
    select  order_date
            ,user_id
            ,hotel_seq
            ,qunar_physical_room_id --物理房型
            ,mdd
            ,sum(case when pay_price_diff >0 then 1 else 0 end) as `lose条数`
    from q_compare_price
    group by 1,2,3,4,5
)
,q_d_compare as (
    select t1.dt,t1.user_id,t1.user_type,t1.mdd,t1.new_mdd
            ,case when `lose条数` > 0  then 'lose' else '其他' end as pay_price_compare_result
    from (
        select *
        from d_uv
    ) t1 
    left join uv_loses t2 on t1.hotel_seq = t2.hotel_seq  and t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1,2,3,4,5,6
)


select t1.dt,t1.pay_price_compare_result
        
       ,count(distinct t1.user_id) as user_cnt
       ,count(distinct case when t2.user_id is not null then t1.user_id end) as user_cnt_1d
       ,count(distinct case when t3.user_id is not null then t1.user_id end) as user_cnt_7d
       ,count(distinct t2.user_id)  / count(distinct t1.user_id) as user_1d_rate
       ,count(distinct t3.user_id)  / count(distinct t1.user_id) as user_7d_rate
from q_d_compare t1
left join uv t2 on t1.user_id = t2.user_id and datediff(t2.dt,t1.dt) = 1
left join uv t3 on t1.user_id = t3.user_id and datediff(t3.dt,t1.dt) = 7
group by 1,2
order by 1,2
;




with qc_price as (
    select order_date
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        -- ,if(grouping(holiday_type)=1,'ALL', holiday_type) as  holiday_type
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,count(distinct case when  pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)      `支付价beat0-3%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
        ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            ,case when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                    when a.country_name in ('日本','韩国','泰国') then a.country_name 
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
            
            -- 【新增】: 使用解析后的 order_date 和 check_in 日期计算提前订分布
            ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) < 0 or datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) = 0 then '凌晨订&当天订'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 1 and 3    then '提前订1-3天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 4 and 7    then '提前订4-7天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14   then '提前订8-14天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
             end as per_type
            ,case
                when check_out between '2026-04-04' and '2026-04-06' then '26年清明'
                when check_out between '2026-02-15' and '2026-02-23' then '26年春节'
                when check_out between '2025-05-01' and '2025-05-05' then '25年五一'
            end as holiday_type
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260301' and dt <= replace(date_sub(current_date, 1),'-','')
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
    )t
    group by 1,cube(per_type,mdd)
)

select order_date,mdd
      ,per_type
      ,`支付价lose率`
      ,`支付价lose深度`
      ,`支付价beat深度`
      ,`支付价beat数`       /   `支付价抓取次数`  `beat率`
      ,`支付价beat0-3%次数`   / `支付价抓取次数`  `支付价beat0-3%率`
      ,`支付价beat3-4%次数`   / `支付价抓取次数`  `支付价beat3-4%率`
      ,`支付价beat4-5%次数`   / `支付价抓取次数`  `支付价beat4-5%率`
      ,`支付价beat5-6%次数`   / `支付价抓取次数`  `支付价beat5-6%率`
      ,`支付价beat6-7%次数`   / `支付价抓取次数`  `支付价beat6-7%率`
      ,`支付价beat7-8%次数`   / `支付价抓取次数`  `支付价beat7-8%率`
      ,`支付价beat8%以上次数` /  `支付价抓取次数`  `支付价beat8%以上率`
from qc_price
order by 1,2,3
;



-- 用户首单日期
with user_type as (
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   -- 海外订单表
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ( -- 分日去重活跃用户
    select dt
          ,case when province_name in ('澳门','香港') then province_name when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
          ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
          ,a.user_id
          ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id
    where dt >= '2025-12-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null
        and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null
        and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,d_uv as ( -- D页流量
    select a.dt
          ,case when province_name in ('澳门','香港') then '港澳' when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长' when a.country_name in ('日本','韩国','泰国') then a.country_name else '其他' end as new_mdd
          ,case when province_name in ('澳门','香港') then province_name when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
          ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
          ,a.user_id
          ,a.hotel_seq
          ,a.qtrace_id as trace_id
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
    left join user_type b on a.user_id = b.user_id
    where a.dt >= '2026-01-01'
        and a.dt <= date_sub(current_date, 1)
        and a.business_type = 'hotel'
        and (a.province_name in ('台湾','澳门','香港') or a.country_name != '中国')
    group by 1,2,3,4,5,6,7
)
,q_d_info as ( -- D页 7日首访用户
    select t1.dt
          ,t1.user_id
          ,t1.user_type
          ,t1.mdd
          ,t1.new_mdd
          ,t1.hotel_seq
          ,t1.trace_id
          ,case when t2.user_id is not null then 'N' else 'Y' end as is_bulking7   -- 是否增量用户
    from d_uv t1
    left join uv t2 on t1.user_id = t2.user_id and t2.dt < t1.dt and datediff(t1.dt, t2.dt) <= 7
    group by 1,2,3,4,5,6,7,8
)
,q_compare_price as ( -- QC比较数据
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
          ,case when province_name in ('澳门','香港') then '港澳' when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长' when a.country_name in ('日本','韩国','泰国') then a.country_name else '其他' end as new_mdd
          ,case when province_name in ('澳门','香港') then province_name when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
          ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
          ,a.id
          ,a.pay_price_diff
          ,a.ctrip_pay_price
          ,a.pay_price_compare_result
          ,a.hotel_seq
          ,a.user_id
          ,a.qunar_price_info['traceId'] as trace_id
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name
    where dt >= '20260101'
        and dt <= replace(date_sub(current_date, 1), '-', '')
        and business_type = 'intl_crawl_cq_api_userview'   -- 流量视角
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and room_type_cover = 'Qmeet'
        and ctrip_room_status = 'true'
        and qunar_room_status = 'true'
)
,q_d_compare as (
    select t1.dt
          ,t1.user_id
          ,t1.user_type
          ,t1.mdd
          ,t1.new_mdd
          ,case when pay_price_compare_result = 'Qlose' then 'lose' when pay_price_compare_result = 'Qbeat' then 'beat' when pay_price_compare_result not in ('Qlose','Qbeat') then 'meet' else '其他' end as pay_price_compare_result
    from (
        select *
        from q_d_info
        where is_bulking7 = 'Y'
    ) t1
    left join q_compare_price t2 on t1.trace_id = t2.trace_id and t1.hotel_seq = t2.hotel_seq and t1.user_id = t2.user_id and t1.dt = t2.order_date
    group by 1,2,3,4,5,6
)

select t1.dt
      ,t1.user_type
      ,t1.mdd
      ,t1.pay_price_compare_result
      ,count(distinct t1.user_id) as user_cnt
      ,count(distinct case when t2.user_id is not null then t1.user_id end) as user_cnt_1d
      ,count(distinct case when t3.user_id is not null then t1.user_id end) as user_cnt_7d
      ,count(distinct t2.user_id) / count(distinct t1.user_id) as user_1d_rate
      ,count(distinct t3.user_id) / count(distinct t1.user_id) as user_7d_rate
from q_d_compare t1
left join uv t2 on t1.user_id = t2.user_id and datediff(t2.dt, t1.dt) = 1
left join uv t3 on t1.user_id = t3.user_id and datediff(t3.dt, t1.dt) = 7
group by 1,2,3,4
order by 1,2,3,4
;