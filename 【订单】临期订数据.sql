
--- 1、订单数据对比--预定口径
with q_order as ( --- Q侧订单明细打标
    select a.order_date
          ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                -- else '提前订31+' 
                else '提前订15+' 
          end as per_type
          ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
        -- 6. 日期分类：holiday、workday、weekend
        ,dd.date_type,dd.holiday_name
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.order_date = dd.date
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
)
,c_order as ( 
    select substr(o.order_date,1,10) as dt
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订15+'  
          end as per_type
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
          -- 6. 日期分类：holiday、workday、weekend
          ,dd.date_type,dd.holiday_name
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.order_date,1,10) = dd.date
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
)
,q_agg as (
    select order_date as dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(mdd)=1, 'ALL', mdd) as mdd
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
    from q_order t
    group by order_date,per_type,linqi_type
    grouping sets (
        (order_date,per_type), -- 核心维度：提前订
        (order_date,per_type,mdd), -- 核心维度：提前订*目的地

        (order_date,linqi_type), 
        (order_date,linqi_type,mdd), 
        (order_date)
    )
)
,c_agg as (
    select dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(mdd)=1, 'ALL', mdd) as mdd
          ,count(distinct order_no) as c_orders
          ,sum(room_night) as c_rn
    from c_order
    group by dt,per_type,linqi_type,mdd
    grouping sets (
        (dt,per_type,mdd),  -- 核心维度：提前订&目的地
        (dt,per_type), -- 核心维度：提前订
         -- 【新增】基于 linqi_type 的分组
        (dt,linqi_type,mdd),  
        (dt,linqi_type), 
        (dt)
    )
)

-- 【合并输出】
select coalesce(q.dt, c.dt) as `日期`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      ,coalesce(q.mdd, c.mdd) as `目的地`
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(c.c_orders, 0) as `C订单量`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(q.q_rn, 0)  / coalesce(c.c_rn, 0)  as `间夜量QC`
from q_agg q
left  join c_agg c on q.dt = c.dt and q.per_type = c.per_type and q.mdd = c.mdd
order by `日期` desc;



--- 2、订单数据对比-离店口径
with q_order as ( --- Q侧订单明细打标
    select a.order_date,checkout_date
          ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                -- else '提前订31+' 
                else '提前订15+' 
          end as per_type
          ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
        -- 6. 日期分类：holiday、workday、weekend
        ,dd.date_type,dd.holiday_name
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.order_date = dd.date
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date >= '2025-01-01' and a.checkout_date <= date_sub(current_date, 1)
)
,c_order as ( 
    select substr(o.order_date,1,10) as dt
          ,substr(checkout_date,1,10) as checkout_date
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订15+'  
          end as per_type
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
          -- 6. 日期分类：holiday、workday、weekend
          ,dd.date_type,dd.holiday_name
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.order_date,1,10) = dd.date
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and order_status <> 'C'
      and substr(checkout_date,1,10) >= '2025-01-01' and substr(checkout_date,1,10) <= cast(date_sub(current_date, 1) as string)
)
,q_agg as (
    select checkout_date as dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(mdd)=1, 'ALL', mdd) as mdd
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
    from q_order t
    group by checkout_date,per_type,linqi_type
    grouping sets (
        (checkout_date,per_type), -- 核心维度：提前订
        (checkout_date,per_type,mdd), -- 核心维度：提前订*目的地

        (checkout_date,linqi_type), 
        (checkout_date,linqi_type,mdd), 
        (checkout_date)
    )
)
,c_agg as (
    select checkout_date dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(mdd)=1, 'ALL', mdd) as mdd
          ,count(distinct order_no) as c_orders
          ,sum(room_night) as c_rn
    from c_order
    group by checkout_date,per_type,linqi_type,mdd
    grouping sets (
        (checkout_date,per_type,mdd),  -- 核心维度：提前订&目的地
        (checkout_date,per_type), -- 核心维度：提前订
         -- 【新增】基于 linqi_type 的分组
        (checkout_date,linqi_type,mdd),  
        (checkout_date,linqi_type), 
        (checkout_date)
    )
)

-- 【合并输出】
select coalesce(q.dt, c.dt) as `日期`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      ,coalesce(q.mdd, c.mdd) as `目的地`
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(c.c_orders, 0) as `C订单量`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(q.q_rn, 0)  / coalesce(c.c_rn, 0)  as `间夜量QC`
from q_agg q
left  join c_agg c on q.dt = c.dt and q.per_type = c.per_type and q.mdd = c.mdd
order by `日期` desc;
