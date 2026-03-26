with user_type as(
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
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金      
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,case when init_gmv / room_night >= 2000 then '2000以上' else '2000以下' end is_adr
    from default.mdw_order_v3_international a 
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
        and order_date >= '2026-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_user_type as (   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = date_sub(current_date, 1)
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,get_json_object(discount_detail, '$.detail[1].amount') as cqe  -- C_券额
            ,case when room_fee / cast(extend_info['room_night'] as double) >= 2000 then '2000以上' else '2000以下' end is_adr
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,q_agg as (
    select order_date as dt
          ,if(grouping(mdd)=1, 'ALL', mdd) as mdd
          ,if(grouping(is_adr)=1, 'ALL', is_adr) as is_adr
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
          ,count(distinct user_id) as q_users
    from q_order_app
    group by order_date, cube(mdd, is_adr)
)
,c_agg as (
    select dt
          ,if(grouping(mdd)=1, 'ALL', mdd) as mdd
          ,if(grouping(is_adr)=1, 'ALL', is_adr) as is_adr
          ,count(distinct order_no) as c_orders
          ,sum(cast(room_night as double)) as c_rn
          ,count(distinct user_id) as c_users
    from c_order
    group by dt, cube(mdd, is_adr)
)

-- 【最终合并展现】
select coalesce(q.dt, c.dt) as `日期`
      ,coalesce(q.mdd, c.mdd) as `目的地`
      ,coalesce(q.is_adr, c.is_adr) as `价格带`
      
      -- 基础指标展示
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(c.c_orders, 0) as `C订单量`
      
      -- QC 比例指标计算
      ,concat(round(coalesce(q.q_rn / c.c_rn * 100, 0), 2), '%') as `间夜量QC`
      ,concat(round(coalesce(q.q_orders / c.c_orders * 100, 0), 2), '%') as `订单量QC`
      
      -- 复合指标：单订单QC = (Q订单/Q用户) / (C订单/C用户)
      ,concat(round(coalesce(
          (q.q_orders / q.q_users) / (c.c_orders / c.c_users), 0) * 100
      , 2), '%') as `单订单QC`
      
      -- 复合指标：单间夜QC = (Q间夜/Q订单) / (C间夜/C订单)
      ,concat(round(coalesce(
          (q.q_rn / q.q_orders) / (c.c_rn / c.c_orders), 0) * 100
      , 2), '%') as `单间夜QC`
from q_agg q
full outer join c_agg c on q.dt = c.dt and q.mdd = c.mdd and q.is_adr = c.is_adr
order by `日期` desc
      -- 让 ALL 的汇总项排在最前面，方便查看总盘
      ,case when coalesce(q.mdd, c.mdd) = 'ALL' then 0 else 1 end
      ,coalesce(q.mdd, c.mdd)
      ,case when coalesce(q.is_adr, c.is_adr) = 'ALL' then 0 else 1 end
      ,coalesce(q.is_adr, c.is_adr)
;