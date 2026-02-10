with uv as (
    select dt,user_name,user_id
            ,case 
                when substr(hotel_seq, -1, 1) in ('1', '2') then '实验组A'
                when substr(hotel_seq, -1, 1) in ('3', '4') then '对照组B'
                when substr(hotel_seq, -1, 1) in ('5', '6') then '对照组C'
                when substr(hotel_seq, -1, 1) in ('7', '8') then '对照组D'
                when substr(hotel_seq, -1, 1) in ('9', '0') then '对照组E'
                else '其他'
            end hotel_seq_type
           ,count(1) pv
           ,count(case when page_type = 'S' then 1 end)  s_pv
           ,count(case when page_type = 'D' then 1 end)  d_pv
           ,count(case when page_type = 'B' then 1 end)  b_pv
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
        ,user_name 
        ,'S' as page_type 
        ,user_id
        ,hotel_seq
            
        from default.dwd_ihotel_flow_app_searchlist_di
        where dt >= '20251201'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and is_display = 1
        and search_type IN (0,16,17)
        group by 1,2,3,4,5

        union all 

        select dt
            ,user_name 
            ,'D' as page_type 
            ,user_id
            ,hotel_seq
        from ihotel_default.dw_user_app_log_detail_visit_di_v1
        where dt >= '2025-12-01'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and business_type = 'hotel'
        group by 1,2,3,4,5

        union all 

        select dt
            ,user_name 
            ,'B' as page_type 
            ,user_id
            ,hotel_seq
        from ihotel_default.dw_user_app_log_booking_di_v1
        where dt >= '2025-12-01'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and business_type = 'hotel'
        group by 1,2,3,4,5

        union all 

        select dt
            ,user_name 
            ,'O' as page_type 
            ,user_id 
            ,hotel_seq
        from ihotel_default.dw_user_app_log_order_submit_di_v1
        where dt >= '2025-12-01'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and business_type = 'hotel'
        group by 1,2,3,4,5
    )t group by 1,2,3,4
)


,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case 
                when substr(hotel_seq, -1, 1) in ('1', '2') then '实验组A'
                when substr(hotel_seq, -1, 1) in ('3', '4') then '对照组B'
                when substr(hotel_seq, -1, 1) in ('5', '6') then '对照组C'
                when substr(hotel_seq, -1, 1) in ('7', '8') then '对照组D'
                when substr(hotel_seq, -1, 1) in ('9', '0') then '对照组E'
                else '其他'
            end hotel_seq_type
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
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-12-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)


select t1.dt
      ,t1.hotel_seq_type
      ,sdbo_UV
      ,s_all_UV
      ,d_s_UV
      ,b_ds_UV
      ,o_ds_order
      ,concat(round(d_s_UV / s_all_UV * 100, 2), '%')  s2d
      ,concat(round(b_ds_UV / d_s_UV * 100, 2), '%')  d2b
      ,concat(round(o_ds_order / b_ds_UV * 100, 2), '%')  b2o
      ,concat(round(o_ds_order / sdbo_UV * 100, 2), '%')  cr

from(---- Q得DBO转化
    select 
         a.dt
        ,a.hotel_seq_type
        ,count(distinct a.user_id) sdbo_UV
        ,count(distinct case when page_type = 'S' then a.user_id  end) s_all_UV
        ,count(distinct case when page_type = 'D' then a.user_id  end) d_all_UV
        ,count(distinct case when page_type = 'B' then a.user_id  end) b_all_UV
        ,count(distinct case when page_type = 'O' then a.user_id  end) o_UV

        ,count(distinct case when page_type = 'D' and  page_type = 'S' then a.user_id  end) d_s_UV
        ,count(distinct case when page_type = 'B' and page_type = 'D' and  page_type = 'S' then  a.user_id  end) b_ds_UV
        ,count(distinct case when b.user_id is not null and page_type = 'D' and  page_type = 'S'  then order_no end) o_ds_order

        ,count(distinct b.user_id) order_user_cnt
    from  uv a  -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.hotel_seq_type=b.hotel_seq_type   -- 订单表
    group by 1,2
)t1  
order by 1,2
;

select t1.dt
      ,t1.hotel_seq_type
      ,sdbo_UV
      ,s_all_UV
      ,d_s_UV
      ,b_ds_UV
      ,o_ds_order
      ,concat(round(d_s_UV / s_all_UV * 100, 2), '%')  s2d
      ,concat(round(b_ds_UV / d_s_UV * 100, 2), '%')  d2b
      ,concat(round(o_ds_order / b_ds_UV * 100, 2), '%')  b2o
      ,concat(round(o_ds_order / sdbo_UV * 100, 2), '%')  cr

from(---- Q得DBO转化
    select 
         a.dt
        ,a.hotel_seq_type
        ,count(distinct a.user_id) sdbo_UV
        ,count(distinct case when s_pv>0 then a.user_id  end) s_all_UV
        ,count(distinct case when d_pv>0 then a.user_id  end) d_all_UV
        ,count(distinct case when b_pv>0 then a.user_id  end) b_all_UV

        ,count(distinct case when d_pv>0 and  s_pv>0 then a.user_id  end) d_s_UV
        ,count(distinct case when b_pv>0 and d_pv>0 and  s_pv>0  then  a.user_id  end) b_ds_UV
        ,count(distinct case when b.user_id is not null and d_pv>0 and  s_pv>0   then order_no end) o_ds_order

        ,count(distinct b.user_id) order_user_cnt
    from  uv a  -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.hotel_seq_type=b.hotel_seq_type   -- 订单表
    group by 1,2
)t1  
order by 1,2
;



select dt,hotel_seq_type,count(distinct user_name)
    ,count(distinct case when page_type='S' then user_name end) s
    ,count(distinct case when page_type='D' then user_name end) d
    ,count(distinct case when page_type='B' then user_name end) b
    ,count(distinct case when page_type='O' then user_name end) o
from uv 
where dt = '2025-12-22' 
group by 1,2
;

---- 最终使用这个sql
with uv as (
    select dt,user_name,user_id
            ,case 
                when substr(hotel_seq, -1, 1) in ('1', '2') then '实验组A'
                when substr(hotel_seq, -1, 1) in ('3', '4') then '对照组B'
                when substr(hotel_seq, -1, 1) in ('5', '6') then '对照组C'
                when substr(hotel_seq, -1, 1) in ('7', '8') then '对照组D'
                when substr(hotel_seq, -1, 1) in ('9', '0') then '对照组E'
                else '其他'
            end hotel_seq_type
           ,count(1) pv
           ,count(case when page_type = 'S' then 1 end)  s_pv
           ,count(case when page_type = 'D' then 1 end)  d_pv
           ,count(case when page_type = 'B' then 1 end)  b_pv
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
        ,user_name 
        ,'S' as page_type 
        ,user_id
        ,hotel_seq
            
        from default.dwd_ihotel_flow_app_searchlist_di
        where dt >= '20251201'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and is_display = 1
        and search_type IN (0,16,17)
        group by 1,2,3,4,5

        union all 

        select dt
            ,user_name 
            ,'D' as page_type 
            ,user_id
            ,hotel_seq
        from ihotel_default.dw_user_app_log_detail_visit_di_v1
        where dt >= '2025-12-01'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and business_type = 'hotel'
        group by 1,2,3,4,5

        union all 

        select dt
            ,user_name 
            ,'B' as page_type 
            ,user_id
            ,hotel_seq
        from ihotel_default.dw_user_app_log_booking_di_v1
        where dt >= '2025-12-01'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and business_type = 'hotel'
        group by 1,2,3,4,5

        union all 

        select dt
            ,user_name 
            ,'O' as page_type 
            ,user_id 
            ,hotel_seq
        from ihotel_default.dw_user_app_log_order_submit_di_v1
        where dt >= '2025-12-01'
        and (province_name in ('台湾', '澳门', '香港')or country_name != '中国')
        and business_type = 'hotel'
        group by 1,2,3,4,5
    )t group by 1,2,3,4
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case 
                when substr(hotel_seq, -1, 1) in ('1', '2') then '实验组A'
                when substr(hotel_seq, -1, 1) in ('3', '4') then '对照组B'
                when substr(hotel_seq, -1, 1) in ('5', '6') then '对照组C'
                when substr(hotel_seq, -1, 1) in ('7', '8') then '对照组D'
                when substr(hotel_seq, -1, 1) in ('9', '0') then '对照组E'
                else '其他'
            end hotel_seq_type
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
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-12-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)


select t1.dt
      ,t1.hotel_seq_type
      ,sdbo_UV
      ,s_all_UV
      ,d_s_UV
      ,b_ds_UV
      ,o_ds_order
      ,concat(round(d_s_UV / s_all_UV * 100, 2), '%')  s2d
      ,concat(round(b_ds_UV / d_s_UV * 100, 2), '%')  d2b
      ,concat(round(o_ds_order / b_ds_UV * 100, 2), '%')  b2o
      ,concat(round(o_ds_order / sdbo_UV * 100, 2), '%')  cr

from(---- Q得DBO转化
    select 
         a.dt
        ,a.hotel_seq_type
        ,count(distinct a.user_id) sdbo_UV
        ,count(distinct case when s_pv>0 then a.user_id  end) s_all_UV
        ,count(distinct case when d_pv>0 then a.user_id  end) d_all_UV
        ,count(distinct case when b_pv>0 then a.user_id  end) b_all_UV

        ,count(distinct case when d_pv>0 and  s_pv>0 then a.user_id  end) d_s_UV
        ,count(distinct case when b_pv>0 and d_pv>0 and  s_pv>0  then  a.user_id  end) b_ds_UV
        ,count(distinct case when b.user_id is not null and d_pv>0 and  s_pv>0   then order_no end) o_ds_order

        ,count(distinct b.user_id) order_user_cnt
    from  uv a  -- 流量表
    left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.hotel_seq_type=b.hotel_seq_type   -- 订单表
    group by 1,2
)t1  
order by 1,2
;