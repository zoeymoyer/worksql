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
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
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
            ,case when ext_flag_map['pay_after_stay_flag']='true' then '后付订单' 
                 else '非后付订单' end is_pay_after --- 后付订单
          
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-11-01' and order_date <= date_sub(current_date,1)
)

,init_uv as
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt >= '2025-11-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)

--- 整体 免息实验和后付交叉
select t1.dt
       ,ab,is_pay_after
       ,count(distinct t1.user_id)  uv
       ,count(distinct t3.order_no)  order_no
       ,count(distinct t3.order_no) / count(distinct t1.user_id) cr 
       ,sum(room_night) room_night
       ,sum(init_gmv) init_gmv
       ,sum(final_commission_after) final_commission_after
from (
    select t1.dt
        ,t1.user_id
        ,ab
    from init_uv t1 
    left join (
        select to_date(dt) dt
            ,trim(user_id)  user_id 
            ,case when ab in ('C','D') then 'CD' else ab end ab
        from temp.temp_zeyz_yang_gj_mx_ab_user_id_list
    )t2 on t1.user_id=t2.user_id and t1.dt=t2.dt
    where t2.user_id is not null
    group by 1,2,3
) t1
left join q_order t3 on t1.dt=t3.order_date and t1.user_id=t3.user_id
group by 1,2,3
order by 1,2,3
;


--- 整体
select t1.dt
       ,ab
       ,count(distinct t1.user_id)  uv
       ,count(distinct t3.order_no)  order_no
       ,count(distinct t3.order_no) / count(distinct t1.user_id) cr 
       ,sum(room_night) room_night
       ,sum(init_gmv) init_gmv
       ,sum(final_commission_after) final_commission_after
       ,sum(amt) amt
from (
    select t1.dt
        ,t1.user_id
        ,ab
    from init_uv t1 
    left join (
        select to_date(dt) dt
            ,trim(user_id)  user_id 
            ,case when ab in ('C','D') then 'CD' else ab end ab
        from temp.temp_zeyz_yang_gj_mx_ab_user_id_list
    )t2 on t1.user_id=t2.user_id and t1.dt=t2.dt
    where t2.user_id is not null
    group by 1,2,3
) t1
left join q_order t3 on t1.dt=t3.order_date and t1.user_id=t3.user_id
left join (  --- 成本
    select order_no
           ,iou_amt
           ,iou_back_amt
           ,iou_amt - iou_back_amt amt   --- 成本
    from (
        select  trim(order_no) order_no 
                ,iou_amt
                ,case when  iou_back_amt = 'null' then 0 else iou_back_amt end iou_back_amt
        from temp.temp_zeyz_yang_mx_ab_order_list
    ) 
) t4 on t3.order_no = t4.order_no
group by 1,2
order by 1,2
;
 
---- 新老客
select t1.dt
       ,ab
       ,t1.user_type
       ,count(distinct t1.user_id)  uv
       ,count(distinct t3.order_no)  order_no
       ,count(distinct t3.order_no) / count(distinct t1.user_id) cr 
       ,sum(room_night) room_night
       ,sum(init_gmv) init_gmv
       ,sum(final_commission_after) final_commission_after
       ,sum(amt) amt
from (
    select t1.dt
        ,t1.user_id
        ,ab
        ,t1.user_type
    from init_uv t1 
    left join (
        select to_date(dt) dt
            ,trim(user_id)  user_id 
            ,case when ab in ('C','D') then 'CD' else ab end ab
        from temp.temp_zeyz_yang_gj_mx_ab_user_id_list
    )t2 on t1.user_id=t2.user_id and t1.dt=t2.dt
    where t2.user_id is not null
    group by 1,2,3,4
) t1
left join q_order t3 on t1.dt=t3.order_date and t1.user_id=t3.user_id and t1.user_type=t3.user_type
left join (  --- 成本
    select order_no
           ,iou_amt
           ,iou_back_amt
           ,iou_amt - iou_back_amt amt   --- 成本
    from (
        select  trim(order_no) order_no 
                ,iou_amt
                ,case when  iou_back_amt = 'null' then 0 else iou_back_amt end iou_back_amt
        from temp.temp_zeyz_yang_mx_ab_order_list
    ) 
) t4 on t3.order_no = t4.order_no
group by 1,2,3
order by 1,2,3
;

--- 分星级
select t1.dt
       ,ab
       ,t1.hotel_grade
       ,count(distinct t1.user_id)  uv
       ,count(distinct t3.order_no)  order_no
       ,count(distinct t3.order_no) / count(distinct t1.user_id) cr 
       ,sum(room_night) room_night
       ,sum(init_gmv) init_gmv
       ,sum(final_commission_after) final_commission_after
       ,sum(amt) amt
from (
    select t1.dt
        ,t1.user_id
        ,ab
        ,t1.hotel_grade
    from init_uv t1 
    left join (
        select to_date(dt) dt
            ,trim(user_id)  user_id 
            ,case when ab in ('C','D') then 'CD' else ab end ab
        from temp.temp_zeyz_yang_gj_mx_ab_user_id_list
    )t2 on t1.user_id=t2.user_id and t1.dt=t2.dt
    where t2.user_id is not null
    group by 1,2,3,4
) t1
left join q_order t3 on t1.dt=t3.order_date and t1.user_id=t3.user_id and t1.hotel_grade=t3.hotel_grade
left join (  --- 成本
    select order_no
           ,iou_amt
           ,iou_back_amt
           ,iou_amt - iou_back_amt amt   --- 成本
    from (
        select  trim(order_no) order_no 
                ,iou_amt
                ,case when  iou_back_amt = 'null' then 0 else iou_back_amt end iou_back_amt
        from temp.temp_zeyz_yang_mx_ab_order_list
    ) 
) t4 on t3.order_no = t4.order_no
group by 1,2,3
order by 1,2,3
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
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
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
            ,case when ext_flag_map['pay_after_stay_flag']='true' then '后付订单' 
                 else '非后付订单' end is_pay_after --- 后付订单
            ,order_status
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        -- and order_status not in ('CANCELLED', 'REJECTED')
        and order_no <> '103576132435'
        and order_date >= '2025-11-01' and order_date <= date_sub(current_date,1)
)

select order_date,count(distinct t1.order_no),count(distinct t2.order_no)
from q_order t1
join temp.temp_zeyz_yang_gj_int_free_order_list t2 on t1.order_no = trim(t2.order_no)
group by 1