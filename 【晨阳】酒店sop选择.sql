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
,hotle_score as (  --- 酒店评分
    select obj_seq 
        ,max(reference_score) as score
    from default.ods_qunar_review_obj_score a
    where dt = '%(DATE)s'
          and tag = '1'
    group by 1
)
,brand_table as (   --- 酒店集团
    select chain_id,chain_name
    from temp.temp_yuchen_shen_chainhotel_id_list_20250702
    where chain_tag <> 'Local Chain'
    group by 1,2
)
,hotel_grade as (  --- 酒店等级商圈
    select  distinct hi.hotel_seq,
            hi.hotel_name,
            -- hotel_grade, --- 星级
            -- hotel_group_name, --- 所属集团
            -- chain_name,     --- 所属集团
            -- hotel_brand_name,
            x.BizZones,  --- 商圈
            Standard_Hotel
    from (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,hotel_grade --- 星级
            ,hotel_group_id  --- 所属集团
            ,hotel_group_name  --- 所属集团
            ,chain_name         --- 所属集团
            ,hotel_brand_name   --- 品牌
            ,case when attrs['hotelSubCategory'] in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标'
                when attrs['hotelSubCategory'] is null then '非标'
                else '标准' end as Standard_Hotel  --- 非标
        from ihotel_default.dim_hotel_info_intl_v3  a
        left join brand_table b on a.hotel_group_id = b.chain_id
        where a.dt = '%(DATE)s'
        group by 1,2,3,4,5,6,7,8
    ) hi
    left join (
        select distinct
            business_id AS hotel_seq,
            regexp_replace(
                regexp_replace(
                get_json_object(attrs, '$.BizZones'),
                '\\\\"', '"'  -- 处理转义双引号
                ),
                '\\\\\\\\', '\\\\'  -- 处理转义反斜杠
            ) AS BizZones,  --- 商圈
            get_json_object(attrs, 'CityCode') as citycode
        FROM hotel.ods_hotel_search_xds_entity_index_1_da
        WHERE dt = date_sub(current_date, 1)
        AND get_json_object(attrs, '$.Channel') = '1'
        AND (
            (
            get_json_object(attrs, '$.Status') IS NULL
            OR get_json_object(attrs, '$.Status') = 'on'
            )
            AND status = 0
        )
        AND get_json_object(attrs, '$.BizZones') IS NOT NULL
    )x
    on hi.hotel_seq = x.hotel_seq
)
,platform_new as (--- 判定平台新
    select  dt,user_pk
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 30)
        and dict_type = 'pncl_wl_username'
    group by 1,2
)
,q_order as (----订单明细表表包含取消 
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,user_name,order_no,init_gmv,room_night
            ,hotel_grade
            ,case when init_gmv / room_night < 400  then '1[0,400)'
                  when init_gmv / room_night >= 400 and init_gmv / room_night < 800  then '2[400,800)'
                  when init_gmv / room_night >= 800 and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' end adr
            ,a.country_name
            ,hotel_seq
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        -- and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date,1)
)
,init_uv as(
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
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,red as (
    select flow_dt as dt,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between '2025-06-01' and date_sub(current_date,1)
    and query_platform = 'redbook'
    group by 1,2
)
,red_res as (--- 小红书生单人群
    select uv.dt,uv.user_id,uv.user_type
           ,case
                when (uv.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when uv.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
    from(
        select distinct uv.dt,uv.user_id,user_type,uv.user_name
        from init_uv uv
        left join red r on uv.user_name = r.user_name
        where r.dt >= date_sub(uv.dt, 7) and r.dt <= uv.dt and r.user_name is not null
    ) uv
    left join platform_new t2 on uv.dt=t2.dt and uv.user_name=t2.user_pk
    left join q_order ord on uv.user_id = ord.user_id
    and uv.dt = ord.order_date
    where ord.user_id is not null
)
,qc_price as (
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
        ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
        ,hotel_seq
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
        ,count(distinct id) as `比价次数`
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    where dt >= '%(DATE_30)s'  and dt <=  '%(DATE)s'
        and business_type = 'intl_crawl_cq_spa'
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and room_type_cover = 'Qmeet'
        and ctrip_room_status = 'true' 
        and qunar_room_status = 'true'
        and ctrip_price_info['match_mainland_before_coupons_price_result'] = 1
    group by 1,2,3
)
,qc_price_data as (
    -- select hotel_seq
    --       ,sum(`beat率`) / count(1) `beat率`
    --       ,sum(`支付价beat0-3%率`) / count(1) `支付价beat0-3%率`
    --       ,sum(`支付价beat3-4%率`) / count(1) `支付价beat3-4%率`
    --       ,sum(`支付价beat4-5%率`) / count(1) `支付价beat4-5%率`
    --       ,sum(`支付价beat5-6%率`) / count(1) `支付价beat5-6%率`
    --       ,sum(`支付价beat6-7%率`) / count(1) `支付价beat6-7%率`
    --       ,sum(`支付价beat7-8%率`) / count(1) `支付价beat7-8%率`
    --       ,sum(`支付价beat8%以上率` ) / count(1) `支付价beat8%以上率` 
    -- from (
        select hotel_seq
            ,sum(`支付价beat数`)        / sum(`支付价抓取次数`) `beat率`
            ,sum(`支付价beat0-3%次数`)  / sum(`支付价抓取次数`)  `支付价beat0-3%率`
            ,sum(`支付价beat3-4%次数`)  / sum(`支付价抓取次数`)  `支付价beat3-4%率`
            ,sum(`支付价beat4-5%次数`)  / sum(`支付价抓取次数`)  `支付价beat4-5%率`
            ,sum(`支付价beat5-6%次数`)  / sum(`支付价抓取次数`)  `支付价beat5-6%率`
            ,sum(`支付价beat6-7%次数`)  / sum(`支付价抓取次数`)  `支付价beat6-7%率`
            ,sum(`支付价beat7-8%次数`)  / sum(`支付价抓取次数`)  `支付价beat7-8%率`
            ,sum(`支付价beat8%以上次数`) / sum(`支付价抓取次数`)  `支付价beat8%以上率`       
        from qc_price
        group by 1
    -- ) t group by 1
)

select t1.hotel_seq,t4.hotel_name,t1.hotel_grade,adr,t3.score,BizZones,t1.mdd,user_type1,room_night
      ,`beat率`
      ,`支付价beat0-3%率`
      ,`支付价beat3-4%率`
      ,`支付价beat4-5%率`
      ,`支付价beat5-6%率`
      ,`支付价beat6-7%率`
      ,`支付价beat7-8%率`
      ,`支付价beat8%以上率`  
from (
    select t1.hotel_seq,t1.hotel_grade,t1.mdd
        ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
            ,sum(room_night) room_night
            ,case when sum(init_gmv) / sum(room_night) < 400  then '1[0,400)'
                when   sum(init_gmv) / sum(room_night) >= 400 and  sum(init_gmv) / sum(room_night) < 800  then '2[400,800)'
                when   sum(init_gmv) / sum(room_night) >= 800 and  sum(init_gmv) / sum(room_night) < 1200  then '3[800,1200)'
                when   sum(init_gmv) / sum(room_night) >= 1200 and sum(init_gmv) / sum(room_night) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
            end adr
    from q_order t1 
    left join platform_new t2 on t1.order_date=t2.dt and t1.user_name=t2.user_pk
    group by 1,2,3,4
) t1 
left join hotle_score t3 on t1.hotel_seq=t3.obj_seq
left join hotel_grade t4 on t1.hotel_seq=t4.hotel_seq
left join qc_price_data t5 on t1.hotel_seq=t5.hotel_seq 

union all

select t1.hotel_seq,t4.hotel_name,t1.hotel_grade,adr,t3.score,BizZones,t1.mdd,'小红书-平台新业务新' as user_type1,room_night
      ,`beat率`
      ,`支付价beat0-3%率`
      ,`支付价beat3-4%率`
      ,`支付价beat4-5%率`
      ,`支付价beat5-6%率`
      ,`支付价beat6-7%率`
      ,`支付价beat7-8%率`
      ,`支付价beat8%以上率`  
from (
    select t1.hotel_seq,t1.hotel_grade,t1.mdd
        ,case
                when (t1.user_type = '新客' and t2.user_pk is not null) then '平台新业务新'
                when t1.user_type = '新客' then '平台老业务新'
                else '老客'
            end as user_type1
            ,sum(room_night) room_night
            ,case when sum(init_gmv) / sum(room_night) < 400  then '1[0,400)'
                when   sum(init_gmv) / sum(room_night) >= 400 and  sum(init_gmv) / sum(room_night) < 800  then '2[400,800)'
                when   sum(init_gmv) / sum(room_night) >= 800 and  sum(init_gmv) / sum(room_night) < 1200  then '3[800,1200)'
                when   sum(init_gmv) / sum(room_night) >= 1200 and sum(init_gmv) / sum(room_night) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
            end adr
    from q_order t1 
    left join (--- 筛选小红书平台新业务新订单
        select t1.dt as order_date
            ,t1.user_id
        from red_res t1 
        where user_type1 = '平台新业务新'
    ) t2 on  t1.order_date=t2.order_date and t1.user_id=t2.user_id
    where t2.user_id is not null
    group by 1,2,3,4
) t1 
left join hotle_score t3 on t1.hotel_seq=t3.obj_seq
left join hotel_grade t4 on t1.hotel_seq=t4.hotel_seq
left join qc_price_data t5 on t1.hotel_seq=t5.hotel_seq 
;


---- 分星级CR对比
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
,init_uv as(
    select  dt 
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
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_order as (----订单明细表表包含取消 
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
            ,case when hotel_grade in (4,5) then '高星'
                  when hotel_grade in (3) then '中星'
                  else '低星' end hotel_grade
            ,case when init_gmv / room_night < 400  then '1[0,400)'
                  when init_gmv / room_night >= 400 and init_gmv / room_night < 800  then '2[400,800)'
                  when init_gmv / room_night >= 800 and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' end adr
            ,a.country_name
            ,hotel_seq
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and terminal_channel_type = 'app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) --- 剔除当日取消单
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_no <> '103576132435'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date,1)
)


select t1.dt
     ,t1.hotel_grade
     ,order_no / uv cr
     ,uv
     ,order_no
from (
    select dt,hotel_grade,count(distinct user_id) uv
    from init_uv
    group by 1,2
)t1 left join (
    select order_date,hotel_grade,count(distinct order_no) order_no
    from q_order
    group by 1,2
)t2 on t1.dt=t2.order_date and t1.hotel_grade=t2.hotel_grade
group by 1,2
order by 1,2
;