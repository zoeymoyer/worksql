
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
,q_order as ( --- Q侧订单明细打标
    select a.order_date,substr(order_date, 1, 7) mth
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
          ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          -- 2. 价格带逻辑
          ,case when init_gmv / nullif(room_night, 0) < 400   then '1[0,400)'
                when init_gmv / nullif(room_night, 0) >= 400  and init_gmv / nullif(room_night, 0) < 800   then '2[400,800)'
                when init_gmv / nullif(room_night, 0) >= 800  and init_gmv / nullif(room_night, 0) < 1200  then '3[800,1200)'
                when init_gmv / nullif(room_night, 0) >= 1200 and init_gmv / nullif(room_night, 0) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
          end as adr_type 
          -- 3. Q侧货源判定逻辑
          ,case when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'C2Q直采' 
                when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'Agoda' then 'C2Q-Agoda'
                when qta_supplier_id in ('1615667','800000164') then 'C2Q-代理'
                when qta_supplier_id not in ('1615667','800000164') and wrapper_id not in ('hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s','hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'Q代理'
                else 'Q-ABE' 
          end as supplier_raw
          -- 4. 不可取消判定
          ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end as is_non_ref 
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
        -- 5. 单晚多晚
        ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
        -- 6. 日期分类：holiday、workday、weekend
        ,dd.date_type,dd.holiday_name
        ,a.user_id,a.order_no, a.init_gmv, a.room_night,mobile_platform
        ,case when ext_flag_map['ord_children_num'] > 0 then '亲子' else '非亲子' end as is_child
        ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
        ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补
        ,coalesce(get_json_object(extendinfomap,'$.platform_amount'),0) * room_night  plat_amt  --- 平台补
        ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0) * room_night + coalesce(cashbackmap['framework_amount'],0)  xyb_amt  --- 协议补
        ,case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
                then coalesce(follow_price_amount,0) end zjb_amt --- 追价补
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join (select order_no, max(purchase_order_no) as purchase_order_no from ihotel_default.dw_purchase_order_info_v3 where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') group by order_no) p on a.order_no = p.order_no
    left join (select distinct partner_order_no, extend_info['vendor_name'] as vendor_name from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da where dt = cast(date_sub(current_date, 1) as string)) c on p.purchase_order_no = c.partner_order_no
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.order_date = dd.date
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
        and province_name in ('澳门','香港') -- 只分析港澳订单
)
,user_profile as (
    select  user_id
            ,user_name
            ,gender            -- 性别
            ,city_name         -- 常驻地
            ,prov_name
            ,city_level
            ,birth_year_month
            ,level_desc        -- 大众/白银/黄金/铂金/钻石
    from pub.dim_user_profile_nd
)
,search_child as (
    select dt,user_id
    from (
        select  user_id,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
        from default.dw_user_app_search_di_v3
        where dt >= '20250101' and dt <= '20260531'
            and device_id is not null
            and device_id <> ''
            and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
            and business_type = 'hotel'
            and (   --- 亲子标签筛选
                    (guestinfos['child_num'] is not null and guestinfos['child_num'] > 0) 
                    or (query like '%童%' or query like '%孩%' or query like '%婴%' or query like '%加床%'  or query like '%亲子%')
                )
        group by 1,2
        union all
        select user_id ,dt
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
        where dt <= '2026-05-31' and dt >= '2025-01-01'        
            and guestinfos['child_num'] > 0
        group by 1,2
    ) group by 1,2
)
,shanglv_order as (--- 商旅  开票情况
    select order_no
        ,case when quality_type in ('1', '2') then  'Y' else 'N' end is_bus_gov
    from fuwu.dwd_xcd_htl_complete_di
    where is_international = 1
    and dt >= '2024-01-01'
    group by 1,2
)
,order_result as (
    select *
           ,case when age <= 18 then '1(0,18]'
                 when age >= 19 and age <= 24 then '2[19,24]'
                 when age >= 25 and age <= 30 then '3[25,30]'
                 when age >= 31 and age <= 35 then '4[31,35]'
                 when age >= 36 and age <= 40 then '5[36,40]'
                 when age >= 41 and age <= 45 then '6[41,45]'
                 when age >= 46 and age <= 50 then '7[46,50]'
                 when age > 50 then '8[51+)'
            else '未知' end as age_level  -- 年龄段
    from (
        select o.order_no
            ,o.user_id
            ,o.user_name
            ,o.order_date
            ,o.mth
            ,o.room_night
            ,new_mdd   --- 日韩泰港澳海长其他
            ,mdd       --- 晨报目的地
            ,o.user_type --- 用户类型
            ,o.linqi_type --- 临期订
            ,o.per_type   --- 临期订分布
            ,o.is_more_roomnight --- 单多晚
            ,adr_type  --- ADR分布
            ,supplier_raw  --- 货源分布
            ,case when o.is_non_ref = 'Y' then '不可取消' else '可取消' end as is_non_ref   --- 不可取消
            ,o.mobile_platform  --- 手机平台
            ,gender --- 性别
            ,coalesce(u.city_level, '未知')  city_level   --- 城市等级
            ,level_desc  --- 用户等级
            ,birth_year_month --- 出生年月
            ,case when birth_year_month is null then '未知' else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int) end as age  --- 年龄
            ,case when (s.user_id is not null or o.is_child ='亲子') then '亲子' else '非亲子' end as is_child  --- 亲子
            ,case when is_bus_gov = 'Y' then '商旅' else '非商旅' end as is_bus_gov  --- 商旅
        from q_order o
        left join user_profile u on u.user_id = o.user_id
        left join search_child s on s.user_id = o.user_id and s.dt=o.order_date
        left join shanglv_order sh on sh.order_no = o.order_no
    )t
)

,monthly_total as (--- 计算每个月的总订单量和总间夜量，用于后续计算占比
    select  mth
           ,count(distinct order_no) as total_order_cnt
           ,sum(room_night) as total_room_night
    from q_order
    group by mth
)
,dimension_unpivot as (
    --- 1. 基础画像侧
    select mth, order_no, room_night, '年龄段' as label_name, age_level as label_value from order_result union all
    select mth, order_no, room_night, '性别' as label_name, gender as label_value from order_result union all
    select mth, order_no, room_night, '城市等级' as label_name, city_level as label_value from order_result union all
    select mth, order_no, room_night, '用户等级' as label_name, level_desc as label_value from order_result union all
    select mth, order_no, room_night, '手机平台' as label_name, mobile_platform as label_value from order_result union all
    select mth, order_no, room_night, '亲子标签' as label_name, is_child as label_value from order_result union all
    select mth, order_no, room_night, '商旅标签' as label_name, is_bus_gov as label_value from order_result union all
    
    --- 2. 订单分布侧
    select mth, order_no, room_night, '海长目的地' as label_name, new_mdd as label_value from order_result union all
    select mth, order_no, room_night, '目的地' as label_name, mdd as label_value from order_result union all
    select mth, order_no, room_night, '用户类型' as label_name, user_type as label_value from order_result union all
    select mth, order_no, room_night, '单多晚' as label_name, is_more_roomnight as label_value from order_result union all
    select mth, order_no, room_night, 'ADR分布' as label_name, adr_type as label_value from order_result union all
    select mth, order_no, room_night, '货源分布' as label_name, supplier_raw as label_value from order_result union all
    select mth, order_no, room_night, '不可取消分布' as label_name, is_non_ref as label_value from order_result union all
    select mth, order_no, room_night, 'linqi_type' as label_name, linqi_type as label_value from order_result union all
    select mth, order_no, room_night, 'per_type' as label_name, per_type as label_value from order_result
)
,tag_result as ( --- 分月分标签占比
    select  a.mth
            ,a.label_name
            ,a.label_value
            ,count(distinct a.order_no) as order_cnt
            ,sum(a.room_night) as room_night
    from dimension_unpivot a
    group by 1,2,3
)
select  t.mth 
       ,t.label_name 
       ,coalesce(t.label_value, '未知')  label_value
       ,order_cnt 
       ,room_night 
       ,m.total_order_cnt 
       ,m.total_room_night 
       --- 计算占比 (保留4位小数以百分比形式展现)
       ,round(order_cnt / m.total_order_cnt, 4) as orders_ratio
       ,round(room_night / m.total_room_night, 4) as room_night_ratio
from tag_result t
join monthly_total m on t.mth = m.mth
order by mth desc, label_name asc, order_cnt desc;