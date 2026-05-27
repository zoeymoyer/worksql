--- 优惠先享漏斗数据 1.19上线，开开关关；2.7之后保持5%流量常开
--- 漏斗：LD页曝光、B页曝光、成单
--- 勾选：区分默勾最终勾选、默勾最终取消勾选、未默勾，最终勾选、未默勾，最终未勾选
--- 开启任务=最终勾选用户
--- 成功任务：用户完成先享任务，区分首单离店、次单7日内复购(非同酒店)、次单40日内离店
--- 失败任务：用户先享任务失败，区分首单取消拒单、次单7日内未复购（非同酒店）、次单40日内未离店
--- 进行中任务：用户尚未完成先享任务，区分首单未取消未离店、首单后未达7天、复购后未达40天
--- 扣款，对失败任务扣款，扣款成功任务数、扣款成功金额。扣款成功率 = 扣款成功任务数 / 失败任务数；追回率 = 扣款成功金额 / 失败任务需扣款金额
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
    select order_date,order_time
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
            ,checkout_date
            ,checkin_date,order_status
            ,hotel_seq
            ,get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount') as promotionAmount  --- 优惠先享金额
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
        and order_date >= '2026-02-07' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and ext_flag_map['use_promotion_enjoy_first'] is not null   --- 优惠先享
)

select order_date
        ,count(distinct order_no)  `优惠先享订单`
        ,sum(room_night) `优惠先享间夜` 
        ,sum(promotionAmount) `优惠先享金额`
        ,sum(init_gmv) `优惠先享订单gmv`
        ,sum(promotionAmount) / sum(init_gmv) `优惠先享补贴率`

from q_order_app
group by 1 order by 1
;

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
    select order_date,order_time
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
            ,checkout_date
            ,checkin_date,order_status
            ,hotel_seq
            ,get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount') as promotionAmount  --- 优惠先享金额
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
        and order_date >= '2026-02-08' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and ext_flag_map['use_promotion_enjoy_first'] is not null   --- 优惠先享
)
,q_order_app_info as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date,order_time
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
            ,checkout_date
            ,checkin_date,order_status
            ,hotel_seq
            ,get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount') as promotionAmount  --- 优惠先享金额
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
        and order_date >= '2026-02-07' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        -- and ext_flag_map['use_promotion_enjoy_first'] is not null   --- 优惠先享
)

select t1.order_date,`优惠先享订单`
    ,coalesce(`成功任务订单`,0) `成功任务订单`,coalesce(`失败任务订单`,0) `失败任务订单` 
    ,`优惠先享订单`- coalesce(`成功任务订单`,0) - coalesce(`失败任务订单`,0) as `进行中任务订单`
from (
    select order_date
        ,count(distinct order_no) `优惠先享订单`
    from q_order_app
    group by 1
) t1 
left join( --- 成功订单：首单离店且7日内复购订单离店且复购订单离店-预定在40天之内且不是同一酒店
    select t1.order_date
        ,count(distinct t1.order_no) `成功任务订单`
    from ( --- 优惠先享订单已离店订单
        select order_date,user_id,hotel_seq,order_status,order_time,order_no
        from q_order_app
        where order_status = 'CHECKED_OUT' 
    ) t1 
    left join (--- 复购订单离店
        select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
        from q_order_app_info 
        where order_status = 'CHECKED_OUT' 
    )t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
    where t1.hotel_seq != t2.hotel_seq 
        and datediff(t2.checkout_date, t2.order_date) <= 40
    group by 1 
) t2 on t1.order_date=t2.order_date
left join (--- 失败订单：首单取消拒单or首单未取消且7日内未复购or首单未取消且7日内复购条件不符合(复购订单取消拒单or复购订单同一酒店or复购订单预离时间不符)
    select order_date,count(distinct order_no) `失败任务订单`
    from (
        ---- 1、首单取消拒单
        select order_date,order_no
        from q_order_app
        where order_status in ('CANCELLED', 'REJECTED')

        union all 

        ---- 2、首单未取消单且满7日订单未复购
        select t1.order_date,t1.order_no
        from (--- 首单未取消拒单且不是7日内订单
            select order_date,user_id,hotel_seq,order_status,order_time,order_no
            from q_order_app
            where order_status not in ('CANCELLED', 'REJECTED') 
            and order_date <= date_sub(current_date, 8)
        )t1 
        left join (
            select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
            from q_order_app_info 
        )t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
        where t2.user_id is null  

        union all 

        ---- 3、首单未取消单且满7日订单复购条件不符合
        select t1.order_date,t1.order_no
        from (--- 首单未取消拒单且不是7日内订单
            select order_date,user_id,hotel_seq,order_status,order_time,order_no
            from q_order_app
            where order_status not in ('CANCELLED', 'REJECTED') 
            and order_date <= date_sub(current_date, 8)
        )t1 
        left join (
            select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
            from q_order_app_info 
        )t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
        where t2.user_id is not  null   
            and (
                t2.order_status in ('CANCELLED', 'REJECTED') 
              or (t2.order_status not in ('CANCELLED', 'REJECTED','CHECKED_OUT') and (t2.hotel_seq=t1.hotel_seq or datediff(t2.checkout_date, t2.order_date) > 40))
            )
    ) group by 1
) t3 on t1.order_date=t3.order_date
order by 1
;
-- left join (--- 进行中任务：首单未取消拒单离店且首单后未达7天or首单未取消拒单复购未离店
--     ---- 1、近7日内首单未离店且未复购订单
--     select t1.order_date,t1.order_no
--     from (
--         select order_date,user_id,hotel_seq,order_status,order_time,order_no
--         from q_order_app
--         where order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT') 
--             and order_date >= datediff(date_sub(current_date, 8))
--     )t1 
--     left join (
--         select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
--         from q_order_app_info 
--     )t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
--     where t2.user_id is null

--     union all 
    
--     ---- 2、近7日内首单未离店且复购订单未离店
--     select t1.order_date,t1.order_no
--     from (
--         select order_date,user_id,hotel_seq,order_status,order_time,order_no
--         from q_order_app
--         where order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT') 
--             and order_date >= datediff(date_sub(current_date, 8))
--     )t1 
--     left join (
--         select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
--         from q_order_app_info 
--         where order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT')
--     )t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
--     where t2.user_id is not null
-- )



select t1.order_date
    ,count(distinct t1.order_no) `优惠先享订单`
    --- 成功订单：首单离店且7日内复购订单离店且复购订单离店-预定在40天之内且不是同一酒店
    ,count(distinct case when t1.order_status = 'CHECKED_OUT' and t2.order_status = 'CHECKED_OUT' 
                           and t1.hotel_seq!=t2.hotel_seq and datediff(t2.checkout_date,t2.order_date) <= 40 then t1.order_no end) `成功任务订单`
    --- 失败订单：首单取消拒单or首单未取消且7日内未复购or首单未取消且7日内复购条件不符合(复购订单取消拒单or复购订单同一酒店or复购订单预离时间不符)
    ,count(distinct case when (t1.order_status in ('CANCELLED', 'REJECTED'))  
                        or (t1.order_status not in ('CANCELLED', 'REJECTED') and t2.user_id is null) 
                        or (t1.order_status not in ('CANCELLED', 'REJECTED') and t2.user_id is not null 
                             and(t2.order_status in ('CANCELLED', 'REJECTED') or t2.hotel_seq=t1.hotel_seq or datediff(t2.checkout_date, t2.order_date)) > 40)
                        then t1.order_no end) `失败任务订单`
    --- 进行中任务：首单未取消拒单离店且首单后未达7天or首单未取消拒单复购未离店
    ,count(distinct case when (t1.order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT') and datediff(date_sub(current_date, 1), t1.order_date) <= 7)
                            or(t1.order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT') and t2.order_status not in ('CANCELLED', 'REJECTED', 'CHECKED_OUT'))
                        then t1.order_no end) `任务进行订单`
from ( --- 优惠先享订单已离店订单
    select order_date,user_id,hotel_seq,order_status,order_time,order_no
    from q_order_app
) t1 
left join (
    select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
    from q_order_app_info 
)t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
group by 1
order by 1
;


--- 前端数据
select  concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) dt
    -- ,count(distinct case when key = 'ihotel/list/listPage/show/hotelCellShow' and get_json_object(value, '$.ext.ifBenefitfront') = '1' then user_name end) `L页曝光uv`
    ,count(distinct case when key = 'ihotel/Detail/PriceList/show/ifBenefitfrontBanner' then user_name end) `D页曝光uv`
    ,count(distinct case when key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' then user_name end) `B页曝光uv`
    ,count(distinct case when key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '1' then user_name end) `B页曝光uv默勾`
    ,count(distinct case when key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '2' then user_name end) `B页曝光uv未默勾`
    ,count(distinct case when key = 'ihotel/Booking/Access/click/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '1' then user_name end) `B页主动勾选uv`
    ,count(distinct case when key = 'ihotel/Booking/Access/click/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '2' then user_name end) `B页主动取消uv`

    -- ,count(distinct case when key = 'ihotel/booking/submitOrder/click/submitOrder' and get_json_object(value, '$.ext.ifselectBenefitfront') = '1'  then user_name end) `开启任务uv(最终勾选)`
    -- ,count(distinct case when key = 'ihotel/booking/submitOrder/click/submitOrder' and get_json_object(value, '$.ext.ifselectBenefitfront') = '1' 
    --         and key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '1' then user_name end) `默勾最终勾选uv`
    -- ,count(distinct case when key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '1' 
    --         and key = 'ihotel/booking/submitOrder/click/submitOrder' and get_json_object(value, '$.ext.ifselectBenefitfront') = '2' 
    --         then user_name end) `默勾最终未勾选uv`
    -- ,count(distinct case when key = 'ihotel/booking/submitOrder/click/submitOrder' and get_json_object(value, '$.ext.ifselectBenefitfront') != '1' 
    --         and key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') = '1' then user_name end) `未默勾最终勾选uv`
    -- ,count(distinct case when key = 'ihotel/Booking/Access/show/ifBenefitfrontButton' and get_json_object(value, '$.ext.ifChecked') != '1' 
    --         and key = 'ihotel/booking/submitOrder/click/submitOrder' and get_json_object(value, '$.ext.ifselectBenefitfront') = '2' 
    --         then user_name end) `未默勾最终未勾选uv`
from default.dw_qav_ihotel_track_info_di
where dt >= '20260207'  
and key in (
    'ihotel/list/listPage/show/hotelCellShow'   ---- L页酒店卡片曝光 ifBenefitfront=1 优惠先享
    ,'ihotel/Detail/PriceList/show/ifBenefitfrontBanner'   ----D页优惠先享飘条曝光
    ,'ihotel/Booking/Access/show/ifBenefitfrontButton'   ----B页优惠先享勾选模块曝光 get_json_object(value, '$.ext.ifChecked') = '1' 默认勾选
    ,'ihotel/Booking/Access/click/ifBenefitfrontButton'  ----B页优惠先享勾选模块点击 get_json_object(value, '$.ext.ifChecked') = '1' 点击勾选，=2取消勾选
    ,'ihotel/booking/submitOrder/click/submitOrder'   ---- 填单页点击提交订单 ifselectBenefitfront=1 勾选
) 
group by 1
order by 1
;


--优惠先享5%小流量阶段实验数据
with abt as ( 
    select  ab_exp_value as user_id
       ,case when ab_version in ('A') then 'A空白组85%' 
             when ab_version in ('B') then 'B实验组5%' 
             when ab_version in ('C') then 'C对照组5%' 
             when ab_version in ('D') then 'D对照组5%' 
             else 'null' end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt  >= '2026-02-08'  and dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
    group by 1,2
)
,q_order_info as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and t.ab_type is not null
)
,q_uv as (
    select 
        t.ab_type,dt
        ,count(distinct a.user_id) as `q_uv`
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id 
    where a.dt  >= '2026-02-08'  and a.dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
    group by 1,2
)
,q_order as (
    select 
        ab_type,order_date
        ,count(distinct user_id) as `q_生单用户`   
        ,count(order_no) as `q_订单量` 
        ,sum(room_night) as `q_间夜量` 
        ,sum(q_yj) as `q_收益额`
        ,sum(init_gmv) as `q_GMV`
        ,sum(coupon_substract_summary) as `q_券补贴额`
        ,sum(q_jf) as `q_积分补贴额`
        ,sum(case when use_promotion_enjoy_first is not null then promotionAmount end) as `q_优惠先享补贴额`
        ,count(case when use_promotion_enjoy_first is not null then order_no end) as `q_先享订单量` 
    from q_order_info
    group by 1,2
)
,q_order_reorder as (
    select 
        t1.ab_type,t1.order_date
        ,count(distinct t1.user_id) as `q_复购生单用户`   
        ,count(distinct t1.order_no) as `q_复购订单量` 
    from q_order_info t1 
    left join q_order_info t2 on t1.user_id=t2.user_id and t2.order_time > t1.order_time and datediff(t2.order_date, t1.order_date) <= 7 
    where t2.user_id is not null
    group by 1,2
)

select 
    a.ab_type as `实验分组`,
    sum(b.`q_uv`) as uv,
    sum(a.`q_生单用户`) as `q_生单用户`,
    sum(a.`q_订单量`) as `订单量`,
    sum(a.`q_间夜量`) as `间夜量`,
    round(sum(a.`q_收益额`),1) as `收益额`,
    round(sum(a.`q_GMV`),1) as `GMV`,
    round(sum(a.`q_券补贴额`),1) as `券补贴额`,
    round(sum(a.`q_积分补贴额`),1) as `积分补额`,
    round(sum(a.`q_优惠先享补贴额`),1) as `优惠先享补贴额`,
    concat(round(sum(a.`q_生单用户`)/sum(b.`q_uv`)*100,2),'%') as U2O,
    concat(round(sum(a.`q_订单量`)/sum(b.`q_uv`)*100,2),'%') as CR,
    concat(round(sum(a.`q_收益额`)/sum(a.`q_GMV`)*100,2),'%') as `佣金率`,
    concat(round(sum(a.`q_券补贴额`)/sum(a.`q_GMV`)*100,2),'%') as `券补贴率`,
    concat(round(sum(a.`q_优惠先享补贴额`)/sum(a.`q_GMV`)*100,2),'%') as `优惠先享补贴率`,
    round(sum(a.`q_间夜量`)/sum(b.`q_uv`),4) as `单UV间夜`,
    round(sum(a.`q_收益额`)/sum(b.`q_uv`),4) as `单UV收益`,
    round(sum(a.`q_优惠先享补贴额`)/sum(b.`q_uv`),4) as `单UV先享`,
    sum(a.`q_先享订单量`) as `先享订单量`,
    concat(round(sum(a.`q_先享订单量`)/sum(a.`q_订单量`)*100,2),'%') as `先享订单占比`,
    sum(c.`q_复购生单用户`) as `q_复购生单用户`,
    sum(c.`q_复购订单量`) as `复购订单量`
from q_order a
left join q_uv b on a.ab_type = b.ab_type and a.order_date=b.dt
left join q_order_reorder c on a.ab_type = c.ab_type and a.order_date=c.order_date
group by 1
order by a.ab_type asc
;


---- 订单优惠先享数据
with q_order_app as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and ext_flag_map['use_promotion_enjoy_first'] is not null   --- 优惠先享
)

select count(distinct order_no) as ord
        ,count(case when final_commission_after - promotionAmount < 0  then order_no end) as fy_ord
        ,count(case when final_commission_after - promotionAmount < 0  then order_no end) / count(distinct order_no) as rate   
        
        ,sum(final_commission_after)
        ,sum(promotionAmount)
from q_order_app a
;



----大盘预定口径 7日复购率
with q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,a.user_id,init_gmv,order_no,room_night,order_time
            
    from default.mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
select t1.order_date
        ,count(distinct t1.user_id) ord_uv
        ,count(distinct t1.order_no) order_no
        ,count(distinct t2.user_id) re_ord_uv
        ,count(distinct t2.order_no) re_order_no
        ,count(distinct t2.user_id) / count(distinct t1.user_id) re_rate
        ,count(distinct t1.order_no) / count(distinct t1.user_id) per_ord
        ,count(distinct t2.order_no) / count(distinct t2.user_id) re_per_ord

from q_order_app t1 
left join q_order_app t2  on t1.user_id=t2.user_id and t2.order_time > t1.order_time and datediff(t2.order_date, t1.order_date) <= 7 
group by 1
order by 1
;




--优惠先享5%小流量阶段实验数据 最新0320
with order_90 as (
    select user_name,
            count(order_no) as order_nos_90,
            sum(room_night) as room_nights_90
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status not in ('CANCELLED','REJECTED')
      and order_date >= date_sub(current_date, 90)
      and order_date <= date_sub(current_date, 1)
    group by 1
)
,no_user as (--- 大单用户
    select user_name
    from order_90
    where order_nos_90 >= 10
)
,abt as ( 
    select  ab_exp_value as user_id,dt
       ,case when ab_version in ('A') then 'A空白组85%' 
             when ab_version in ('B') then 'B实验组5%' 
             when ab_version in ('C') then 'C对照组5%' 
             when ab_version in ('D') then 'D对照组5%' 
             else 'null' end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt  >= '2026-02-08'  and dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
    group by 1,2,3
)
,q_order_info as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on lower(a.user_id) = lower(t.user_id)  and a.order_date=t.dt
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and t.ab_type is not null
        and user_name not in (select user_name from no_user)  -- 剔除大单用户
)
,q_uv as (
    select 
        t.ab_type,a.dt
        ,count(distinct a.user_id) as `q_uv`
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on lower(a.user_id) = lower(t.user_id)  and a.dt=t.dt
    where a.dt  >= '2026-02-08'  and a.dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
        and a.user_name not in (select user_name from no_user)  -- 剔除大单用户
    group by 1,2
)
,q_order as (
    select 
        ab_type,order_date
        ,count(distinct user_id) as `q_生单用户`   
        ,count(order_no) as `q_订单量` 
        ,sum(room_night) as `q_间夜量` 
        ,sum(ld_yj) as `q_收益额`
        ,sum(init_gmv) as `q_GMV`
        ,sum(coupon_substract_summary) as `q_券补贴额`
        ,sum(q_jf) as `q_积分补贴额`
        ,sum(case when use_promotion_enjoy_first is not null then promotionAmount end) as `q_优惠先享补贴额`
        ,count(case when use_promotion_enjoy_first is not null then order_no end) as `q_先享订单量` 
    from q_order_info
    group by 1,2
)
,q_order_reorder as (
    select 
        t1.ab_type,t1.order_date
        ,count(distinct t1.user_id) as `复购生单用户`   
        ,count(distinct t2.order_no) as `复购订单量` 
        ,sum(t2.ld_yj) as `复购收益`
        ,sum(t2.room_night) as `复购间夜`
    from q_order_info t1 
    left join q_order_info t2 on t1.user_id=t2.user_id and t2.order_time > t1.order_time and datediff(t2.order_date, t1.order_date) <= 7 
    where t2.user_id is not null 
          ---- 限定7天之前的复购（满7日）
          and t1.order_date <= date_sub(current_date, 8) 
    group by 1,2
)

select a.order_date,
    a.ab_type as `实验分组`,
    b.`q_uv` as uv,
    a.`q_生单用户` as `生单用户`,
    a.`q_订单量` as `订单量`,
    a.`q_间夜量` as `间夜量`,
    round(a.`q_收益额`) as `收益额`,
    round(a.`q_GMV`) as `GMV`,
    round(a.`q_券补贴额`,1) as `券补贴额`,
    round(a.`q_积分补贴额`,1) as `积分补额`,
    round(a.`q_优惠先享补贴额`,1) as `优惠先享补贴额`,
    concat(round((a.`q_生单用户`/b.`q_uv`)*100,2),'%') as U2O,
    concat(round((a.`q_订单量`/b.`q_uv`)*100,2),'%') as CR,
    concat(round((a.`q_收益额`/a.`q_GMV`)*100,2),'%') as `佣金率`,
    concat(round((a.`q_券补贴额`/a.`q_GMV`)*100,2),'%') as `券补贴率`,
    concat(round((a.`q_优惠先享补贴额`/a.`q_GMV`)*100,2),'%') as `优惠先享补贴率`,
    round(a.`q_间夜量`/b.`q_uv`,4) as `单UV间夜`,
    round(a.`q_收益额`/b.`q_uv`,4) as `单UV收益`,
    round(a.`q_优惠先享补贴额`/b.`q_uv`,4) as `单UV先享`,
    a.`q_先享订单量` as `先享订单量`,
    concat(round((a.`q_先享订单量`/a.`q_订单量`)*100,2),'%') as `先享订单占比`,
    c.`复购生单用户` as `复购生单用户`,
    c.`复购订单量` as `复购订单量`,
    c.`复购收益` as `复购收益`,
    c.`复购间夜` as `复购间夜`
from q_order a
left join q_uv b on a.ab_type = b.ab_type and a.order_date=b.dt
left join q_order_reorder c on a.ab_type = c.ab_type and a.order_date=c.order_date
order by a.ab_type asc,a.order_date asc
;



--优惠先享5%小流量阶段实验数据 最新0320
with abt as ( 
    select  ab_exp_value as user_id,dt
       ,case when ab_version in ('A') then 'A空白组85%' 
             when ab_version in ('B') then 'B实验组5%' 
             when ab_version in ('C') then 'C对照组5%' 
             when ab_version in ('D') then 'D对照组5%' 
             else 'null' end as ab_type
    from ihotel_default.dw_ihotel_abtest_index_di_v2
    where dt  >= '2026-02-08'  and dt <= date_sub(current_date, 1)
        and type = 'flow'
        and user_id_type = 'user_id' 
        and ab_exp_id = '251205_ho_gj_youhuixianxiangTEST'
    group by 1,2,3
)
,q_order_info as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
            ,t.ab_type
    from default.mdw_order_v3_international a 
    left join abt t on a.user_id = t.user_id  and a.order_date=t.dt
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        and t.ab_type is not null
)
,q_order_app as (
    select order_date,a.user_id,order_no,order_time
            ,room_night,init_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0) end q_yj
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else final_commission_after+coalesce(ext_plat_certificate,0) end ld_yj
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0) q_jf --q_积分补贴额
            ,ext_flag_map['use_promotion_enjoy_first'] use_promotion_enjoy_first -- 优惠先享
            ,coalesce(get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount'), 0) promotionAmount -- 优惠先享补贴金额
    from default.mdw_order_v3_international a 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid = '1'
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and order_date >= '2026-02-08'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,q_uv as (
    select 
        t.ab_type,a.dt
        ,count(distinct a.user_id) as `q_uv`
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
    left join abt t on a.user_id = t.user_id  and a.dt=t.dt
    where a.dt  >= '2026-02-08'  and a.dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        and t.ab_type is not null
    group by 1,2
)
,q_order as (
    select 
        ab_type,order_date
        ,count(distinct user_id) as `q_生单用户`   
        ,count(order_no) as `q_订单量` 
        ,sum(room_night) as `q_间夜量` 
        ,sum(ld_yj) as `q_收益额`
        ,sum(init_gmv) as `q_GMV`
        ,sum(coupon_substract_summary) as `q_券补贴额`
        ,sum(q_jf) as `q_积分补贴额`
        ,sum(case when use_promotion_enjoy_first is not null then promotionAmount end) as `q_优惠先享补贴额`
        ,count(case when use_promotion_enjoy_first is not null then order_no end) as `q_先享订单量` 
    from q_order_info
    group by 1,2
)
,q_order_reorder as (
    select 
        t1.ab_type,t1.order_date
        ,count(distinct t1.user_id) as `复购生单用户`   
        ,count(distinct t2.order_no) as `复购订单量` 
        ,sum(t2.ld_yj) as `复购收益`
        ,sum(t2.room_night) as `复购间夜`
    from q_order_info t1 
    left join q_order_app t2 on t1.user_id=t2.user_id and t2.order_time > t1.order_time and datediff(t2.order_date, t1.order_date) <= 7 
    where t2.user_id is not null 
          ---- 限定7天之前的复购（满7日）
          and t1.order_date <= date_sub(current_date, 8) 
    group by 1,2
)

select a.order_date,
    a.ab_type as `实验分组`,
    b.`q_uv` as uv,
    a.`q_生单用户` as `生单用户`,
    a.`q_订单量` as `订单量`,
    a.`q_间夜量` as `间夜量`,
    round(a.`q_收益额`) as `收益额`,
    round(a.`q_GMV`) as `GMV`,
    round(a.`q_券补贴额`,1) as `券补贴额`,
    round(a.`q_积分补贴额`,1) as `积分补额`,
    round(a.`q_优惠先享补贴额`,1) as `优惠先享补贴额`,
    concat(round((a.`q_生单用户`/b.`q_uv`)*100,2),'%') as U2O,
    concat(round((a.`q_订单量`/b.`q_uv`)*100,2),'%') as CR,
    concat(round((a.`q_收益额`/a.`q_GMV`)*100,2),'%') as `佣金率`,
    concat(round((a.`q_券补贴额`/a.`q_GMV`)*100,2),'%') as `券补贴率`,
    concat(round((a.`q_优惠先享补贴额`/a.`q_GMV`)*100,2),'%') as `优惠先享补贴率`,
    round(a.`q_间夜量`/b.`q_uv`,4) as `单UV间夜`,
    round(a.`q_收益额`/b.`q_uv`,4) as `单UV收益`,
    round(a.`q_优惠先享补贴额`/b.`q_uv`,4) as `单UV先享`,
    a.`q_先享订单量` as `先享订单量`,
    concat(round((a.`q_先享订单量`/a.`q_订单量`)*100,2),'%') as `先享订单占比`,
    c.`复购生单用户` as `复购生单用户`,
    c.`复购订单量` as `复购订单量`,
    c.`复购收益` as `复购收益`,
    c.`复购间夜` as `复购间夜`
from q_order a
left join q_uv b on a.ab_type = b.ab_type and a.order_date=b.dt
left join q_order_reorder c on a.ab_type = c.ab_type and a.order_date=c.order_date
order by a.ab_type asc,a.order_date asc
;



--- 20260507
with q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date,order_time
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
            ,checkout_date
            ,checkin_date,order_status
            ,hotel_seq
            ,get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount') as promotionAmount  --- 优惠先享金额
    from default.mdw_order_v3_international a 
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
        and ext_flag_map['use_promotion_enjoy_first'] is not null   --- 优惠先享
)
,q_order_app_info as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date,order_time
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
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
            ,checkout_date
            ,checkin_date,order_status
            ,hotel_seq
            ,get_json_object(ext_flag_map['promotion_enjoy_first_info'], '$.promotionAmount') as promotionAmount  --- 优惠先享金额
    from default.mdw_order_v3_international a 
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

select t1.order_date,`优惠先享订单`
    ,coalesce(`成功任务订单`,0) `成功任务订单`,`成功任务订单补贴金额`,`优惠先享订单补贴金额`
from (
    select order_date
        ,count(distinct order_no) `优惠先享订单`
        ,sum(promotionAmount)  `优惠先享订单补贴金额`
    from q_order_app
    group by 1
) t1 
left join( --- 成功订单：首单离店且7日内复购订单离店且复购订单离店-预定在40天之内且不是同一酒店
    select t1.order_date
            ,count(distinct t2.order_no) `成功任务订单`
            ,sum(t2.promotionAmount) `成功任务订单补贴金额`
    from (
        select t1.order_date
            ,t1.order_no
        from ( --- 优惠先享订单已离店订单
            select order_date,user_id,hotel_seq,order_status,order_time,order_no,promotionAmount
            from q_order_app
            where order_status = 'CHECKED_OUT' 
        ) t1 
        left join (--- 复购订单离店
            select order_date,order_time,checkout_date,order_no,user_id,order_status,hotel_seq
            from q_order_app_info 
            where order_status = 'CHECKED_OUT' 
        )t2 on t1.user_id=t2.user_id and datediff(t2.order_date, t1.order_date) <= 7 and t2.order_time > t1.order_time 
        where t1.hotel_seq != t2.hotel_seq 
            and datediff(t2.checkout_date, t2.order_date) <= 40 and t2.user_id is not null
        group by 1,2
    ) t1 
    join q_order_app t2 on t1.order_no = t2.order_no 
    group by 1  
) t2 on t1.order_date=t2.order_date
order by 1
;