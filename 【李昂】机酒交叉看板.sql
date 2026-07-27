with user_type as (
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
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_order_app as (----订单明细表
    select order_date,order_time
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,user_name
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
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after_new+coalesce(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
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
,inter_flight as (
    select  substr(create_time, 1, 10) as dt
            ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
                    when s_arrcityname in ('香港','澳门') then s_arrcityname
                    when e.area in ('欧洲','亚太','美洲') then e.area
                    else '其他'
                end as s_arrcountryname
            ,case when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内'
                    and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '1-出境'
                    when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                    and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '2-海外飞海外'
                    when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                    and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内' then '3-入境'
                    else '5-其他'
                end as flight_type
            ,o.o_qunarusername as user_name
            ,biz_order_no
            ,min(substr(create_time, 1, 10)) as min_pay_time
    from f_fuwu.dw_fact_inter_order_wide o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on o.s_arrcountryname = e.country_name
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
        -- and substr(create_time, 1, 10) >= '2026-01-01'
        -- and substr(create_time, 1, 10) <= date_sub(current_date, 16) -- 当天及往前推15天内的机票用户T-14~T
        and ticket_time is not null -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao' -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    group by 1,2,3,4,5
)

select t1.dt
       ,if(grouping(t1.flight_type)=1,'ALL',t1.flight_type) flight_type
       ,if(grouping(t1.s_arrcountryname)=1,'ALL',t1.s_arrcountryname)  mdd
       ,count(distinct t1.user_name) as flight_uv
       ,count(distinct t1.biz_order_no) as flight_orders
       ,count(distinct t2.user_name) as ihotel_act_uv
       ,count(distinct case when t2.user_type = '新客' then t2.user_name end) as ihotel_act_new_uv
       ,count(distinct t3.user_name) as ihotel_ord_uv
       ,count(distinct t3.order_no) as ihotel_orders
       ,sum(room_night) room_night
       ,sum(final_commission_after) final_commission_after
       ,sum(coupon_substract_summary) coupon_substract_summary
       ,sum(init_gmv) init_gmv

       ,count(distinct case when t3.user_type = '新客' then t3.user_name end) as ihotel_ord_uv_nu
       ,count(distinct case when t3.user_type = '新客' then t3.order_no end) as ihotel_orders_nu
       ,sum(case when t3.user_type = '新客' then t3.room_night else 0 end) room_night_nu
       ,sum(case when t3.user_type = '新客' then t3.final_commission_after else 0 end) final_commission_after_nu
       ,sum(case when t3.user_type = '新客' then t3.coupon_substract_summary else 0 end) coupon_substract_summary_nu
       ,sum(case when t3.user_type = '新客' then t3.init_gmv else 0 end) init_gmv_nu
from inter_flight t1 
left join uv t2 on t1.user_name=t2.user_name and t1.dt=t2.dt and t1.s_arrcountryname=t2.mdd
left join q_order_app t3 on t1.user_name=t3.user_name and t1.dt=t3.order_date and t1.s_arrcountryname=t3.mdd and order_time >= min_pay_time
group by  
grouping sets(
    (t1.dt),
    (t1.dt, t1.flight_type),
    (t1.dt, t1.s_arrcountryname),
    (t1.dt, t1.flight_type, t1.s_arrcountryname)
)






with shanglv_order as (--- 商旅  开票情况
    select order_no 
    from fuwu.dwd_xcd_htl_complete_di
    where is_international = 1
    and dt >= '2025-01-01' and quality_type in ('1', '2')
    group by 1
)
,q_order_app as (----订单明细表 app
    select substr(order_date, 1, 7) mth
           ,a.order_no,a.room_night
           ,case when c.order_no is not null then '商旅' else '非商旅' end as label_value
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join shanglv_order c on a.order_no=c.order_no
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)
select mth as "归属月份"
      ,label_name as "分析维度"
      ,label_value as "属性值"
      ,"订单量"
      ,"间夜量"
      ,"当月总订单量"
      ,"当月总间夜量"
      ,"订单量" / "当月总订单量" as "订单量占比"
      ,"间夜量" / "当月总间夜量" as "间夜量占比"
      --- 1. 新增：分析维度全局排序字段
      ,case label_name 
            --- 基础画像侧
            when '年龄段' then 1
            when '性别' then 2
            when '城市等级' then 3
            when '用户等级' then 4
            when '手机平台' then 5
            when '亲子标签' then 6
            when '商旅标签' then 7
            --- 订单分布侧
            when '海长目的地' then 8
            when '目的地' then 9
            when '用户类型' then 10
            when 'ADR分布' then 11
            when '货源分布' then 12
            when '临期分布' then 13
            when '提前订分布' then 14
            when '单多晚' then 15
            when '不可取消分布' then 16
            else 99 
       end as "维度排序"
      --- 2. 新增：维值内部属性排序字段
      ,case 
            --- 年龄段
            when label_value = '1(0,18]' then 1
            when label_value = '2[19,24]' then 2
            when label_value = '3[25,30]' then 3
            when label_value = '3[25,30]' then 3
            when label_value = '4[31,35]' then 4
            when label_value = '5[36,40]' then 5
            when label_value = '6[41,45]' then 6
            when label_value = '7[46,50]' then 7
            when label_value = '8[51+)' then 8
            when label_value = '未知' then 9
            --- 性别
            when label_value = '男' then 1
            when label_value = '女' then 2
            when label_value = '未知' then 3
            --- 城市等级
            when label_value = '一线' then 1
            when label_value = '新一线' then 2
            when label_value = '二线' then 3
            when label_value = '三线' then 4
            when label_value = '四线' then 5
            when label_value = '五线' then 6
            when label_value = '未知' then 7
            --- 用户等级
            when label_value = '大众' then 1
            when label_value = '白银' then 2
            when label_value = '黄金' then 3
            when label_value = '铂金' then 4
            when label_value = '钻石' then 5
            --- 手机平台
            when label_value = 'iPhone' then 1
            when label_value = 'AndroidPhone' then 2
            when label_value = 'Harmony' then 3
            --- 业务标签二元分类
            when label_value = '亲子' then 1
            when label_value = '非亲子' then 2
            when label_value = '商旅' then 1
            when label_value = '非商旅' then 2
            --- 海长目的地
            when label_value = '港澳' then 1
            when label_value = '日本' then 2
            when label_value = '韩国' then 3
            when label_value = '泰国' then 4
            when label_value = '海长' then 5
            when label_value = '其他' then 6
            --- 目的地
            when label_value = '香港' then 1
            when label_value = '澳门' then 2
            when label_value = '日本' then 3
            when label_value = '韩国' then 4
            when label_value = '泰国' then 5
            when label_value = '马来西亚' then 6
            when label_value = '新加坡' then 7
            when label_value = '越南' then 8
            when label_value = '亚太' then 9
            when label_value = '俄罗斯' then 10
            when label_value = '美国' then 11
            when label_value = '美洲' then 12
            when label_value = '欧洲' then 13
            when label_value = '印度尼西亚' then 14
            when label_value = '其他' then 15
            --- 用户类型
            when label_value = '新客' then 1
            when label_value = '老客' then 2
            --- ADR分布
            when label_value = '1[0,400)' then 1
            when label_value = '2[400,800)' then 2
            when label_value = '3[800,1200)' then 3
            when label_value = '4[1200,1600)' then 4
            when label_value = '5[1600+]' then 5
            --- 货源分布
            when label_value = 'C2Q直采' then 1
            when label_value = 'Q代理' then 2
            when label_value = 'C2Q-代理' then 3
            when label_value = 'C2Q-Agoda' then 4
            when label_value = 'Q-ABE' then 5
            --- 临期分布
            when label_value = '临期订' then 1
            when label_value = '非临期订' then 2
            --- 提前订分布
            when label_value = '凌晨订&当天订' then 1
            when label_value = '提前订1-3天' then 2
            when label_value = '提前订4-7天' then 3
            when label_value = '提前订8-14天' then 4
            when label_value = '提前订15-30天' then 5
            when label_value = '提前订31+' then 6
            --- 单多晚
            when label_value = '单晚' then 1
            when label_value = '多晚' then 2
            --- 可不可取消
            when label_value = '不可取消' then 1
            when label_value = '可取消' then 2
            else 99 
       end as "属性排序"
from (
    select mth 
        ,label_name 
        ,label_value 
        
        ,max(order_cnt) as "订单量"
        ,max(room_night) as "间夜量"
        ,max(total_order_cnt) as "当月总订单量"
        ,max(total_room_night) as "当月总间夜量"
    from ihotel_default.ads_intl_hotel_user_order_profile_mth_di
    group by 1,2,3
    union all    

    select mth
        ,'商旅标签' label_name
        ,label_value
        ,count(distinct order_no) as "订单量"
        ,sum(room_night) as "间夜量"
        ,sum(count(distinct order_no)) over(partition by mth) as "当月总订单量"
        ,sum(sum(room_night)) over(partition by mth) as "当月总间夜量"
    from q_order_app
    group by 1,3
)
order by mth desc
      ,"维度排序" asc      --- 优先按大维度逻辑顺序排列
      ,"属性排序" asc      --- 其次按设定的业务逻辑顺序排列
;
        

---- 20260701最新看板正交叉
with user_type as (--- 判定新老客
    select user_name
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (---- 国酒分日去重活跃用户
    select dt
        ,user_name
        ,case when fromforlog in ('4104','4106')  then '二屏内容贴' 
                    when fromforlog in ('131','913','914')  then 'App首页宫格-海外酒店' 
                    when fromforlog in ('96','795','671','1096')  then '大搜相关' 
                    when fromforlog='352' then '再次预订' 
                    when fromforlog='422' then '酒店详情推荐模块-酒店单品' 
                    when fromforlog='200000081' then '二屏商卡' 
                    when fromforlog='200000083' then '市场活动去使用' 
                    when fromforlog='200000105' then '天天领券任务' 
                    when fromforlog='200000121' then '答题领积分任务'  
                    when fromforlog='200000118' then '国酒活动去使用' 
                    when fromforlog='200000119' then '机票实时短信' 
                    when fromforlog='200000120' then '带参数push' 
                    when fromforlog='200000122' then '国酒大搜落地页商卡' 
                    when fromforlog='200000123' then '带参数短信' 
                    when fromforlog='200000124' then '机票浏览实时短信'
                    when fromforlog='200000125' then '机票浏览实时push'
  
                    -- when fromforlog='671' then '大搜落地页-酒店tab' 
                    -- when fromforlog='96' then '大搜' 
                    -- when fromforlog='913' then 'App首页宫格-酒店频道-海外酒店tab' 
                    -- when fromforlog='914' then 'App首页-海外酒店' 
                    when fromforlog='4604' then '国际酒店H页快筛标签' 
                    when fromforlog='824' then '收藏跳转到酒店详情页' 

                    when fromforlog='4621' then '机票支付完成页' 
                    when fromforlog='4622' then '机票订单详情页' 
                    when fromforlog='4623' then '机票航班动态页' 
                    when fromforlog='4624' then '公共行程页' 
                    when fromforlog='4625' then '公共订单列表页' 
                    when fromforlog='4626' then '公共我的页面弹窗' 
                    when fromforlog='4627' then '公共我的页面浮标' 
                    else '其他'
        end as fromforlog_type
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3
)
,q_order_app as (---- 国酒分日去重下单用户
    select order_date as dt
        ,user_name,room_night,order_no
    from default.mdw_order_v3_international a
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
,q_channel_uv as (--- Q分渠道明细
    select dt,channel,user_name
    from ihotel_default.dwd_flow_ug_channel_di
    where dt >= '2025-01-01' and dt <= '2026-06-30'
    group by 1,2,3
)
,inter_flight as (---- 国际机票下单用户
    select substr(create_time, 1, 10) as dt
        ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
              when s_arrcityname in ('香港','澳门') then s_arrcityname
              when e.area in ('欧洲','亚太','美洲') then e.area
              else '其他' end as mdd
        ,case when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内'
                and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '1-出境'
              when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '2-海外飞海外'
              when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内' then '3-入境'
              else '5-其他' end as flight_type
        ,o.o_qunarusername as user_name
        ,biz_order_no
    from f_fuwu.dw_fact_inter_order_wide o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on o.s_arrcountryname = e.country_name
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
        -- and substr(create_time, 1, 10) >= '2026-01-01'
        -- and substr(create_time, 1, 10) <= date_sub(current_date, 16) -- 当天及往前推15天内的机票用户T-14~T
        and ticket_time is not null -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao' -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    group by 1,2,3,4,5
)
,flight_base as (
    select t1.dt
        ,case when t1.dt > t2.min_order_date then '老客' else '新客' end as user_type
        ,t1.flight_type
        ,t1.mdd
        ,t1.user_name
    from inter_flight t1
    left join user_type t2
        on t1.user_name = t2.user_name
    group by 1,2,3,4,5
)
,pos_cross as (--- 正交叉：机票UV固定为当天机票下单用户，T+N看当天起未来N天内国酒活跃/下单
    select t1.dt
        ,if(grouping(t1.user_type) = 1, 'ALL', t1.user_type) as user_type
        ,if(grouping(t1.flight_type) = 1, 'ALL', t1.flight_type) as flight_type
        ,if(grouping(t1.mdd) = 1, 'ALL', t1.mdd) as mdd
        ,t2.period_type
        ,count(distinct t1.user_name) as flight_uv
        ,count(distinct t3.user_name) as active_user_cnt
        ,count(distinct t4.user_name) as order_user_cnt
    from flight_base t1
    join (
        select 'T+0' as period_type,1 as days_num
        union all
        select 'T+7' as period_type,7 as days_num
        union all
        select 'T+15' as period_type,15 as days_num
    ) t2
    left join uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    left join q_order_app t4
        on t1.user_name = t4.user_name
        and t4.dt >= t1.dt
        and t4.dt <= date_add(t1.dt, t2.days_num - 1)
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by grouping sets (
        (t1.dt,t2.period_type,t1.user_type,t1.flight_type,t1.mdd),
        (t1.dt,t2.period_type,t1.user_type,t1.flight_type),
        (t1.dt,t2.period_type,t1.user_type,t1.mdd),
        (t1.dt,t2.period_type,t1.flight_type,t1.mdd),
        (t1.dt,t2.period_type,t1.user_type),
        (t1.dt,t2.period_type,t1.flight_type),
        (t1.dt,t2.period_type,t1.mdd),
        (t1.dt,t2.period_type)
    )
)
,cross_all as (
    select '正交叉' as cross_type
        ,dt
        ,user_type
        ,flight_type
        ,mdd
        ,period_type
        ,flight_uv
        ,active_user_cnt
        ,order_user_cnt
        ,case when flight_uv > 0 then active_user_cnt * 1.0000 / flight_uv else 0 end as active_cross_rate
        ,case when active_user_cnt > 0 then order_user_cnt * 1.0000 / active_user_cnt else 0 end as cross_cvr
        ,case when flight_uv > 0 then order_user_cnt * 1.0000 / flight_uv else 0 end as cross_rate
    from pos_cross
)
select dt as `日期`
    ,cross_type as `交叉类型`
    ,user_type as `新老客`
    ,flight_type as `出入境`
    ,mdd as `目的地`
    ,max(case when period_type = 'T+0' then flight_uv end) as `T+0_机票UV`
    ,max(case when period_type = 'T+0' then active_user_cnt end) as `T+0_活跃用户数`
    ,max(case when period_type = 'T+0' then order_user_cnt end) as `T+0_下单用户数`
    ,max(case when period_type = 'T+0' then active_cross_rate end) as `T+0_活跃交叉率`
    ,max(case when period_type = 'T+0' then cross_cvr end) as `T+0_交叉转化率`
    ,max(case when period_type = 'T+0' then cross_rate end) as `T+0_交叉率`
    ,max(case when period_type = 'T+7' then flight_uv end) as `T+7_机票UV`
    ,max(case when period_type = 'T+7' then active_user_cnt end) as `T+7_活跃用户数`
    ,max(case when period_type = 'T+7' then order_user_cnt end) as `T+7_下单用户数`
    ,max(case when period_type = 'T+7' then active_cross_rate end) as `T+7_活跃交叉率`
    ,max(case when period_type = 'T+7' then cross_cvr end) as `T+7_交叉转化率`
    ,max(case when period_type = 'T+7' then cross_rate end) as `T+7_交叉率`
    ,max(case when period_type = 'T+15' then flight_uv end) as `T+15_机票UV`
    ,max(case when period_type = 'T+15' then active_user_cnt end) as `T+15_活跃用户数`
    ,max(case when period_type = 'T+15' then order_user_cnt end) as `T+15_下单用户数`
    ,max(case when period_type = 'T+15' then active_cross_rate end) as `T+15_活跃交叉率`
    ,max(case when period_type = 'T+15' then cross_cvr end) as `T+15_交叉转化率`
    ,max(case when period_type = 'T+15' then cross_rate end) as `T+15_交叉率`
from cross_all
group by 1,2,3,4,5
order by 1 desc,2,3,4,5
;





---- 20260701最新看板正交叉
with user_type as (--- 判定新老客
    select user_name
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (---- 国酒分日去重活跃用户
    select dt
        ,user_name
        ,case when fromforlog in ('4104','4106')  then '二屏内容贴' 
                    when fromforlog in ('131','913','914')  then 'App首页宫格-海外酒店' 
                    when fromforlog in ('96','795','671','1096')  then '大搜相关' 
                    when fromforlog='352' then '再次预订' 
                    when fromforlog='422' then '酒店详情推荐模块-酒店单品' 
                    when fromforlog='200000081' then '二屏商卡' 
                    when fromforlog='200000083' then '市场活动去使用' 
                    when fromforlog='200000105' then '天天领券任务' 
                    when fromforlog='200000121' then '答题领积分任务'  
                    when fromforlog='200000118' then '国酒活动去使用' 
                    when fromforlog='200000119' then '机票实时短信' 
                    when fromforlog='200000120' then '带参数push' 
                    when fromforlog='200000122' then '国酒大搜落地页商卡' 
                    when fromforlog='200000123' then '带参数短信' 
                    when fromforlog='200000124' then '机票浏览实时短信'
                    when fromforlog='200000125' then '机票浏览实时push'
  
                    -- when fromforlog='671' then '大搜落地页-酒店tab' 
                    -- when fromforlog='96' then '大搜' 
                    -- when fromforlog='913' then 'App首页宫格-酒店频道-海外酒店tab' 
                    -- when fromforlog='914' then 'App首页-海外酒店' 
                    when fromforlog='4604' then '国际酒店H页快筛标签' 
                    when fromforlog='824' then '收藏跳转到酒店详情页' 

                    when fromforlog='4621' then '机票支付完成页' 
                    when fromforlog='4622' then '机票订单详情页' 
                    when fromforlog='4623' then '机票航班动态页' 
                    when fromforlog='4624' then '公共行程页' 
                    when fromforlog='4625' then '公共订单列表页' 
                    when fromforlog='4626' then '公共我的页面弹窗' 
                    when fromforlog='4627' then '公共我的页面浮标' 
                    else '其他'
        end as fromforlog_type
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= '2026-01-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3
)
,q_order_app as (---- 国酒分日去重下单用户
    select order_date as dt
        ,user_name,room_night,order_no
    from default.mdw_order_v3_international a
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
,q_channel_uv as (--- Q分渠道明细
    select dt,channel,user_name
    from ihotel_default.dwd_flow_ug_channel_di
    where dt >= '2026-01-01' and dt <= '2026-06-30'
    group by 1,2,3
)
,inter_flight as (---- 国际机票下单用户
    select substr(create_time, 1, 10) as dt
        ,case when s_arrcountryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then s_arrcountryname
              when s_arrcityname in ('香港','澳门') then s_arrcityname
              when e.area in ('欧洲','亚太','美洲') then e.area
              else '其他' end as mdd
        ,case when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内'
                and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '1-出境'
              when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外' then '2-海外飞海外'
              when if(s_depcountryname = '中国' and s_depcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国外'
                and if(s_arrcountryname = '中国' and s_arrcityname not in ('香港','澳门','台湾'), '国内', '国外') = '国内' then '3-入境'
              else '5-其他' end as flight_type
        ,o.o_qunarusername as user_name
        ,biz_order_no
    from f_fuwu.dw_fact_inter_order_wide o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on o.s_arrcountryname = e.country_name
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
        -- and substr(create_time, 1, 10) >= '2026-01-01'
        -- and substr(create_time, 1, 10) <= date_sub(current_date, 16) -- 当天及往前推15天内的机票用户T-14~T
        and ticket_time is not null -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao' -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    group by 1,2,3,4,5
)
,flight_base as (
    select t1.dt
        ,case when t1.dt > t2.min_order_date then '老客' else '新客' end as user_type
        ,t1.flight_type
        ,t1.mdd
        ,t1.user_name
    from inter_flight t1
    left join user_type t2
        on t1.user_name = t2.user_name
    group by 1,2,3,4,5
)
,pos_period as (
    select 'T+0' as period_type,1 as days_num
    union all
    select 'T+7' as period_type,7 as days_num
    union all
    select 'T+15' as period_type,15 as days_num
)
,pos_overall_user as (--- 整体交叉用户池
    select t1.dt
        ,t2.period_type
        ,t1.user_name
        ,max(case when t3.user_name is not null then 1 else 0 end) as active_flag
    from flight_base t1
    join pos_period t2
    left join uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by 1,2,3
)
,pos_overall as (--- 整体交叉
    select t1.dt
        ,'正交叉' as cross_type
        ,'ALL' as user_type
        ,'ALL' as flight_type
        ,'ALL' as mdd
        ,'整体' as dim_type
        ,'ALL' as dim_value
        ,t1.period_type
        ,count(distinct t1.user_name) as flight_uv
        ,count(distinct case when t1.active_flag = 1 then t1.user_name end) as active_user_cnt
        ,count(distinct t2.order_no) as order_cnt
        ,sum(t2.room_night) as room_night
    from pos_overall_user t1
    left join q_order_app t2
        on t1.user_name = t2.user_name
        and t2.dt >= t1.dt
        and t2.dt <= date_add(t1.dt, case when t1.period_type = 'T+0' then 0 when t1.period_type = 'T+7' then 6 when t1.period_type = 'T+15' then 14 end)
    group by 1,2,3,4,5,6,7,8
)
,pos_channel_user as (--- 国酒流量 x 渠道
    select t1.dt
        ,t2.period_type
        ,t1.user_name
        ,coalesce(t3.channel, '未知') as dim_value
    from flight_base t1
    join pos_period t2
    join q_channel_uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by 1,2,3,4
)
,pos_channel as (
    select t1.dt
        ,'正交叉' as cross_type
        ,'ALL' as user_type
        ,'ALL' as flight_type
        ,'ALL' as mdd
        ,'渠道' as dim_type
        ,t1.dim_value
        ,t1.period_type
        ,count(distinct t1.user_name) as flight_uv
        ,count(distinct t1.user_name) as active_user_cnt
        ,count(distinct t2.order_no) as order_cnt
        ,sum(t2.room_night) as room_night
    from pos_channel_user t1
    left join q_order_app t2
        on t1.user_name = t2.user_name
        and t2.dt >= t1.dt
        and t2.dt <= date_add(t1.dt, case when t1.period_type = 'T+0' then 0 when t1.period_type = 'T+7' then 6 when t1.period_type = 'T+15' then 14 end)
    group by 1,2,3,4,5,6,7,8
)
,pos_fromforlog_user as (--- 国酒流量 x fromforlog
    select t1.dt
        ,t2.period_type
        ,t1.user_name
        ,coalesce(t3.fromforlog_type, '未知') as dim_value
    from flight_base t1
    join pos_period t2
    join uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by 1,2,3,4
)
,pos_fromforlog as (
    select t1.dt
        ,'正交叉' as cross_type
        ,'ALL' as user_type
        ,'ALL' as flight_type
        ,'ALL' as mdd
        ,'fromforlog' as dim_type
        ,t1.dim_value
        ,t1.period_type
        ,count(distinct t1.user_name) as flight_uv
        ,count(distinct t1.user_name) as active_user_cnt
        ,count(distinct t2.order_no) as order_cnt
        ,sum(t2.room_night) as room_night
    from pos_fromforlog_user t1
    left join q_order_app t2
        on t1.user_name = t2.user_name
        and t2.dt >= t1.dt
        and t2.dt <= date_add(t1.dt, case when t1.period_type = 'T+0' then 0 when t1.period_type = 'T+7' then 6 when t1.period_type = 'T+15' then 14 end)
    group by 1,2,3,4,5,6,7,8
)
,cross_all as (
    select *
    from pos_overall
    union all
    select *
    from pos_channel
    union all
    select *
    from pos_fromforlog
)
,cross_final as (
    select t1.dt
        ,t1.cross_type
        ,t1.user_type
        ,t1.flight_type
        ,t1.mdd
        ,t1.dim_type
        ,t1.dim_value
        ,t1.period_type
        ,t1.flight_uv
        ,t1.active_user_cnt
        ,t1.order_cnt
        ,t1.room_night
        ,case when t1.flight_uv > 0 then t1.active_user_cnt * 1.0000 / t1.flight_uv else 0 end as active_cross_rate
        ,case when t1.active_user_cnt > 0 then t1.order_cnt * 1.0000 / t1.active_user_cnt else 0 end as cross_cvr
        ,case when t1.flight_uv > 0 then t1.order_cnt * 1.0000 / t1.flight_uv else 0 end as cross_rate
        ,case when t2.active_user_cnt > 0 then t1.active_user_cnt * 1.0000 / t2.active_user_cnt else 0 end as active_ratio
        ,case when t2.room_night > 0 then t1.room_night * 1.0000 / t2.room_night else 0 end as room_night_ratio
    from cross_all t1
    left join pos_overall t2
        on t1.dt = t2.dt
        and t1.period_type = t2.period_type
        and t2.dim_type = '整体'
)
select dt as `日期`
    ,cross_type as `交叉类型`
    ,user_type as `新老客`
    ,flight_type as `出入境`
    ,mdd as `目的地`
    ,dim_type as `交叉维度类型`
    ,dim_value as `交叉维度值`
    ,max(case when period_type = 'T+0' then flight_uv end) as `T+0_机票UV`
    ,max(case when period_type = 'T+0' then active_user_cnt end) as `T+0_活跃用户数`
    ,max(case when period_type = 'T+0' then order_cnt end) as `T+0_订单量`
    ,max(case when period_type = 'T+0' then room_night end) as `T+0_间夜量`
    ,max(case when period_type = 'T+0' then active_cross_rate end) as `T+0_活跃交叉率`
    ,max(case when period_type = 'T+0' then cross_cvr end) as `T+0_交叉转化率`
    ,max(case when period_type = 'T+0' then cross_rate end) as `T+0_交叉率`
    ,max(case when period_type = 'T+0' then active_ratio end) as `T+0_流量占比`
    ,max(case when period_type = 'T+0' then room_night_ratio end) as `T+0_间夜占比`
    ,max(case when period_type = 'T+7' then flight_uv end) as `T+7_机票UV`
    ,max(case when period_type = 'T+7' then active_user_cnt end) as `T+7_活跃用户数`
    ,max(case when period_type = 'T+7' then order_cnt end) as `T+7_订单量`
    ,max(case when period_type = 'T+7' then room_night end) as `T+7_间夜量`
    ,max(case when period_type = 'T+7' then active_cross_rate end) as `T+7_活跃交叉率`
    ,max(case when period_type = 'T+7' then cross_cvr end) as `T+7_交叉转化率`
    ,max(case when period_type = 'T+7' then cross_rate end) as `T+7_交叉率`
    ,max(case when period_type = 'T+7' then active_ratio end) as `T+7_流量占比`
    ,max(case when period_type = 'T+7' then room_night_ratio end) as `T+7_间夜占比`
    ,max(case when period_type = 'T+15' then flight_uv end) as `T+15_机票UV`
    ,max(case when period_type = 'T+15' then active_user_cnt end) as `T+15_活跃用户数`
    ,max(case when period_type = 'T+15' then order_cnt end) as `T+15_订单量`
    ,max(case when period_type = 'T+15' then room_night end) as `T+15_间夜量`
    ,max(case when period_type = 'T+15' then active_cross_rate end) as `T+15_活跃交叉率`
    ,max(case when period_type = 'T+15' then cross_cvr end) as `T+15_交叉转化率`
    ,max(case when period_type = 'T+15' then cross_rate end) as `T+15_交叉率`
    ,max(case when period_type = 'T+15' then active_ratio end) as `T+15_流量占比`
    ,max(case when period_type = 'T+15' then room_night_ratio end) as `T+15_间夜占比`
from cross_final
group by 1,2,3,4,5,6,7
order by 1 desc,2,3,4,5,6,7
;




---- 机酒正交叉看板：
with uv as (---- 国酒分日去重活跃用户
    select dt
        ,user_name
        ,case when fromforlog in ('4104','4106') then '二屏内容贴'
              when fromforlog in ('131','913','914') then 'App首页宫格-海外酒店'
              when fromforlog in ('96','795','671','1096') then '大搜相关'
              when fromforlog = '352' then '再次预订'
              when fromforlog = '422' then '酒店详情推荐模块-酒店单品'
              when fromforlog = '200000081' then '二屏商卡'
              when fromforlog = '200000083' then '市场活动去使用'
              when fromforlog = '200000105' then '天天领券任务'
              when fromforlog = '200000121' then '答题领积分任务'
              when fromforlog = '200000118' then '国酒活动去使用'
              when fromforlog = '200000119' then '机票实时短信'
              when fromforlog = '200000120' then '带参数push'
              when fromforlog = '200000122' then '国酒大搜落地页商卡'
              when fromforlog = '200000123' then '带参数短信'
              when fromforlog = '200000124' then '机票浏览实时短信'
              when fromforlog = '200000125' then '机票浏览实时push'
              when fromforlog = '4604' then '国际酒店H页快筛标签'
              when fromforlog = '824' then '收藏跳转到酒店详情页'
              when fromforlog = '4621' then '机票支付完成页'
              when fromforlog = '4622' then '机票订单详情页'
              when fromforlog = '4623' then '机票航班动态页'
              when fromforlog = '4624' then '公共行程页'
              when fromforlog = '4625' then '公共订单列表页'
              when fromforlog = '4626' then '公共我的页面弹窗'
              when fromforlog = '4627' then '公共我的页面浮标'
              else '其他' end as fromforlog_type
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    where dt >= '2025-01-01'
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3
)
,q_order_app as (---- 国酒分日下单明细
    select order_date as dt
        ,user_name
        ,room_night
        ,order_no
    from default.mdw_order_v3_international a
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
        and (first_rejected_time is null or date(first_rejected_time) > order_date)
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid = '1'
        and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,red as (--- 小红书渠道流量，往前多取6天用于2025-01-01回看7天
    select flow_dt as dt
        ,user_name
    from pp_pub.dwd_redbook_global_flow_detail_di
    where dt between date_sub('2025-01-01', 6) and date_sub(current_date, 1)
        -- and business_type = 'hotel-inter'  --宽口径不用该字段
        and query_platform = 'redbook'
    group by 1,2
)
,inter_flight as (---- 国际机票下单用户
    select substr(create_time, 1, 10) as dt
        ,o.o_qunarusername as user_name
        ,biz_order_no
    from f_fuwu.dw_fact_inter_order_wide o
    where dt >= '2025-01-01' and dt <= date_sub(current_date, 1)
        -- and substr(create_time, 1, 10) >= '2025-01-01'
        -- and substr(create_time, 1, 10) <= date_sub(current_date, 16)
        and ticket_time is not null -- 出票完成时间
        and refund_complete_time is null -- 已出票未退款
        and platform <> 'fenxiao' -- 去分销
        and (s_arrcountryname != '中国' or s_depcountryname != '中国')
    group by 1,2,3
)
,flight_base as (
    select dt
        ,user_name
    from inter_flight
    group by 1,2
)
,pos_period as (
    select 'T+15' as period_type,15 as days_num
)
,pos_overall_user as (--- 整体交叉用户池
    select t1.dt
        ,t2.period_type
        ,t1.user_name
        ,max(case when t3.user_name is not null then 1 else 0 end) as active_flag
    from flight_base t1
    join pos_period t2
    left join uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by 1,2,3
)
,pos_overall as (--- 整体交叉
    select t1.dt
        ,'ALL' as dim_type
        ,'ALL' as dim_value
        ,t1.period_type
        ,count(distinct t1.user_name) as flight_uv
        ,count(distinct case when t1.active_flag = 1 then t1.user_name end) as active_user_cnt
        ,count(distinct t2.order_no) as order_cnt
        ,sum(t2.room_night) as room_night
    from pos_overall_user t1
    left join q_order_app t2
        on t1.user_name = t2.user_name
        and t2.dt >= t1.dt
        and t2.dt <= date_add(t1.dt, case when t1.period_type = 'T+0' then 0 when t1.period_type = 'T+7' then 6 when t1.period_type = 'T+15' then 14 end)
    group by 1,2,3,4
)
,pos_red_user as (--- 国酒流量 x 小红书：当日国酒活跃用户，往前7天有小红书触达
    select t1.dt
        ,t2.period_type
        ,t1.user_name
        ,'小红书' as dim_value
    from flight_base t1
    join pos_period t2
    join uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    join red t4
        on t3.user_name = t4.user_name
        and t4.dt >= date_sub(t3.dt, 6)
        and t4.dt <= t3.dt
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by 1,2,3,4
)
,pos_red as (
    select t1.dt
        ,'渠道' as dim_type
        ,t1.dim_value
        ,t1.period_type
        ,max(t3.flight_uv) as flight_uv
        ,count(distinct t1.user_name) as active_user_cnt
        ,count(distinct t2.order_no) as order_cnt
        ,sum(t2.room_night) as room_night
    from pos_red_user t1
    left join q_order_app t2
        on t1.user_name = t2.user_name
        and t2.dt >= t1.dt
        and t2.dt <= date_add(t1.dt, case when t1.period_type = 'T+0' then 0 when t1.period_type = 'T+7' then 6 when t1.period_type = 'T+15' then 14 end)
    left join pos_overall t3
        on t1.dt = t3.dt
        and t1.period_type = t3.period_type
        and t3.dim_type = '整体'
    group by 1,2,3,4
)
,pos_fromforlog_user as (--- 国酒流量 x fromforlog
    select t1.dt
        ,t2.period_type
        ,t1.user_name
        ,coalesce(t3.fromforlog_type, '未知') as dim_value
    from flight_base t1
    join pos_period t2
    join uv t3
        on t1.user_name = t3.user_name
        and t3.dt >= t1.dt
        and t3.dt <= date_add(t1.dt, t2.days_num - 1)
    where date_add(t1.dt, t2.days_num - 1) <= date_sub(current_date, 1)
    group by 1,2,3,4
)
,pos_fromforlog as (
    select t1.dt
        ,'fromforlog' as dim_type
        ,t1.dim_value
        ,t1.period_type
        ,max(t3.flight_uv) as flight_uv
        ,count(distinct t1.user_name) as active_user_cnt
        ,count(distinct t2.order_no) as order_cnt
        ,sum(t2.room_night) as room_night
    from pos_fromforlog_user t1
    left join q_order_app t2
        on t1.user_name = t2.user_name
        and t2.dt >= t1.dt
        and t2.dt <= date_add(t1.dt, case when t1.period_type = 'T+0' then 0 when t1.period_type = 'T+7' then 6 when t1.period_type = 'T+15' then 14 end)
    left join pos_overall t3
        on t1.dt = t3.dt
        and t1.period_type = t3.period_type
        and t3.dim_type = '整体'
    group by 1,2,3,4
)
,cross_all as (
    select *
    from pos_overall
    union all
    select *
    from pos_red
    union all
    select *
    from pos_fromforlog
)
,cross_final as (
    select t1.dt
        ,t1.dim_type
        ,t1.dim_value
        ,t1.period_type
        ,t1.flight_uv
        ,t1.active_user_cnt
        ,t1.order_cnt
        ,t1.room_night
        ,case when t1.flight_uv > 0 then t1.active_user_cnt * 1.0000 / t1.flight_uv else 0 end as active_cross_rate
        ,case when t1.active_user_cnt > 0 then t1.order_cnt * 1.0000 / t1.active_user_cnt else 0 end as cross_cvr
        ,case when t1.flight_uv > 0 then t1.order_cnt * 1.0000 / t1.flight_uv else 0 end as cross_rate
        ,case when t2.active_user_cnt > 0 then t1.active_user_cnt * 1.0000 / t2.active_user_cnt else 0 end as active_ratio
        ,case when t2.room_night > 0 then t1.room_night * 1.0000 / t2.room_night else 0 end as room_night_ratio
    from cross_all t1
    left join pos_overall t2
        on t1.dt = t2.dt
        and t1.period_type = t2.period_type
        and t2.dim_type = '整体'
)
select dt as `日期`
    ,dim_type as `交叉维度类型`
    ,dim_value as `交叉维度值`
    
    ,max(case when period_type = 'T+15' then flight_uv end) as `T+15_机票UV`
    ,max(case when period_type = 'T+15' then active_user_cnt end) as `T+15_活跃用户数`
    ,max(case when period_type = 'T+15' then order_cnt end) as `T+15_订单量`
    ,max(case when period_type = 'T+15' then room_night end) as `T+15_间夜量`
    ,max(case when period_type = 'T+15' then active_cross_rate end) as `T+15_活跃交叉率`
    ,max(case when period_type = 'T+15' then cross_cvr end) as `T+15_交叉转化率`
    ,max(case when period_type = 'T+15' then cross_rate end) as `T+15_交叉率`
    ,max(case when period_type = 'T+15' then active_ratio end) as `T+15_流量占比`
    ,max(case when period_type = 'T+15' then room_night_ratio end) as `T+15_间夜占比`
from cross_final
group by 1,2,3
order by 1 desc,2,3
;