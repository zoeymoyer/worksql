with user_type as (-----新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as ----分日去重活跃用户
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15)
    and dt <= date_sub(current_date, 1)
    and business_type = 'hotel'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    and (search_pv + detail_pv + booking_pv + order_pv) > 0
    and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
    and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)
,q_order as (----订单明细表表包含取消  分目的地、新老维度 全端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
           
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        --and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date,15) and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
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
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after_new+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after_new+COALESCE(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
            ,case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                   then (init_commission_after+nvl(coupon_substract_summary ,0)) 
                   when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') 
                   then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0)) 
                else init_commission_after+nvl(ext_plat_certificate,0) end
              +nvl(follow_price_amount,0) 
              +nvl(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night 
              +nvl(cashbackmap['framework_amount'],0)  qyjcsj_cb
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
        and order_date >= date_sub(current_date,60) and order_date <= date_sub(current_date,1)
        and order_no <> '103576132435'
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
 )
,c_uv as
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,count(distinct uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= date_sub(current_date, 15) and dt<= date_sub(current_date,1)
    group by 1,2,3
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
            ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= date_sub(current_date, 15)
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)



select dt `日期`
       ,date_format(dt,'u') `星期`
       ,sum(q_room_nights)`间夜量`
       ,concat(round(sum(q_yj)/10000,1),'万')`收益额`
       ,concat(round(sum(q_room_nights_app)/sum(c_room_nights_app)*100,1),'%')`间夜QC`
       ,concat(round(sum(q_uv)/sum(c_uv)*100,1),'%')`流量QC`
       ,concat(round((sum(q_orders_app)/sum(q_uv))/(sum(c_orders_app)/sum(c_uv))*100,1),'%')`转化QC`
       ,concat(round((sum(q_room_nights_app)/sum(q_orders_app))/(sum(c_room_nights_app)/sum(c_orders_app))*100,1),'%')`单间夜QC`
       ,concat(round(sum(q_yj_app)/sum(c_yj_app)*100,1),'%')`收益QC`
       ,concat(round(sum(q_yj_app)/sum(q_gmv_app)*100,2),'%')`Q_收益率`
       ,concat(round(sum(c_yj_app)/sum(c_gmv_app)*100,2),'%')`C_收益率`
       ,concat(round(((sum(q_yj_app)/sum(q_gmv_app))-(sum(c_yj_app)/sum(c_gmv_app)))*100,2),'%')`收益率QC差`
       ,concat(round(((sum(qyjcsj_cb)/sum(q_gmv_app))-(sum(c_yj_app)/sum(c_gmv_app)))*100,2),'%')`收益率QC差(C视角)`
       ,concat(round(sum(q_qe_app)/sum(q_gmv_app)*100,2),'%')`Q_券补贴率`
       ,concat(round(sum(c_qe_app)/sum(c_gmv_app)*100,2),'%')`C_券补贴率`
       ,concat(round(((sum(q_qe_app)/sum(q_gmv_app))-(sum(c_qe_app)/sum(c_gmv_app)))*100,2),'%')`券补贴率QC差`
       ,concat(round(sum(q_room_night_high_app)/sum(c_room_night_high_app)*100,1),'%')`高星间夜QC`
       ,concat(round(sum(q_room_night_middle_app)/sum(c_room_night_middle_app)*100,1),'%')`中星间夜QC`
       ,concat(round(sum(q_room_night_low_app)/sum(c_room_night_low_app)*100,1),'%')`低星间夜QC`
       ,concat(round(sum(q_room_night_high_app)/sum(q_room_nights_app)*100,1),'%')`Q_高星间夜占比`
       ,concat(round(sum(q_room_night_middle_app)/sum(q_room_nights_app)*100,1),'%')`Q_中星间夜占比`
       ,concat(round(sum(q_room_night_low_app)/sum(q_room_nights_app)*100,1),'%')`Q_低星间夜占比`
       ,sum(q_uv)q_uv
       ,sum(c_uv)c_uv
       ,concat(round(sum(q_orders_app)/sum(q_uv)*100,1),'%')`Q_CR`
       ,concat(round(sum(c_orders_app)/sum(c_uv)*100,1),'%')`C_CR`
       ,round(sum(q_room_nights_app)/sum(q_orders_app),2)`Q_单订单间夜`
       ,round(sum(c_room_nights_app)/sum(c_orders_app),2)`C_单订单间夜`
       ,round(sum(q_gmv_app)/sum(q_room_nights_app),0)`Q_ADR`
       ,CAST(round(sum(c_gmv_app)/sum(c_room_nights_app),0) as int)`C_ADR`
from (
select t1.dt
       ,t1.user_type,t1.mdd,t1.q_uv,t2.c_uv
       ,q_orders_app,q_room_nights_app,t3.q_yj_app,t3.q_yj_byc_app,q_gmv_app,q_qe_app
       ,q_room_night_high_app,q_room_night_middle_app,q_room_night_low_app
       ,c_orders_app,c_room_nights_app,c_yj_app,c_gmv_app
       ,c_qe_app,c_room_night_high_app,c_room_night_middle_app,c_room_night_low_app
       ,q_yj,q_room_nights
       ,qyjcsj_cb
from (
    select dt,mdd,user_type,count(user_id) q_uv
    from uv
    group by 1,2,3
)t1 
left join (
    select dt,mdd,user_type,sum(c_uv) c_uv
    from c_uv
    group by 1,2,3  
)t2 on t1.dt=t2.dt and t1.mdd=t2.mdd and t1.user_type=t2.user_type
left join (
    select order_date,mdd,user_type
            ,sum(final_commission_after) as q_yj_app
            ,sum(qyj) + sum(zbj) + sum(xyb) + sum(qb) as q_yj_byc_app
            ,sum(init_gmv) as q_gmv_app
            ,sum(coupon_substract_summary) as q_qe_app
            ,count(distinct order_no) as q_orders_app
            ,count(distinct user_id) as q_order_uv_app
            ,sum(room_night) as q_room_nights_app
            ,count(distinct case when is_user_conpon = 'Y' then order_no else null end)   as q_order_use_conpon_app
            ,sum(case when hotel_grade in (4,5) then room_night else 0 end ) as q_room_night_high_app
            ,sum(case when hotel_grade in (3) then room_night else 0 end ) as q_room_night_middle_app
            ,sum(case when hotel_grade not in (3,4,5) then room_night else 0 end ) as q_room_night_low_app
            ,sum(qyjcsj_cb) qyjcsj_cb
    from q_app_order
    group by 1,2,3
)t3 on t1.dt=t3.order_date and t1.mdd=t3.mdd and t1.user_type=t3.user_type
left join (
    select order_date,mdd,user_type
            ,sum(final_commission_after) as q_yj
            ,sum(room_night) as q_room_nights
    from q_order
    group by 1,2,3
)t4 on t1.dt=t4.order_date and t1.mdd=t4.mdd and t1.user_type=t4.user_type
left join (-- C订单 APP端
    select dt,mdd,user_type
            ,sum(comission) as c_yj_app
            ,sum(room_fee) as c_gmv_app
            ,sum(cqe) as c_qe_app
            ,count(distinct order_no) as c_orders_app
            ,count(distinct user_id) as c_order_uv_app
            ,sum(room_night) as c_room_nights_app
            ,sum(case when star in (4,5) then room_night else 0 end ) as c_room_night_high_app
            ,sum(case when star in (3) then room_night else 0 end ) as c_room_night_middle_app
            ,sum(case when star not in (3,4,5) then room_night else 0 end ) as c_room_night_low_app
    from c_order
    group by 1,2,3
)t5 on t1.dt=t5.dt and t1.mdd=t5.mdd and t1.user_type=t5.user_type
) where mdd='日本'
group by 1
order by dt desc
;