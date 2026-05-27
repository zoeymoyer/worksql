--- 1、酒店整体分布
with base_info as (
    select  hotel_seq
           ,attrs['whenBuiltV2'] as bus_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,max(attrs['hotelSubCategory']) as hotel_sub_category
    from default.dim_hotel_info_intl_v3  a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and attrs['whenBuiltV2'] is not null 
        and hotel_operating_status = '营业中'
    group by 1, 2, 3
)
,bucket_data as (--- 2. 梳理非标标签，并直接计算开业时长打标签
    select  hotel_seq
           ,case when hotel_sub_category in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标'
                 when hotel_sub_category is null then '非标'
                 else '标准' 
            end as standard_hotel
           ,case when to_date(bus_date) >= add_months(date_sub(current_date, 1), -6)  then '1_6个月以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -12) then '2_1年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -24) then '3_2年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -36) then '4_3年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -60) then '5_5年以内'
                 when to_date(bus_date) <  add_months(date_sub(current_date, 1), -60) then '6_5年以上'
                 else '7_未知或异常'
            end as open_bucket
    from base_info
)
,agg_data as (
    --- 3. 分桶聚合计算各项数量
    select  open_bucket
           ,count(distinct hotel_seq) as hotel_cnt
           ,count(distinct case when standard_hotel = '标准' then hotel_seq end) as standard_hotel_cnt
           ,count(distinct case when standard_hotel = '非标' then hotel_seq end) as non_standard_hotel_cnt
    from bucket_data
    group by 1
)
--- 4. 结合窗口函数计算大盘占比并输出（占比：本桶数量 / 大盘整体数量）
select  open_bucket
       ,hotel_cnt
       ,concat(round(hotel_cnt / sum(hotel_cnt) over() * 100, 2), '%') as hotel_cnt_ratio
       ,standard_hotel_cnt
       ,concat(round(standard_hotel_cnt / sum(standard_hotel_cnt) over() * 100, 2), '%') as standard_hotel_cnt_ratio
       ,non_standard_hotel_cnt
       ,concat(round(non_standard_hotel_cnt / sum(non_standard_hotel_cnt) over() * 100, 2), '%') as non_standard_hotel_cnt_ratio
from agg_data
order by 1
;


--- 2、分目的地酒店分布
with base_info as (
    select  hotel_seq
           ,attrs['whenBuiltV2'] as bus_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,max(attrs['hotelSubCategory']) as hotel_sub_category
    from default.dim_hotel_info_intl_v3  a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and attrs['whenBuiltV2'] is not null 
        and hotel_operating_status = '营业中'
    group by 1, 2, 3
)
,bucket_data as (--- 2. 梳理非标标签，并直接计算开业时长打标签
    select  hotel_seq,mdd
           ,case when hotel_sub_category in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标'
                 when hotel_sub_category is null then '非标'
                 else '标准' 
            end as standard_hotel
           ,case when to_date(bus_date) >= add_months(date_sub(current_date, 1), -6)  then '1_6个月以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -12) then '2_1年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -24) then '3_2年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -36) then '4_3年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -60) then '5_5年以内'
                 when to_date(bus_date) <  add_months(date_sub(current_date, 1), -60) then '6_5年以上'
                 else '7_未知或异常'
            end as open_bucket
    from base_info
)
,agg_data as (
    --- 3. 分桶聚合计算各项数量
    select  open_bucket,mdd
           ,count(distinct hotel_seq) as hotel_cnt
           ,count(distinct case when standard_hotel = '标准' then hotel_seq end) as standard_hotel_cnt
           ,count(distinct case when standard_hotel = '非标' then hotel_seq end) as non_standard_hotel_cnt
    from bucket_data
    group by 1,2
)
--- 4. 结合窗口函数计算大盘占比并输出（占比：本桶数量 / 大盘整体数量）
select  open_bucket,mdd
       ,hotel_cnt
       ,concat(round(hotel_cnt / sum(hotel_cnt) over(partition by open_bucket) * 100, 2), '%') as hotel_cnt_ratio
       ,standard_hotel_cnt
       ,concat(round(standard_hotel_cnt / sum(standard_hotel_cnt) over(partition by open_bucket) * 100, 2), '%') as standard_hotel_cnt_ratio
       ,non_standard_hotel_cnt
       ,concat(round(non_standard_hotel_cnt / sum(non_standard_hotel_cnt) over(partition by open_bucket) * 100, 2), '%') as non_standard_hotel_cnt_ratio
from agg_data
order by 1
;



--- 3、不同开业时长酒店订单表现
with base_info as (
    select  hotel_seq
           ,attrs['whenBuiltV2'] as bus_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,max(attrs['hotelSubCategory']) as hotel_sub_category
    from default.dim_hotel_info_intl_v3  a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and attrs['whenBuiltV2'] is not null 
        and hotel_operating_status = '营业中'
    group by 1, 2, 3
)
,bucket_data as (--- 2. 梳理非标标签，并直接计算开业时长打标签
    select  hotel_seq,mdd
           ,case when hotel_sub_category in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标'
                 when hotel_sub_category is null then '非标'
                 else '标准' 
            end as standard_hotel
           ,case when to_date(bus_date) >= add_months(date_sub(current_date, 1), -6)  then '1_6个月以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -12) then '2_1年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -24) then '3_2年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -36) then '4_3年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -60) then '5_5年以内'
                 when to_date(bus_date) <  add_months(date_sub(current_date, 1), -60) then '6_5年以上'
                 else '7_未知或异常'
            end as open_bucket
    from base_info
)
,q_c_hotel_mapping as (
    select t1.hotel_seq,t1.partner_hotel_id,t2.open_bucket
    from (
        select hotel_seq,
            partner_hotel_id
        from ihotel_default.dim_hotel_mapping_intl_v3
        where dt = '%(DATE)s'
        and partner = 'ctrip'
        group by 1,2
    ) t1 left join bucket_data t2 on t1.hotel_seq = t2.hotel_seq
    group by 1,2,3
)
,q_data as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when a.country_name = '日本' then 'Y' else 'N' end is_jp
            ,case when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'DC'
                  when  qta_supplier_id in ('1615667','800000164') then 'C2Q'
                  when wrapper_id in ('hca908oh60s','hca908oh60t') then 'ABE'  --- Agoda
                  when wrapper_id in ('hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s') then 'ABE'  --- Booking
                  when wrapper_id in ('hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'ABE'  --- EAN
                  else '代理' 
            end as supply_channel
            ,case when qta_supplier_id in ('1615667','800000164') and vendor_name = 'DC' then 'C2Q直采' 
                  when qta_supplier_id in ('1615667','800000164') and vendor_name = 'Agoda' then 'C2Q-Agoda'
                  when qta_supplier_id in ('1615667','800000164') then 'C2Q-代理'
                  when qta_supplier_id not in ('1615667','800000164') and wrapper_id not in ('hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s','hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'Q代理'
                  else 'Q-ABE' 
            end as supplier
            ,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status,init_gmv,room_night
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            --- 预定当日是否取消或拒单
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
                   and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                   and (refund_time is null or date(refund_time) > order_date) 
                   then 'Y' else 'N' 
            end is_not_cancel_d0 
            --- 是否取消订单，剔除了预定当日
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                   or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                   or (refund_time is not null and date(refund_time) > order_date) 
                   then 'Y' else 'N' 
            end is_cancel_d0  
            --- 是否不可取消订单，Y不可取消订单。 使用spark引擎，值=2为可取消订单，但存在null和空情况占比2%左右。数据最早看25年4月份之后
            ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end is_non_ref 
            ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
            ,case when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
            ,o.open_bucket  --- 开业时长分桶
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join(
        select order_no,max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
    ) b 
    on a.order_no = b.order_no
    -- C关联信息表-用于提供供应商信息
    left join(
        select distinct partner_order_no,extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
    ) c
    on b.purchase_order_no = c.partner_order_no
    left join bucket_data o on a.hotel_seq = o.hotel_seq
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and order_date >= '2026-01-01' and order_date <= date_sub(current_date, 1)
)
,c_order as( --- C订单
    select  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                  when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                  when c.area in ('欧洲','亚太','美洲') then c.area
                  else '其他' 
            end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,o.user_id,order_no,room_fee,order_status,comission
            ,extend_info['room_night'] room_night
            --- 预定当日是否有取消单或被拒单
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' 
            end as supplier
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 0 and 3    then '提前订1-3天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                  else '提前订31+'  
            end  per_type
            ,case when room_fee / extend_info['room_night'] < 400  then '1[0,400)'
                  when room_fee / extend_info['room_night'] >= 400 and  room_fee / extend_info['room_night'] < 800  then '2[400,800)'
                  when room_fee / extend_info['room_night'] >= 800 and  room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                  when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
            ,m.open_bucket
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join q_c_hotel_mapping m on o.hotel_seq = m.partner_hotel_id
    where   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        and o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        and o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        and o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        and o.terminal_channel_type = 'app'
        and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
        and substr(order_date,1,10) >= '2026-01-01'
        and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,uv as (---D页流量25年春节
    select dt
        ,o.open_bucket
        ,count(distinct a.user_id) as q_uv
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join bucket_data o on a.hotel_seq = o.hotel_seq
    where dt >= '2026-01-01' 
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and a.country_name = '马来西亚'
    group by 1,2
)
,c_uv as (
    select a.dt
        ,open_bucket
        ,count(distinct a.uid) c_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join q_c_hotel_mapping m on a.masterhotelid = m.partner_hotel_id
    where a.dt >= '2026-01-01' and a.dt <= date_sub(current_date, 1)
      and a.device_chl = 'app' and a.page_short_domain = 'dbo'
      and countryname = '马来西亚'
    group by 1,2
)
,qc_price as (
    select order_date,open_bucket
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
             ,o.open_bucket
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        left join bucket_data o on a.hotel_seq = o.hotel_seq
        where dt >= '20260101' and dt <= replace(date_sub(current_date, 1),'-','')
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
    )t
    where mdd='马来西亚'
    group by 1,2
)
,qcprice_final as (
    select order_date
        ,open_bucket
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
)

select t1.order_date,t1.open_bucket,t1.order_cnt,t1.room_night_sum,t2.c_order_cnt,t2.c_room_night_sum,t3.q_uv,t4.c_uv
    ,t1.order_cnt / t2.c_order_cnt as qc_order_cnt
    ,t1.room_night_sum / t2.c_room_night_sum as qc_room_night
    ,t1.order_cnt / t3.q_uv as q_cr
    ,t2.c_order_cnt / t4.c_uv as c_cr
    ,t3.q_uv / t4.c_uv as qc_uv
    ,(t1.order_cnt / t3.q_uv) / (t2.c_order_cnt / t4.c_uv) as qc_cr
    ,t5.`支付价lose率`,t5.`支付价lose深度`,t5.`beat率`,t5.`支付价beat深度`
    ,gmv,yj,c_gmv,c_yj
    ,yj / gmv as q_yj_rate
    ,c_yj / c_gmv as c_yj_rate
    ,(yj / gmv) - (c_yj / c_gmv) as qc_yj_rate
from (
    select t1.order_date,open_bucket,count(distinct order_no) as order_cnt,sum(room_night) as room_night_sum,sum(init_gmv) as gmv,sum(final_commission_after) as yj
    from q_data t1 
    where mdd='马来西亚'
    group by 1,2
) t1 
left join (
    select t1.order_date,open_bucket,count(distinct order_no) as c_order_cnt,sum(room_night) as c_room_night_sum,sum(room_fee) as c_gmv,sum(comission) as c_yj
    from c_order t1 
    where mdd='马来西亚'
    group by 1,2  
) t2 on t1.order_date = t2.order_date and t1.open_bucket = t2.open_bucket
left join uv t3 on t1.order_date = t3.dt and t1.open_bucket = t3.open_bucket
left join c_uv t4 on t1.order_date = t4.dt and t1.open_bucket = t4.open_bucket
left join qcprice_final t5 on t1.order_date = t5.order_date and t1.open_bucket = t5.open_bucket
order by t1.order_date,open_bucket
;









---- 港澳星级*价格带间夜数据
with q_order as (
    select order_date 
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
           ,checkout_date,order_no,room_night
           ,final_gmv
           ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
           ,a.user_id
           ,hotel_grade
           ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
           ,case  when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
          ,case  when init_gmv / room_night < 600   then '1[0,600)'
                    when init_gmv / room_night >= 600  and init_gmv / room_night < 800   then '2[600,800)'
                    when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                    when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                    else '5[1600+]' 
            end adr_type_new 
           ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
            ,case 
                when datediff(checkin_date,order_date) <= 0 then '凌晨&当天订'
                when datediff(checkin_date,order_date) between 1 and 7 then '1-7天'
                when datediff(checkin_date,order_date) between 8 and 14 then '8-14天'
                when datediff(checkin_date,order_date) between 15 and 21 then '15-21天'
                when datediff(checkin_date,order_date) between 22 and 28 then '22-28天'
                when datediff(checkin_date,order_date) >= 29 then '29+天'
                else '其他' end as early_day
            ,case when hotel_grade in (4,5) then '高星' 
                  when hotel_grade in (3) then '中星' 
                  else '低星' end as hotel_grade_type
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app' 
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and ((order_date >= '2025-01-01' and order_date <= '2025-05-12') or (order_date >= '2026-01-01' and order_date <= '2026-05-12'))
        and order_no <> '103576132435'
)
,c_order as (--- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'
                when extend_info['COUNTRY'] in ('泰国','日本','韩国') then extend_info['COUNTRY']
                else '其他' end as new_mdd
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,case  when room_fee / extend_info['room_night'] < 400   then '1[0,400)'
                   when room_fee / extend_info['room_night'] >= 400  and room_fee / extend_info['room_night'] < 800   then '2[400,800)'
                   when room_fee / extend_info['room_night'] >= 800  and room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                   when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                   else '5[1600+]' 
            end adr_type 
            ,case when extend_info['STAR'] in (4,5) then '高星' 
                  when extend_info['STAR'] in (3) then '中星' 
                  else '低星' end as hotel_grade_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and ((substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= '2025-05-12') or (substr(order_date,1,10) >= '2026-01-01' and substr(order_date,1,10) <= '2026-05-12'))
)

select t1.order_date,t1.hotel_grade_type,t1.adr_type
       ,nvl(t1.`Q间夜量`,0) as q_room_night
       ,nvl(t2.`C间夜量`,0) as c_room_night
from (
    select order_date
            ,hotel_grade_type
            ,adr_type
            ,sum(room_night) `Q间夜量`
    from q_order 
    where new_mdd='港澳' 
    group by  1,2,3
) t1
left join (
    select dt
            ,hotel_grade_type
            ,adr_type
            ,sum(room_night) `C间夜量`
    from c_order 
    where new_mdd='港澳' 
    group by 1,2,3
) t2 on t1.order_date = t2.dt and t1.hotel_grade_type = t2.hotel_grade_type and t1.adr_type = t2.adr_type
order by t1.order_date desc
;







---- 港澳星级*价格带间夜数据 离店口径
with q_order as (
    select order_date 
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when province_name in ('澳门','香港') then '港澳'  when a.country_name in ('泰国','日本','韩国') then a.country_name  else '其他' end as new_mdd
           ,checkout_date,order_no,room_night
           ,final_gmv
           ,final_commission_after + nvl(ext_plat_certificate,0) ld_yj
           ,a.user_id
           ,hotel_grade
           ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
           ,case  when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
          ,case  when init_gmv / room_night < 600   then '1[0,600)'
                    when init_gmv / room_night >= 600  and init_gmv / room_night < 800   then '2[600,800)'
                    when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                    when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                    else '5[1600+]' 
            end adr_type_new 
           ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
            ,case 
                when datediff(checkin_date,order_date) <= 0 then '凌晨&当天订'
                when datediff(checkin_date,order_date) between 1 and 7 then '1-7天'
                when datediff(checkin_date,order_date) between 8 and 14 then '8-14天'
                when datediff(checkin_date,order_date) between 15 and 21 then '15-21天'
                when datediff(checkin_date,order_date) between 22 and 28 then '22-28天'
                when datediff(checkin_date,order_date) >= 29 then '29+天'
                else '其他' end as early_day
            ,case when hotel_grade in (4,5) then '高星' 
                  when hotel_grade in (3) then '中星' 
                  else '低星' end as hotel_grade_type
            --- qyj + zbj + xyb + qb = C视角Q佣金
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after_new+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after_new+coalesce(ext_plat_certificate,0) end as qyj  --- Q佣金
            ,case when coalesce(four_a, third_a) is not null and dt <= "20221124" then round(coalesce(((coalesce(second_a, first_a) - coalesce(four_a, third_a)) * room_night),(((bp + final_cost) *(1 + p_i_incr) - coalesce(four_a, third_a)) * room_night)),2)
                   when coalesce(four_a, third_a) is not null and order_date <= "2024-03-29" then (coalesce(four_a_reduce, third_a_reduce)*room_night)
                   else coalesce(cashbackmap['follow_price_amount']*room_night,0) end as zbj  --追价补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0)*room_night as xyb  ---协议补
            ,coalesce(cashbackmap['framework_amount'],0) as qb  ---券补
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and ((checkout_date >= '2025-01-01' and checkout_date <= '2025-05-13') or (checkout_date >= '2026-01-01' and checkout_date <= '2026-05-13'))
        and order_no <> '103576132435'
)
,c_order as (--- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'
                when extend_info['COUNTRY'] in ('泰国','日本','韩国') then extend_info['COUNTRY']
                else '其他' end as new_mdd
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,case  when room_fee / extend_info['room_night'] < 400   then '1[0,400)'
                   when room_fee / extend_info['room_night'] >= 400  and room_fee / extend_info['room_night'] < 800   then '2[400,800)'
                   when room_fee / extend_info['room_night'] >= 800  and room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                   when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                   else '5[1600+]' 
            end adr_type 
            ,case when extend_info['STAR'] in (4,5) then '高星' 
                  when extend_info['STAR'] in (3) then '中星' 
                  else '低星' end as hotel_grade_type
            ,substr(checkout_date,1,10) checkout_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and order_status <> 'C'
      and ((substr(checkout_date,1,10) >= '2025-01-01' and substr(checkout_date,1,10) <= '2025-05-13') or (substr(checkout_date,1,10) >= '2026-01-01' and substr(checkout_date,1,10) <= '2026-05-13'))
)

select t1.checkout_date,t1.hotel_grade_type,t1.adr_type
       ,nvl(t1.`Q间夜量`,0) as q_room_night
       ,nvl(t2.`C间夜量`,0) as c_room_night
       ,nvl(t1.gmv,0) as q_gmv
       ,nvl(t1.total_commission,0) as q_total_commission
       ,nvl(t1.total_commission,0) / nullif(t1.gmv,0) as q_commission_rate
from (
    select checkout_date
            ,hotel_grade_type
            ,adr_type
            ,sum(room_night) `Q间夜量`
            ,sum(final_gmv) as gmv
            ,(sum(qyj) + sum(zbj) + sum(xyb) + sum(qb)) as total_commission
    from q_order 
    where new_mdd='港澳' 
    group by  1,2,3
) t1
left join (
    select checkout_date
            ,hotel_grade_type
            ,adr_type
            ,sum(room_night) `C间夜量`
    from c_order 
    where new_mdd='港澳' 
    group by 1,2,3
) t2 on t1.checkout_date = t2.checkout_date and t1.hotel_grade_type = t2.hotel_grade_type and t1.adr_type = t2.adr_type
order by t1.checkout_date desc
;







--- 1、酒店整体分布
with base_info as (
    select  hotel_seq
           ,attrs['whenBuiltV2'] as bus_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,max(attrs['hotelSubCategory']) as hotel_sub_category
    from default.dim_hotel_info_intl_v3  a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s' 
        and attrs['whenBuiltV2'] is not null 
        and hotel_operating_status = '营业中'
    group by 1, 2, 3
)
,bucket_data as (--- 2. 梳理非标标签，并直接计算开业时长打标签
    select  hotel_seq,bus_date,mdd
           ,case when hotel_sub_category in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标'
                 when hotel_sub_category is null then '非标'
                 else '标准' 
            end as standard_hotel
           ,case when to_date(bus_date) >= add_months(date_sub(current_date, 1), -6)  then '1_6个月以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -12) then '2_1年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -24) then '3_2年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -36) then '4_3年以内'
                 when to_date(bus_date) >= add_months(date_sub(current_date, 1), -60) then '5_5年以内'
                 when to_date(bus_date) <  add_months(date_sub(current_date, 1), -60) then '6_5年以上'
                 else '7_未知或异常'
            end as open_bucket
    from base_info
)
,q_data as (
    select order_date
            ,room_night
            ,o.open_bucket  --- 开业时长分桶
            ,o.mdd
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join bucket_data o on a.hotel_seq = o.hotel_seq
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and order_date >= date_sub(bus_date, 30) and  order_date <= date_add(current_date, 1)  
)

--- 4. 结合窗口函数计算大盘占比并输出（占比：本桶数量 / 大盘整体数量）
select  open_bucket
       ,mdd
       ,sum(room_night) as room_night_sum
from q_data
order by 1
;