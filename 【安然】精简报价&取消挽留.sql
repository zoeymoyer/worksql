-- 1、挽留核心指标
with cancel_page AS ( --- O页取消页
    select concat(substr(dt, 1, 4),'-',substr(dt, 5, 2),'-',substr(dt, 7, 2)) AS dt
         ,user_name
         ,get_json_object(get_json_object(value,'$.ext.exposeLogData'), '$.orderNo') as orderNo
         ,get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace('2026-05-01' ,'-','') and replace(date_sub(current_date, 1) ,'-','')
        and key = 'ihotel/OrderDetail/OrderInfo/click/actionBtn'
        and get_json_object(value, '$.ext.button.menu') = '取消订单'
    group by 1,2,3,4
)
,wanliu_show as (--- 挽留弹窗曝光
    select  concat(substr(dt, 1, 4),'-',substr(dt, 5, 2),'-',substr(dt, 7, 2)) AS dt
            ,user_name
            ,get_json_object(value, '$.common.traceId') as trace_id
            ,count(1) pv
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace('2026-05-01' ,'-','') and replace(date_sub(current_date, 1) ,'-','')
        and key in ('ihotel/OrderDetail/cancelReason/show/cancelBlock')
        and get_json_object(value, '$.ext.trendType') in ('cash','all') --限制领取红包和红包+积分
    group by 1,2,3
)
,wanliu_order as ( --- 挽留成功：点击领取
    select  concat(substr(dt, 1, 4),'-',substr(dt, 5, 2),'-',substr(dt, 7, 2)) AS dt,
            user_name,
            get_json_object(value, '$.common.traceId') as trace_id
    from default.dw_qav_ihotel_track_info_di
    where  dt between replace('2026-05-01' ,'-','') and replace(date_sub(current_date, 1) ,'-','')
        and key = 'ihotel/OrderDetail/cancelReason/click/cancelBlocked'
        and get_json_object(value, '$.ext.trendType') in ('cash','all')--限制领取红包和红包+积分
    group by 1,2,3
)
,cancelOrder AS (--- 取消订单
    select  order_no,
            DATE(first_cancelled_time) AS cancelDate,
            user_id,
            user_name,
            hotel_seq,
            room_night,
            case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                    then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                else init_commission_after+coalesce(ext_plat_certificate,0)
                end as cancel_yj
            ,checkout_date
    from default.mdw_order_v3_international
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') 
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type = 'app'
        and first_cancelled_time is not null
        and order_status = 'CANCELLED'
        and is_valid = '1'
        and order_no <> '103576132435'
        and DATE(first_cancelled_time) >= '2025-02-03'
        and DATE(first_cancelled_time) <= date_sub(current_date, 1)
)
,order_all as (---所有订单
    select *
            ,case when yj / init_gmv  < 0 then '0负佣'  
                  when yj / init_gmv  >= 0 and yj / init_gmv < 0.03 then '1低佣[0-3%]' 
                  when yj / init_gmv  >= 0.03 and yj / init_gmv < 0.1 then '2中佣(3-10%]'
                  else '3高佣(10%+]' 
            end as yj_type
            ,case when bp_realized is not null then 'Y' else 'N' end is_bxt  -- 是否变现提
            ,case when fx is null then '0未返现' 
                  when fx / init_gmv <  0.05 then '1挽留深度[0-5%)' 
                  when fx / init_gmv >= 0.05 and fx / init_gmv < 0.1  then '2挽留深度[5-10%)'
                  when fx / init_gmv >= 0.1  and fx / init_gmv < 0.15 then '3挽留深度[10-15%)'
                  when fx / init_gmv >= 0.15 and fx / init_gmv < 0.2  then '4挽留深度[15-20%)'
                  else '5挽留深度(20%+]' 
            end as cb_type
    from (
        select order_no,user_name,room_night,init_gmv,final_gmv
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as yj
            ,case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                  then (final_commission_after+coalesce(coupon_substract_summary ,0)) 
                  when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') 
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0)) 
                else final_commission_after+coalesce(ext_plat_certificate,0) 
                end as  final_commission_after
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,get_json_object(cancel_red_packet_data_track_map, '$.actual_cash_back_amount')as fx
            ,case when a.country_name = '日本' then '日本' else '非日本' end is_jp
            ,coalesce(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) as bp_realized --实际变现底价优势金额（间夜均） 变现提
            ,order_status,checkout_date,order_date,is_valid
        from default.mdw_order_v3_international a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') 
    )
)
,checkout_fx_order as ( --- 实际返现离店订单数据
    select checkout_date
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,count(distinct order_no) as fx_order_cnt_ld  -- 实际返现订单量
            ,sum(fx) as fx_amt_ld                         -- 实返金额
            ,sum(room_night) as fx_room_night_ld          -- 返现间夜量
            ,sum(final_commission_after) as fx_commission_ld -- 返现佣金
            ,sum(final_gmv) as fx_gmv_ld                  -- 返现GMV
    from order_all
    where order_status not in ('CANCELLED','REJECTED')
         and (fx > 0)  -- 返现金额大于0
         and checkout_date >= '2026-05-01' and checkout_date <= date_sub(current_date, 1)
         and is_valid = 1 and order_no <> '103576132435'
    group by checkout_date,cube(mdd)
)
,checkout_fx_orde_all as ( --- 离店领取返现订单数据
    select checkout_date
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,count(distinct order_no) as fx_order_cnt_ld_all  -- 实际返现订单量
            ,sum(fx) as fx_amt_ld_all                         -- 实返金额
            ,sum(room_night) as fx_room_night_ld_all          -- 返现间夜量
            ,sum(final_commission_after) as fx_commission_ld_all -- 返现佣金
            ,sum(final_gmv) as fx_gmv_ld_all                  -- 返现GMV
    from order_all
    where (fx > 0)  -- 返现金额大于0
         and checkout_date >= '2026-05-01' and checkout_date <= date_sub(current_date, 1)
         and is_valid = 1 and order_no <> '103576132435'
    group by checkout_date,cube(mdd)
)
,checkout_order as (  --- 所有离店订单数据
    select checkout_date
            ,if(grouping(mdd)=1,'ALL',mdd) as mdd
            ,count(distinct order_no) as order_cnt_ld  -- 实际返现订单量
            ,sum(fx) as amt_ld                         -- 实返金额
            ,sum(room_night) as room_night_ld          -- 返现间夜量
            ,sum(final_commission_after) as commission_ld -- 返现佣金
            ,sum(final_gmv) as gmv_ld                  -- 返现GMV
    from order_all
    where order_status not in ('CANCELLED','REJECTED')
         and checkout_date >= '2026-05-01' and checkout_date <= date_sub(current_date, 1)
         and is_valid = 1 and order_no <> '103576132435'
    group by checkout_date,cube(mdd)
)  
,q_data as ( -- Q取消率数据
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when a.country_name = '日本' then 'Y' else 'N' end is_jp
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status,init_gmv,room_night
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
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2026-05-01' and date_sub(current_date, 1)
)
,c_order as( --- C取消率数据
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                  when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                  when c.area in ('欧洲','亚太','美洲') then c.area
                  else '其他' 
            end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,o.user_id,order_no,room_fee,order_status
            ,extend_info['room_night'] room_night
            --- 预定当日是否有取消单或被拒单
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ,case when (order_status = 'C' and substr(o.extend_info['LastCancelTime'],1,10) > substr(order_date,1,10)) 
                   then 'Y' else 'N' 
            end is_cancel_d0 
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
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
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2026-05-01' and date_sub(current_date, 1) -- 退房日期范围
)
,qc_cancel_data as (
    select t1.checkout_date
        ,t1.mdd
        ,`Q订单`
        ,`Q取消率`   -- `Q取消订单_当日` / `Q未取消订单_当日`
        ,`Q不可取消订单占比`  -- `Q不可取消订单` / `Q订单`
        ,`Q未取消订单_当日`
        ,`Q取消订单_当日`
        ,`Q不可取消订单`
        ,`C订单`
        ,`C取消率`          -- 1-`C已离店订单` / `C未取消订单_当日`
        ,`C不可取消订单占比`  -- `C不可取消订单` / `C订单`
        ,`C不可取消订单`
        ,`C已离店订单`
        ,`C未取消订单_当日`
    from (
        select checkout_date
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,count(distinct order_no) as `Q订单`
            ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单_当日`
            ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单_当日`
            ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
            ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
            ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        from q_data
        group by checkout_date,cube(mdd)
    ) t1 left join (
        select checkout_date
            ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
            ,count(distinct order_no) as `C订单`
            ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单_当日`
            ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as      `C取消订单_当日`
            ,count(distinct case when order_status <> 'C' then order_no end) as    `C已离店订单`
            ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
            ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
            ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
            ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
        from c_order
        group by checkout_date,cube(mdd)

    )t2 on t1.checkout_date=t2.checkout_date and t1.mdd=t2.mdd
)


select t1.dt
       ,t1.mdd
       ,t1.`进入取消页面订单量`
       ,t1.`进入取消页面展示弹窗订单量`
       ,t1.`挽留成功订单量(预定)`
       ,t1.`挽留成功间夜量(预定)`
       ,t1.`挽留成功佣金(预定)`
       ,t1.`挽留成功GMV(预定)`
       ,t1.`挽留成功返现金额(预定)`
       ,t1.`取消订单量`
       ,t1.`取消率`
       ,t1.`挽留成功率(预定)`
       ,t1.`挽留弹窗覆盖率`
       ,t2.fx_order_cnt_ld  -- 实际返现订单量(离店)
       ,t2.fx_amt_ld  -- 实际返现金额(离店)
       ,t2.fx_room_night_ld -- 实际返现间夜量(离店)
       ,t2.fx_commission_ld -- 实际返现佣金(离店)
       ,t2.fx_gmv_ld -- 实际返现GMV(离店)
       ,t3.order_cnt_ld -- 离店订单量(离店)
       ,t3.room_night_ld -- 离店间夜量(离店)
       ,t3.commission_ld -- 离店佣金(离店)
       ,t3.gmv_ld  -- 离店GMV(离店)
       ,t4.`Q订单`
       ,t4.`Q取消率`
       ,t4.`Q不可取消订单占比`
       ,t4.`Q未取消订单_当日`
       ,t4.`Q取消订单_当日`
       ,t4.`Q不可取消订单`
       ,t4.`C订单`
       ,t4.`C取消率`
       ,t4.`C不可取消订单占比`
       ,t4.`C不可取消订单`
       ,t4.`C已离店订单`
       ,t4.`C未取消订单_当日`
       ,t5.fx_order_cnt_ld_all 
       ,t5.fx_amt_ld_all 
       ,t5.fx_room_night_ld_all 
       ,t5.fx_commission_ld_all 
       ,t5.fx_gmv_ld_all

       ,t1.`点击领取订单量(预定)`
       ,t1.`点击领取间夜量(预定)`
       ,t1.`点击领取佣金(预定)`
       ,t1.`点击领取GMV(预定)`
       ,t1.`点击领取返现金额(预定)`
from (
    select  a.dt,
            if(grouping(mdd)=1,'ALL', mdd) as  mdd,
            count(distinct a.orderNo)  as `进入取消页面订单量`,
            count(distinct case when d.user_name is not null then a.orderNo end)  as `进入取消页面展示弹窗订单量`,

            count(distinct case when e.user_name is not null then a.orderNo end)  as `点击领取订单量(预定)`,
            sum(case when e.user_name is not null then a.room_night end)  as `点击领取间夜量(预定)`,
            sum(case when e.user_name is not null then a.yj end)  as `点击领取佣金(预定)`,
            sum(case when e.user_name is not null then a.init_gmv end)  as `点击领取GMV(预定)`,
            sum(case when e.user_name is not null then a.fx end)  as `点击领取返现金额(预定)`,

            count(distinct case when e.user_name is not null and c.order_no is not null then a.orderNo end)  as `挽留成功订单量(预定)`,
            sum(case when e.user_name is not null  and c.order_no is not null then a.room_night end)  as `挽留成功间夜量(预定)`,
            sum(case when e.user_name is not null  and c.order_no is not null then a.yj end)  as `挽留成功佣金(预定)`,
            sum(case when e.user_name is not null  and c.order_no is not null then a.init_gmv end)  as `挽留成功GMV(预定)`,
            sum(case when e.user_name is not null  and c.order_no is not null then a.fx end)  as `挽留成功返现金额(预定)`,
            count(distinct c.order_no) as `取消订单量`,

            count(distinct c.order_no) / count(distinct a.orderNo) as `取消率`,
            count(distinct case when e.user_name is not null then a.orderNo end) / count(distinct a.orderNo) as `挽留成功率(预定)`,
            count(distinct case when d.user_name is not null then a.orderNo end) / count(distinct a.orderNo) as `挽留弹窗覆盖率`
    from (
        select t1.*,order_no,mdd,fx,room_night,final_commission_after,init_gmv,yj
        from  cancel_page t1 
        left join order_all t2 on t1.orderNo=t2.order_no
    ) a
    -- 取消订单
    left join cancelOrder c on a.user_name = c.user_name and a.dt = c.cancelDate and c.order_no = a.orderNo
    -- 挽留弹窗曝光
    left join wanliu_show d on a.user_name = d.user_name and a.dt = d.dt and a.trace_id=d.trace_id
    -- 挽留成功订单（点击领取）
    left join wanliu_order e on a.user_name=e.user_name and a.dt=e.dt
    group by a.dt,cube(a.mdd)
) t1
left join checkout_fx_order t2 on t1.dt = t2.checkout_date and t1.mdd = t2.mdd 
left join checkout_order t3 on t1.dt = t3.checkout_date and t1.mdd = t3.mdd 
left join qc_cancel_data t4 on t1.dt = t4.checkout_date and t1.mdd = t4.mdd
left join checkout_fx_orde_all t5 on t1.dt=t5.checkout_date and t1.mdd = t5.mdd
order by t1.dt desc, t1.mdd
;





--- 2、精简报价需求
with display_table as (
    select  a.dt
        --    ,case when c_isagent = 'false'                                 then '直采'
        --          when c_isagent = 'true'                                  then 'C2Q'
        --          when d.wrapper_name in ('Agoda', 'Booking', 'Expedia') then 'ABE'
        --          when e.company_main is not null                          then e.company_main
        --          else '其他小代理' 
        --     end                                                           as `渠道`
           ,traceId
           ,room_id 
           ,hotel_seq
           ,physical_room_id
           ,price
        -- case 
        --                         when cancel_type = 0 then '不可取消'
        --                         when cancel_type = 2 then '可取消'
        --                         else '其他'
        --         end as cancel_type,
        -- case
        --     when ppb_payment = 0 then '现付'
        --     when ppb_payment = 1 then '预付'
        --     else '其他'
        -- end as `付款方式`,   
        -- case    
        --     when vendor_type = 2 then '猜喜'
        --     else '其他'
        -- end as `猜喜标签`,
        -- case 
        --     when tiled_price = 'false' then '折叠'
        --     else '平铺'
        -- end as `是否折叠`
    from (
        select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as dt
               ,hotel_seq
               ,get_json_object(extendinfomap, '$.traceId')                           as traceId
               ,substring_index(room_id, '_', 1)                                      as room_id 
               ,physical_room_id
               ,max(price)                                                            as price
            --    ,max(c_isagent)                                                        as c_isagent
            --    ,max(source_wrapper_id)                                                as wrapper_id
            --    ,min(product_room_index)                                               as product_room_index
            --    ,max(qs_extend_map['vendor_type'])                                     as vendor_type
        from ihotel_default.dw_hotel_price_display_v2
        where dt between '20260515' and '20260525'
            and get_json_object(extendinfomap, '$.traceId') is not null -- traceid不为空
            and source_wrapper_id is not null      -- 渠道id不为空
            and substring_index(room_id, '_', 1) is not null -- 产品房型不为空
            -- and adults_num < actual_max_num -- 搜索人数小于最大可入住人数（排除不符合入住条件曝光产品）
            and price is not null    -- 价格不为空
            and physical_room_id is not null -- 物理房型不为空
            and product_room_index in (1, 2, 3, 4, 5, 6)  --- 只取前6个房型
        group by 1,2,3,4,5
    ) a
    -- left join (
    --     select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da
    -- ) d on a.wrapper_id = d.wrapper_id
    -- left join (
    --     select distinct dt
    --            ,wrapper_id
    --            ,company_main 
    --     from default.dwd_supply_wrapper_detail_di 
    --     where dt between '2026-03-11' and '2026-03-17'
    -- ) e on a.dt = e.dt and a.wrapper_id = e.wrapper_id
    group by 1,2,3,4,5,6
)
,q_order_app as (----订单明细表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade
            ,hotel_seq
            ,physical_room_id
            ,qta_product_id -- room_id
            ,get_json_object(extendinfomap,'$.traceId') as traceId
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2025-05-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,min_price_per_trace_physical as (
    select dt
           ,traceId
           ,hotel_seq
           ,physical_room_id
           ,min(price)                                                    as min_price
    from display_table
    group by 1,2,3,4
)
,max_price_per_trace_physical as (
    select dt
           ,traceId
           ,hotel_seq
           ,physical_room_id
           ,max(price)                                                    as max_price
    from display_table
    group by 1,2,3,4
)
,order_price as (
    select t1.dt
           ,t1.traceId
           ,t1.hotel_seq
           ,t1.physical_room_id
           ,max(t1.price) as order_price
    from display_table t1
    inner join q_order_app t4 on t1.dt = t4.order_date
        and t1.traceId = t4.traceId
        and t1.hotel_seq = t4.hotel_seq
        and t1.physical_room_id = t4.physical_room_id   
        and t1.room_id = t4.qta_product_id
    group by 1,2,3,4
)

,flagged as (
    select t1.dt
           ,t1.traceId
           ,t1.hotel_seq
           ,t1.physical_room_id
           ,t1.max_price
           ,t3.min_price
           ,t4.order_price
    from max_price_per_trace_physical t1
    left join min_price_per_trace_physical t3 on t1.dt = t3.dt
        and t1.traceId = t3.traceId
        and t1.hotel_seq = t3.hotel_seq
        and t1.physical_room_id = t3.physical_room_id
    left join order_price t4 on t1.dt = t4.dt
        and t1.traceId = t4.traceId
        and t1.hotel_seq = t4.hotel_seq
        and t1.physical_room_id = t4.physical_room_id   
)


select dt,price_max_min_diff_level
        ,'ALL' price_ord_max_diff_level
        ,count(distinct traceId) as traceId_cnt
        ,count(distinct hotel_seq) as hotel_cnt
        ,count(distinct physical_room_id) as physical_room_cnt
from (
    select dt,traceId,hotel_seq,physical_room_id,max_price,min_price,order_price
        ,case when max_price / min_price <= 1.2 then '1.2倍内'
                when max_price / min_price > 1.2 and max_price / min_price <= 1.5 then '1.2-1.5倍'
                when max_price / min_price > 1.5 and max_price / min_price <= 2 then '1.5-2倍'
                when max_price / min_price > 2 and max_price / min_price <= 3 then '2-3倍'
                else '3倍以上' end as price_max_min_diff_level
        ,case when order_price is null then '未成单'
            when order_price / min_price <= 1.2 then '1.2倍内'
            when order_price / min_price > 1.2 and order_price / min_price <= 1.5 then '1.2-1.5倍'
            when order_price / min_price > 1.5 and order_price / min_price <= 2 then '1.5-2倍'
            when order_price / min_price > 2 and order_price / min_price <= 3 then '2-3倍'
            else '3倍以上' end as price_ord_max_diff_level
    from flagged
) t
group by 1,2
union all
select dt,'ALL' price_max_min_diff_level
        ,price_ord_max_diff_level
        ,count(distinct traceId) as traceId_cnt
        ,count(distinct hotel_seq) as hotel_cnt
        ,count(distinct physical_room_id) as physical_room_cnt
from (
    select dt,traceId,hotel_seq,physical_room_id,max_price,min_price,order_price
        ,case when max_price / min_price <= 1.2 then '1.2倍内'
                when max_price / min_price > 1.2 and max_price / min_price <= 1.5 then '1.2-1.5倍'
                when max_price / min_price > 1.5 and max_price / min_price <= 2 then '1.5-2倍'
                when max_price / min_price > 2 and max_price / min_price <= 3 then '2-3倍'
                else '3倍以上' end as price_max_min_diff_level
        ,case when order_price is null then '未成单'
            when order_price / min_price <= 1.2 then '1.2倍内'
            when order_price / min_price > 1.2 and order_price / min_price <= 1.5 then '1.2-1.5倍'
            when order_price / min_price > 1.5 and order_price / min_price <= 1.6 then '1.5-1.6倍'
            when order_price / min_price > 1.6 and order_price / min_price <= 1.7 then '1.6-1.7倍'
            when order_price / min_price > 1.7 and order_price / min_price <= 1.8 then '1.7-1.8倍'
            when order_price / min_price > 1.8 and order_price / min_price <= 1.9 then '1.8-1.9倍'
            when order_price / min_price > 1.9 and order_price / min_price <= 2 then '1.9-2倍'
            when order_price / min_price > 2 and order_price / min_price <= 3 then '2-3倍'
            else '3倍以上' end as price_ord_max_diff_level
    from flagged
) t
group by 1,3
;