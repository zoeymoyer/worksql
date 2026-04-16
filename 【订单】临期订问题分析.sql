--- 1、订单数据对比
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
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          
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
          
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join (select order_no, max(purchase_order_no) as purchase_order_no from ihotel_default.dw_purchase_order_info_v3 where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') group by order_no) p on a.order_no = p.order_no
    left join (select distinct partner_order_no, extend_info['vendor_name'] as vendor_name from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da where dt = cast(date_sub(current_date, 1) as string)) c on p.purchase_order_no = c.partner_order_no
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
)
,q_agg as (
    select substr(order_date,1,7) as dt
          ,if(grouping(new_mdd)=1, 'ALL', new_mdd) as new_mdd
          ,if(grouping(user_type)=1, 'ALL', user_type) as user_type
          ,if(grouping(per_type)=1, 'ALL', per_type) as per_type
          ,if(grouping(adr_type)=1, 'ALL', adr_type) as adr_type
          ,if(grouping(supplier_type)=1, 'ALL', supplier_type) as supplier_type
          
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
          ,sum(init_gmv) as q_gmv
          ,sum(final_commission_after) as q_yj
          ,sum(coupon_substract_summary) as q_qe
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as q_non_ref_orders
    from (
        select *
              ,case when supplier_raw = 'C2Q直采' then '直采'
                    when supplier_raw in ('C2Q-Agoda', 'Q-ABE') then 'ABE'
                    when supplier_raw in ('C2Q-代理', 'Q代理') then '代理'
                    else '其他'
               end as supplier_type
        from q_order
    ) t
    group by 1, cube(new_mdd,user_type, per_type, adr_type, supplier_type)
)

,c_user_type as(   --- 用于判定c新老客
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
,c_order as ( 
    select substr(o.order_date,1,10) as dt
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
          ,case when u.min_order_date = substr(o.order_date,1,10) then '新客' else '老客' end as user_type
          
          -- 1. 新版提前订逻辑
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订31+'  
          end as per_type
          
          -- 2. 价格带逻辑
          ,case when room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 400  then '1[0,400)'
                when room_fee / nullif(cast(extend_info['room_night'] as double), 0) >= 400 and room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 800  then '2[400,800)'
                when room_fee / nullif(cast(extend_info['room_night'] as double), 0) >= 800 and room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 1200  then '3[800,1200)'
                when room_fee / nullif(cast(extend_info['room_night'] as double), 0) >= 1200 and room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
          end as adr_type
          
          -- 3. C侧货源判定逻辑
          ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' 
          end as supplier_type
          
          -- 4. 不可取消判定
          ,case when substr(order_date,1,10) >= substr(o.extend_info['LastCancelTime'],1,10) then 'Y' else 'N' end as is_no_cancle
          
          ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
          ,get_json_object(orig_discount_detail, '$.detail[1].amount') as cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id = u.user_id
    left join (
        select distinct order_no as order_no_oc
            , orig_discount_detail
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da_patch
        where dt = '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            -- and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
            and terminal_channel_type = 'app'
            and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
    ) oc on o.order_no = oc.order_no_oc
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
)
,c_agg as (
    select substr(dt,1,7) dt
          ,if(grouping(new_mdd)=1, 'ALL', new_mdd) as new_mdd
          ,if(grouping(user_type)=1, 'ALL', user_type) as user_type
          ,if(grouping(per_type)=1, 'ALL', per_type) as per_type
          ,if(grouping(adr_type)=1, 'ALL', adr_type) as adr_type
          ,if(grouping(supplier_type)=1, 'ALL', supplier_type) as supplier_type
          
          ,count(distinct order_no) as c_orders
          ,sum(room_night) as c_rn
          ,sum(room_fee) as c_gmv
          ,sum(comission) as c_yj
          ,sum(cqe) as c_qe
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as c_non_ref_orders
    from c_order
    group by 1, cube(new_mdd,user_type, per_type, adr_type, supplier_type)
)

-- 【合并输出】
select coalesce(q.dt, c.dt) as `日期`
      ,coalesce(q.new_mdd, c.new_mdd) as `目的地`
      ,coalesce(q.user_type, c.user_type) as `用户类型`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      ,coalesce(q.adr_type, c.adr_type) as `价格带`
      ,coalesce(q.supplier_type, c.supplier_type) as `货源`
      
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(q.q_non_ref_orders, 0) as `Q不可取消订单量`
      ,concat(round(coalesce(q.q_non_ref_orders / nullif(q.q_orders, 0), 0) * 100, 2), '%') as `Q不可取消占比`
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(q.q_gmv, 0) as `Q_GMV`
      ,coalesce(q.q_yj, 0) as `Q佣金`
      ,coalesce(q.q_qe, 0) as `Q券额`
      
      ,coalesce(c.c_orders, 0) as `C订单量`
      ,coalesce(c.c_non_ref_orders, 0) as `C不可取消订单量`
      ,concat(round(coalesce(c.c_non_ref_orders / nullif(c.c_orders, 0), 0) * 100, 2), '%') as `C不可取消占比`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(c.c_gmv, 0) as `C_GMV`
      ,coalesce(c.c_yj, 0) as `C佣金`
      ,coalesce(c.c_qe, 0) as `C券额`
      
      ,coalesce(q.q_rn, 0)  / coalesce(c.c_rn, 0)  as `间夜量QC`
      ,coalesce(q.q_yj, 0)  / coalesce(c.c_yj, 0)  as `收益QC`
      ,(coalesce(q.q_rn, 0) / coalesce(q.q_orders, 0))  / (coalesce(c.c_rn, 0) / coalesce(c.c_orders, 0))  as `单间夜QC`

from q_agg q
full  join c_agg c on q.dt = c.dt and q.new_mdd = c.new_mdd and q.per_type = c.per_type and q.adr_type = c.adr_type and q.supplier_type = c.supplier_type and q.user_type=c.user_type
order by `日期` desc;



--- 2、流量转化
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
,c_user_type as(   --- 用于判定c新老客
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
,q_traf as (
    select a.dt 
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type
          -- 流量中包含的提前订判断
          ,case when datediff(checkin_date, a.dt) < 0 or datediff(checkin_date, a.dt) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, a.dt) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, a.dt) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, a.dt) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, a.dt) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          ,a.user_id
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join user_type b on a.user_id = b.user_id 
    where a.dt >= '2025-01-01' and a.dt <= date_sub(current_date, 1)
      and a.business_type = 'hotel'
      and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    group by 1,2,3,4,5
)
,c_traf as (
    select a.dt
          ,case when provincename in ('澳门','香港') then '港澳'  
                when a.countryname in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.countryname in ('日本','韩国','泰国') then a.countryname 
                else '其他' end as new_mdd
          ,case when a.dt > b.min_order_date then '老客' else '新客' end as user_type 
          
          -- 流量中包含的提前订判断
          ,case when datediff(substr(check_in,1,10), a.dt) < 0 or datediff(substr(check_in,1,10), a.dt) = 0 then '凌晨订&当天订'
                when datediff(substr(check_in,1,10), a.dt) between 1 and 3    then '提前订1-3天'
                when datediff(substr(check_in,1,10), a.dt) between 4 and 7    then '提前订4-7天'
                when datediff(substr(check_in,1,10), a.dt) between 8 and 14   then '提前订8-14天'
                when datediff(substr(check_in,1,10), a.dt) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          ,a.uid
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid = b.ubt_user_id
    where a.dt >= '2025-01-01' and a.dt <= date_sub(current_date, 1)
      and a.device_chl = 'app' and a.page_short_domain = 'dbo'
    group by 1,2,3,4,5
)
,q_order as (
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0 then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          -- 4. 不可取消判定
          ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end as is_non_ref 
          
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
      and terminal_channel_type = 'app' 
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
      and (first_rejected_time is null or date(first_rejected_time) > order_date) 
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid='1'
      and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
)
,c_order as (
    select substr(o.order_date,1,10) as dt
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
          ,case when u.min_order_date = substr(o.order_date,1,10) then '新客' else '老客' end as user_type
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0 then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订31+'  
          end as per_type
          -- 4. 不可取消判定
          ,case when substr(order_date,1,10) >= substr(o.extend_info['LastCancelTime'],1,10) then 'Y' else 'N' end as is_no_cancle
          
          ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
          ,get_json_object(orig_discount_detail, '$.detail[1].amount') as cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id = u.user_id
    left join (
        select distinct order_no as order_no_oc
            , orig_discount_detail
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da_patch
        where dt = '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            -- and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
            and terminal_channel_type = 'app'
            and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
    ) oc on o.order_no = oc.order_no_oc
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(o.order_date,1,10) >= '2025-01-01' and substr(o.order_date,1,10) <= date_sub(current_date, 1)
)
,q_uv_agg as (
    select substr(t.dt,1,7) as mth
          ,if(grouping(t.new_mdd)=1, 'ALL', t.new_mdd) as new_mdd
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.per_type)=1, 'ALL', t.per_type) as per_type
          ,count(distinct t.user_id) as q_mau
    from q_traf t
    group by 1, cube(t.new_mdd, t.user_type, t.per_type)
)
,q_ord_agg as (
    select substr(t.order_date,1,7) as mth
          ,if(grouping(t.new_mdd)=1, 'ALL', t.new_mdd) as new_mdd
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.per_type)=1, 'ALL', t.per_type) as per_type
          ,count(distinct t.order_no) as q_orders
          ,sum(t.room_night) as q_rn
          ,sum(t.init_gmv) as q_gmv
          ,sum(t.final_commission_after) as q_yj
          ,sum(t.coupon_substract_summary) as q_qe
          ,count(distinct case when t.is_non_ref = 'Y' then t.order_no end) as q_non_ref_orders
    from q_order t
    group by 1, cube(t.new_mdd, t.user_type, t.per_type)
)
,c_uv_agg as (
    select substr(t.dt,1,7) as mth
          ,if(grouping(t.new_mdd)=1, 'ALL', t.new_mdd) as new_mdd
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.per_type)=1, 'ALL', t.per_type) as per_type
          ,count(distinct t.uid) as c_mau
    from c_traf t
    group by 1, cube(t.new_mdd, t.user_type, t.per_type)
)
,c_order_agg as (
    select substr(t.dt,1,7) as mth
          ,if(grouping(t.new_mdd)=1, 'ALL', t.new_mdd) as new_mdd
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.per_type)=1, 'ALL', t.per_type) as per_type
          ,count(distinct t.order_no) as c_orders
          ,sum(t.room_night) as c_rn
          ,sum(t.room_fee) as c_gmv
          ,sum(t.comission) as c_yj
          ,sum(t.cqe) as c_qe
          ,count(distinct case when t.is_no_cancle = 'Y' then t.order_no end) as c_non_ref_orders
    from c_order t
    group by 1, cube(t.new_mdd, t.user_type, t.per_type)
)
select coalesce(q.mth, c.mth) as `月份`
      ,coalesce(q.new_mdd, c.new_mdd) as `目的地`
      ,coalesce(q.user_type, c.user_type) as `新老客`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      
      ,coalesce(q.q_mau, 0) as `Q_MAU`
      ,coalesce(qo.q_orders, 0) as `Q订单量`
      ,coalesce(qo.q_rn, 0) as `Q间夜量`
      ,coalesce(qo.q_gmv, 0) as `Q_GMV`
      ,coalesce(qo.q_yj, 0) as `Q佣金`
      ,coalesce(qo.q_qe, 0) as `Q券额`
      ,coalesce(qo.q_non_ref_orders, 0) as `Q不可取消订单量`
      
      ,coalesce(c.c_mau, 0) as `C_MAU`
      ,coalesce(co.c_orders, 0) as `C订单量`
      ,coalesce(co.c_rn, 0) as `C间夜量`
      ,coalesce(co.c_gmv, 0) as `C_GMV`
      ,coalesce(co.c_yj, 0) as `C佣金`
      ,coalesce(co.c_qe, 0) as `C券额`
      ,coalesce(co.c_non_ref_orders, 0) as `C不可取消订单量`

      ,concat(round((qo.q_orders / q.q_mau) * 100, 2), '%') as `Q_CR`
      ,concat(round((co.c_orders / c.c_mau) * 100, 2), '%') as `C_CR`
      ,concat(round((coalesce(qo.q_yj, 0) / coalesce(qo.q_gmv, 0)) * 100, 2), '%') as `Q佣金率`
      ,concat(round((coalesce(co.c_yj, 0) / coalesce(co.c_gmv, 0)) * 100, 2), '%') as `C佣金率`
      ,concat(round((coalesce(qo.q_qe, 0) / coalesce(qo.q_gmv, 0)) * 100, 2), '%') as `Q补贴率`
      ,concat(round((coalesce(co.c_qe, 0) / coalesce(co.c_gmv, 0)) * 100, 2), '%') as `C补贴率`
      ,concat(round((coalesce(qo.q_non_ref_orders, 0) / coalesce(qo.q_orders, 0)) * 100, 2), '%') as `Q不可取消率占比`
      ,concat(round((coalesce(co.c_non_ref_orders, 0) / coalesce(co.c_orders, 0)) * 100, 2), '%') as `C不可取消率占比`

      ,concat(round((coalesce(qo.q_rn, 0) / nullif(coalesce(co.c_rn, 0), 0)) * 100, 2), '%') as `间夜量QC`
      ,concat(round((coalesce(q.q_mau, 0) / nullif(coalesce(c.c_mau, 0), 0)) * 100, 2), '%') as `流量QC`
      ,concat(round(((qo.q_orders / q.q_mau) / (co.c_orders / c.c_mau)) * 100, 2), '%') as `转化率QC`
      ,concat(round(((qo.q_rn / qo.q_orders) / (co.c_rn / co.c_orders)) * 100, 2), '%') as `单间夜QC`
      ,concat(round(coalesce(qo.q_yj, 0) / nullif(coalesce(co.c_yj, 0), 0), 2), '%') as `收益QC`
      ,concat(round(((coalesce(qo.q_yj, 0) / coalesce(qo.q_gmv, 0)) - (coalesce(co.c_yj, 0) / coalesce(co.c_gmv, 0))) * 100, 2), '%') as `佣金率GAP`
      ,concat(round(((coalesce(qo.q_qe, 0) / coalesce(qo.q_gmv, 0)) - (coalesce(co.c_qe, 0) / coalesce(co.c_gmv, 0))) * 100, 2), '%') as `补贴率GAP`
from q_uv_agg q
full outer join c_uv_agg c on q.mth = c.mth and q.new_mdd = c.new_mdd and q.user_type = c.user_type and q.per_type = c.per_type
full outer join q_ord_agg qo on q.mth = qo.mth and q.new_mdd = qo.new_mdd and q.user_type = qo.user_type and q.per_type = qo.per_type
full outer join c_order_agg co on c.mth = co.mth and c.new_mdd = co.new_mdd and c.user_type = co.user_type and c.per_type = co.per_type
order by coalesce(q.mth, c.mth) desc;   


--- 3、产品力数据
with qc_price as (
    select order_date
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as  mdd
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
             
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260101' and dt <= replace(date_sub(current_date, 1),'-','')
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
    )t
    group by order_date,cube(per_type,user_type,new_mdd)
)

select order_date,mdd
      ,user_type
      ,per_type
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
order by 1,2,3,4
;


--- 4、顺畅度数据
with user_type as(
    select user_id
            ,min(order_date) as min_order_date
    from default.mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by user_id
)
,wrapper_mapping as (
    select distinct id 
        ,supplier_wrapper_group as wrapper_id 
    from ihotel_default.ods_qta_supplier
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))=date_sub(current_date, 1) and inter_flag=1
)
,qc_room_mapping as (
    select 
        dt,
        product_id,
        partner_product_id
    from ihotel_default.dwd_supply_qc_product_mapping_di
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
    group by dt, product_id, partner_product_id
)
,is_agent_mapping as (
    select 
        concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) d,
        product_id as room,
        grouptype
    from default.ceq_three_sync_pull_ctrip_qunar_adm_cq_fenxiao_detail a
    left join qc_room_mapping b on a.room = b.partner_product_id and concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) = b.dt
    where concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) >= '2026-01-01'
      and concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)) <= date_sub(current_date, 1)
    group by concat(substr(a.d,1,4),'-',substr(a.d,5,2),'-',substr(a.d,7,2)), product_id, grouptype
)

select a.datas as `日期`
       ,a.supplier,a.per_type,
       `L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
       `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
       `B2O-房态房价一致率`,
       round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas,
            supplier,per_type,
            round((1-(b-e)/nullif((a-e), 0))*100,2) as `L2D-房价一致率`,
            round((1-e/nullif(a, 0))*100,2) as `L2D-房态一致率`,
            round((1-(b-e)/nullif((a-e), 0))*(1-e/nullif(a, 0))*100,2) as `L2D-房态房价一致率`
    from (
        select a.dt as  datas
                ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
                ,if(grouping(per_type)=1,'ALL', per_type) as  per_type,

                count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
                count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b,
                count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e
        from (
            select dt
                    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
                    ,case when province_name in ('澳门','香港') then '港澳'  
                        when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                        when a.country_name in ('日本','韩国','泰国') then a.country_name 
                        else '其他' end as new_mdd
                    ,case when dt > f.min_order_date then '老客' else '新客' end as user_type
                    
                    -- 【新增】L2D阶段的提前订逻辑
                    ,case when datediff(checkin_date, dt) < 0  or datediff(checkin_date, dt) = 0 then '凌晨订&当天订'
                          when datediff(checkin_date, dt) between 1 and 3    then '提前订1-3天'
                          when datediff(checkin_date, dt) between 4 and 7    then '提前订4-7天'
                          when datediff(checkin_date, dt) between 8 and 14   then '提前订8-14天'
                          when datediff(checkin_date, dt) between 15 and 30  then '提前订15-30天'
                          else '提前订31+' 
                     end as per_type
                     
                    ,log_id
                    ,case 
                        when b.grouptype = 'DC' then 'DC'
                        when split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                        when d.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                        else '其他' end as supplier
                    ,ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice
                    ,ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price
                    ,regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full
                    ,get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up
                    ,get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult
                    ,split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] as supplier_id
                    ,split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[0] as room_id
                    ,action_entrance_map['fromforlog'] as is_list
                    ,checkin_date
                    ,checkout_date
                    ,a.user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
            left join is_agent_mapping b on split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[0] = b.room and a.dt = b.d
            left join wrapper_mapping c on split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] = c.id
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) d on c.wrapper_id = d.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                -- and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                -- and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
                and a.user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874','27117935','1263291304','1475831104','1534870051','175004090','223703725','428927726','1005465130','134486580','1534045148','169408570','185495343','185711487','263070154','125896658','140775252','1424343583','1554251482','1070931535','137263924','162660539','273860152','1409683183','1607050360','139741136','196432845')
        ) a
        group by a.dt,cube(supplier,per_type)
    ) a
) a
left join(
    select a.booking_date,
            supplier,per_type,
            round((1-b/nullif(c, 0))*100,2) as `D2B-房态一致率`,
            round((1-a/nullif((c-b), 0))*100,2) as `D2B-房价一致率`,
            round((1-b/nullif(c, 0))*(1-a/nullif((c-b), 0))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
                ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
                ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
                
                ,count(distinct case when ischange='true' and ret='true' then q_trace_id else null end) as a
                ,count(distinct if((ret='false' or ret is null),q_trace_id,null)) as b
                ,count(distinct q_trace_id) as c
        from(
            select dt,log_time,q_trace_id,ret,a.country_name,province_name,err_code,err_message,err_sys,ischange,a.user_id
                    ,case when b.grouptype = 'DC' then 'DC'
                         when supplier_id in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                         when c.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                         else '其他' end as supplier
                    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                          when e.area in ('欧洲','亚太','美洲') then e.area
                          else '其他' end as mdd
                    ,case when province_name in ('澳门','香港') then '港澳'  
                        when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                        when a.country_name in ('日本','韩国','泰国') then a.country_name 
                        else '其他' end as new_mdd
                    ,case when dt > f.min_order_date then '老客' else '新客' end as user_type
                    
                    -- 【新增】D2B阶段的提前订逻辑
                    ,case when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) < 0  or datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) = 0 then '凌晨订&当天订'
                          when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) between 1 and 3    then '提前订1-3天'
                          when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) between 4 and 7    then '提前订4-7天'
                          when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) between 8 and 14   then '提前订8-14天'
                          when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) between 15 and 30  then '提前订15-30天'
                          else '提前订31+' 
                     end as per_type         
            from default.view_dw_user_app_booking_qta_di a
            left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) = b.d
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= '2026-01-01'
                 and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
                 and source='app_intl'
                 and platform in ('adr','ios')
                 and (province_name in ('香港','澳门','台湾') or a.country_name!='中国')
                 and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                 and q_trace_id not like 'f_inter_autotest%'
        )a
        group by concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)),cube(supplier,per_type)
    )a
) b on a.datas=b.booking_date and a.supplier = b.supplier  and a.per_type=b.per_type
left join(
    select booking_date
         ,supplier
         ,per_type
        
         ,round((1-(total_submit_fail-total_submit_coupon)/nullif(total_submit_count, 0))*100,2) as `B2O-房态房价一致率`
    from (
        select booking_date
                ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
                ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
                
            
                ,count(if((ret='false' or ret is null),user_id,null)) as total_submit_fail
                ,count(if((ret='false' or ret is null) and err_message='领券人与入住人不符',user_id,null)) as total_submit_coupon
                ,count(user_id) as total_submit_count
        from(
            select to_date(log_time) as booking_date
                    ,case when b.grouptype = 'DC' then 'DC'
                        when supplier_id in ('1615667','800000164') and (b.grouptype <> 'DC' or b.grouptype is null) then 'C2Q'
                        when c.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                        else '其他' end as supplier
                    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他' end as mdd
                    ,case when province_name in ('澳门','香港') then '港澳'  
                        when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                        when a.country_name in ('日本','韩国','泰国') then a.country_name 
                        else '其他' end as new_mdd
                    ,case when dt > f.min_order_date then '老客' else '新客' end as user_type
                    
                    -- 【新增】B2O阶段的提前订逻辑
                    ,case when datediff(checkin_date, to_date(log_time)) < 0 or datediff(checkin_date, to_date(log_time)) = 0 then '凌晨订&当天订'
                          when datediff(checkin_date, to_date(log_time)) between 1 and 3    then '提前订1-3天'
                          when datediff(checkin_date, to_date(log_time)) between 4 and 7    then '提前订4-7天'
                          when datediff(checkin_date, to_date(log_time)) between 8 and 14   then '提前订8-14天'
                          when datediff(checkin_date, to_date(log_time)) between 15 and 30  then '提前订15-30天'
                          else '提前订31+' 
                     end as per_type
                     
                    ,ret,err_message,a.user_id
            from default.dw_user_app_submit_qta_di a
            left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and to_date(log_time) = b.d
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= '2026-01-01'
                and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
                and source='app_intl'
                and platform in ('adr','ios','AndroidPhone','iPhone')
                and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
                and err_code not in( '-98','784','785')
        ) y 
        group by booking_date,cube(supplier,per_type)
    )a
) c on a.datas=c.booking_date and a.supplier = c.supplier  and a.per_type=c.per_type
order by  1 desc



--- 5、用户画像对比
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
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0 then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          
          -- 2. 价格带逻辑
          ,case when init_gmv / nullif(room_night, 0) < 400   then '1[0,400)'
                when init_gmv / nullif(room_night, 0) >= 400  and init_gmv / nullif(room_night, 0) < 800   then '2[400,800)'
                when init_gmv / nullif(room_night, 0) >= 800  and init_gmv / nullif(room_night, 0) < 1200  then '3[800,1200)'
                when init_gmv / nullif(room_night, 0) >= 1200 and init_gmv / nullif(room_night, 0) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
          end as adr_type 
          
          -- 4. 不可取消判定
          ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end as is_non_ref 
        
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
          ,
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2026-01-01' and a.order_date <= date_sub(current_date, 1)
)
,user_profile as ( --- 用户画像打标
    select *
        ,case when age <= 20 then '青年'
                when age >= 21 and age <= 30 then '年轻'
                when age >= 31 and age <= 45 then '成熟'
                when age > 45 then '中老年' else '未知' end as age_level
    from (
        select user_id,
                gender,     --性别
                city_name,  --常驻地
                prov_name,
                city_level,
                birth_year_month
                ,case when city_level in ('一线','新一线','二线')  then '高线'
                    when city_level in ('三线','四线','五线')  then '低线'
                else  '未知' end as  city_level_type
                ,case when birth_year_month is null then '未知'
                    else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int)
                end AS age
                ,level_desc
        from pub.dim_user_profile_nd
    )
)

select t1.order_month, t1.new_mdd, t1.adr_type, t1.per_type, t1.city_level_type, t1.gender, t1.age_level, t1.level_desc, t1.order_cnt, t1.room_night
from (
    select substr(order_date,1,7) as order_month
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as new_mdd
        ,if(grouping(adr_type)=1,'ALL', adr_type) as adr_type
        ,if(grouping(per_type)=1,'ALL', per_type) as per_type
        ,if(grouping(city_level_type)=1,'ALL', coalesce(city_level_type,'未知')) as city_level_type,
         if(grouping(gender)=1,'ALL', coalesce(gender,'未知')) as gender,
         if(grouping(age_level)=1,'ALL', coalesce(age_level,'未知')) as age_level
        ,if(grouping(level_desc)=1,'ALL', coalesce(level_desc,'未知')) as level_desc
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
    from q_order a
    left join user_profile b on a.user_id = b.user_id
    group by 1,cube(new_mdd,adr_type,per_type,city_level_type,gender,age_level,level_desc)
) t1

order by order_month desc
;

--- 6、用户画像对比
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
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0 then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
          end as per_type
          
          -- 2. 价格带逻辑
          ,case when init_gmv / nullif(room_night, 0) < 400   then '1[0,400)'
                when init_gmv / nullif(room_night, 0) >= 400  and init_gmv / nullif(room_night, 0) < 800   then '2[400,800)'
                when init_gmv / nullif(room_night, 0) >= 800  and init_gmv / nullif(room_night, 0) < 1200  then '3[800,1200)'
                when init_gmv / nullif(room_night, 0) >= 1200 and init_gmv / nullif(room_night, 0) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
          end as adr_type 
          
          -- 4. 不可取消判定
          ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end as is_non_ref 
          
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.order_date >= '2026-01-01' and a.order_date <= date_sub(current_date, 1)
)
,c_user_type as(   --- 用于判定c新老客
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
,c_order as ( 
    select substr(o.order_date,1,10) as dt
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
          ,case when u.min_order_date = substr(o.order_date,1,10) then '新客' else '老客' end as user_type
          
          -- 1. 新版提前订逻辑
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订31+'  
          end as per_type
          
          -- 2. 价格带逻辑
          ,case when room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 400  then '1[0,400)'
                when room_fee / nullif(cast(extend_info['room_night'] as double), 0) >= 400 and room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 800  then '2[400,800)'
                when room_fee / nullif(cast(extend_info['room_night'] as double), 0) >= 800 and room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 1200  then '3[800,1200)'
                when room_fee / nullif(cast(extend_info['room_night'] as double), 0) >= 1200 and room_fee / nullif(cast(extend_info['room_night'] as double), 0) < 1600  then '4[1200,1600)'
                else '5[1600+]' 
          end as adr_type
          
          -- 3. C侧货源判定逻辑
          ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' 
          end as supplier_type
          
          -- 4. 不可取消判定
          ,case when substr(order_date,1,10) >= substr(o.extend_info['LastCancelTime'],1,10) then 'Y' else 'N' end as is_no_cancle
          
          ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
          ,case when extend_info['user_grade'] = 'Normal' then '大众'
                when extend_info['user_grade'] = 'Silver' then '白银'
                when extend_info['user_grade'] = 'Gold' then '黄金'
                when extend_info['user_grade'] = 'Platnium' then '铂金'
                when extend_info['user_grade'] = 'Diamond' then '钻石'
                when extend_info['user_grade'] = 'Gold Diamond' then '金钻'
                when extend_info['user_grade'] = 'Black Diamond' then '黑钻'
                else '其他' end as user_level
          ,get_json_object(orig_discount_detail, '$.detail[1].amount') as cqe  -- C_券额
          ,user_cityname
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id = u.user_id
    left join (
        select distinct order_no as order_no_oc
            , orig_discount_detail
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da_patch
        where dt = '%(FORMAT_DATE)s'
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            -- and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
            and terminal_channel_type = 'app'
            and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
    ) oc on o.order_no = oc.order_no_oc
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
)
,user_profile as ( --- 用户画像打标
    select *
        ,case when age <= 20 then '青年'
                when age >= 21 and age <= 30 then '年轻'
                when age >= 31 and age <= 45 then '成熟'
                when age > 45 then '中老年' else '未知' end as age_level
    from (
        select user_id,
                gender,     --性别
                city_name,  --常驻地
                prov_name,
                city_level,
                birth_year_month
                ,case when city_level in ('一线','新一线','二线')  then '高线'
                    when city_level in ('三线','四线','五线')  then '低线'
                else  '未知' end as  city_level_type
                ,case when birth_year_month is null then '未知'
                    else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int)
                end AS age
                ,level_desc
        from pub.dim_user_profile_nd
    )
)
,q_agg as (
        select substr(order_date,1,7) as order_month
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as new_mdd
        ,if(grouping(adr_type)=1,'ALL', adr_type) as adr_type
        ,if(grouping(per_type)=1,'ALL', per_type) as per_type
        ,if(grouping(level_desc)=1,'ALL', coalesce(level_desc,'未知')) as level_desc
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
    from q_order a
    left join user_profile b on a.user_id = b.user_id
    group by 1,cube(new_mdd,adr_type,per_type,level_desc)
)
,c_agg as (
    select substr(dt,1,7) as order_month
        ,if(grouping(new_mdd)=1,'ALL', new_mdd) as new_mdd
        ,if(grouping(adr_type)=1,'ALL', adr_type) as adr_type
        ,if(grouping(per_type)=1,'ALL', per_type) as per_type
        ,if(grouping(user_level)=1,'ALL', coalesce(user_level,'未知')) as level_desc
        ,count(distinct order_no) as order_cnt
        ,sum(room_night) as room_night
    from c_order 
    group by 1,cube(new_mdd,adr_type,per_type,user_level)
)

select t1.order_month, t1.new_mdd, t1.adr_type, t1.per_type, coalesce(t1.level_desc, t2.level_desc) as level_desc, t1.order_cnt as q_order_cnt, t1.room_night as q_room_night
    ,t2.order_cnt as c_order_cnt, t2.room_night as c_room_night
from q_agg t1
full join c_agg t2 on t1.order_month = t2.order_month and t1.new_mdd = t2.new_mdd and t1.adr_type = t2.adr_type and t1.per_type = t2.per_type and t1.level_desc = t2.level_desc
order by t1.order_month desc
;



-- 7、流量(同质化+物理)
with reason_table as (
    select distinct
        compare_result_id
        ,first_level_ascribe
        ,second_level_ascribe
    from ihotel_default.ods_ihotel_cq_lose_case_ascribe_realtime
    where dt between '2026-01-01' and date_sub(current_date,1)
        AND business_type='intl_crawl_cq_api_userview'
        AND lose_type='DISCOUNT_BASE_PRICE_LOSE' 
        AND compare_type='PHYSICAL_ROOM_TYPE_LOWEST' 
)
select a.order_date
    , case 
            when province_name in ('澳门','香港') then province_name 
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
            when e.area in ('欧洲','亚太','美洲') then e.area 
            else '其他' 
    end as `目的地` 
    ,chased_discount_price_compare_result
    ,first_level_ascribe
    ,second_level_ascribe
    ,per_type
    ,count(distinct id) as `次数`
    ,sum(bp_advantage_amount) as `折后底价Q-C`
    ,sum(ctrip_pay_price) as `C支付价`
from(
        select id
        , order_date
        , orderNum
        , country_name
        , province_name
        , chased_discount_price_compare_result
        , bp_advantage_amount
        , ctrip_pay_price
        from(
            select *
                ,concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2)) as order_date
                ,qunar_price_info['traceId'] as trace_id
                ,qunar_price_info['orderNum'] as orderNum
                ,-chased_discount_price_diff as bp_advantage_amount
                ,-chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate
                ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) < 0 or datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) = 0 then '凌晨订&当天订'
                    when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 1 and 3    then '提前订1-3天'
                    when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 4 and 7    then '提前订4-7天'
                    when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14   then '提前订8-14天'
                    when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 30  then '提前订15-30天'
                    else '提前订31+' 
                end as per_type
            from default.dwd_hotel_cq_compare_price_result_intl_hi a
            where concat(substr(a.dt, 1, 4), '-', substr(a.dt, 5, 2), '-', substr(a.dt, 7, 2)) between '2026-01-01' and date_sub(current_date,1)
                and business_type='intl_crawl_cq_api_userview'
                and compare_type='PHYSICAL_ROOM_TYPE_LOWEST' --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
                and room_type_cover='Qmeet'
                and ctrip_room_status='true'
                and qunar_room_status='true'
            ) cq_compare_raw
        left join reason_table b on cq_compare_raw.id = b.compare_result_id
) a
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
group by 1,2,3,4,5,6