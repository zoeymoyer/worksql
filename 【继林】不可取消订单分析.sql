--- 1、分货源分ADR
with q_data as (
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)


select t1.checkout_date,t1.supplier,t1.adr_type,t1.per_type
      ,`Q订单`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C不可取消订单`, `C不可取消订单占比`
      ,`Q不可取消订单间夜`,`Q不可取消订单GMV`,`Q不可取消订单ADR`
      ,`Q可取消订单间夜`,`Q可取消订单GMV`,`Q可取消订单ADR`
      ,`C不可取消订单间夜`,`C不可取消订单GMV`,`C不可取消订单ADR`
      ,`C可取消订单间夜`,`C可取消订单GMV`,`C可取消订单ADR`
from (
    select checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单间夜`
        ,sum(case when is_non_ref='Y' then init_gmv end) `Q不可取消订单GMV`
        ,sum(case when is_non_ref='Y' then init_gmv end) / sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单ADR`
        ,sum(case when is_non_ref='N' then room_night end) `Q可取消订单间夜`
        ,sum(case when is_non_ref='N' then init_gmv end) `Q可取消订单GMV`
        ,sum(case when is_non_ref='N' then init_gmv end) / sum(case when is_non_ref='N' then room_night end) `Q可取消订单ADR`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by checkout_date,cube(supplier_type,adr_type,per_type)
) t1 left join (
    select checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
          ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单间夜`
          ,sum(case when is_no_cancle='Y' then room_fee end) `C不可取消订单GMV`
          ,sum(case when is_no_cancle='Y' then room_fee end) / sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单ADR`
          ,sum(case when is_no_cancle='N' then room_night end) `C可取消订单间夜`
          ,sum(case when is_no_cancle='N' then room_fee end) `C可取消订单GMV`
          ,sum(case when is_no_cancle='N' then room_fee end) / sum(case when is_no_cancle='N' then room_night end) `C可取消订单ADR`
    from c_order
    group by checkout_date,cube(supplier,adr_type,per_type)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.adr_type=t2.adr_type and t1.per_type=t2.per_type
order by 1,2,3
;


--- 2、分酒店月度直采渠道数据
with q_data as (
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)
,q_c_hotel_mapping as (
    select
        distinct
        hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
)

select t1.mth,t1.hotel_seq,hotel_name
      ,`Q订单`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C不可取消订单`, `C不可取消订单占比`
      ,`Q不可取消订单间夜`,`Q不可取消订单GMV`,`Q不可取消订单ADR`
      ,`Q可取消订单间夜`,`Q可取消订单GMV`,`Q可取消订单ADR`
      ,`C不可取消订单间夜`,`C不可取消订单GMV`,`C不可取消订单ADR`
      ,`C可取消订单间夜`,`C可取消订单GMV`,`C可取消订单ADR`
from (
    select substr(checkout_date,1,7)mth,hotel_seq
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单间夜`
        ,sum(case when is_non_ref='Y' then init_gmv end) `Q不可取消订单GMV`
        ,sum(case when is_non_ref='Y' then init_gmv end) / sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单ADR`
        ,sum(case when is_non_ref='N' then room_night end) `Q可取消订单间夜`
        ,sum(case when is_non_ref='N' then init_gmv end) `Q可取消订单GMV`
        ,sum(case when is_non_ref='N' then init_gmv end) / sum(case when is_non_ref='N' then room_night end) `Q可取消订单ADR`
    from (
        select * 
        from q_data
        where supplier = 'C2Q直采'
    )t
    group by 1,2
) t1 left join (
    select substr(checkout_date,1,7)mth,t2.hotel_seq
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单间夜`
          ,sum(case when is_no_cancle='Y' then room_fee end) `C不可取消订单GMV`
          ,sum(case when is_no_cancle='Y' then room_fee end) / sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单ADR`
          ,sum(case when is_no_cancle='N' then room_night end) `C可取消订单间夜`
          ,sum(case when is_no_cancle='N' then room_fee end) `C可取消订单GMV`
          ,sum(case when is_no_cancle='N' then room_fee end) / sum(case when is_no_cancle='N' then room_night end) `C可取消订单ADR`
    from c_order t1
    left join q_c_hotel_mapping t2 on t1.hotel_seq=t2.partner_hotel_id
    where supplier = '直采'
    group by 1,2

)t2 on t1.mth=t2.mth  and t1.hotel_seq=t2.hotel_seq
left join (select hotel_seq,hotel_name from default.dim_hotel_info_intl_v3 where dt = '20260304') t3 on t1.hotel_seq=t3.hotel_seq
order by 1,2
;




--- 分货源分ADR
with q_data as (
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-04-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
        AND substr(o.checkout_date, 1, 10) between '2025-04-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)


select t1.checkout_date,t1.supplier,t1.per_type
      ,`Q订单`,`Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C非当日取消率`,`C不可取消订单`, `C不可取消订单占比`
from (
    select checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by checkout_date,cube(supplier_type,per_type)
) t1 left join (
    select checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C非当日取消率`
    from c_order
    group by checkout_date,cube(supplier,per_type)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.per_type=t2.per_type
order by 1,2,3
;



--- 4、月度分货源分ADR
with q_data as (
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)


select t1.checkout_date,t1.supplier,t1.adr_type,t1.per_type
      ,`Q订单`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C不可取消订单`, `C不可取消订单占比`
      ,`Q不可取消订单间夜`,`Q不可取消订单GMV`,`Q不可取消订单ADR`
      ,`Q可取消订单间夜`,`Q可取消订单GMV`,`Q可取消订单ADR`
      ,`C不可取消订单间夜`,`C不可取消订单GMV`,`C不可取消订单ADR`
      ,`C可取消订单间夜`,`C可取消订单GMV`,`C可取消订单ADR`
from (
    select substr(checkout_date,1,7) checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单间夜`
        ,sum(case when is_non_ref='Y' then init_gmv end) `Q不可取消订单GMV`
        ,sum(case when is_non_ref='Y' then init_gmv end) / sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单ADR`
        ,sum(case when is_non_ref='N' then room_night end) `Q可取消订单间夜`
        ,sum(case when is_non_ref='N' then init_gmv end) `Q可取消订单GMV`
        ,sum(case when is_non_ref='N' then init_gmv end) / sum(case when is_non_ref='N' then room_night end) `Q可取消订单ADR`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by substr(checkout_date,1,7),cube(supplier_type,adr_type,per_type)
) t1 left join (
    select substr(checkout_date,1,7) checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
          ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单间夜`
          ,sum(case when is_no_cancle='Y' then room_fee end) `C不可取消订单GMV`
          ,sum(case when is_no_cancle='Y' then room_fee end) / sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单ADR`
          ,sum(case when is_no_cancle='N' then room_night end) `C可取消订单间夜`
          ,sum(case when is_no_cancle='N' then room_fee end) `C可取消订单GMV`
          ,sum(case when is_no_cancle='N' then room_fee end) / sum(case when is_no_cancle='N' then room_night end) `C可取消订单ADR`
    from c_order
    group by substr(checkout_date,1,7),cube(supplier,adr_type,per_type)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.adr_type=t2.adr_type and t1.per_type=t2.per_type
order by 1,2,3
;


--- 同质化条件下价差
with base as (
    select  order_date
           ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
           ,order_status,init_gmv,room_night
           ,split(get_json_object(extendInfoMap, '$.homogenizationKey'), '\\|') as ationKey
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and a.order_no <> '103576132435'
      and checkout_date between '2025-10-01' and date_sub(current_date, 1)
      and a.country_name = '日本'
),
keyed as (
    select  order_date
           ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
           ,order_status,init_gmv,room_night
           ,ationKey
           ,ationKey[2] as third_val
           -- 除第三个值(ationKey[2])外，其它值拼成分组key
           ,concat_ws('|',
                coalesce(ationKey[0],''), coalesce(ationKey[1],''),
                coalesce(ationKey[3],''), coalesce(ationKey[4],''), coalesce(ationKey[5],''),
                coalesce(ationKey[6],''), coalesce(ationKey[7],''), coalesce(ationKey[8],''),
                coalesce(ationKey[9],''), coalesce(ationKey[10],''), coalesce(ationKey[11],''),
                coalesce(ationKey[12],''), coalesce(ationKey[13],''), coalesce(ationKey[14],''),
                coalesce(ationKey[15],''), coalesce(ationKey[16],''), coalesce(ationKey[17],''),
                coalesce(ationKey[18],''), coalesce(ationKey[19],''), coalesce(ationKey[20],''),
                coalesce(ationKey[21],''), coalesce(ationKey[22],'')
            ) as same_except_third_key
    from base
)

select  t1.checkout_date,t1.hotel_seq,t1.third_val,count(distinct t1.order_no) orders,min(t1.init_gmv) init_gmv,min(t1.room_night)room_night
from keyed t1
left join keyed t2 on t1.hotel_seq=t2.hotel_seq and t1.checkout_date=t2.checkout_date 
where t1.same_except_third_key=t2.same_except_third_key and t2.hotel_seq is not null
group by 1,2,3
order by 1,4 desc


--- 餐食最大入住天数
with q_data as (
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
            ,case when breakfast < 0 then '0' 
                  when breakfast = 0 then '0'  
                  when breakfast = 1 then '1'  
                  when breakfast = 2 then '2'  
                  when breakfast = 3 then '3'  
                  when breakfast = 4 then '4'  
                  else '5+' end breakfast --- 早餐数
            ,case when coalesce(max_c,0) = 0 then '0'  
                  when coalesce(max_c,0) = 1 then '1'  
                  when coalesce(max_c,0) = 2 then '2'  
                  when coalesce(max_c,0) = 3 then '3'  
                  when coalesce(max_c,0) = 4 then '4'  
                  when coalesce(max_c,0) = 5 then '5'  
                  else '6+' end max_c --- 房间人数
            ,size(split(customer_names,',')) order_persons -- 预定人数
            ,get_json_object(extendinfomap,'$.member_level_q') as user_grade ----用户等级
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
            ,case when coalesce(breakfast_num,0) < 0 then '0' 
                  when coalesce(breakfast_num,0) = 0 then '0'  
                  when coalesce(breakfast_num,0) = 1 then '1'  
                  when coalesce(breakfast_num,0) = 2 then '2'  
                  when coalesce(breakfast_num,0) = 3 then '3'  
                  when coalesce(breakfast_num,0) = 4 then '4'  
                  else '5+' end breakfast_num --- 早餐数
            ,case when extend_info['roompersons'] = 0 then '0'  
                  when extend_info['roompersons'] = 1 then '1'  
                  when extend_info['roompersons'] = 2 then '2'  
                  when extend_info['roompersons'] = 3 then '3'  
                  when extend_info['roompersons'] = 4 then '4'  
                  when extend_info['roompersons'] = 5 then '5'  
                  else '6+' end max_c --- 房间人数
            ,order_persons --- 预定人数
            ,extend_info['user_grade'] user_grade -- 用户等级
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)


select t1.checkout_date,t1.supplier,t1.adr_type,t1.per_type,t1.max_c,t1.breakfast
      ,`Q订单`,`Q间夜量`,`Q_GMV`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C间夜量`,`C_GMV`,`C不可取消订单`, `C不可取消订单占比`
      ,`Q不可取消订单间夜`,`Q不可取消订单GMV`,`Q不可取消订单ADR`
      ,`Q可取消订单间夜`,`Q可取消订单GMV`,`Q可取消订单ADR`
      ,`C不可取消订单间夜`,`C不可取消订单GMV`,`C不可取消订单ADR`
      ,`C可取消订单间夜`,`C可取消订单GMV`,`C可取消订单ADR`
from (
    select checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,if(grouping(breakfast)=1,'ALL', breakfast) as  breakfast
        ,if(grouping(max_c)=1,'ALL', max_c) as  max_c
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night) `Q间夜量`
        ,sum(init_gmv)   `Q_GMV`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单间夜`
        ,sum(case when is_non_ref='Y' then init_gmv end) `Q不可取消订单GMV`
        ,sum(case when is_non_ref='Y' then init_gmv end) / sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单ADR`
        ,sum(case when is_non_ref='N' then room_night end) `Q可取消订单间夜`
        ,sum(case when is_non_ref='N' then init_gmv end) `Q可取消订单GMV`
        ,sum(case when is_non_ref='N' then init_gmv end) / sum(case when is_non_ref='N' then room_night end) `Q可取消订单ADR`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by checkout_date,cube(supplier_type,adr_type,per_type,breakfast,max_c)
) t1 left join (
    select checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
          ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
          ,if(grouping(max_c)=1,'ALL', max_c) as  max_c
          ,if(grouping(breakfast_num)=1,'ALL', breakfast_num) as  breakfast_num
          ,count(distinct order_no) as `C订单`
          ,sum(room_night) `C间夜量`
          ,sum(room_fee)   `C_GMV`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单间夜`
          ,sum(case when is_no_cancle='Y' then room_fee end) `C不可取消订单GMV`
          ,sum(case when is_no_cancle='Y' then room_fee end) / sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单ADR`
          ,sum(case when is_no_cancle='N' then room_night end) `C可取消订单间夜`
          ,sum(case when is_no_cancle='N' then room_fee end) `C可取消订单GMV`
          ,sum(case when is_no_cancle='N' then room_fee end) / sum(case when is_no_cancle='N' then room_night end) `C可取消订单ADR`
    from c_order
    group by checkout_date,cube(supplier,adr_type,per_type,breakfast_num,max_c)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.adr_type=t2.adr_type and t1.per_type=t2.per_type and t1.max_c=t2.max_c and t1.breakfast=t2.breakfast_num
order by 1,2,3
;




--CQ不可取消订单数据月度
with user_type as(  --- 用于判定Q新老客
    select user_id
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,q_app_order as (--- Q订单 APP
    select order_date
           ,substr(checkout_date,1,10) checkout_date
           ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
           ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
           ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
           ,a.user_id,order_no,room_night
           ,order_status  
           ,pay_type   --- 支付状态，预付和现付
           ,product_order_refund_type   --- 预付后的退款规则 NO_CANCEL不可取消
           ,product_order_cancel_type   --- 预付后的退款规则 NO_CANCEL不可取消
           --- 是否非当日取消拒单  Y为当日非取消非拒单订单
           ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 
           ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                            or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                            or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
        --    ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
           ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end is_non_ref 
    from mdw_order_v3_international a
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid = '1'
        and order_date between '2025-04-01' and date_sub(current_date, 1)
        and a.order_no <> '103576132435'
)
,c_order as( --- C订单
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
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.order_date, 1, 10) between '2025-04-01' and date_sub(current_date, 1)
)

select t1.mth, "Q总预定订单量", "Q总不可取消订单量","Q总不可取消订单量" / "Q总预定订单量" "Q不可取消订单量占比","C总预定订单量","C总不可取消订单量","C总不可取消订单量" / "C总预定订单量" "C不可取消订单量占比"
from (
    select substr(order_date,1,7) mth
        ,count(distinct order_no) "Q总预定订单量"
        ,count(distinct case when is_non_ref = 'Y' then order_no end) "Q总不可取消订单量"
    from q_app_order
    group by 1
)t1 left join (
    select substr(order_date,1,7) mth
        ,count(distinct order_no) "C总预定订单量"
        ,count(distinct case when is_no_cancle = 'Y' then order_no end) "C总不可取消订单量"
    from c_order
    group by 1
)t2 on t1.mth=t2.mth
order by 1
;



---- 分货源情况取消率
with q_data as (
    select order_date as `日期`
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case 
                    when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'DC'
                    when  qta_supplier_id in ('1615667','800000164') then 'C2Q'
                    when wrapper_id in ('hca908oh60s','hca908oh60t') then 'ABE'
                    when wrapper_id in ('hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s') then 'ABE'
                    when wrapper_id in ('hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'ABE'
                    else '代理' 
            end as `渠道`
            ,case when qta_supplier_id in ('1615667','800000164') and vendor_name = 'DC' then 'C2Q直采' 
                when qta_supplier_id in ('1615667','800000164') and vendor_name = 'Agoda' then 'C2Q-Agoda'
                when qta_supplier_id in ('1615667','800000164') then 'C2Q-代理'
                when qta_supplier_id not in ('1615667','800000164') and wrapper_id not in ('hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s','hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'Q代理'
                else 'Q-ABE' 
            end as supplier
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
            ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join
        (select order_no 
            , max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
        ) b 
    on a.order_no = b.order_no
    -- C关联信息表-用于提供供应商信息
    left join
        (select distinct partner_order_no
            , extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
        ) c
    on b.purchase_order_no = c.partner_order_no
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and a.order_no <> '103576132435'
            and checkout_date between '2025-01-01' and date_sub(current_date, 1)
            and a.country_name = '日本'
)
,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,o.user_id,order_no,room_fee
            ,order_status
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' end as supplier
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
)

select t1.checkout_date,t1.mdd,t1.supplier,`Q订单`, `Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C取消率`,`C不可取消订单`, `C不可取消订单占比`
from (
    select substr(checkout_date,1,7) checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1,cube(supplier_type,mdd)
) t1 left join (
    select substr(checkout_date,1,7) checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
    from c_order
    group by 1,cube(supplier,mdd)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.mdd=t2.mdd
order by 1,2
;


---- 分Top20代理商
with top20wrapper as (
    select distinct company_main,wrapper_id from temp.temp_pengpeng_yuan_20260305_Q_KA_agent
)
,q_data as (
    select order_date as `日期`
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case 
                    when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'DC'
                    when  qta_supplier_id in ('1615667','800000164') then 'C2Q'
                    when a.wrapper_id in ('hca908oh60s','hca908oh60t') then 'ABE'
                    when  a.wrapper_id in ('hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s') then 'ABE'
                    when  a.wrapper_id in ('hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'ABE'
                    else '代理' 
            end as `渠道`
            ,case when qta_supplier_id in ('1615667','800000164') and vendor_name = 'DC' then 'C2Q直采' 
                when qta_supplier_id in ('1615667','800000164') and vendor_name = 'Agoda' then 'C2Q-Agoda'
                when qta_supplier_id in ('1615667','800000164') then 'C2Q-代理'
                when qta_supplier_id not in ('1615667','800000164') and  a.wrapper_id not in ('hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s','hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'Q代理'
                else 'Q-ABE' 
            end as supplier
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
            ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end is_non_ref --- 是否可取消订单
            ,case when company_main is null then '其他' else company_main end company_main
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join
        (select order_no 
            , max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
        ) b 
    on a.order_no = b.order_no
    -- C关联信息表-用于提供供应商信息
    left join
        (select distinct partner_order_no
            , extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
        ) c
    on b.purchase_order_no = c.partner_order_no
    left join top20wrapper t2 on a.wrapper_id=t2.wrapper_id
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and a.order_no <> '103576132435'
            and checkout_date between '2025-01-01' and date_sub(current_date, 1)
)



select t1.checkout_date,t1.mdd,t1.company_main,`Q订单`, `Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`
from (
    select substr(checkout_date,1,7) checkout_date
        ,if(grouping(company_main)=1,'ALL', company_main) as  company_main
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t1 where supplier_type='代理'
    group by 1,cube(company_main,mdd)
) t1
order by 1,2
;



---- 分货源情况取消率日维度
with q_data as (
    select order_date as `日期`
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case 
                    when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'DC'
                    when  qta_supplier_id in ('1615667','800000164') then 'C2Q'
                    when wrapper_id in ('hca908oh60s','hca908oh60t') then 'ABE'
                    when wrapper_id in ('hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s') then 'ABE'
                    when wrapper_id in ('hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'ABE'
                    else '代理' 
            end as `渠道`
            ,case when qta_supplier_id in ('1615667','800000164') and vendor_name = 'DC' then 'C2Q直采' 
                when qta_supplier_id in ('1615667','800000164') and vendor_name = 'Agoda' then 'C2Q-Agoda'
                when qta_supplier_id in ('1615667','800000164') then 'C2Q-代理'
                when qta_supplier_id not in ('1615667','800000164') and wrapper_id not in ('hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca9008pb7n','hca908pb70o','hca908pb70p','hca908pb70q','hca908pb70r','hca908pb70s','hca908lp9ah','hca908lp9ag','hca908lp9aj','hca908lp9ai','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'Q代理'
                else 'Q-ABE' 
            end as supplier
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
            ,case when cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[2] as int) = 0 then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join
        (select order_no 
            , max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
        ) b 
    on a.order_no = b.order_no
    -- C关联信息表-用于提供供应商信息
    left join
        (select distinct partner_order_no
            , extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
        ) c
    on b.purchase_order_no = c.partner_order_no
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and a.order_no <> '103576132435'
            and checkout_date between '2026-01-01' and date_sub(current_date, 1)
)
,c_order as( --- C订单
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
                    when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                    when c.area in ('欧洲','亚太','美洲') then c.area
                    else '其他' end as mdd
            ,case when extend_info['COUNTRY'] = '日本' then  '日本' else '非日本' end  is_jp
            ,o.user_id,order_no,room_fee
            ,order_status
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' end as supplier
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2026-01-01' and date_sub(current_date, 1) -- 退房日期范围
)

select t1.checkout_date,t1.mdd,t1.supplier,`Q订单`, `Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C取消率`,`C不可取消订单`, `C不可取消订单占比`
from (
    select checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1,cube(supplier_type,mdd)
) t1 left join (
    select checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
    from c_order
    group by 1,cube(supplier,mdd)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.mdd=t2.mdd
order by 1,2
;



--- 5、分酒店分货源渠道月度数据
with q_data as (
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)
,q_c_hotel_mapping as (
    select
        distinct
        hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
)

select t1.mth,t1.hotel_seq,hotel_name,t1.supplier_type
      ,`Q订单`,`Q不可取消订单`,`Q不可取消订单占比`,`Q间夜量`,`Q_GMV`,`Q_ADR`
      ,`C订单`,`C不可取消订单`, `C不可取消订单占比`,`C间夜量`,`C_GMV`,`C_ADR`
      ,`Q不可取消订单间夜`,`Q不可取消订单GMV`,`Q不可取消订单ADR`
      ,`Q可取消订单间夜`,`Q可取消订单GMV`,`Q可取消订单ADR`
      ,`C不可取消订单间夜`,`C不可取消订单GMV`,`C不可取消订单ADR`
      ,`C可取消订单间夜`,`C可取消订单GMV`,`C可取消订单ADR`
from (
    select substr(checkout_date,1,7)mth,hotel_seq,if(grouping(supplier_type)=1,'ALL',supplier_type)supplier_type
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night)  as `Q间夜量`
        ,sum(init_gmv)  as `Q_GMV`
        ,sum(init_gmv) / sum(room_night)  as `Q_ADR`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单间夜`
        ,sum(case when is_non_ref='Y' then init_gmv end) `Q不可取消订单GMV`
        ,sum(case when is_non_ref='Y' then init_gmv end) / sum(case when is_non_ref='Y' then room_night end) `Q不可取消订单ADR`
        ,sum(case when is_non_ref='N' then room_night end) `Q可取消订单间夜`
        ,sum(case when is_non_ref='N' then init_gmv end) `Q可取消订单GMV`
        ,sum(case when is_non_ref='N' then init_gmv end) / sum(case when is_non_ref='N' then room_night end) `Q可取消订单ADR`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1,2,cube(supplier_type)
) t1 left join (
    select substr(checkout_date,1,7)mth,t2.hotel_seq,if(grouping(supplier)=1,'ALL',supplier)supplier
          ,count(distinct order_no) as `C订单`
          ,sum(room_night)  as `C间夜量`
          ,sum(room_fee)  as `C_GMV`
          ,sum(room_fee) / sum(room_night)  as `C_ADR`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单间夜`
          ,sum(case when is_no_cancle='Y' then room_fee end) `C不可取消订单GMV`
          ,sum(case when is_no_cancle='Y' then room_fee end) / sum(case when is_no_cancle='Y' then room_night end) `C不可取消订单ADR`
          ,sum(case when is_no_cancle='N' then room_night end) `C可取消订单间夜`
          ,sum(case when is_no_cancle='N' then room_fee end) `C可取消订单GMV`
          ,sum(case when is_no_cancle='N' then room_fee end) / sum(case when is_no_cancle='N' then room_night end) `C可取消订单ADR`
    from c_order t1
    left join q_c_hotel_mapping t2 on t1.hotel_seq=t2.partner_hotel_id
    -- where supplier = '直采'
    group by 1,2,cube(supplier)

)t2 on t1.mth=t2.mth  and t1.hotel_seq=t2.hotel_seq and t1.supplier_type=t2.supplier
left join (select hotel_seq,hotel_name from default.dim_hotel_info_intl_v3 where dt = '20260304') t3 on t1.hotel_seq=t3.hotel_seq
order by 1,2
;



--- 抓取支付价beat深度lose率
with qc_price as (
    select order_date
        ,if(grouping(is_more_roomnight)=1,'ALL', is_more_roomnight) as  is_more_roomnight
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
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
            ,case   when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('泰国','日本','韩国') then a.country_name  
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
            ,case when datediff(check_out, check_in) >= 2 then '多晚' else '单晚' end is_more_roomnight
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= '20260201' 
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            -- and check_out >= '2026-02-01' and check_out <= '2026-02-05'
    )t
    group by order_date,cube(is_more_roomnight,user_type)
)

select order_date
      ,is_more_roomnight
      ,user_type
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
order by 1,2,3
;


--- 抓取支付价beat深度lose率-分提前订
with qc_price as (
    select order_date
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,if(grouping(user_type)=1,'ALL', user_type) as  user_type
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
            ,case   when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('泰国','日本','韩国') then a.country_name  
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
            ,case when datediff(check_out, check_in) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 0 and 3    then '提前订1-3天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 4 and 7    then '提前订4-7天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14   then '提前订8-14天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 30  then '提前订15-30天'
                  else '提前订31+' 
            end  per_type
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        where dt >= replace(date_sub(current_date, 30),'-','') and dt <=  replace(date_sub(current_date, 1),'-','')
            and business_type = 'intl_crawl_cq_spa'
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            -- and check_out >= '2026-02-01' and check_out <= '2026-02-05'
    )t
    group by order_date,cube(per_type,user_type)
)

select order_date
      ,per_type
      ,user_type
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
order by 1,2,3
;



select order_date
       ,count(distinct order_no)  "取消订单量"
       ,count(distinct case when is_non_ref = 'Y' then order_no end)  "不可取消订单取消订单量"
       ,count(distinct case when is_non_ref = 'Y' and is_90off then final_payamount_price end)  "不可取消订单且扣款90%+取消订单量"
       ,sum(case when is_non_ref = 'Y' then init_gmv end)  "不可取消订单GMV"
       ,sum(case when is_non_ref = 'Y' then final_payamount_price end)  "不可取消订单扣款金额"
       ,count(distinct case when is_non_ref = 'Y' then order_no end) / count(distinct order_no) "不可取消订单取消占比"
       ,count(distinct case when is_non_ref = 'Y' and is_90off then final_payamount_price end) / count(distinct case when is_non_ref = 'Y' then order_no end) "不可取消订单且扣款90%+占比"
       ,sum(case when is_non_ref = 'Y' then final_payamount_price end) / sum(case when is_non_ref = 'Y' then init_gmv end)  "平均扣款比例"
from (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,final_payamount_price
            ,case when final_payamount_price > 0 then 'Y' else 'N' end is_non_ref
            ,case when final_payamount_price > 0 and final_payamount_price / init_gmv > 0.9 then 'Y' else 'N' end is_90off
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_status = 'CANCELLED'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
        -- and final_payamount_price > 0
) group by 1 order by 1 desc
;



--Q取消规则解析
with cancelRuleDetail AS (
    SELECT
     a.order_no,
     a.pay_type,
     a.wrapper_name,
     a.order_status,
     a.order_date,
     a.checkout_date,
     a.checkin_date,
     a.order_time,
     a.effective_confirm_time,
        substr(a.effective_confirm_time,1,10) as `最晚可取消日期`,
     a.cancel_rule,
     product_order_refund_type,
     product_order_cancel_type,
     case 
        when (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
        else '可取消'
    end as `不可取消订单量`,
    case 
        when (product_order_refund_type <> 'NO_CANCEL' and product_order_cancel_type <> 'NO_CANCEL') then '可取消'
        else '不可取消' 
    end as `可取消订单量`,
     CASE
         WHEN a.cancel_rule LIKE '订单确认后%'
             THEN 'X小时免费取消'
         WHEN substr(a.effective_confirm_time,1,10) > a.checkin_date and a.cancel_rule LIKE '%可免费取消%'
             THEN '可免费取消'
         WHEN a.cancel_rule LIKE '%可免费取消%'
             THEN '限时免费取消'
         WHEN a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%'
             THEN '不可取消'
         WHEN a.order_time < a.effective_confirm_time and substr(a.effective_confirm_time,1,10) <= a.checkin_date and (product_order_refund_type = 'BEFORE_RELATIVE_TIME' or product_order_cancel_type = 'BEFORE_RELATIVE_TIME') and a.cancel_rule is null and effective_confirm_time is not null
             THEN '限时免费取消'
         WHEN a.order_time < a.effective_confirm_time and substr(a.effective_confirm_time,1,10) > a.checkin_date and (product_order_refund_type = 'BEFORE_RELATIVE_TIME' or product_order_cancel_type = 'BEFORE_RELATIVE_TIME') and a.cancel_rule is null and effective_confirm_time is not null
             THEN '可免费取消'
         WHEN a.order_time < a.effective_confirm_time and substr(a.effective_confirm_time,1,10) <= a.checkin_date and a.cancel_rule is null and product_order_refund_type is null and product_order_cancel_type is null and effective_confirm_time is not null
             THEN '限时免费取消'
         WHEN a.order_time < a.effective_confirm_time and substr(a.effective_confirm_time,1,10) > a.checkin_date and a.cancel_rule is null and product_order_refund_type is null and product_order_cancel_type is null and effective_confirm_time is not null
             THEN '可免费取消'
         when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
         when a.cancel_rule is null and product_order_refund_type is null and product_order_cancel_type is null and effective_confirm_time is not null
             then '未知'                 
         ELSE '阶梯扣款取消'
         END AS cancelRule
    FROM default.mdw_order_v3_international a
    WHERE a.dt = '%(DATE)s'
    and (province_name in ('台湾', '澳门', '香港') or a.country_name !='中国') 
    --and a.country_name ='日本'
    and terminal_channel_type = 'app'
    and is_valid = '1'
    and a.order_no <> '103576132435'
    and checkout_date between '2025-01-01' and '2025-12-31'
)


select substr(checkout_date,1,7) as `年月`
    ,concat(round(
        count(distinct case when cancelRule = '不可取消' then order_no  end) 
        /count(distinct order_no) *100
        ,2),'%') as `不可取消`
    ,concat(round(
        count(distinct case when cancelRule = '阶梯扣款取消' then order_no  end) 
        /count(distinct order_no) *100
        ,2),'%') as `阶梯扣款取消`
    ,concat(round(
        count(distinct case when cancelRule = '限时免费取消' then order_no  end) 
        /count(distinct order_no) *100
        ,2),'%') as `限时免费取消`
    ,concat(round(
        count(distinct case when cancelRule = '可免费取消' then order_no  end) 
        /count(distinct order_no) *100
        ,2),'%') as `可免费取消`
    ,concat(round(
        count(distinct case when cancelRule = 'X小时免费取消' then order_no  end) 
        /count(distinct order_no) *100
        ,2),'%') as `X小时免费取消订单`
    ,concat(round(
        count(distinct case when cancelRule = '未知' then order_no  end) 
        /count(distinct order_no) *100
        ,2),'%') as `未知`
    ,count(distinct order_no) as `Q总订单`
    ,count(distinct case when cancelRule = 'X小时免费取消' then order_no  end) as `X小时免费取消订单`
    ,count(distinct case when cancelRule = '可免费取消' then order_no  end) as `可免费取消订单`
    ,count(distinct case when cancelRule = '限时免费取消' then order_no  end) as `限时免费取消订单`
    ,count(distinct case when cancelRule = '不可取消' then order_no  end) as `不可取消订单`
    ,count(distinct case when cancelRule = '未知' then order_no  end) as `未知订单`
    ,count(distinct case when cancelRule = '阶梯扣款取消' then order_no  end) as `阶梯扣款取消订单`

from cancelRuleDetail a
group by 1
order by `年月`
;


with order_info as  (
    select checkout_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,init_gmv,order_no,room_night
            ,init_commission_after
            ,final_payamount_price
            ,init_payamount_price
            ,case when final_payamount_price > 0 then 'Y' else 'N' end is_non_ref
            ,case when final_payamount_price > 0 and final_payamount_price / init_gmv > 0.9 then 'Y' else 'N' end is_90off
            ,order_time,first_cancelled_time
            ,order_cancel_reason
            ,case when order_cancel_reason is null or order_cancel_reason = 'null' then array('未知') else split(order_cancel_reason, ',') end as reasons_array

    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and order_status = 'CANCELLED'
        and checkout_date >= date_sub(current_date, 60) and checkout_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
select *,trim(reason) as reason  
from order_info
lateral view explode(reasons_array) as reason

group by 1,2 order by 1 desc
;

with base_orders as (
    select
        checkout_date,
        case
            when province_name in ('澳门','香港') then province_name
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
            when e.area in ('欧洲','亚太','美洲') then e.area
            else '其他'
        end as mdd,
        a.user_id,
        init_gmv,
        order_no,
        room_night,
        init_commission_after,
        final_payamount_price,
        init_payamount_price,
        case when final_payamount_price > 0 then 'Y' else 'N' end as is_non_ref,
        case when final_payamount_price > 0 and final_payamount_price / init_gmv > 0.9 then 'Y' else 'N' end as is_90off,
        order_time,
        first_cancelled_time,
        order_cancel_reason
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
      and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status = 'CANCELLED'
      and checkout_date >= date_sub(current_date, 60)
      and checkout_date <= date_sub(current_date, 1)
      and order_no <> '103576132435'
),
total_order_cnt as (-- 总取消订单数，用于算“订单渗透率”
    select checkout_date,count(distinct order_no) as total_cnt
    from base_orders
    group by 1
),reason_explode as (-- 拆分多选原因
    select
        b.order_no,
        b.user_id,
        b.mdd,
        b.checkout_date,
        trim(reason_item) as reason
    from base_orders b
    lateral view outer explode(
        split(
            case
                when b.order_cancel_reason is null or trim(b.order_cancel_reason) = '' then '未知'
                else b.order_cancel_reason
            end,
            ','
        )
    ) t as reason_item
),reason_normalized as (-- 一级归一化
    select
        order_no,
        user_id,
        mdd,
        checkout_date,
        reason_item,
         case 
            -- 优先识别“其他原因”开头的自定义原因
            when reason like '其他原因%' then '其他原因'
            -- 预定义原因关键词匹配
            when reason like '%行程取消%' or reason like '%行程有变%' then '行程问题'
            when reason like '%信息填错%' then '信息填写错误'
            when reason like '%价格太贵%' or reason like '%更便宜%' then '价格问题'
            when reason like '%优惠%' or reason like '%红包%' or reason like '%返现%' then '价格问题'
            when reason like '%确认时间%' then '确认时间问题'
            when reason like '%未支付%' then '未支付'
            when reason like '%酒店%不满意%' or reason like '%酒店质量%' 
                 or reason like '%酒店前台%' or reason like '%酒店停业%' 
                 or reason like '%酒店让我取消%' then '酒店问题'
            when reason like '%疫情%' or reason like '%航班%' 
                 or reason like '%签证%' or reason like '%战争%' 
                 or reason like '%打仗%' or reason like '%不可抗力%' then '外部不可抗力'
            else '其他'  -- 未匹配到的也归为其他
        end as cancel_group
    from reason_explode
)
select t1.checkout_date,cancel_group,order_cnt,
    round(order_cnt / t2.total_cnt, 4) as order_ratio
from (
    select
        checkout_date,cancel_group
        count(distinct order_no) as order_cnt                             -- 选择过该原因的订单数
    from reason_normalized t1
    group by 1,2
) t1 left join  total_order_cnt t2 on t1.checkout_date=t2.checkout_date
order by t1.checkout_date desc, order_ratio desc;


with base_orders as (
    select
        checkout_date,checkin_date,
        case
            when province_name in ('澳门','香港') then province_name
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
            when e.area in ('欧洲','亚太','美洲') then e.area
            else '其他'
        end as mdd,
        a.user_id,
        init_gmv,
        order_no,
        room_night,
        init_commission_after,
        final_payamount_price,
        init_payamount_price,
        case when final_payamount_price > 0 then 'Y' else 'N' end as is_non_ref,
        case when final_payamount_price > 0 and final_payamount_price / init_gmv > 0.9 then 'Y' else 'N' end as is_90off,
        order_time,
        first_cancelled_time,
        order_cancel_reason
        ,cast((unix_timestamp(first_cancelled_time) - unix_timestamp(order_time)) / 60 as bigint) as cancel_diff_min
        ,datediff(to_date(checkin_date), to_date(first_cancelled_time)) as days_before_checkin
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
        on a.country_name = e.country_name
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
      and (province_name in ('台湾','澳门','香港') or a.country_name != '中国')
      and terminal_channel_type = 'app'
      and is_valid = '1'
      and order_status = 'CANCELLED'
      and checkout_date >= date_sub(current_date, 60)
      and checkout_date <= date_sub(current_date, 1)
      and order_no <> '103576132435'
)
,total_order_cnt as (
    select
        checkout_date,
        count(distinct order_no) as total_cnt
    from base_orders
    group by checkout_date
)
,reason_explode as (
    select
        b.order_no,
        b.user_id,
        b.mdd,
        b.checkout_date,
        trim(reason_item) as reason
        -- 取消距离下单时长分布
        ,case
            when first_cancelled_time is null or order_time is null then '未知'
            when cancel_diff_min < 0 then '异常(取消时间早于下单时间)'
            when cancel_diff_min <= 15 then '15分钟以内'
            when cancel_diff_min <= 24 * 60 then '24小时以内'
            else '24小时之外'
        end as cancel_diff_bucket
        -- 取消距离入住日分布（checkin_date只有日期，按天近似）
        ,case
            when first_cancelled_time is null or checkin_date is null then '未知'
            when days_before_checkin < 0 then '异常(入住日后取消)'
            when days_before_checkin <= 1 then '入住前24小时内'
            when days_before_checkin = 2 then '入住前24-48小时'
            else '入住前48小时外'
        end as cancel_before_checkin_bucket
    from base_orders b
    lateral view outer explode(
        split(
            case
                when b.order_cancel_reason is null or trim(b.order_cancel_reason) = '' then '未知'
                else b.order_cancel_reason
            end,
            ','
        )
    ) t as reason_item
)
,reason_normalized as (
    select
        order_no,
        user_id,
        mdd,
        checkout_date,cancel_diff_bucket,cancel_before_checkin_bucket,
        reason,
        case
            -- 1) 行程问题
            when reason like '%行程取消%' 
              or reason like '%行程有变%'
              or reason like '%行程变%'
              or reason like '%行程调整%'
              or reason like '%行程改变%'
              or reason like '%计划有变%'
              or reason like '%计划变%'
              then '行程问题'

            -- 2) 信息填写错误
            when reason like '%信息填错%'
              or reason like '%填错%'
              or reason like '%写错%'
              or reason like '%订错%'
              or reason like '%定错%'
              or reason like '%选错%'
              or reason like '%买错%'
              or reason like '%搞错%'
              or reason like '%重复预订%'
              or reason like '%重复预定%'
              or reason like '%重复订单%'
              or reason like '%重复下单%'
              or reason like '%订重复%'
              or reason like '%定重复%'
              or reason like '%多订%'
              or reason like '%人数错误%'
              or reason like '%入住人%'
              or reason like '%入住人数%'
              or reason like '%名字登记错误%'
              or reason like '%日期错误%'
              or reason like '%时间错误%'
              then '信息填写错误'

            -- 3) 价格问题
            when reason like '%价格太贵%'
              or reason like '%更便宜%'
              or reason like '%便宜%'
              or reason like '%降价%'
              or reason like '%涨价%'
              or reason like '%差价%'
              or reason like '%其他平台%'
              or reason like '%优惠%'
              or reason like '%红包%'
              or reason like '%返现%'
              or reason like '%现价更低%'
              or reason like '%重新预订更便宜%'
              or reason like '%重新预定更便宜%'
              then '价格问题'

            -- 4) 确认时间问题
            when reason like '%确认时间太长%'
              or reason like '%确认太慢%'
              or reason like '%无法确认%'
              or reason like '%一直没确认%'
              or reason like '%酒店没有确认%'
              or reason like '%回复太慢%'
              or reason like '%联系不上%'
              or reason like '%联系不到%'
              or reason like '%无人回复%'
              or reason like '%没人回复%'
              or reason like '%查不到预定%'
              or reason like '%查不到订单%'
              then '确认时间问题'

            -- 5) 酒店问题
            when reason like '%对酒店不满意%'
              or reason like '%酒店质量%'
              or reason like '%环境差%'
              or reason like '%无配套设施%'
              or reason like '%酒店前台%'
              or reason like '%酒店停业%'
              or reason like '%无合作不能入住%'
              or reason like '%酒店让我取消%'
              or reason like '%酒店装修%'
              or reason like '%酒店爆炸%'
              or reason like '%酒店被炸%'
              or reason like '%满房%'
              or reason like '%无房%'
              or reason like '%不能入住%'
              or reason like '%酒店限制%'
              or reason like '%不接待%'
              or reason like '%不能带儿童%'
              or reason like '%未成年无法入住%'
              or reason like '%房型不符%'
              or reason like '%无窗%'
              or reason like '%没早餐%'
              or reason like '%没有早餐%'
              or reason like '%床太小%'
              or reason like '%房间太小%'
              or reason like '%没有电梯%'
              or reason like '%没有前台%'
              or reason like '%非24小时前台%'
              or reason like '%无法寄存行李%'
              then '酒店问题'

            -- 6) 外部不可抗力
            when reason like '%疫情%'
              or reason like '%航班%'
              or reason like '%飞机%'
              or reason like '%机票%'
              or reason like '%签证%'
              or reason like '%拒签%'
              or reason like '%护照%'
              or reason like '%战争%'
              or reason like '%打仗%'
              or reason like '%战乱%'
              or reason like '%局势%'
              or reason like '%安全问题%'
              or reason like '%地缘政治%'
              or reason like '%不可抗力%'
              or reason like '%火山%'
              or reason like '%地震%'
              or reason like '%暴雪%'
              or reason like '%台风%'
              or reason like '%封路%'
              or reason like '%停飞%'
              or reason like '%停运%'
              or reason like '%生病%'
              or reason like '%住院%'
              or reason like '%骨折%'
              or reason like '%受伤%'
              or reason like '%发烧%'
              or reason like '%身体原因%'
              or reason like '%身体不适%'
              or reason like '%家人生病%'
              or reason like '%亲人离世%'
              or reason like '%去世%'
              or reason like '%车祸%'
              then '外部不可抗力'

            -- 7) 其他/未知
            when reason = '未知'
              or reason like '其他原因%'
              then '其他/未知'
            else '其他/未知'
        end as cancel_group
    from reason_explode
)

select
    t1.checkout_date,
    t1.cancel_group,cancel_before_checkin_bucket,cancel_diff_bucket,
    t1.order_cnt,
    round(t1.order_cnt / t2.total_cnt, 4) as order_ratio
from (
    select checkout_date,
        if(grouping(cancel_group)=1,'ALL',cancel_group) cancel_group,
        if(grouping(cancel_before_checkin_bucket)=1,'ALL',cancel_before_checkin_bucket) cancel_before_checkin_bucket,
        if(grouping(cancel_diff_bucket)=1,'ALL',cancel_diff_bucket) cancel_diff_bucket,
        count(distinct order_no) as order_cnt
    from reason_normalized
    group by checkout_date, cube(cancel_group,cancel_before_checkin_bucket,cancel_diff_bucket)
) t1
left join total_order_cnt t2
    on t1.checkout_date = t2.checkout_date
order by t1.checkout_date desc, order_ratio desc;

--- C取消时间分布
with base_orders as (
    SELECT  substr(o.checkout_date, 1, 10) AS checkout_date
            ,substr(order_date,1,10) as order_date
            ,o.user_id,order_no,room_fee
            ,extend_info['room_night'] room_night
            --- 预定当日是否有取消单或被拒单
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,order_status
            ,o.extend_info['CANCEL_TIME'] cancel_date
            ,o.extend_info['LastCancelTime'] LastCancel_date
            ,order_date
            ,checkin_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,cast((unix_timestamp(o.extend_info['CANCEL_TIME']) - unix_timestamp(order_date)) / 60 as bigint) as cancel_diff_min
            ,datediff(to_date(checkin_date), to_date(o.extend_info['CANCEL_TIME'])) as days_before_checkin
            
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between date_sub(current_date, 60) and date_sub(current_date, 1) -- 退房日期范围
        and order_status = 'C'
)
,reason_explode as (
    select
        b.order_no,
        b.user_id,
        b.checkout_date
        -- 取消距离下单时长分布
        ,case
            when first_cancelled_time is null or order_time is null then '未知'
            when cancel_diff_min < 0 then '异常(取消时间早于下单时间)'
            when cancel_diff_min <= 15 then '15分钟以内'
            when cancel_diff_min <= 24 * 60 then '24小时以内'
            else '24小时之外'
        end as cancel_diff_bucket
        -- 取消距离入住日分布（checkin_date只有日期，按天近似）
        ,case
            when first_cancelled_time is null or checkin_date is null then '未知'
            when days_before_checkin < 0 then '异常(入住日后取消)'
            when days_before_checkin <= 1 then '入住前24小时内'
            when days_before_checkin = 2 then '入住前24-48小时'
            else '入住前48小时外'
        end as cancel_before_checkin_bucket
    from base_orders b
)
,total_order_cnt as (
    select
        checkout_date,
        count(distinct order_no) as total_cnt
    from base_orders
    group by 1
)
select
    t1.checkout_date,cancel_before_checkin_bucket,cancel_diff_bucket,
    t1.order_cnt,
    round(t1.order_cnt / t2.total_cnt, 4) as order_ratio
from (
    select checkout_date,
        if(grouping(cancel_before_checkin_bucket)=1,'ALL',cancel_before_checkin_bucket) cancel_before_checkin_bucket,
        if(grouping(cancel_diff_bucket)=1,'ALL',cancel_diff_bucket) cancel_diff_bucket,
        count(distinct order_no) as order_cnt
    from reason_normalized
    group by checkout_date, cube(cancel_before_checkin_bucket,cancel_diff_bucket)
) t1
left join total_order_cnt t2
    on t1.checkout_date = t2.checkout_date
order by t1.checkout_date desc, order_ratio desc;


--- 月度分货源数据
with q_data as (
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
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
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)
,q_c_hotel_mapping as (
    select
        distinct
        hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
)

select t1.mth,t1.hotel_seq,hotel_name,t1.supplier_type
      ,`Q订单`,`Q不可取消订单`,`Q不可取消订单占比`,`Q间夜量`,`Q_GMV`
      ,`C订单`,`C不可取消订单`, `C不可取消订单占比`,`C间夜量`,`C_GMV`
from (
    select substr(checkout_date,1,7)mth
        ,hotel_seq
        ,supplier_type_new
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night)  as `Q间夜量`
        ,sum(init_gmv)  as `Q_GMV`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
              ,case when supplier = 'C2Q直采'                then '直采'
                    else '非直采'
              end  supplier_type_new
        from q_data
    )t
    group by 1,2,3
) t1 left join (
    select substr(checkout_date,1,7)mth,t2.hotel_seq,if(grouping(supplier)=1,'ALL',supplier)supplier
          ,count(distinct order_no) as `C订单`
          ,sum(room_night)  as `C间夜量`
          ,sum(room_fee)  as `C_GMV`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
    from c_order t1
    left join q_c_hotel_mapping t2 on t1.hotel_seq=t2.partner_hotel_id
    -- where supplier = '直采'
    group by 1,2,cube(supplier)

)t2 on t1.mth=t2.mth  and t1.hotel_seq=t2.hotel_seq and t1.supplier_type=t2.supplier
left join (select hotel_seq,hotel_name from default.dim_hotel_info_intl_v3 where dt = '20260318') t3 on t1.hotel_seq=t3.hotel_seq
order by 1,2
;

--- 月度分酒店分货源产单情况数据
with q_data as (
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
            --- 是否不可取消订单
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
            ,case   when a.order_time >= a.effective_confirm_time OR a.cancel_rule LIKE '预订人因自身原因%' THEN '不可取消'
                    when a.cancel_rule is null and (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then '不可取消' 
                    else '可免费取消'              
                    END AS cancelRule
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join(
        select order_no,max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
    ) b 
    on a.order_no = b.order_no
    left join(
        select distinct partner_order_no,extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
    ) c
    on b.purchase_order_no = c.partner_order_no
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-10-01' and date_sub(current_date, 1)
        and a.country_name = '日本'
)
,c_order as( --- C订单
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
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
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
    WHERE   o.dt = '%(FORMAT_DATE)s'  
        AND o.extend_info['IS_IBU'] = '0'  
        AND o.extend_info['book_channel'] = 'Ctrip'  
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-10-01' and date_sub(current_date, 1) 
        and extend_info['COUNTRY'] = '日本'
)
,q_c_hotel_mapping as (
    select distinct hotel_seq, partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s' and partner = 'ctrip'
)

--- 1. 计算Q侧月度单酒店的基础指标和货源单量
,q_agg as (
    select substr(checkout_date, 1, 7) as mth
          ,hotel_seq
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
          ,sum(init_gmv) as q_gmv
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as q_non_cancel_orders
          -- 统计直采单量和非直采单量，用于后续打标
          ,count(distinct case when supplier = 'C2Q直采' then order_no end) as q_direct_orders
          ,count(distinct case when supplier != 'C2Q直采' then order_no end) as q_non_direct_orders
    from q_data
    group by substr(checkout_date, 1, 7), hotel_seq
)

--- 2. 计算C侧月度单酒店的基础指标和货源单量
,c_agg as (
    select substr(t1.checkout_date, 1, 7) as mth
          ,t2.hotel_seq
          ,count(distinct t1.order_no) as c_orders
          ,sum(t1.room_night) as c_rn
          ,sum(t1.room_fee) as c_gmv
          ,count(distinct case when t1.is_no_cancle = 'Y' then t1.order_no end) as c_non_cancel_orders
          -- 统计直采单量和非直采单量，用于后续打标
          ,count(distinct case when t1.supplier = '直采' then t1.order_no end) as c_direct_orders
          ,count(distinct case when t1.supplier != '直采' then t1.order_no end) as c_non_direct_orders
    from c_order t1
    inner join q_c_hotel_mapping t2 on t1.hotel_seq = t2.partner_hotel_id
    group by substr(t1.checkout_date, 1, 7), t2.hotel_seq
)

--- 3. 最终横向打宽，判定货源产单情况
select coalesce(q.mth, c.mth) as `月份`
      ,coalesce(q.hotel_seq, c.hotel_seq) as `酒店ID`
      ,h.hotel_name as `酒店名称`
      
      --- Q侧数据与货源判定
      ,case when q.q_direct_orders > 0 and q.q_non_direct_orders > 0 then '既有直采又有非直采产单'
            when q.q_direct_orders > 0 and coalesce(q.q_non_direct_orders, 0) = 0 then '仅直采产单'
            when coalesce(q.q_direct_orders, 0) = 0 and q.q_non_direct_orders > 0 then '仅非直采产单'
            else '无单'
       end as `Q产单货源情况`
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(q.q_non_cancel_orders, 0) as `Q不可取消订单量`
      ,concat(round(coalesce(q.q_non_cancel_orders / nullif(q.q_orders, 0), 0) * 100, 2), '%') as `Q不可取消订单占比`
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(q.q_gmv, 0) as `Q_GMV`

      --- C侧数据与货源判定
      ,case when c.c_direct_orders > 0 and c.c_non_direct_orders > 0 then '既有直采又有非直采产单'
            when c.c_direct_orders > 0 and coalesce(c.c_non_direct_orders, 0) = 0 then '仅直采产单'
            when coalesce(c.c_direct_orders, 0) = 0 and c.c_non_direct_orders > 0 then '仅非直采产单'
            else '无单'
       end as `C产单货源情况`
      ,coalesce(c.c_orders, 0) as `C订单量`
      ,coalesce(c.c_non_cancel_orders, 0) as `C不可取消订单量`
      ,concat(round(coalesce(c.c_non_cancel_orders / nullif(c.c_orders, 0), 0) * 100, 2), '%') as `C不可取消订单占比`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(c.c_gmv, 0) as `C_GMV`

from q_agg q
full outer join c_agg c on q.mth = c.mth and q.hotel_seq = c.hotel_seq
left join (select hotel_seq, hotel_name from default.dim_hotel_info_intl_v3 where dt = '20260318') h on coalesce(q.hotel_seq, c.hotel_seq) = h.hotel_seq
order by `月份`, `酒店ID`
;