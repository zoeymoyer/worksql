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
        and checkout_date between '2025-01-01' and date_sub(current_date, 1)
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
        AND substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)


select t1.checkout_date,t1.supplier,t1.adr_type
      ,`Q订单`,`Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`
      ,`C订单`,`C非当日取消率`,`C不可取消订单`, `C不可取消订单占比`
from (
    select checkout_date
        ,if(grouping(supplier_type)=1,'ALL', supplier_type) as  supplier
        ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        -- ,sum(count(distinct case when is_non_ref='Y' then order_no end)) over(partition by checkout_date) `Q不可取消订单ALL`
        -- ,count(distinct case when is_non_ref='Y' then order_no end) / sum(count(distinct case when is_non_ref='Y' then order_no end)) over(partition by checkout_date) `不可取消订单量占比`
    from (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by checkout_date,cube(supplier_type,adr_type)
) t1 left join (
    select checkout_date
          ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,if(grouping(adr_type)=1,'ALL', adr_type) as  adr_type
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C非当日取消率`
        --   ,sum(count(distinct case when is_no_cancle = 'Y' then order_no end)) over(partition by checkout_date) `C不可取消订单ALL`
        --   ,count(distinct case when is_no_cancle = 'Y' then order_no end) / sum(count(distinct case when is_no_cancle = 'Y' then order_no end)) over(partition by checkout_date) `C不可取消订单量占比`
    from c_order
    group by checkout_date,cube(supplier,adr_type)

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier and t1.adr_type=t2.adr_type
order by 1,2,3
;



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
            
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date
            ,order_status
            ,case when (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
              and (first_rejected_time is null or date(first_rejected_time) > order_date) 
              and (refund_time is null or date(refund_time) > order_date) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,case when (order_status = 'CANCELLED' and date(first_cancelled_time) > order_date) 
                                or (order_status = 'REJECTED' and date(first_rejected_time) > order_date) 
                                or (refund_time is not null and date(refund_time) > order_date) then 'Y' else 'N' end is_cancel_d0  --- 当日取消
            ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
    from mdw_order_v3_international a 
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
            and checkout_date between '2025-11-01' and date_sub(current_date, 1)
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
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-11-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)



select t1.mth,t1.hotel_seq,t3.hotel_name,`Q订单`,`Q不可取消订单`,`Q不可取消订单占比`,`C订单`, `C不可取消订单`,`C不可取消订单占比`,`Q不可取消订单占比` - `C不可取消订单占比` `不可取消订单占比gap`
from (
select substr(checkout_date,1,7) mth
       ,hotel_seq
       ,count(distinct order_no) as `Q订单`
       ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
       ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
       ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
       ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
       ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
       ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
from q_data
group by 1,2
)t1 
left join (
 select substr(checkout_date,1,7) mth
          ,t2.hotel_seq
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C非当日取消率`
    from c_order t1
    left join ihotel_default.dim_hotel_mapping_intl_v3 t2 on t1.hotel_seq=t2.partner_hotel_id
    group by 1,2
) t2 on t1.mth=t2.mth and t1.hotel_seq=t2.hotel_seq
left join (select hotel_seq,hotel_name from default.dim_hotel_info_intl_v3 where dt = '20260208') t3 on t1.hotel_seq=t3.hotel_seq
where  t2.mth  is not null
order by 1
;


--- 提前订分布
with q_data as (
    select order_date 
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
            ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
            ,case when datediff(checkin_date, order_date) between 0 and 3    then '提前订1-3天'
                  when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                  when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                  when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
              else '提前订31+'  end  per_type
            ,breakfast
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    left join(
        select order_no 
            ,max(purchase_order_no) as purchase_order_no
        from ihotel_default.dw_purchase_order_info_v3
        where dt = '%(DATE)s'
        group by 1
        ) b 
    on a.order_no = b.order_no
    -- C关联信息表-用于提供供应商信息
    left join (
        select  partner_order_no
            , extend_info['vendor_name'] as vendor_name
        from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
        where dt = '%(FORMAT_DATE)s'
        group by 1,2
        ) c
    on b.purchase_order_no = c.partner_order_no
    where dt = '%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and is_valid='1'
            and a.order_no <> '103576132435'
            and checkout_date between '2025-06-01' and date_sub(current_date, 1)
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
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 0 and 3    then '提前订1-3天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                  when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
              else '提前订31+'  end  per_type
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then 'C代理'
                else 'ABE' end as supplier
            ,breakfast_num
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-06-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)


select t1.checkout_date,t1.per_type,`Q订单量`,`Q不可取消订单量`,`Q含餐订单量`,`Q不可取消订单占比`,`Q含餐订单占比`,`C订单量`,`C不可取消订单量`,`C含餐订单量`,`C不可取消订单占比`,`C含餐订单占比`
from (---Q数据
    select checkout_date
            ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
            ,count(distinct order_no) `Q订单量`
            ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单量`
            ,count(distinct case when breakfast > 0  then order_no end) `Q含餐订单量`
            ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no)  `Q不可取消订单占比`
            ,count(distinct case when breakfast > 0 then order_no end) / count(distinct order_no)  `Q含餐订单占比`
    from q_data
    group by checkout_date,cube(per_type)
)t1
left join (---C数据
select checkout_date,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,count(distinct order_no) `C订单量`
        ,count(distinct case when is_no_cancle = 'Y' then order_no end) `C不可取消订单量`
        ,count(distinct case when breakfast_num > 0  then order_no end) `C含餐订单量`
        ,count(distinct case when is_no_cancle = 'Y' then order_no end)  / count(distinct order_no) `C不可取消订单占比`
        ,count(distinct case when breakfast_num > 0 then order_no end)  / count(distinct order_no) `C含餐订单占比`
        ,sum(count(distinct order_no)) over(partition by substr(checkout_date,1,7) ) all_order_c
from c_order
group by checkout_date,cube(per_type)  
) t2 on t1.checkout_date=t2.checkout_date and t1.per_type=t2.per_type
order by 1,2
;


--- D页曝光
select dt,count(1) pv,count(case when is_can=0 then 1 end) no_can_pv,count(case when is_can=0 then 1 end) / count(1) can_rate
from (
select dt,cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[3] as int) is_can,get_json_object(extendinfomap,'$.traceId') as traceId,
                SUBSTRING_INDEX(room_id, '_', 1) AS room_id 
from ihotel_default.dw_hotel_price_display_v2
where dt>='20251101'  
and cast(split(get_json_object(extendInfoMap, '$.homogenizationKey'),'\\|')[3] as int) is not null
and country_name = '日本'
group by 1,2,3,4
) group by 1 order by 1
;
--- 可售产品不可取消占比
select dt,count(1) pv,count(case when is_can=0 then 1 end) no_can_pv,count(case when is_can=0 then 1 end) / count(1) can_rate
from (
select dt,cast(split(homogenize_tag,'\\|')[3] as int) is_can,trd
from ihotel_default.dw_data_log_hour_xray_intl_galaxy_out
where dt >= '20260101'
-- and q_room_id = '256835492276_1623242'
-- and checkin_date = '2026-02-14'
-- and hotel_seq = 'i-tokyo_2032'
and country_name = '日本'
and cast(split(homogenize_tag,'\\|')[3] as int) is not null
group by 1,2,3
)group by 1 order by 1
;


---- 分货源情况
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
            ,case when  (product_order_refund_type = 'NO_CANCEL' or product_order_cancel_type = 'NO_CANCEL') then 'Y' else 'N' end is_non_ref --- 是否可取消订单
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
            and checkout_date between '2025-11-01' and date_sub(current_date, 1)
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
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then 'C代理'
                else 'ABE' end as supplier
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2025-11-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['COUNTRY'] = '日本'
)

select t1.checkout_date,t1.supplier,`Q订单`, `Q取消率`,`Q不可取消订单`,`Q不可取消订单占比`, `Q不可取消订单ALL`, `不可取消订单量占比`
      ,`C订单`,`C非当日取消率`,`C不可取消订单`, `C不可取消订单占比`,`C不可取消订单ALL`,`C不可取消订单量占比`
from (
    select checkout_date
        --    ,`渠道`
        ,case when supplier = 'C2Q直采'  then '直采'
              when supplier in ('C2Q-Agoda', 'Q-ABE' )  then 'ABE'
              when supplier in ('C2Q-代理', 'Q代理' )  then 'C代理'
         end  supplier
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单-当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单-当日`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q已离店订单-总共`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,sum(count(distinct case when is_non_ref='Y' then order_no end)) over(partition by checkout_date) `Q不可取消订单ALL`
        ,count(distinct case when is_non_ref='Y' then order_no end) / sum(count(distinct case when is_non_ref='Y' then order_no end)) over(partition by checkout_date) `不可取消订单量占比`
    from q_data
    group by 1,2
) t1 left join (
    select checkout_date
          ,supplier
          ,count(distinct order_no) as `C订单`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when order_status <> 'C' then order_no end) as     `C已离店订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C非当日取消率`
          ,sum(count(distinct case when is_no_cancle = 'Y' then order_no end)) over(partition by checkout_date) `C不可取消订单ALL`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / sum(count(distinct case when is_no_cancle = 'Y' then order_no end)) over(partition by checkout_date) `C不可取消订单量占比`
    from c_order
    group by 1,2

)t2 on t1.checkout_date=t2.checkout_date and t1.supplier=t2.supplier
order by 1,2
;


