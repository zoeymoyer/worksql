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
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date,room_night
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
            and checkout_date between '2026-01-01' and '2026-07-30'
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
            ,extend_info['room_night'] room_night
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
        AND substr(o.checkout_date, 1, 10) between '2026-01-01' and '2026-07-30' -- 退房日期范围
)

select coalesce(t1.checkout_date, t2.checkout_date) as checkout_date
      ,`Q离店间夜量`,`C离店间夜量`,`Q离店间夜量` / `C离店间夜量` as `离店间夜量QC`
      ,`Q预定间夜量`,`C预定间夜量`,`Q预定间夜量` / `C预定间夜量` as `预定间夜量QC`
      ,`Q取消间夜量`,`C取消间夜量`,`Q取消间夜量` / `C取消间夜量` as `取消间夜量QC`
      ,`Q离店订单`,`C离店订单`,`Q离店订单` / `C离店订单` as `离店订单量QC`
      ,`Q预定订单量`,`C预定订单量`,`Q预定订单量` / `C预定订单量` as `预定订单量QC`
      ,`Q取消订单量`,`C取消订单量`,`Q取消订单量` / `C取消订单量` as `取消订单量QC`
      ,`Q离店生单uv`,`C离店生单uv`,`Q离店生单uv` / `C离店生单uv` as `离店生单uvQC`
      ,`Q预定生单uv`,`C预定生单uv`,`Q预定生单uv` / `C预定生单uv` as `预定生单uvQC`
      ,`Q取消生单uv`,`C取消生单uv`,`Q取消生单uv` / `C取消生单uv` as `取消生单uvQC`
      ,(`Q离店间夜量` / `Q离店订单`) / (`C离店间夜量` / `C离店订单`) as `离店单订单间夜QC`
      ,(`Q预定间夜量` / `Q预定订单量`) / (`C预定间夜量` / `C预定订单量`) as `预定单订单间夜QC`
      ,(`Q取消间夜量` / `Q取消订单量`) / (`C取消间夜量` / `C取消订单量`) as `取消单订单间夜QC`
      ,`Q取消率`,`C取消率`,`Q不可取消订单占比`,`C不可取消订单占比`
from (
    select substr(checkout_date,1,10) checkout_date

        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,count(distinct user_id) as `Q生单uv`
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night) as `Q间夜量`

        ,count(distinct case when order_status in ('CHECKED_OUT') then user_id end) as `Q离店生单uv`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q离店订单`
        ,sum(case when order_status in ('CHECKED_OUT') then room_night end) as `Q离店间夜量`

        ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `Q预定生单uv`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q预定订单量`
        ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `Q预定间夜量`

        ,count(distinct case when order_status in ('CANCELLED','REJECTED')  then user_id end) as `Q取消生单uv`
        ,count(distinct case when order_status in ('CANCELLED','REJECTED') then order_no end) as `Q取消订单量`
        ,sum(case when order_status in ('CANCELLED','REJECTED') then room_night end) as `Q取消间夜量`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1
) t1 
full join (
    select substr(checkout_date,1,10) checkout_date
        --   ,if(grouping(supplier)=1,'ALL', supplier) as  supplier
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
          
          ,count(distinct user_id) as `C生单uv`
          ,count(distinct order_no) as `C订单`
          ,sum(room_night) as `C间夜量`
          ,count(distinct case when order_status <> 'C' then user_id end) as `C离店生单uv`
          ,count(distinct case when order_status <> 'C' then order_no end) as `C离店订单`
          ,sum(case when order_status <> 'C' then room_night end) as `C离店间夜量`
          ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `C预定生单uv`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C预定订单量`
          ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `C预定间夜量`
          ,count(distinct case when order_status = 'C'  then user_id end) as `C取消生单uv`
          ,count(distinct case when order_status = 'C' then order_no end) as `C取消订单量`
          ,sum(case when order_status = 'C' then room_night end) as `C取消间夜量`
    from c_order
    group by 1

)t2 on t1.checkout_date=t2.checkout_date
order by 1,2
;


--- 入住日期维度
with q_data as (
    select order_date 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
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
            ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
                end as per_type
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date,room_night
            ,order_status,checkin_date
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
            and checkin_date between '2026-01-01' and date_sub(current_date, 1)
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
            ,order_status,substr(o.checkin_date, 1, 10) AS checkin_date
            ,extend_info['room_night'] room_night
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' end as supplier
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订31+'  
          end as per_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkin_date, 1, 10) between '2026-01-01' and date_sub(current_date, 1) 
)

select coalesce(t1.checkin_date, t2.checkin_date) as checkin_date,coalesce(t1.per_type, t2.per_type) as per_type
      ,coalesce(t1.mdd, t2.mdd) as mdd
      ,`Q离店间夜量`,`C离店间夜量`,`Q离店间夜量` / `C离店间夜量` as `离店间夜量QC`
      ,`Q预定间夜量`,`C预定间夜量`,`Q预定间夜量` / `C预定间夜量` as `预定间夜量QC`
      ,`Q取消间夜量`,`C取消间夜量`,`Q取消间夜量` / `C取消间夜量` as `取消间夜量QC`
      ,`Q离店订单`,`C离店订单`,`Q离店订单` / `C离店订单` as `离店订单量QC`
      ,`Q预定订单量`,`C预定订单量`,`Q预定订单量` / `C预定订单量` as `预定订单量QC`
      ,`Q取消订单量`,`C取消订单量`,`Q取消订单量` / `C取消订单量` as `取消订单量QC`
      ,`Q离店生单uv`,`C离店生单uv`,`Q离店生单uv` / `C离店生单uv` as `离店生单uvQC`
      ,`Q预定生单uv`,`C预定生单uv`,`Q预定生单uv` / `C预定生单uv` as `预定生单uvQC`
      ,`Q取消生单uv`,`C取消生单uv`,`Q取消生单uv` / `C取消生单uv` as `取消生单uvQC`
      ,(`Q离店间夜量` / `Q离店订单`) / (`C离店间夜量` / `C离店订单`) as `离店单订单间夜QC`
      ,(`Q预定间夜量` / `Q预定订单量`) / (`C预定间夜量` / `C预定订单量`) as `预定单订单间夜QC`
      ,(`Q取消间夜量` / `Q取消订单量`) / (`C取消间夜量` / `C取消订单量`) as `取消单订单间夜QC`
      ,`Q取消率`,`C取消率`,`Q不可取消订单占比`,`C不可取消订单占比`
from (
    select substr(checkin_date,1,10) checkin_date
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd

        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,count(distinct user_id) as `Q生单uv`
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night) as `Q间夜量`

        ,count(distinct case when order_status in ('CHECKED_OUT') then user_id end) as `Q离店生单uv`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q离店订单`
        ,sum(case when order_status in ('CHECKED_OUT') then room_night end) as `Q离店间夜量`

        ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `Q预定生单uv`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q预定订单量`
        ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `Q预定间夜量`

        ,count(distinct case when order_status in ('CANCELLED','REJECTED')  then user_id end) as `Q取消生单uv`
        ,count(distinct case when order_status in ('CANCELLED','REJECTED') then order_no end) as `Q取消订单量`
        ,sum(case when order_status in ('CANCELLED','REJECTED') then room_night end) as `Q取消间夜量`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1,cube(per_type,mdd)
) t1 
full join (
    select substr(checkin_date,1,10) checkin_date
           ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
          
          ,count(distinct user_id) as `C生单uv`
          ,count(distinct order_no) as `C订单`
          ,sum(room_night) as `C间夜量`
          ,count(distinct case when order_status <> 'C' then user_id end) as `C离店生单uv`
          ,count(distinct case when order_status <> 'C' then order_no end) as `C离店订单`
          ,sum(case when order_status <> 'C' then room_night end) as `C离店间夜量`
          ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `C预定生单uv`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C预定订单量`
          ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `C预定间夜量`
          ,count(distinct case when order_status = 'C'  then user_id end) as `C取消生单uv`
          ,count(distinct case when order_status = 'C' then order_no end) as `C取消订单量`
          ,sum(case when order_status = 'C' then room_night end) as `C取消间夜量`
    from c_order
    group by 1,cube(per_type,mdd)

)t2 on t1.checkin_date=t2.checkin_date and t1.per_type=t2.per_type and t1.mdd=t2.mdd
order by 1,2
;


--- 离店日期维度
with q_data as (
    select order_date 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
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
            ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
                end as per_type
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date,room_night
            ,order_status,checkin_date
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
            ,order_status,substr(o.checkin_date, 1, 10) AS checkin_date
            ,extend_info['room_night'] room_night
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' end as supplier
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订31+'  
          end as per_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND substr(o.checkout_date, 1, 10) between '2026-01-01' and date_sub(current_date, 1) 
)

select coalesce(t1.checkout_date, t2.checkout_date) as checkout_date,coalesce(t1.per_type, t2.per_type) as per_type
      ,coalesce(t1.mdd, t2.mdd) as mdd
      ,`Q离店间夜量`,`C离店间夜量`,`Q离店间夜量` / `C离店间夜量` as `离店间夜量QC`
      ,`Q预定间夜量`,`C预定间夜量`,`Q预定间夜量` / `C预定间夜量` as `预定间夜量QC`
      ,`Q取消间夜量`,`C取消间夜量`,`Q取消间夜量` / `C取消间夜量` as `取消间夜量QC`
      ,`Q离店订单`,`C离店订单`,`Q离店订单` / `C离店订单` as `离店订单量QC`
      ,`Q预定订单量`,`C预定订单量`,`Q预定订单量` / `C预定订单量` as `预定订单量QC`
      ,`Q取消订单量`,`C取消订单量`,`Q取消订单量` / `C取消订单量` as `取消订单量QC`
      ,`Q离店生单uv`,`C离店生单uv`,`Q离店生单uv` / `C离店生单uv` as `离店生单uvQC`
      ,`Q预定生单uv`,`C预定生单uv`,`Q预定生单uv` / `C预定生单uv` as `预定生单uvQC`
      ,`Q取消生单uv`,`C取消生单uv`,`Q取消生单uv` / `C取消生单uv` as `取消生单uvQC`
      ,(`Q离店间夜量` / `Q离店订单`) / (`C离店间夜量` / `C离店订单`) as `离店单订单间夜QC`
      ,(`Q预定间夜量` / `Q预定订单量`) / (`C预定间夜量` / `C预定订单量`) as `预定单订单间夜QC`
      ,(`Q取消间夜量` / `Q取消订单量`) / (`C取消间夜量` / `C取消订单量`) as `取消单订单间夜QC`
      ,`Q取消率`,`C取消率`,`Q不可取消订单占比`,`C不可取消订单占比`
from (
    select substr(checkout_date,1,10) checkout_date
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd

        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,count(distinct user_id) as `Q生单uv`
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night) as `Q间夜量`

        ,count(distinct case when order_status in ('CHECKED_OUT') then user_id end) as `Q离店生单uv`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q离店订单`
        ,sum(case when order_status in ('CHECKED_OUT') then room_night end) as `Q离店间夜量`

        ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `Q预定生单uv`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q预定订单量`
        ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `Q预定间夜量`

        ,count(distinct case when order_status in ('CANCELLED','REJECTED')  then user_id end) as `Q取消生单uv`
        ,count(distinct case when order_status in ('CANCELLED','REJECTED') then order_no end) as `Q取消订单量`
        ,sum(case when order_status in ('CANCELLED','REJECTED') then room_night end) as `Q取消间夜量`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1,cube(per_type,mdd)
) t1 
full join (
    select substr(checkout_date,1,10) checkout_date
           ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
          
          ,count(distinct user_id) as `C生单uv`
          ,count(distinct order_no) as `C订单`
          ,sum(room_night) as `C间夜量`
          ,count(distinct case when order_status <> 'C' then user_id end) as `C离店生单uv`
          ,count(distinct case when order_status <> 'C' then order_no end) as `C离店订单`
          ,sum(case when order_status <> 'C' then room_night end) as `C离店间夜量`
          ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `C预定生单uv`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C预定订单量`
          ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `C预定间夜量`
          ,count(distinct case when order_status = 'C'  then user_id end) as `C取消生单uv`
          ,count(distinct case when order_status = 'C' then order_no end) as `C取消订单量`
          ,sum(case when order_status = 'C' then room_night end) as `C取消间夜量`
    from c_order
    group by 1,cube(per_type,mdd)

)t2 on t1.checkout_date=t2.checkout_date and t1.per_type=t2.per_type and t1.mdd=t2.mdd
order by 1,2
;






--- 入住日期维度
with q_data as (
    select order_date 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when a.country_name = '日本' then  '日本' else '非日本' end  is_jp
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
            ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                else '提前订31+' 
                end as per_type
            ,case
                when checkout_date between '2026-04-04' and '2026-04-06' then '26年清明'
                when checkout_date between '2026-02-15' and '2026-02-23' then '26年春节'
                when checkout_date between '2025-05-01' and '2025-05-05' then '25年五一'
            end as holiday_type
            ,hotel_seq,hotel_name,a.order_no,a.user_id,checkout_date,room_night
            ,order_status,checkin_date
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
            and (checkout_date between '2026-04-04' and '2026-04-06' or checkout_date between '2026-02-15' and '2026-02-23' or checkout_date between '2025-05-01' and '2025-05-05')
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
            ,order_status,substr(o.checkin_date, 1, 10) AS checkin_date
            ,extend_info['room_night'] room_night
            ,case when o.extend_info['CANCEL_TIME'] IS NULL OR o.extend_info['CANCEL_TIME'] = 'NULL' OR substr(o.extend_info['CANCEL_TIME'], 1, 10) > substr(o.order_date, 1, 10) then 'Y' else 'N' end is_not_cancel_d0 --- 当日是否取消拒单
            ,substr(o.extend_info['CANCEL_TIME'],1,10) cancel_date
            ,substr(o.extend_info['LastCancelTime'],1,10) LastCancel_date
            ---- 是否不可取消订单  Y为不可取消订单
            ,case when order_date >= o.extend_info['LastCancelTime']  then 'Y' else 'N' end is_no_cancle
            ,hotel_seq
            ,case when extend_info['vendor_name'] = 'DC' then '直采'
                when extend_info['vendor_name'] not in ('Agoda', 'Booking', 'Expedia') then '代理'
                else 'ABE' end as supplier
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订31+'  
            end as per_type
            ,case
                when substr(o.checkout_date, 1, 10) between '2026-04-04' and '2026-04-06' then '26年清明'
                when substr(o.checkout_date, 1, 10) between '2026-02-15' and '2026-02-23' then '26年春节'
                when substr(o.checkout_date, 1, 10) between '2025-05-01' and '2025-05-05' then '25年五一'
            end as holiday_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    WHERE   o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        AND o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        AND o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        AND o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        AND o.terminal_channel_type = 'app'
        AND (substr(o.checkout_date, 1, 10) between '2026-04-04' and '2026-04-06' or substr(o.checkout_date, 1, 10) between '2026-02-15' and '2026-02-23' or substr(o.checkout_date, 1, 10) between '2025-05-01' and '2025-05-05')
)

select coalesce(t1.checkout_date, t2.checkout_date) as checkout_date,coalesce(t1.per_type, t2.per_type) as per_type
      ,coalesce(t1.mdd, t2.mdd) as mdd,coalesce(t1.holiday_type, t2.holiday_type) as holiday_type
      ,`Q离店间夜量`,`C离店间夜量`,`Q离店间夜量` / `C离店间夜量` as `离店间夜量QC`
      ,`Q预定间夜量`,`C预定间夜量`,`Q预定间夜量` / `C预定间夜量` as `预定间夜量QC`
      ,`Q取消间夜量`,`C取消间夜量`,`Q取消间夜量` / `C取消间夜量` as `取消间夜量QC`
      ,`Q离店订单`,`C离店订单`,`Q离店订单` / `C离店订单` as `离店订单量QC`
      ,`Q预定订单量`,`C预定订单量`,`Q预定订单量` / `C预定订单量` as `预定订单量QC`
      ,`Q取消订单量`,`C取消订单量`,`Q取消订单量` / `C取消订单量` as `取消订单量QC`
      ,`Q离店生单uv`,`C离店生单uv`,`Q离店生单uv` / `C离店生单uv` as `离店生单uvQC`
      ,`Q预定生单uv`,`C预定生单uv`,`Q预定生单uv` / `C预定生单uv` as `预定生单uvQC`
      ,`Q取消生单uv`,`C取消生单uv`,`Q取消生单uv` / `C取消生单uv` as `取消生单uvQC`
      ,(`Q离店间夜量` / `Q离店订单`) / (`C离店间夜量` / `C离店订单`) as `离店单订单间夜QC`
      ,(`Q预定间夜量` / `Q预定订单量`) / (`C预定间夜量` / `C预定订单量`) as `预定单订单间夜QC`
      ,(`Q取消间夜量` / `Q取消订单量`) / (`C取消间夜量` / `C取消订单量`) as `取消单订单间夜QC`
      ,`Q取消率`,`C取消率`,`Q不可取消订单占比`,`C不可取消订单占比`
from (
    select substr(checkout_date,1,10) checkout_date
        ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
        ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
        ,if(grouping(holiday_type)=1,'ALL', holiday_type) as  holiday_type

        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
        ,count(distinct user_id) as `Q生单uv`
        ,count(distinct order_no) as `Q订单`
        ,sum(room_night) as `Q间夜量`

        ,count(distinct case when order_status in ('CHECKED_OUT') then user_id end) as `Q离店生单uv`
        ,count(distinct case when order_status in ('CHECKED_OUT') then order_no end) as `Q离店订单`
        ,sum(case when order_status in ('CHECKED_OUT') then room_night end) as `Q离店间夜量`

        ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `Q预定生单uv`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q预定订单量`
        ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `Q预定间夜量`

        ,count(distinct case when order_status in ('CANCELLED','REJECTED')  then user_id end) as `Q取消生单uv`
        ,count(distinct case when order_status in ('CANCELLED','REJECTED') then order_no end) as `Q取消订单量`
        ,sum(case when order_status in ('CANCELLED','REJECTED') then room_night end) as `Q取消间夜量`
    from  (
        select * 
              ,case when supplier = 'C2Q直采'                then '直采'
                    when supplier in ('C2Q-Agoda', 'Q-ABE')  then 'ABE'
                    when supplier in ('C2Q-代理', 'Q代理')  then '代理'
              end  supplier_type
        from q_data
    )t
    group by 1,cube(per_type,mdd,holiday_type)
) t1 
full join (
    select substr(checkout_date,1,10) checkout_date
           ,if(grouping(per_type)=1,'ALL', per_type) as  per_type
           ,if(grouping(mdd)=1,'ALL', mdd) as  mdd
           ,if(grouping(holiday_type)=1,'ALL', holiday_type) as  holiday_type
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单-当日`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
          ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
          
          ,count(distinct user_id) as `C生单uv`
          ,count(distinct order_no) as `C订单`
          ,sum(room_night) as `C间夜量`
          ,count(distinct case when order_status <> 'C' then user_id end) as `C离店生单uv`
          ,count(distinct case when order_status <> 'C' then order_no end) as `C离店订单`
          ,sum(case when order_status <> 'C' then room_night end) as `C离店间夜量`
          ,count(distinct case when is_not_cancel_d0 = 'Y'  then user_id end) as `C预定生单uv`
          ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C预定订单量`
          ,sum(case when is_not_cancel_d0 = 'Y' then room_night end) as `C预定间夜量`
          ,count(distinct case when order_status = 'C'  then user_id end) as `C取消生单uv`
          ,count(distinct case when order_status = 'C' then order_no end) as `C取消订单量`
          ,sum(case when order_status = 'C' then room_night end) as `C取消间夜量`
    from c_order
    group by 1,cube(per_type,mdd,holiday_type)

)t2 on t1.checkout_date=t2.checkout_date and t1.per_type=t2.per_type and t1.mdd=t2.mdd and t1.holiday_type=t2.holiday_type
order by 1,2
;