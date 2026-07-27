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
                -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                -- else '提前订31+' 
                else '提前订15+' 
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
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
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
                -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订15+'  
          end as per_type
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
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
          -- 5. 单晚多晚
          ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
          -- 6. 日期分类：holiday、workday、weekend
          ,dd.date_type,dd.holiday_name
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
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.order_date,1,10) = dd.date
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2025-01-01' and substr(order_date,1,10) <= cast(date_sub(current_date, 1) as string)
      and extend_info['PROVINCE'] in ('澳门','香港')
)
,q_agg as (
    select substr(order_date,1,7) as dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(date_type)=1, 'ALL', date_type) as date_type
          ,if(grouping(user_type)=1, 'ALL', user_type) as user_type
          ,if(grouping(adr_type)=1, 'ALL', adr_type) as adr_type
          ,if(grouping(supplier_type)=1, 'ALL', supplier_type) as supplier_type
          
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
          ,sum(init_gmv) as q_gmv
          ,sum(final_commission_after) as q_yj
          ,sum(coupon_substract_summary) as q_qe  -- 券额
          ,sum(jf_amt) q_jf_amt  -- 积分补
          ,sum(djb_amt) q_djb_amt -- 定价补
          ,sum(plat_amt) q_plat_amt -- 平台补
          ,sum(xyb_amt) q_xyb_amt -- 协议补
          ,sum(zjb_amt) q_zjb_amt -- 追价补
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as q_non_ref_orders
          ,count(distinct case when is_more_roomnight = '多晚' then order_no end) as q_more_roomnight_orders
    from (
        select *
              ,case when supplier_raw = 'C2Q直采' then '直采'
                    when supplier_raw in ('C2Q-Agoda', 'Q-ABE') then 'ABE'
                    when supplier_raw in ('C2Q-代理', 'Q代理') then '代理'
                    else '其他'
               end as supplier_type
        from q_order
    ) t
    group by substr(order_date,1,7),per_type,date_type,user_type,adr_type,supplier_type,linqi_type
    grouping sets (
        (substr(order_date,1,7),per_type,user_type),  -- 核心维度：提前订&新老客
        (substr(order_date,1,7),per_type,adr_type),   -- 核心维度：提前订&价格带
        (substr(order_date,1,7),per_type,date_type),  -- 核心维度：提前订&日期类型
        (substr(order_date,1,7),per_type,supplier_type), -- 核心维度：提前订&货源
        (substr(order_date,1,7),per_type), -- 核心维度：提前订

        -- 【新增】基于 linqi_type 的分组，保持同等维度下钻
        (substr(order_date,1,7),linqi_type,user_type),  
        (substr(order_date,1,7),linqi_type,adr_type),   
        (substr(order_date,1,7),linqi_type,date_type),  
        (substr(order_date,1,7),linqi_type,supplier_type), 
        (substr(order_date,1,7),linqi_type), 

        (substr(order_date,1,7))
    )
    
)
,c_agg as (
    select substr(dt,1,7) dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(date_type)=1, 'ALL', date_type) as date_type
          ,if(grouping(user_type)=1, 'ALL', user_type) as user_type
          ,if(grouping(adr_type)=1, 'ALL', adr_type) as adr_type
          ,if(grouping(supplier_type)=1, 'ALL', supplier_type) as supplier_type
          
          ,count(distinct order_no) as c_orders
          ,sum(room_night) as c_rn
          ,sum(room_fee) as c_gmv
          ,sum(comission) as c_yj
          ,sum(cqe) as c_qe
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as c_non_ref_orders
          ,count(distinct case when is_more_roomnight = '多晚' then order_no end) as c_more_roomnight_orders
    from c_order
    group by substr(dt,1,7),per_type,date_type,user_type,adr_type,supplier_type,linqi_type
    grouping sets (
        (substr(dt,1,7),per_type,user_type),  -- 核心维度：提前订&用户类型
        (substr(dt,1,7),per_type,adr_type),   -- 核心维度：提前订&价格带
        (substr(dt,1,7),per_type,date_type),  -- 核心维度：提前订&日期类型
        (substr(dt,1,7),per_type,supplier_type), -- 核心维度：提前订&货源
        (substr(dt,1,7),per_type), -- 核心维度：提前订
         -- 【新增】基于 linqi_type 的分组
        (substr(dt,1,7),linqi_type,user_type),  
        (substr(dt,1,7),linqi_type,adr_type),   
        (substr(dt,1,7),linqi_type,date_type),  
        (substr(dt,1,7),linqi_type,supplier_type), 
        (substr(dt,1,7),linqi_type), 
        (substr(dt,1,7))
    )
)

-- 【合并输出】
select coalesce(q.dt, c.dt) as `日期`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      ,coalesce(q.date_type, c.date_type) as `日期类型`
      ,coalesce(q.user_type, c.user_type) as `用户类型`
      ,coalesce(q.adr_type, c.adr_type) as `价格带`
      ,coalesce(q.supplier_type, c.supplier_type) as `货源`
      
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(q.q_non_ref_orders, 0) as `Q不可取消订单量`
      ,coalesce(q.q_more_roomnight_orders, 0) as `Q多晚订单量`
      ,concat(round(coalesce(q.q_non_ref_orders / nullif(q.q_orders, 0), 0) * 100, 2), '%') as `Q不可取消占比`
      ,concat(round(coalesce(q.q_more_roomnight_orders / nullif(q.q_orders, 0), 0) * 100, 2), '%') as `Q多晚订单占比`
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(q.q_gmv, 0) as `Q_GMV`
      ,coalesce(q.q_yj, 0) as `Q佣金`
      ,coalesce(q.q_qe, 0) as `Q券额`
      ,coalesce(q.q_jf_amt, 0) as `Q积分补`
      ,coalesce(q.q_djb_amt, 0) as `Q定价补`
      ,coalesce(q.q_plat_amt, 0) as `Q平台补`
      ,coalesce(q.q_xyb_amt, 0) as `Q协议补`
      ,coalesce(q.q_zjb_amt, 0) as `Q追价补`  
      
      ,coalesce(c.c_orders, 0) as `C订单量`
      ,coalesce(c.c_non_ref_orders, 0) as `C不可取消订单量`
      ,coalesce(c.c_more_roomnight_orders, 0) as `C多晚订单量`
      ,concat(round(coalesce(c.c_non_ref_orders / nullif(c.c_orders, 0), 0) * 100, 2), '%') as `C不可取消占比`
      ,concat(round(coalesce(c.c_more_roomnight_orders / nullif(c.c_orders, 0), 0) * 100, 2), '%') as `C多晚订单占比`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(c.c_gmv, 0) as `C_GMV`
      ,coalesce(c.c_yj, 0) as `C佣金`
      ,coalesce(c.c_qe, 0) as `C券额`
      
      ,coalesce(q.q_rn, 0)  / coalesce(c.c_rn, 0)  as `间夜量QC`
      ,coalesce(q.q_yj, 0)  / coalesce(c.c_yj, 0)  as `收益QC`
      ,(coalesce(q.q_rn, 0) / coalesce(q.q_orders, 0))  / (coalesce(c.c_rn, 0) / coalesce(c.c_orders, 0))  as `单间夜QC`

from q_agg q
left  join c_agg c on q.dt = c.dt and q.per_type = c.per_type and q.date_type = c.date_type and q.adr_type = c.adr_type and q.supplier_type = c.supplier_type and q.user_type=c.user_type
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
                -- when datediff(checkin_date, a.dt) between 15 and 30  then '提前订15-30天'
                else '提前订15+' 
          end as per_type
          -- 新增：临期订 & 非临期订 流量打标维度
          ,case when datediff(checkin_date, a.dt) <= 7 then '临期订'
                else '非临期订'
          end as linqi_type
          ,a.user_id
          -- 5. 单晚多晚
        ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
        ,dd.date_type
    from ihotel_default.dw_user_app_log_detail_visit_di_v1 a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.dt = dd.date
    where a.dt >= '2025-01-01' and a.dt <= date_sub(current_date, 1)
      and a.business_type = 'hotel'
      and (a.province_name in ('台湾','澳门','香港') or a.country_name !='中国')
      and province_name in ('澳门','香港')
    group by 1,2,3,4,5,6,7,8
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
                -- when datediff(substr(check_in,1,10), a.dt) between 15 and 30  then '提前订15-30天'
                else '提前订15+' 
          end as per_type
          -- 新增：临期订 & 非临期订 流量打标维度
          ,case when datediff(substr(check_in,1,10), a.dt) <= 7 then '临期订'
                else '非临期订'
          end as linqi_type
          ,a.uid
          ,case when datediff(substr(check_out,1,10), substr(check_in,1,10)) >= 2 then '多晚' else '单晚' end is_more_roomnight
          ,dd.date_type
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid = b.ubt_user_id
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.dt = dd.date
    where a.dt >= '2025-01-01' and a.dt <= date_sub(current_date, 1)
      and a.device_chl = 'app' and a.page_short_domain = 'dbo' and provincename in ('澳门','香港')
    group by 1,2,3,4,5,6,7,8
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
                    -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                    else '提前订15+' 
            end as per_type
            ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                else '非临期订' 
            end as linqi_type
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
            -- 5. 单晚多晚
            ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
            -- 6. 日期分类：holiday、workday、weekend
            ,dd.date_type,dd.holiday_name

            ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
            ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补
            ,coalesce(get_json_object(extendinfomap,'$.platform_amount'),0) * room_night  plat_amt  --- 平台补
            ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0) * room_night + coalesce(cashbackmap['framework_amount'],0)  xyb_amt  --- 协议补
            ,case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
                    then coalesce(follow_price_amount,0) end zjb_amt --- 追价补
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.order_date = dd.date
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
      and terminal_channel_type = 'app' 
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
      and (first_rejected_time is null or date(first_rejected_time) > order_date) 
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid='1'
      and a.order_date >= '2025-01-01' and a.order_date <= date_sub(current_date, 1)
      and province_name in ('澳门','香港') -- 只分析港澳订单
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
                    -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                    else '提前订15+'  
            end as per_type
            -- 新增：临期订 & 非临期订 订单打标维度
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订'
                    else '非临期订'
            end as linqi_type
            -- 4. 不可取消判定
            ,case when substr(order_date,1,10) >= substr(o.extend_info['LastCancelTime'],1,10) then 'Y' else 'N' end as is_no_cancle
            
            ,o.user_id,order_no, room_fee, cast(extend_info['room_night'] as double) as room_night,comission
            ,get_json_object(orig_discount_detail, '$.detail[1].amount') as cqe  -- C_券额
            -- 5. 单晚多晚
            ,case when datediff(substr(checkout_date,1,10), substr(checkin_date,1,10)) >= 2 then '多晚' else '单晚' end is_more_roomnight
            -- 6. 日期分类：holiday、workday、weekend
            ,dd.date_type,dd.holiday_name
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
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.order_date,1,10) = dd.date
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(o.order_date,1,10) >= '2025-01-01' and substr(o.order_date,1,10) <= date_sub(current_date, 1)
      and extend_info['PROVINCE'] in ('澳门','香港') -- 只 analysis 港澳订单
)
,q_uv_agg as (
    select substr(t.dt,1,7) as mth
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(t.date_type)=1, 'ALL', t.date_type) as date_type
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.is_more_roomnight)=1, 'ALL', t.is_more_roomnight) as is_more_roomnight
          ,count(distinct t.user_id) as q_mau
    from q_traf t
    group by substr(t.dt,1,7),t.date_type, t.user_type, t.per_type, t.linqi_type, is_more_roomnight
    grouping sets (
        (substr(t.dt,1,7)),
        (substr(t.dt,1,7),t.per_type),
        (substr(t.dt,1,7),t.per_type,t.user_type),
        (substr(t.dt,1,7),t.per_type,t.date_type),
        (substr(t.dt,1,7),t.per_type,t.is_more_roomnight),
        -- 新增：临期大维度的聚合组合
        (substr(t.dt,1,7),t.linqi_type),
        (substr(t.dt,1,7),t.linqi_type,t.user_type),
        (substr(t.dt,1,7),t.linqi_type,t.date_type),
        (substr(t.dt,1,7),t.linqi_type,t.is_more_roomnight)
    )
)
,q_ord_agg as (
    select substr(t.order_date,1,7) as mth
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(t.date_type)=1, 'ALL', t.date_type) as date_type
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.is_more_roomnight)=1, 'ALL', t.is_more_roomnight) as is_more_roomnight
          ,count(distinct t.order_no) as q_orders
          ,sum(t.room_night) as q_rn
          ,sum(t.init_gmv) as q_gmv
          ,sum(t.final_commission_after) as q_yj
          ,sum(coupon_substract_summary) as q_qe  -- 券额
          ,sum(jf_amt) q_jf_amt  -- 积分补
          ,sum(djb_amt) q_djb_amt -- 定价补
          ,sum(plat_amt) q_plat_amt -- 平台补
          ,sum(xyb_amt) q_xyb_amt -- 协议补
          ,sum(zjb_amt) q_zjb_amt -- 追价补
          ,count(distinct case when t.is_non_ref = 'Y' then t.order_no end) as q_non_ref_orders
    from q_order t
    group by substr(t.order_date,1,7),t.date_type, t.user_type, t.per_type, t.linqi_type, is_more_roomnight
    grouping sets (
        (substr(t.order_date,1,7)),
        (substr(t.order_date,1,7),t.per_type),
        (substr(t.order_date,1,7),t.per_type,t.user_type),
        (substr(t.order_date,1,7),t.per_type,t.date_type),
        (substr(t.order_date,1,7),t.per_type,t.is_more_roomnight),
        -- 新增：临期大维度的聚合组合
        (substr(t.order_date,1,7),t.linqi_type),
        (substr(t.order_date,1,7),t.linqi_type,t.user_type),
        (substr(t.order_date,1,7),t.linqi_type,t.date_type),
        (substr(t.order_date,1,7),t.linqi_type,t.is_more_roomnight)
    )
)
,c_uv_agg as (
    select substr(t.dt,1,7) as mth
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(t.date_type)=1, 'ALL', t.date_type) as date_type
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.is_more_roomnight)=1, 'ALL', t.is_more_roomnight) as is_more_roomnight
          ,count(distinct t.uid) as c_mau
    from c_traf t
    group by substr(t.dt,1,7),t.date_type, t.user_type, t.per_type, t.linqi_type, is_more_roomnight
    grouping sets (
        (substr(t.dt,1,7)),
        (substr(t.dt,1,7),t.per_type),
        (substr(t.dt,1,7),t.per_type,t.user_type),
        (substr(t.dt,1,7),t.per_type,t.date_type),
        (substr(t.dt,1,7),t.per_type,t.is_more_roomnight),
        -- 新增：临期大维度的聚合组合
        (substr(t.dt,1,7),t.linqi_type),
        (substr(t.dt,1,7),t.linqi_type,t.user_type),
        (substr(t.dt,1,7),t.linqi_type,t.date_type),
        (substr(t.dt,1,7),t.linqi_type,t.is_more_roomnight)
    )
)
,c_order_agg as (
    select substr(t.dt,1,7) as mth
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(t.date_type)=1, 'ALL', t.date_type) as date_type
          ,if(grouping(t.user_type)=1, 'ALL', t.user_type) as user_type
          ,if(grouping(t.is_more_roomnight)=1, 'ALL', t.is_more_roomnight) as is_more_roomnight
          ,count(distinct t.order_no) as c_orders
          ,sum(t.room_night) as c_rn
          ,sum(t.room_fee) as c_gmv
          ,sum(t.comission) as c_yj
          ,sum(t.cqe) as c_qe
          ,count(distinct case when t.is_no_cancle = 'Y' then t.order_no end) as c_non_ref_orders
    from c_order t
    group by substr(t.dt,1,7),t.date_type, t.user_type, t.per_type, t.linqi_type, is_more_roomnight
    grouping sets (
        (substr(t.dt,1,7)),
        (substr(t.dt,1,7),t.per_type),
        (substr(t.dt,1,7),t.per_type,t.user_type),
        (substr(t.dt,1,7),t.per_type,t.date_type),
        (substr(t.dt,1,7),t.per_type,t.is_more_roomnight),
        -- 新增：临期大维度的聚合组合
        (substr(t.dt,1,7),t.linqi_type),
        (substr(t.dt,1,7),t.linqi_type,t.user_type),
        (substr(t.dt,1,7),t.linqi_type,t.date_type),
        (substr(t.dt,1,7),t.linqi_type,t.is_more_roomnight)
    )
)


select coalesce(q.mth, c.mth) as `月份`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      ,coalesce(q.is_more_roomnight, c.is_more_roomnight) as `是否多晚订单`
      ,coalesce(q.user_type, c.user_type) as `新老客`
      ,coalesce(q.date_type, c.date_type) as `日期类型`
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
left join c_uv_agg c on q.mth = c.mth and q.date_type = c.date_type and q.user_type = c.user_type and q.per_type = c.per_type and q.is_more_roomnight = c.is_more_roomnight
left join q_ord_agg qo on q.mth = qo.mth and q.date_type = qo.date_type and q.user_type = qo.user_type and q.per_type = qo.per_type and q.is_more_roomnight = qo.is_more_roomnight
left join c_order_agg co on c.mth = co.mth and c.date_type = co.date_type and c.user_type = co.user_type and c.per_type = co.per_type and c.is_more_roomnight = co.is_more_roomnight
order by coalesce(q.mth, c.mth) desc;


-- 3、离店口径数据
with q_order as ( --- Q侧订单明细打标
    select a.order_date,a.checkout_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                -- else '提前订31+' 
                else '提前订15+' 
          end as per_type
          ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
          -- 2. 价格带逻辑
          ,case when final_gmv / nullif(room_night, 0) < 400   then '1[0,400)'
                when final_gmv / nullif(room_night, 0) >= 400  and final_gmv / nullif(room_night, 0) < 800   then '2[400,800)'
                when final_gmv / nullif(room_night, 0) >= 800  and final_gmv / nullif(room_night, 0) < 1200  then '3[800,1200)'
                when final_gmv / nullif(room_night, 0) >= 1200 and final_gmv / nullif(room_night, 0) < 1600  then '4[1200,1600)'
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
          ,a.user_id,a.order_no, a.init_gmv, a.room_night,final_gmv
          ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (final_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else final_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
          ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
        -- 5. 单晚多晚
        ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
        -- 6. 日期分类：holiday、workday、weekend
        ,dd.date_type,dd.holiday_name

        ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
        ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补
        ,coalesce(get_json_object(extendinfomap,'$.platform_amount'),0) * room_night  plat_amt  --- 平台补
        ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0) * room_night + coalesce(cashbackmap['framework_amount'],0)  xyb_amt  --- 协议补
        ,case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
                then coalesce(follow_price_amount,0) end zjb_amt --- 追价补
    from default.mdw_order_v3_international a 
    left join (select order_no, max(purchase_order_no) as purchase_order_no from ihotel_default.dw_purchase_order_info_v3 where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') group by order_no) p on a.order_no = p.order_no
    left join (select distinct partner_order_no, extend_info['vendor_name'] as vendor_name from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da where dt = cast(date_sub(current_date, 1) as string)) c on p.purchase_order_no = c.partner_order_no
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.checkout_date = dd.date
    where a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and a.order_no <> '103576132435'
        and a.checkout_date >= '2025-01-01' and a.checkout_date <= date_sub(current_date, 1)
        and province_name in ('澳门','香港') -- 只分析港澳订单
)
,c_order as ( 
    select substr(o.checkout_date,1,10) as dt
          ,case when extend_info['PROVINCE'] in ('澳门','香港') then '港澳'  
                when extend_info['COUNTRY'] in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when extend_info['COUNTRY'] in ('日本','韩国','泰国') then extend_info['COUNTRY'] 
                else '其他' end as new_mdd
          
          -- 1. 新版提前订逻辑
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                else '提前订15+'  
          end as per_type
          ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订' 
                else '非临期订' 
          end as linqi_type
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
          -- 5. 单晚多晚
          ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
          -- 6. 日期分类：holiday、workday、weekend
          ,dd.date_type,dd.holiday_name
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
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
            and substr(checkout_date,1,10) >= '2025-01-01' and substr(checkout_date,1,10) <= cast(date_sub(current_date, 1) as string)
    ) oc on o.order_no = oc.order_no_oc
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.checkout_date,1,10) = dd.date
    where dt = cast(date_sub(current_date, 1) as string)
      and extend_info['IS_IBU'] = '0' and extend_info['book_channel'] = 'Ctrip' 
      and extend_info['sub_book_channel'] = 'Direct-Ctrip' 
      and terminal_channel_type = 'app' 
      and order_status <> 'C'
      and substr(checkout_date,1,10) >= '2025-01-01' and substr(checkout_date,1,10) <= cast(date_sub(current_date, 1) as string)
      and extend_info['PROVINCE'] in ('澳门','香港')
)
,q_agg as (
    select substr(checkout_date,1,7) as dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(date_type)=1, 'ALL', date_type) as date_type
          ,if(grouping(adr_type)=1, 'ALL', adr_type) as adr_type
          ,if(grouping(supplier_type)=1, 'ALL', supplier_type) as supplier_type
          
          ,count(distinct order_no) as q_orders
          ,sum(room_night) as q_rn
          ,sum(init_gmv) as q_gmv
          ,sum(final_commission_after) as q_yj
          ,sum(coupon_substract_summary) as q_qe  -- 券额
          ,sum(jf_amt) q_jf_amt  -- 积分补
          ,sum(djb_amt) q_djb_amt -- 定价补
          ,sum(plat_amt) q_plat_amt -- 平台补
          ,sum(xyb_amt) q_xyb_amt -- 协议补
          ,sum(zjb_amt) q_zjb_amt -- 追价补
          ,count(distinct case when is_non_ref = 'Y' then order_no end) as q_non_ref_orders
          ,count(distinct case when is_more_roomnight = '多晚' then order_no end) as q_more_roomnight_orders
    from (
        select *
              ,case when supplier_raw = 'C2Q直采' then '直采'
                    when supplier_raw in ('C2Q-Agoda', 'Q-ABE') then 'ABE'
                    when supplier_raw in ('C2Q-代理', 'Q代理') then '代理'
                    else '其他'
               end as supplier_type
        from q_order
    ) t
    group by substr(checkout_date,1,7),per_type,date_type,adr_type,supplier_type
    grouping sets (
        (substr(checkout_date,1,7),per_type,adr_type),   -- 核心维度：提前订&价格带
        (substr(checkout_date,1,7),per_type,date_type),  -- 核心维度：提前订&日期类型
        (substr(checkout_date,1,7),per_type,supplier_type), -- 核心维度：提前订&货源
        (substr(checkout_date,1,7),per_type), -- 核心维度：提前订

        -- 【新增】基于 linqi_type 的分组，保持同等维度下钻
        (substr(checkout_date,1,7),linqi_type,adr_type),   
        (substr(checkout_date,1,7),linqi_type,date_type),  
        (substr(checkout_date,1,7),linqi_type,supplier_type), 
        (substr(checkout_date,1,7),linqi_type), 

        (substr(checkout_date,1,7))
    )
)
,c_agg as (
    select substr(dt,1,7) dt
          ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
          ,if(grouping(date_type)=1, 'ALL', date_type) as date_type
          ,if(grouping(adr_type)=1, 'ALL', adr_type) as adr_type
          ,if(grouping(supplier_type)=1, 'ALL', supplier_type) as supplier_type
          
          ,count(distinct order_no) as c_orders
          ,sum(room_night) as c_rn
          ,sum(room_fee) as c_gmv
          ,sum(comission) as c_yj
          ,sum(cqe) as c_qe
          ,count(distinct case when is_no_cancle = 'Y' then order_no end) as c_non_ref_orders
          ,count(distinct case when is_more_roomnight = '多晚' then order_no end) as c_more_roomnight_orders
    from c_order
    group by substr(dt,1,7),per_type,date_type,adr_type,supplier_type
    grouping sets (
        (substr(dt,1,7),per_type,adr_type),   -- 核心维度：提前订&价格带
        (substr(dt,1,7),per_type,date_type),  -- 核心维度：提前订&日期类型
        (substr(dt,1,7),per_type,supplier_type), -- 核心维度：提前订&货源
        (substr(dt,1,7),per_type), -- 核心维度：提前订
         -- 【新增】基于 linqi_type 的分组
        (substr(dt,1,7),linqi_type,adr_type),   
        (substr(dt,1,7),linqi_type,date_type),  
        (substr(dt,1,7),linqi_type,supplier_type), 
        (substr(dt,1,7),linqi_type), 
        (substr(dt,1,7))
    )
)

-- 【合并输出】
select coalesce(q.dt, c.dt) as `日期`
      ,coalesce(q.per_type, c.per_type) as `提前订类型`
      ,coalesce(q.date_type, c.date_type) as `日期类型`
      ,coalesce(q.adr_type, c.adr_type) as `价格带`
      ,coalesce(q.supplier_type, c.supplier_type) as `货源`
      
      ,coalesce(q.q_orders, 0) as `Q订单量`
      ,coalesce(q.q_non_ref_orders, 0) as `Q不可取消订单量`
      ,coalesce(q.q_more_roomnight_orders, 0) as `Q多晚订单量`
      ,concat(round(coalesce(q.q_non_ref_orders / nullif(q.q_orders, 0), 0) * 100, 2), '%') as `Q不可取消占比`
      ,concat(round(coalesce(q.q_more_roomnight_orders / nullif(q.q_orders, 0), 0) * 100, 2), '%') as `Q多晚订单占比`
      ,coalesce(q.q_rn, 0) as `Q间夜量`
      ,coalesce(q.q_gmv, 0) as `Q_GMV`
      ,coalesce(q.q_yj, 0) as `Q佣金`
      ,coalesce(q.q_qe, 0) as `Q券额`
      ,coalesce(q.q_jf_amt, 0) as `Q积分补`
      ,coalesce(q.q_djb_amt, 0) as `Q定价补`
      ,coalesce(q.q_plat_amt, 0) as `Q平台补`
      ,coalesce(q.q_xyb_amt, 0) as `Q协议补`
      ,coalesce(q.q_zjb_amt, 0) as `Q追价补`  
      
      ,coalesce(c.c_orders, 0) as `C订单量`
      ,coalesce(c.c_non_ref_orders, 0) as `C不可取消订单量`
      ,coalesce(c.c_more_roomnight_orders, 0) as `C多晚订单量`
      ,concat(round(coalesce(c.c_non_ref_orders / nullif(c.c_orders, 0), 0) * 100, 2), '%') as `C不可取消占比`
      ,concat(round(coalesce(c.c_more_roomnight_orders / nullif(c.c_orders, 0), 0) * 100, 2), '%') as `C多晚订单占比`
      ,coalesce(c.c_rn, 0) as `C间夜量`
      ,coalesce(c.c_gmv, 0) as `C_GMV`
      ,coalesce(c.c_yj, 0) as `C佣金`
      ,coalesce(c.c_qe, 0) as `C券额`
      
      ,coalesce(q.q_rn, 0)  / coalesce(c.c_rn, 0)  as `间夜量QC`
      ,coalesce(q.q_yj, 0)  / coalesce(c.c_yj, 0)  as `收益QC`
      ,(coalesce(q.q_rn, 0) / coalesce(q.q_orders, 0))  / (coalesce(c.c_rn, 0) / coalesce(c.c_orders, 0))  as `单间夜QC`

from q_agg q
left  join c_agg c on q.dt = c.dt and q.per_type = c.per_type and q.date_type = c.date_type and q.adr_type = c.adr_type and q.supplier_type = c.supplier_type
order by `日期` desc;



-- 4、离店取消率数据
with q_data as ( -- Q取消率数据
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
            -- 1. 新版提前订逻辑
            ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                    when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                    when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                    when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                    -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                    -- else '提前订31+' 
                    else '提前订15+' 
            end as per_type
            ,case when datediff(checkin_date, order_date) <= 7 then '临期订' 
                    else '非临期订' 
            end as linqi_type
            ,case when init_gmv / room_night < 400   then '1[0,400)'
                  when init_gmv / room_night >= 400  and init_gmv / room_night < 800   then '2[400,800)'
                  when init_gmv / room_night >= 800  and init_gmv / room_night < 1200  then '3[800,1200)'
                  when init_gmv / room_night >= 1200 and init_gmv / room_night < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type 
            ,dd.date_type,dd.holiday_name
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.checkout_date = dd.date 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and is_valid='1'
        and a.order_no <> '103576132435'
        and checkout_date between '2025-01-01' and date_sub(current_date, 1)
        and province_name in ('澳门','香港')  -- 只分析港澳订单
)
,c_data as( --- C取消率数据
    select  substr(o.checkout_date, 1, 10) AS checkout_date
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
            -- 1. 新版提前订逻辑
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) < 0 or datediff(substr(checkin_date,1,10), substr(order_date,1,10)) = 0  then '凌晨订&当天订'
                    when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 1 and 3    then '提前订1-3天'
                    when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 4 and 7    then '提前订4-7天'
                    when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 8 and 14   then '提前订8-14天'
                    -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                    else '提前订15+'  
            end as per_type
            ,case when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) <= 7 then '临期订' 
                    else '非临期订' 
            end as linqi_type
            ,case when room_fee / extend_info['room_night'] < 400  then '1[0,400)'
                  when room_fee / extend_info['room_night'] >= 400 and  room_fee / extend_info['room_night'] < 800  then '2[400,800)'
                  when room_fee / extend_info['room_night'] >= 800 and  room_fee / extend_info['room_night'] < 1200  then '3[800,1200)'
                  when room_fee / extend_info['room_night'] >= 1200 and room_fee / extend_info['room_night'] < 1600  then '4[1200,1600)'
                  else '5[1600+]' 
            end adr_type
            ,dd.date_type
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on substr(o.checkout_date,1,10) = dd.date
    where  o.dt = '%(FORMAT_DATE)s'  -- 数据分区日期
        and o.extend_info['IS_IBU'] = '0'  -- 非IBU订单
        and o.extend_info['book_channel'] = 'Ctrip'  -- 携程主渠道
        and o.extend_info['sub_book_channel'] = 'Direct-Ctrip'  -- 携程直连子渠道
        and o.terminal_channel_type = 'app'
        and substr(o.checkout_date, 1, 10) between '2025-01-01' and date_sub(current_date, 1) -- 退房日期范围
        and extend_info['PROVINCE'] in ('澳门','香港')
)

select t1.mth
    ,t1.per_type
    ,t1.date_type
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
    select substr(checkout_date,1,7) mth
        ,case when grouping(per_type)=0 then per_type 
            when grouping(linqi_type)=0 then linqi_type
            else 'ALL' 
        end as per_type
        ,if(grouping(date_type)=1, 'ALL', date_type) as date_type
        ,count(distinct order_no) as `Q订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `Q未取消订单_当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as     `Q取消订单_当日`
        ,count(distinct case when is_non_ref='Y' then order_no end) `Q不可取消订单`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end)  `Q取消率`
        ,count(distinct case when is_non_ref='Y' then order_no end) / count(distinct order_no) `Q不可取消订单占比`
    from q_data
    group by substr(checkout_date,1,7),per_type,date_type
    grouping sets (
        (substr(checkout_date,1,7),per_type,date_type),  -- 核心维度：提前订&日期类型
        (substr(checkout_date,1,7),per_type), -- 核心维度：提前订
        -- 【新增】基于 linqi_type 的分组，保持同等维度下钻
        (substr(checkout_date,1,7),linqi_type,date_type),  
        (substr(checkout_date,1,7),linqi_type), 
        (substr(checkout_date,1,7))
    )
) t1 
left join (
    select substr(checkout_date,1,7) mth
        ,case when grouping(per_type)=0 then per_type 
            when grouping(linqi_type)=0 then linqi_type
            else 'ALL' 
        end as per_type
        ,if(grouping(date_type)=1, 'ALL', date_type) as date_type
        ,count(distinct order_no) as `C订单`
        ,count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as `C未取消订单_当日`
        ,count(distinct case when is_cancel_d0 = 'Y' then order_no end) as      `C取消订单_当日`
        ,count(distinct case when order_status <> 'C' then order_no end) as    `C已离店订单`
        ,count(distinct case when is_no_cancle = 'Y' then order_no end) as     `C不可取消订单`
        ,count(distinct case when order_status = 'C' then order_no end) as     `C取消订单`
        ,count(distinct case when is_no_cancle = 'Y' then order_no end) / count(distinct order_no)  `C不可取消订单占比`
        ,1- count(distinct case when order_status <> 'C' then order_no end) / count(distinct case when is_not_cancel_d0 = 'Y' then order_no end) as  `C取消率`
    from c_data
    group by substr(checkout_date,1,7),per_type,date_type
    grouping sets (
        (substr(checkout_date,1,7),per_type,date_type),  -- 核心维度：提前订&日期类型
        (substr(checkout_date,1,7),per_type), -- 核心维度：提前订
        -- 【新增】基于 linqi_type 的分组，保持同等维度下钻
        (substr(checkout_date,1,7),linqi_type,date_type),  
        (substr(checkout_date,1,7),linqi_type), 
        (substr(checkout_date,1,7))
    )
)t2 on t1.mth=t2.mth and t1.per_type=t2.per_type and t1.date_type=t2.date_type
order by t1.mth desc, t1.per_type, t1.date_type;




--- 5、产品力数据QC对比
with qc_price as (
    select order_date
        ,case when grouping(per_type)=0 then per_type 
                when grouping(linqi_type)=0 then linqi_type
                else 'ALL' 
           end as per_type
        ,if(grouping(channel_type)=1,'ALL', channel_type) as  channel_type
        ,if(grouping(date_type)=1,'ALL', date_type) as  date_type
        ,if(grouping(is_more_roomnight)=1,'ALL', is_more_roomnight) as  is_more_roomnight
        ,count(distinct case when business_type='intl_crawl_cq_spa' and pay_price_compare_result = 'Qlose' then id end) 
            / count(distinct case when business_type='intl_crawl_cq_spa' then id end) as `支付价lose率`

        ,count(distinct case when business_type='intl_crawl_cq_api_userview' and chased_discount_price_compare_result = 'Qlose' then id end) 
            / count(distinct case when business_type='intl_crawl_cq_api_userview' then id end) as `底价lose率`  


        ,sum(case when business_type='intl_crawl_cq_spa' and pay_price_diff > 0 then pay_price_diff end) / sum(case when business_type='intl_crawl_cq_spa' and pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when business_type='intl_crawl_cq_spa' and pay_price_diff < 0 then pay_price_diff end) / sum(case when business_type='intl_crawl_cq_spa' and pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`

        ,sum(case when business_type='intl_crawl_cq_api_userview' and bp_advantage_amount < 0 then bp_advantage_amount end) / sum(case when business_type='intl_crawl_cq_api_userview' and bp_advantage_amount < 0  then ctrip_pay_price end) as `底价lose深度`

        ,sum(case when business_type='intl_crawl_cq_api_userview' then bp_advantage_amount end)  `折后底价Q-C`
        ,sum(case when business_type='intl_crawl_cq_api_userview' then ctrip_pay_price end) as `底价C支付价`

        ,count(distinct case when business_type='intl_crawl_cq_spa' then id end) as `支付价抓取次数`
        ,count(distinct case when business_type='intl_crawl_cq_spa' and pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when business_type='intl_crawl_cq_spa' and pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`

        ,count(distinct case when business_type='intl_crawl_cq_api_userview' then id end) as `底价次数`
        ,count(distinct case when business_type='intl_crawl_cq_api_userview' and chased_discount_price_compare_result = 'Qlose' then id end) as `底价lose数`
        ,count(distinct case when business_type='intl_crawl_cq_api_userview' and chased_discount_price_compare_result = 'Qbeat' then id end) as `底价beat数`
    from (
        select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
            ,case when province_name in ('澳门','香港') then '港澳'  
                    when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                    when a.country_name in ('日本','韩国','泰国') then a.country_name 
                    else '其他' end as new_mdd
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
            ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
            ,case   when SPLIT(a.qunar_product_room_id, '_')[1] in ('1615667','800000164') and qunar_price_info['agentPriceFlag'] = 'false' then 'DC'
                    when SPLIT(a.qunar_product_room_id, '_')[1] in ('1615667','800000164') then 'C2Q'
                    when d.wrapper_name in ('Agoda','Booking','Expedia') then 'ABE'
                    else '小代理'
            end as channel_type
            ,id,pay_price_diff,ctrip_pay_price,business_type
            
            -- 【新增】: 使用解析后的 order_date 和 check_in 日期计算提前订分布
            ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) < 0 or datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) = 0 then '凌晨订&当天订'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 1 and 3    then '提前订1-3天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 4 and 7    then '提前订4-7天'
                  when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 8 and 14   then '提前订8-14天'
                --   when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) between 15 and 30  then '提前订15-30天'
                  else '提前订15+' 
             end as per_type
             ,case when datediff(check_in, concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))) <= 7    then '临期订'
                  else '非临期订' 
             end as linqi_type
             ,case when datediff(check_out, check_in) >= 2 then '多晚' else '单晚' end is_more_roomnight
            ,pay_price_compare_result  --- 支付价比价结果
            ,chased_discount_price_compare_result --- 底价比较结果
            ,-chased_discount_price_diff as bp_advantage_amount
            ,dd.date_type
        from default.dwd_hotel_cq_compare_price_result_intl_hi a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
        left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.dt = replace(dd.date, '-', '')
        left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) d on a.qunar_wrapper_id = d.wrapper_id
        where dt >= '20250101' and dt <= replace(date_sub(current_date, 1),'-','')
            and business_type in ('intl_crawl_cq_spa','intl_crawl_cq_api_userview')
            and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
            and room_type_cover = 'Qmeet'
            and ctrip_room_status = 'true' 
            and qunar_room_status = 'true'
            and province_name in ('澳门','香港') 
    )t
    group by order_date,per_type,channel_type,linqi_type,date_type,is_more_roomnight
    grouping sets (
        (order_date),
        (order_date, per_type),
        (order_date, per_type,channel_type),
        (order_date, per_type,date_type),
        (order_date, per_type,is_more_roomnight),

        (order_date, linqi_type),
        (order_date, linqi_type,channel_type),
        (order_date, linqi_type,date_type),
        (order_date, linqi_type,is_more_roomnight)
    )
)

select order_date,per_type
      ,channel_type
      ,date_type,is_more_roomnight
      ,`支付价lose率`
      ,`支付价beat数`       /   `支付价抓取次数`  `支付价beat率`
      ,`支付价lose深度`
      ,`支付价beat深度`

      ,`底价lose率` 
      ,`底价beat数`       /   `底价次数`  `底价beat率`
      ,`底价lose深度`
      ,`折后底价Q-C` / `底价C支付价`  `底价优势率`
from qc_price
order by 1,2,3,4
;

--- 6、产品力数据QM对比
select dt,per_type
    ,count(trace) as `比价次数`
    ,concat(round((count(case when stotalprice- vendorfinalprice_new>0 then 1 end)/ count(trace))*100, 2),"%") AS `lose率`
    ,concat(round((count(case when stotalprice- vendorfinalprice_new>0 and finalcost- vendorfinalprice_new>1 then 1 end)/ count(trace))*100, 2),"%") AS `底卖倒挂lose率`
    ,concat(round((count(case when stotalprice- vendorfinalprice_new>0 and finalcost- vendorfinalprice_new<=1 then 1 end)/ count(trace))*100, 2),"%") AS `非底卖倒挂lose率`

    ,concat(round((sum(case when stotalprice- vendorfinalprice_new>0 then (stotalprice- vendorfinalprice_new) end)/ sum(case when stotalprice- vendorfinalprice_new>0 then vendorfinalprice_new end))*100, 2),"%") AS `lose深度`
    ,concat(round((sum(case when stotalprice- vendorfinalprice_new>0 and finalcost- vendorfinalprice_new>1 then (stotalprice- vendorfinalprice_new) end)/ sum(case when stotalprice- vendorfinalprice_new>0 and finalcost- vendorfinalprice_new>1 then vendorfinalprice_new end))*100, 2),"%") AS `底卖倒挂lose深度`
    ,concat(round((sum(case when stotalprice- vendorfinalprice_new>0 and finalcost- vendorfinalprice_new<=1 then (stotalprice- vendorfinalprice_new) end)/ sum(case when stotalprice- vendorfinalprice_new>0 and finalcost- vendorfinalprice_new<=1 then vendorfinalprice_new end))*100, 2),"%") AS `非底卖倒挂lose深度`

from(
    select dt
        ,trace
        ,hotelid
        ,vendorroomid
        ,vendoradvantage
        ,finalcost
        ,vendorfinalprice
        ,vendorfinalprice_new
        ,finalprice
        ,stotalprice
        ,case when datediff(substr(checkindate,1,10), dt) < 0 or datediff(substr(checkindate,1,10), dt) = 0  then '凌晨订&当天订'
                    when datediff(substr(checkindate,1,10), dt) between 1 and 3    then '提前订1-3天'
                    when datediff(substr(checkindate,1,10), dt) between 4 and 7    then '提前订4-7天'
                    when datediff(substr(checkindate,1,10), dt) between 8 and 14   then '提前订8-14天'
                    -- when datediff(substr(checkin_date,1,10), substr(order_date,1,10)) between 15 and 30  then '提前订15-30天'
                    else '提前订15+'  
            end as per_type
    from ihotel_default.dw_compet_cpare_hotel_compareprice_settable_result_di i
    left join temp.temp_yuehan_cao_M_HK_dc_20260522 t on i.hotelid = t.hotel_seq
    where dt>='2026-01-01' and dt<='%(FORMAT_DATE)s'
        and scenestype='CQM_PRICE_COMPARE'
        and comparegranularity='ALL_ROOMS'
        and t.hotel_seq is not null
)a
left join(
    select distinct hotel_seq
         ,hotel_name
         ,country_name
         ,province_name
         ,case when province_name='香港' then'香港'
            when province_name='澳门' then'澳门'
            when country_name='日本' then '日本'
            else '其他' end as `区域`
        ,case when hotel_grade in('高档型','豪华型','4','5') then '高星'
              when hotel_grade in('舒适型','3') then '中星'
                    else '低星' end as hotel_grade
    from ihotel_default.dim_hotel_info_intl_v3 
    where dt='%(DATE)s' 
        and province_name in ('香港','澳门')
)b on a.hotelid = b.hotel_seq
where b.hotel_seq is not null
group by 1,2
order by 1,3 desc
;


--- 7、Q用户画像数据
with q_order as ( --- Q侧订单明细打标
    select a.order_date
          ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
        --   ,case when a.order_date = b.min_order_date then '新客' else '老客' end as user_type 
          -- 1. 新版提前订逻辑
          ,case when datediff(checkin_date, order_date) < 0 or datediff(checkin_date, order_date) = 0  then '凌晨订&当天订'
                when datediff(checkin_date, order_date) between 1 and 3    then '提前订1-3天'
                when datediff(checkin_date, order_date) between 4 and 7    then '提前订4-7天'
                when datediff(checkin_date, order_date) between 8 and 14   then '提前订8-14天'
                -- when datediff(checkin_date, order_date) between 15 and 30  then '提前订15-30天'
                -- else '提前订31+' 
                else '提前订15+' 
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
          ,a.user_id,a.order_no, a.init_gmv, a.room_night
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
        ,case when ext_flag_map['ord_children_num'] > 0 then '亲子' else '非亲子' end as is_child
        ,coalesce(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'),0) jf_amt --- 积分补
        ,coalesce(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) * room_night  djb_amt  --- 定价补
        ,coalesce(get_json_object(extendinfomap,'$.platform_amount'),0) * room_night  plat_amt  --- 平台补
        ,coalesce(get_json_object(extendinfomap,'$.frame_amount'),0) * room_night + coalesce(cashbackmap['framework_amount'],0)  xyb_amt  --- 协议补
        ,case when supplier_code in ('hca9008oc4l','hca908oh60s','hca908oh60t','hca9008pb7m','hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r','hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an','hca1f71a00i','hca1f71a00j')
                then coalesce(follow_price_amount,0) end zjb_amt --- 追价补
    from default.mdw_order_v3_international a 
    -- left join user_type b on a.user_id = b.user_id 
    left join (select order_no, max(purchase_order_no) as purchase_order_no from ihotel_default.dw_purchase_order_info_v3 where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd') group by order_no) p on a.order_no = p.order_no
    left join (select distinct partner_order_no, extend_info['vendor_name'] as vendor_name from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da where dt = cast(date_sub(current_date, 1) as string)) c on p.purchase_order_no = c.partner_order_no
    left join temp.temp_zeyz_yang_dim_date_2024_2026 dd on a.order_date = dd.date
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
    select user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
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
,order_result as (
    select user_id, order_date,substr(order_date, 1, 7) as mth,linqi_type,per_type
           ,order_no   -- 订单号
           ,gender     -- 性别
           ,city_level -- 城市等级
           ,birth_year_month -- 出生年月
           ,age      -- 年龄
           ,case when age <= 18 then '1(0,18]'
                 when age >= 19 and age <= 24 then '2[19,24]'
                 when age >= 25 and age <= 30 then '3[25,30]'
                 when age >= 31 and age <= 35 then '4[31,35]'
                 when age >= 36 and age <= 40 then '5[36,40]'
                 when age >= 41 and age <= 45 then '6[41,45]'
                 when age >= 46 and age <= 50 then '7[46,50]'
                 when age > 50 then '8[51+)'
            else '未知' end as age_level  -- 年龄段
            -- ,mobile_platform  -- 手机平台
            ,is_child  -- 亲子
    from (
        select o.order_no
            ,o.user_id,o.order_date,linqi_type,per_type
            ,gender
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,case when birth_year_month is null then '未知'
                else cast(substr('%(DATE)s', 1, 4) as int) - cast(substr(birth_year_month, 1, 4) as int)
                end as age
            ,case when (s.user_id is not null or o.is_child ='亲子') then '亲子' else '非亲子' end as is_child
        from q_order o
        left join user_profile u on u.user_id = o.user_id
        left join search_child s on s.user_id = o.user_id and s.dt=o.order_date
    )t
)

select 
     mth 
    ,if(grouping(linqi_type) = 1, 'ALL', linqi_type) as linqi_type
    ,if(grouping(gender) = 1, 'ALL', gender) as gender
    ,if(grouping(age_level) = 1, 'ALL', age_level) as age_level
    ,if(grouping(city_level) = 1, 'ALL', city_level) as city_level
    ,if(grouping(is_child) = 1, 'ALL', is_child) as is_child
    ,count(distinct order_no) as order_cnt
from order_result
group by 
    mth, 
    gender, 
    age_level, 
    city_level, 
    is_child,linqi_type
grouping sets (
    (mth, gender),
    (mth, age_level),
    (mth, city_level), 
    (mth, is_child),
    (mth, linqi_type),
    (mth,linqi_type,gender),
    (mth,linqi_type,age_level),
    (mth,linqi_type,city_level),
    (mth,linqi_type,is_child)
)
order by mth desc
;



--- 8、顺畅度数据
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
    select  id 
        ,supplier_wrapper_group as wrapper_id 
    from ihotel_default.ods_qta_supplier
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2))=date_sub(current_date, 1) and inter_flag=1
    group by 1,2
)
,qc_room_mapping as (
    select 
        dt,
        product_id,
        partner_product_id
    from ihotel_default.dwd_supply_qc_product_mapping_di
    where dt >= '2026-01-01' and dt <= date_sub(current_date, 1)
    group by 1,2,3
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
    group by 1,2,3
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
                ,if(grouping(linqi_type)=1,'ALL', linqi_type) as  per_type,

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
                        --   when datediff(checkin_date, dt) between 15 and 30  then '提前订15-30天'
                          else '提前订15+' 
                     end as per_type
                     ,case when datediff(checkin_date, dt) <= 7 then '临期订'
                          else '提前订' 
                     end as linqi_type
                     
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
                -- and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
                and province_name in ('香港','澳门')
                and a.user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874','27117935','1263291304','1475831104','1534870051','175004090','223703725','428927726','1005465130','134486580','1534045148','169408570','185495343','185711487','263070154','125896658','140775252','1424343583','1554251482','1070931535','137263924','162660539','273860152','1409683183','1607050360','139741136','196432845')
        ) a
        group by a.dt,cube(supplier,linqi_type)
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
                ,if(grouping(linqi_type)=1,'ALL', linqi_type) as  per_type
                
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
                        --   when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) between 15 and 30  then '提前订15-30天'
                          else '提前订15+' 
                     end as per_type 
                     ,case when datediff(checkin_date, concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) <=7  then '临期订'
                          else '提前订' 
                     end as linqi_type          
            from default.view_dw_user_app_booking_qta_di a
            left join is_agent_mapping b on split(a.room_id,'\\_')[0] = b.room and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) = b.d
            left join (select distinct * from ihotel_default.dwd_supply_multi_dimension_bml_da) c on a.wrapper_id = c.wrapper_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            left join user_type f on a.user_id = f.user_id 
            where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= '2026-01-01'
                 and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
                 and source='app_intl'
                 and platform in ('adr','ios')
                --  and (province_name in ('香港','澳门','台湾') or a.country_name!='中国')
                 and province_name in ('香港','澳门')
                 and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                 and q_trace_id not like 'f_inter_autotest%'
        )a
        group by concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)),cube(supplier,linqi_type)
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
                ,if(grouping(linqi_type)=1,'ALL', linqi_type) as  per_type
                
            
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
                        --   when datediff(checkin_date, to_date(log_time)) between 15 and 30  then '提前订15-30天'
                          else '提前订15+' 
                     end as per_type
                     ,case when datediff(checkin_date, to_date(log_time)) <= 7    then '临期订'
                          else '提前订' 
                     end as linqi_type
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
                -- and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
                and province_name in ('香港','澳门')
                and err_code not in( '-98','784','785')
        ) y 
        group by booking_date,cube(supplier,linqi_type)
    )a
) c on a.datas=c.booking_date and a.supplier = c.supplier  and a.per_type=c.per_type
order by  1 desc
