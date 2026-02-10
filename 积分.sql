--- 笛卡尔积参数设置
set hive.strict.checks.cartesian.product=false;
set hive.mapred.mode=nonstrict;

--积分报表
with c_order as (
--C积分订单
select substr(order_date,1,10) as `日期` 
    ,count(distinct a.hotel_seq) as `C_积分酒店数`
    ,count(distinct a.order_no) as `C_积分订单量`
    --,sum(extend_info['room_night']) as `C_积分间夜量`
    ,sum(costdiscountcnyamount) as `C_积分抵扣金额` --当前无卖价抵扣金额，暂用底价抵扣金额
    ,sum(comission) as `C_积分佣金`
    ,sum(room_fee) as `C_积分GMV`
from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da a
join(
        select distinct order_no 
        ,prepaycampaignid 
        ,prepaycampaignname 
        ,costdiscountcnyamount
        from default.ceq_three_sync_pull_ctrip_htl_order_promotion 
        where dt='%(DATE)s'
        and prepaycampaignid='2913'
        )promotion 
on a.order_no=promotion.order_no 
where a.dt='%(FORMAT_DATE)s'
and extend_info['IS_IBU']='0' 
and extend_info['book_channel']='Ctrip' 
and extend_info['sub_book_channel']='Direct-Ctrip'
--and order_status<>'C'
and terminal_channel_type = 'app'
and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
and extend_info['discount_ebk'] is not null
and to_date(order_date)>=date_sub(current_date,15) and to_date(order_date)<=date_sub(current_date,1)
group by 1
)

, c_sugernight as (
--C积分间夜
select substr(order_date,1,10) as `日期` 
    ,sum(extend_info['room_night']) as `C_积分间夜量`
from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da a
join(
        select distinct order_no 
        from default.ceq_three_sync_pull_ctrip_htl_order_promotion 
        where dt='%(DATE)s'
        and prepaycampaignid='2913'
        )promotion 
on a.order_no=promotion.order_no 
where a.dt='%(FORMAT_DATE)s'
and extend_info['IS_IBU']='0' 
and extend_info['book_channel']='Ctrip' 
and extend_info['sub_book_channel']='Direct-Ctrip'
--and order_status<>'C'
and terminal_channel_type = 'app'
and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
and extend_info['discount_ebk'] is not null
and to_date(order_date)>=date_sub(current_date,15) and to_date(order_date)<=date_sub(current_date,1)
group by 1
)

, c_hotel as (
--C积分订单酒店
        select distinct masterhotelid as masterhotelid, orderdate
        from default.ceq_three_sync_pull_ctrip_htl_order_promotion 
        where dt='%(DATE)s'
        and prepaycampaignid='2913'
)

, c_order_sugerhotel as (
--C积分酒店
select substr(order_date,1,10) as `日期` 
    ,count(distinct a.order_no) as `C_积分酒店订单量`
    ,sum(extend_info['room_night']) as `C_积分酒店间夜量`
from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da a
join c_hotel b on substr(a.order_date,1,10)=b.orderdate and a.hotel_seq=b.masterhotelid
where a.dt='%(FORMAT_DATE)s'
and extend_info['IS_IBU']='0' 
and extend_info['book_channel']='Ctrip' 
and extend_info['sub_book_channel']='Direct-Ctrip'
--and order_status<>'C'
and terminal_channel_type = 'app'
and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
and to_date(order_date)>=date_sub(current_date,15) and to_date(order_date)<=date_sub(current_date,1)
group by 1
)

, c_order_all as (
--C整体
select substr(order_date,1,10) as `日期` 
    ,count(distinct a.hotel_seq) as `C_酒店数`
    ,count(distinct a.order_no) as `C_订单量`
    ,sum(extend_info['room_night']) as `C_间夜量`
    ,sum(comission) as `C_佣金`
    ,sum(room_fee) as `C_GMV`
from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da a
where a.dt='%(FORMAT_DATE)s'
and extend_info['IS_IBU']='0' 
and extend_info['book_channel']='Ctrip' 
and extend_info['sub_book_channel']='Direct-Ctrip'
--and order_status<>'C'
and terminal_channel_type = 'app'
and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
and to_date(order_date)>=date_sub(current_date,15) and to_date(order_date)<=date_sub(current_date,1)
group by 1
)


, q_sugar_order as ( --全量积分订单
select distinct order_no
from default.mdw_order_v3_international
lateral view explode(supplier_promotion_code) bb as promotion_ids
where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    --and order_date>='2025-02-21' and order_date<='2025-03-06'
    and qta_supplier_id='1615667' 
    -- and supplier_promotion_code like '%2913%'
    and promotion_ids='2913'
)

, q_sugar_hotel as ( --积分酒店
select distinct hotel_seq, order_date
from default.mdw_order_v3_international
lateral view explode(supplier_promotion_code) bb as promotion_ids
where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    --and order_date>='2025-02-21' and order_date<='2025-03-06'
    and qta_supplier_id='1615667' 
    -- and supplier_promotion_code like '%2913%'
    and promotion_ids='2913'
)

, q_order_sugerhotel as (
--积分酒店订单
select a.order_date as `日期` 
    ,count(distinct order_no) as `Q_积分酒店订单量`
    ,sum(room_night) as `Q_积分酒店间夜量`
from default.mdw_order_v3_international a
join q_sugar_hotel b on a.order_date=b.order_date and a.hotel_seq=b.hotel_seq
-- lateral view explode(supplier_promotion_code) bb as promotion_ids
where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
    and (refund_time is null or date(refund_time) > a.order_date)
    and a.order_date >= date_sub(current_date,15) and a.order_date <= date_sub(current_date,1)
    --and qta_supplier_id='1615667' 
    --and a.order_no in (select order_no from q_sugar_order)
group by 1
) 

, q_order as (
--积分订单
select order_date as `日期` 
    ,count(distinct hotel_seq) as `Q_积分酒店数`
    ,count(distinct order_no) as `Q_积分订单量`
    ,sum(room_night) as `Q_积分间夜量`
    ,sum(NVL(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.exchangeAmount'), 0)) as `Q_积分抵扣金额`
    ,sum(NVL(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.autoSendPointsDeductibleAmount'), 0)) as `Q_积分定向金额`
    ,sum(NVL(get_json_object(promotion_score_info, '$.deductionPointsInfoV2.couponAmount'), 0)) AS `Q_券转积分金额`
    ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                 then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                 else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q_积分佣金`
    ,sum(init_gmv) as `Q_积分GMV`
from default.mdw_order_v3_international
-- lateral view explode(supplier_promotion_code) bb as promotion_ids
where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    and order_date >= date_sub(current_date,15) and order_date <= date_sub(current_date,1)
    and qta_supplier_id='1615667' 
    and order_no in (select order_no from q_sugar_order)
group by 1
) 

, q_uv as (
--流量
select a.`日期`
,sum(q_uv) as q_uv
from (
select dt as `日期`
    ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
    ,count(distinct if((search_pv + detail_pv + booking_pv + order_pv)>0,a.user_id,null)) as q_uv
from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
join (
select distinct hotel_seq
from default.mdw_order_v3_international a
join q_sugar_order b on a.order_no=b.order_no
where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    and order_date >= date_sub(current_date,15) and order_date <= date_sub(current_date,1)
    and qta_supplier_id='1615667' 
) b on a.hotel_seq =b.hotel_seq
left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
where dt>= date_sub(current_date,15) and dt<= date_sub(current_date,1)
        and business_type = 'hotel'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
        and (search_pv + detail_pv + booking_pv + order_pv)>0
group by 1,2
) a 
group by 1
)

, q_order_all as (
--整体
select order_date as `日期` 
    ,count(distinct hotel_seq) as `Q_酒店数`
    ,count(distinct order_no) as `Q_订单量`
    ,sum(room_night) as `Q_间夜量`
    ,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                 then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                 else init_commission_after+nvl(ext_plat_certificate,0) end) as `Q_佣金`
    ,sum(init_gmv) as `Q_GMV`
from default.mdw_order_v3_international
where dt='%(DATE)s'
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    and order_date >= date_sub(current_date,15) and order_date <= date_sub(current_date,1)
group by 1
) 

, q_point_amount as (
SELECT order_date as `日期`
--,order_no
--,get_json_object(extendinfomap, '$.supplierPromotions') as supplierPromotions
--,promotion
--,get_json_object(promotion, '$.promotionId') as promotionId
--,get_json_object(promotion, '$.samount') as samount
,SUM(CAST(get_json_object(promotion, '$.samount')*room_night AS DOUBLE)) AS `Q_积分商家出资`
FROM mdw_order_v3_international a
LATERAL VIEW EXPLODE(
    SPLIT(
        REGEXP_REPLACE(
            REGEXP_REPLACE(get_json_object(extendinfomap, '$.supplierPromotions'), '^\\[|\\]$', ''),  -- 去除首尾方括号
            '\\}\\,\\s*\\{',                         -- 替换分隔符为特殊标记
            '}\\|\\|\\{'
        ),
        '\\|\\|'                                   -- 按标记分割JSON对象
    )
) promotions AS promotion
WHERE a.dt='%(DATE)s' 
    and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
    and is_valid='1'
    and terminal_channel_type = 'app'
    and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
    and (first_rejected_time is null or date(first_rejected_time) > order_date) 
    and (refund_time is null or date(refund_time) > order_date)
    AND order_date >= date_sub(current_date,15) and order_date <= date_sub(current_date,1)
    AND get_json_object(promotion, '$.promotionId') = '2913'
group by 1
)

select a.`日期`
,date_format(a.`日期`,'u')`星期`
,`Q_积分酒店数`
,`C_积分酒店数`

,`Q_积分订单量`
,`C_积分订单量`

,concat(round(`Q_积分订单量`/`Q_积分酒店订单量`*100,1),'%') as `Q_多倍积分订单占比积分酒店`
,concat(round(`C_积分订单量`/`C_积分酒店订单量`*100,1),'%') as `C_多倍积分订单占比积分酒店`

,concat(round(`Q_积分间夜量`/`Q_间夜量`*100,1),'%') as `Q_间夜占比`
,concat(round(`C_积分间夜量`/`C_间夜量`*100,1),'%') as `C_多倍积分间夜占比`
,concat(round(`Q_积分订单量`/`Q_订单量`*100,1),'%') as `Q_多倍积分订单占比`
,concat(round(`C_积分订单量`/`C_订单量`*100,1),'%') as `C_多倍积分订单占比大盘`

--,concat(round(`Q_积分佣金`/`Q_积分GMV`*100,2),'%') as `Q_多倍积分佣金率`
--,concat(round(`Q_积分订单量`/q_uv*100,2),'%') as `Q_CR`
,concat(round(`Q_积分订单量`/`C_积分订单量`*100,2),'%') as `积分订单QC`
,concat(round(`Q_积分间夜量`/`C_积分间夜量`*100,2),'%') as `积分间夜QC`
--,concat(round(`Q_积分佣金`/`C_积分佣金`*100,2),'%') as `积分收益QC`
--,concat(round(`Q_积分GMV`/`Q_积分间夜量`/(`C_积分GMV`/`C_积分间夜量`)*100,1),'%') as `积分ADR QC`
--,concat(round(`Q_积分佣金`/`Q_积分GMV`/(`C_积分佣金`/`C_积分GMV`)*100,2),'%') as `积分佣金率QC`
--,concat(round(((`Q_积分佣金`+`Q_积分抵扣金额`)/`Q_积分GMV`)/((`C_积分佣金`+`C_积分抵扣金额`)/`C_积分GMV`)*100,2),'%') as `积分佣金率QC（抵扣前）`
--,concat(round((`Q_积分佣金`/`Q_积分GMV` - (`C_积分佣金`/`C_积分GMV`))*100,2),'%') as `积分佣金率QC差`
--,concat(round(((`Q_积分佣金`+`Q_积分抵扣金额`)/`Q_积分GMV` - ((`C_积分佣金`+`C_积分抵扣金额`)/`C_积分GMV`))*100,2),'%') as `积分佣金率QC差（抵扣前）`
,concat(round(`Q_积分抵扣金额`/`Q_积分GMV`*100,2),'%') as `Q_积分补贴率`
,concat(round(`Q_积分定向金额`/`Q_积分GMV`*100,2),'%') as `Q_定向补贴率`
,concat(round(`Q_券转积分金额`/`Q_积分GMV`*100,2),'%') as `Q_券转积分补贴率`

--,concat(round(`C_积分抵扣金额`/`C_积分GMV`*100,2),'%') as `C_积分补贴率`
,ROUND(`Q_积分商家出资`, 2) as `Q_积分商家出资`
,ROUND(`Q_积分抵扣金额`, 2) as `Q_积分抵扣金额`
,ROUND(`Q_积分GMV`, 2) as `Q_积分GMV`

from q_uv a 
left join q_order b on a.`日期`=b.`日期`
left join q_order_all c on a.`日期`=c.`日期`
left join c_order d on a.`日期`=d.`日期`
left join c_order_all e on a.`日期`=e.`日期`
left join q_point_amount f on a.`日期`=f.`日期`
left join c_sugernight g on a.`日期`=g.`日期`
left join c_order_sugerhotel h on a.`日期`=h.`日期`
left join q_order_sugerhotel i on a.`日期`=i.`日期`

order by a.`日期` desc

