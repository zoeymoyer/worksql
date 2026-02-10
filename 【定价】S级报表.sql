
-- 定价近30日预订口径核心数据-仅APP端-修正当日取消
with q_user_type as (
        select user_id 
        ,min(order_date) as min_order_date
        from default.mdw_order_v3_international
        where dt = '%(DATE)s'
                and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
                and terminal_channel_type in ('www','app','touch') 
                and order_status not in ('CANCELLED','REJECTED')
                and is_valid='1'
        group by 1
        )

,c_user_type as (
        select 
        user_id,
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



,q_uv as (
  select `日期`
  ,sum(q_uv) as q_uv
  ,sum(q_uv_new) as q_uv_new
  ,sum(q_uv_old) as q_uv_old
  from (
  select `日期`
  ,`目的地`
  ,count(distinct a.user_id) as q_uv       
  --新老客
  ,count(distinct if(user_type='新客' ,a.user_id,null)) as q_uv_new
  ,count(distinct if(user_type='老客' ,a.user_id,null)) as q_uv_old
  from(
  select dt as `日期`
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
        ,case when dt > b.min_order_date then '老客' else '新客' end as user_type 
        ,a.user_id
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        left join q_user_type b on a.user_id = b.user_id 
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where dt >= date_sub(current_date,30) and dt <= date_sub(current_date,1)
            and business_type = 'hotel'
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
            and (search_pv + detail_pv + booking_pv + order_pv)>0
        )a
  group by 1,2
  ) b 
  group by 1
  )

,c_uv as (
  select `日期`
  ,sum(c_uv) as c_uv
  ,sum(c_uv_new) as c_uv_new
  ,sum(c_uv_old) as c_uv_old
  from (
  select `日期`
  ,`目的地`
  ,count(distinct uid) c_uv
        
  --新老客
  ,count(distinct case when user_type='新客' then uid end) as c_uv_new
  ,count(distinct case when user_type='老客' then uid end) as c_uv_old
  from (
        select dt as `日期`
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type 
        ,uid
        from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a 
        left join c_user_type b on a.uid=b.ubt_user_id
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
        where device_chl='app'
        and  dt>= date_sub(current_date,30) and dt<= date_sub(current_date,1)
    )a
  group by 1,2
  ) b 
  group by 1
)        




,q_sugar_order as ( --全量积分订单
    select 
        distinct order_no
    from 
        default.mdw_order_v3_international
        lateral view explode(supplier_promotion_code) bb as promotion_ids
    where 
        dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and is_valid='1'
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_date >= '%(FORMAT_DATE_30)s' and order_date <= '%(FORMAT_DATE)s'
        --and order_date>='2025-02-21' and order_date<='2025-03-06'
        and qta_supplier_id='1615667' 
        -- and supplier_promotion_code like '%2913%'
        and promotion_ids='2913'
)



,q_order as ( --Q间夜量，返后佣金，ADR
    select order_date
        --,case when hotel_grade in (4,5) then '1高星' 
        --      when hotel_grade in (3) then '2中星' else '3低星' end as hotel_grade
        --,`目的地`
        ,count(distinct hotel_seq) as hotel_num
        ,count(distinct order_no) as order_num
        ,sum(Q_room_night) as Q_room_night
        ,sum(Q_commission) as Q_commission
        ,sum(case when Q_commission<0 then Q_room_night end) as Q_losing_room_night
        ,sum(case when Q_commission<0 then Q_commission end) as Q_losing_commission
        ,sum(bp_realized*Q_room_night) as `变现提`
        ,sum(case when qta_supplier_id!='1615667' then bp_realized*Q_room_night end) as `非C变现提`
        ,sum(beat_amount*Q_room_night) as `定价补`
        ,sum(nvl(frame_amount,0)*Q_room_night)+sum(nvl(framework_amount,0)) as `协议补`
        ,sum(nvl(platform_amount,0)*Q_room_night) as `平台补`
        ,sum(case when supplier_group!='小代理' then nvl(follow_amount,0) end) as `追价补`
        ,sum(coupon_amount) as `券补`
        ,sum(exchange_amount) as `积分补`
        ,sum(exchange_amount_duobei) as `多倍积分补`
        ,sum(exchange_amount_feiduobei) as `非多倍积分补`
        ,sum(Q_GMV) as Q_GMV
        ,sum(Q_commission)/sum(Q_GMV) as Q_commission_rate
        ,sum(Q_GMV)/sum(Q_room_night) as Q_ADR
        ,sum(ctrip_commission_amount) as compare_c_commission_amount
        ,sum(qunar_commission_amount) as compare_q_commission_amount
        ,sum(ctrip_before_coupons_cashback_price) as c_sp_sum
        ,sum(qunar_before_coupons_cashback_price) as q_sp_sum
        ,sum(bp_advantage_amount) as bp_advantage_amount
        ,sum(case when qta_supplier_id!='1615667' then bp_advantage_amount end) as non_c_bp_advantage_amount
        ,sum(case when qta_supplier_id!='1615667' then bp_advantage_amount_limit20 end) as non_c_bp_advantage_amount_limit20
        ,sum(sp_advantage_amount) as sp_advantage_amount
        ,sum(case when pricing_ccr is not null then Q_GMV*pricing_ccr end) as pricing_c_commission_amount
        ,sum(case when pricing_ccr is not null then Q_GMV end) as pricing_c_gmv
       
       --新老客
        ,count(distinct case when user_type='新客' then order_no end) as order_num_new
        ,count(distinct case when user_type='老客' then order_no end) as order_num_old

        ,sum(case when user_type='新客' then Q_room_night end) as Q_room_night_new
                ,sum(case when user_type='老客' then Q_room_night end) as Q_room_night_old

        ,sum(case when user_type='新客' then Q_commission end) as Q_commission_new
        ,sum(case when user_type='老客' then Q_commission end) as Q_commission_old

        --高中低星佣金率
        ,sum(case when hotel_grade in (4,5) then Q_commission end) as Q_commission_45
        ,sum(case when hotel_grade in (4,5) then Q_GMV end) as Q_GMV_45
        ,sum(case when hotel_grade in (3) then Q_commission end) as Q_commission_3
        ,sum(case when hotel_grade in (3) then Q_GMV end) as Q_GMV_3
        ,sum(case when hotel_grade not in (3,4,5) then Q_commission end) as Q_commission_012
        ,sum(case when hotel_grade not in (3,4,5) then Q_GMV end) as Q_GMV_012

        ,sum(case when hotel_grade in (4,5) and supplier_group!='小代理' then nvl(follow_amount,0) end) as `追价补_45`
        ,sum(case when hotel_grade in (3) and supplier_group!='小代理' then nvl(follow_amount,0) end) as `追价补_3`
        ,sum(case when hotel_grade not in (3,4,5) and supplier_group!='小代理' then nvl(follow_amount,0) end) as `追价补_012`

                ,sum(case when hotel_grade in (4,5) then nvl(frame_amount,0)*Q_room_night+nvl(framework_amount,0) end) as `协议补_45`
        ,sum(case when hotel_grade in (3) then nvl(frame_amount,0)*Q_room_night+nvl(framework_amount,0) end) as `协议补_3`
        ,sum(case when hotel_grade not in (3,4,5) then nvl(frame_amount,0)*Q_room_night+nvl(framework_amount,0) end) as `协议补_012`

    from (
        select 
            order_date 
            ,hotel_grade
            ,hotel_seq
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,case when supplier_code in ('hca9008oc4l') then 'Ctrip'
            when supplier_code in ('hca908oh60s','hca908oh60t') then 'Agoda'
            when supplier_code in ('hca9008pb7m', 'hca9008pb7k','hca908pb70p','hca908pb70o','hca908pb70q','hca908pb70s','hca908pb70r') then 'Booking'
            when supplier_code in ('hca908lp9aj','hca908lp9ag','hca908lp9ai','hca908lp9ah','hca9008lp9v','hca908lp9ak','hca908lp9al','hca908lp9am','hca908lp9an') then 'EAN'
            when supplier_code in ('hca1f71a00i','hca1f71a00j') then 'HB'
            else '小代理' end as supplier_group
            ,a.order_no
            ,physical_room_name
            ,case when a.batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') then (init_commission_after+nvl(coupon_substract_summary ,0)) when (a.batch_series like '%23base_ZK_728810%' or a.batch_series like '%23extra_ZK_ce6f99%') then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0)) else init_commission_after+nvl(ext_plat_certificate,0) end as Q_commission
            ,room_night as Q_room_night
            ,init_gmv as Q_GMV
            ,nvl(follow_price_amount,0) as follow_amount
            ,case when (coupon_substract_summary is null or batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0 else nvl(coupon_substract_summary,0) end as coupon_amount
            ,get_json_object(extendinfomap,'$.v2_c_incr') as pricing_ccr--定价参考C佣金率
            ,nvl(get_json_object(extendinfomap,'$.bp_adv_amount_realized'),0) as bp_realized --实际变现底价优势金额（间夜均）
            ,nvl(get_json_object(extendinfomap,'$.V2_BEAT_AMOUNT_AF'),0) as beat_amount --实际beat金额（间夜均）
            ,get_json_object(extendinfomap,'$.frame_amount') as frame_amount --基础定价协议beat金额（间夜均）
            ,cashbackmap['framework_amount'] as framework_amount   --券补协议后返金额（订单）
            ,get_json_object(extendinfomap,'$.platform_amount') as platform_amount --平台beat金额（间夜均）
            ,NVL(get_json_object(promotion_score_info, '$.deductionPointsInfo.exchangeAmount'), 0) as exchange_amount --积分抵扣金额（订单总）
            ,case when c.order_no is not null then NVL(get_json_object(promotion_score_info, '$.deductionPointsInfo.exchangeAmount'), 0) else 0 end as exchange_amount_duobei --多倍积分抵扣金额（订单总）
            ,case when c.order_no is null then NVL(get_json_object(promotion_score_info, '$.deductionPointsInfo.exchangeAmount'), 0) else 0 end as exchange_amount_feiduobei --非多倍积分抵扣金额（订单总）
            ,qta_supplier_id
        from 
            default.mdw_order_v3_international a
            left join q_user_type b on a.user_id = b.user_id 
            left join q_sugar_order c on a.order_no = c.order_no
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where 
            a.dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
            and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
            and terminal_channel_type = 'app'
            and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
            and (first_rejected_time is null or date(first_rejected_time) > order_date) 
            and (refund_time is null or date(refund_time) > order_date)
            and is_valid='1'
            and order_date >= '%(FORMAT_DATE_30)s' and order_date <= '%(FORMAT_DATE)s'
            --and hotel_grade in (1,2,3,4,5)
    ) order_intl

    left join

    (
        select 
            distinct uniq_id,qunar_price_info['orderNum'] as orderNum,--qunar_price_info,qunar_price_info['traceId'] as trace_id,
            qunar_physical_room_name,ctrip_before_coupons_cashback_price,qunar_before_coupons_cashback_price,
            -- 佣金率信息
            qunar_before_coupons_cashback_price-qunar_chased_discount_price as qunar_commission_amount,
            (qunar_before_coupons_cashback_price-qunar_chased_discount_price)/qunar_before_coupons_cashback_price as qunar_commission_rate,
            ctrip_before_coupons_cashback_price-ctrip_discount_base_price as ctrip_commission_amount,
            (ctrip_before_coupons_cashback_price-ctrip_discount_base_price)/ctrip_before_coupons_cashback_price as ctrip_commission_rate,
            -- 底价优势信息
            -chased_discount_price_diff as bp_advantage_amount,
            -chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate,
            case when -chased_discount_price_diff/qunar_before_coupons_cashback_price>0.2 then qunar_before_coupons_cashback_price*0.2 else -chased_discount_price_diff end as bp_advantage_amount_limit20,
            -- 卖价优势信息
            -pay_price_diff as sp_advantage_amount
        from 
            default.dwd_hotel_cq_compare_price_result_intl_hi
        where 
            dt between '%(DATE_30)s' and '%(DATE)s'
            and business_type = 'intl_crawl_cq_api_order'
            and compare_type="PHYSICAL_ROOM_TYPE_LOWEST" --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
            and room_type_cover="Qmeet" 
            and ctrip_room_status="true"
            and ctrip_pay_type="预付"
            and qunar_pay_type="预付"
            and qunar_room_status="true"
            --and qunar_price_info['order_product_similar_lowest']="1"
            and substr(uniq_id,1,11) = "h_datacube_"
    ) cq_compare on cq_compare.orderNum = order_intl.order_no and cq_compare.qunar_physical_room_name = order_intl.physical_room_name
    
    group by 1
)

,c_order as  ( --C间夜量，返后佣金
    select 
        order_date
        --,`目的地`
        --,case when hotel_grade in (4,5) then '1高星' 
        --      when hotel_grade in (3) then '2中星' else '3低星' end as hotel_grade
        ,sum(C_room_night)as C_room_night
        ,sum(C_GMV) as C_GMV
        ,sum(C_commission) as C_commission
        ,sum(C_commission)/sum(C_GMV) as C_commission_rate
        ,count(distinct order_no) as order_num

        --新老客
        ,count(distinct case when user_type='新客' then order_no end) as order_num_new
        ,count(distinct case when user_type='老客' then order_no end) as order_num_old

        ,sum(case when user_type='新客' then C_room_night end)as C_room_night_new
        ,sum(case when user_type='老客' then C_room_night end)as C_room_night_old

        ,sum(case when user_type='新客' then C_commission end) as C_commission_new
        ,sum(case when user_type='老客' then C_commission end) as C_commission_old

        --高中低星佣金率
        ,sum(case when hotel_grade in (4,5) then C_commission end) as C_commission_45
        ,sum(case when hotel_grade in (4,5) then C_GMV end) as C_GMV_45
        ,sum(case when hotel_grade in (3) then C_commission end) as C_commission_3
        ,sum(case when hotel_grade in (3) then C_GMV end) as C_GMV_3
        ,sum(case when hotel_grade not in (3,4,5) then C_commission end) as C_commission_012
        ,sum(case when hotel_grade not in (3,4,5) then C_GMV end) as C_GMV_012

        --预售间夜量
        ,sum(`预售间夜量`) as `预售间夜量`
    from
    (
        select 
            substr(order_date,1,10) as order_date
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
                when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
                when c.area in ('欧洲','亚太','美洲') then c.area
                else '其他' end as `目的地`
            ,extend_info['STAR'] as hotel_grade
            ,extend_info['PROVINCE'] as province_name
            ,extend_info['COUNTRY'] as country_name
            ,order_no
            ,extend_info['room_night'] as C_room_night
            ,room_fee as C_GMV
            ,room_fee/ extend_info['room_night']as C_ADR
            ,comission as C_commission
            ,case when extend_info['is_apoint']=1 then extend_info['room_night'] end as `预售间夜量`
        from 
            ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
            left join c_user_type u on o.user_id=u.user_id
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
        where 
            dt = from_unixtime(unix_timestamp() -86400, 'yyyy-MM-dd')
            and extend_info['IS_IBU'] = '0'
            and extend_info['book_channel'] = 'Ctrip'
            and extend_info['sub_book_channel'] = 'Direct-Ctrip'
            and terminal_channel_type = 'app'
            and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
            and substr(order_date,1,10) between '%(FORMAT_DATE_30)s' and '%(FORMAT_DATE)s'
            --and extend_info['STAR'] in (1,2,3,4,5)
    ) a
    group by 1
 ) 

, cq_compare as (
    -- 用户视角支付价产品力分布情况
    select 
        substr(crawl_time,1,10) as crawl_date
        --,`目的地`
        --,case when qunar_hotel_grade in (4,5) then '1高星' 
        --      when qunar_hotel_grade in (3) then '2中星' else '3低星' end as hotel_grade
        ,count(*) as all_count
        ,count(case when sp_advantage_ratio<0 then 1 end) as lose_count
        ,count(case when sp_advantage_ratio=0 then 1 end) as meet_count
        ,count(case when sp_advantage_ratio>0 then 1 end) as beat_count
        ,count(case when sp_advantage_ratio>=0.03 and sp_advantage_ratio<=0.05 then 1 end) as heli_beat_count
        ,count(case when sp_advantage_amount>0 and sp_advantage_amount<=1 then 1 end) as  diff01heli_beat_count
        ,count(case when sp_advantage_amount>1 and sp_advantage_ratio<0.03 then 1 end) as diff13heli_beat_count
        ,count(case when sp_advantage_ratio>0.05 and sp_advantage_ratio<=0.07 then 1 end) as diff57_beat_count
        ,count(case when sp_advantage_ratio>0.07 then 1 end) as diff7_beat_count  
        ,count(case when identity="R4" then 1 end) as r4_count
        ,count(case when identity="R4" and bsp_advantage_ratio>0.07 then 1 end) as r4_overbeat_count
    from 
    (
        select *
            --,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
            ,ctrip_pay_price as c_sp
            ,pay_price_compare_result as sp_result
            ,-pay_price_diff as sp_advantage_amount
            ,-pay_price_diff/night_number as sp_advantage_amount_avg
            ,-pay_price_diff/ctrip_pay_price as sp_advantage_ratio
            ,ctrip_before_coupons_cashback_price as c_bsp
            ,before_coupons_cashback_price_compare_result as bsp_result
            ,-before_coupons_cashback_price_diff as bsp_advantage_amount
            ,-before_coupons_cashback_price_diff/night_number as bsp_advantage_amount_avg
            ,-before_coupons_cashback_price_diff/ctrip_pay_price as bsp_advantage_ratio
            ,case when split(qunar_product_room_id, '_')[1] in ("1615667","800000164") then "Ctrip"
            when split(qunar_product_room_id, '_')[1] in ("800000191","800000650") then "Agoda"
            when split(qunar_product_room_id, '_')[1] in ("1617596","1617599") then "Booking"
            when split(qunar_product_room_id, '_')[1] in ("800000218","800000227","800000221","800000224","1625282") then "Expedia"
            else "Other" end as supplier_group
        from 
            default.dwd_hotel_cq_compare_price_result_intl_hi a 
            --left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
        where 
            dt between '%(DATE_30)s' and '%(DATE)s'
            and business_type='intl_crawl_cq_api_userview'
            and compare_type="PHYSICAL_ROOM_TYPE_LOWEST"
            and room_type_cover="Qmeet"
            and ctrip_room_status="true" 
            and qunar_room_status="true"
    ) cq_compare_raw
    group by 1
)



select 
    q.order_date as `预订日`
    ,date_format(q.order_date,'u') as `周`
    --核心产量
    ,sum(hotel_num) as `酒店数`
    ,sum(Q_room_night) as `间夜Q`
    ,round(sum(Q_commission)/10000,1) as `收益万`
    --核心QC比
    ,concat(round((sum(q_uv)/sum(c_uv))*100,1),"%") as `流量QC`
    ,concat(round((sum(q_uv_new)/sum(c_uv_new))*100,1),"%") as `新客流量QC`
    ,concat(round((sum(q_uv_old)/sum(c_uv_old))*100,1),"%") as `老客流量QC`
    
    ,concat(round(((sum(q.order_num)/sum(q_uv))/(sum(c.order_num)/sum(c_uv)))*100,1),"%") as `转化QC`
    ,concat(round(((sum(q.order_num_new)/sum(q_uv_new))/(sum(c.order_num_new)/sum(c_uv_new)))*100,1),"%") as `新客转化QC`
    ,concat(round(((sum(q.order_num_old)/sum(q_uv_old))/(sum(c.order_num_old)/sum(c_uv_old)))*100,1),"%") as `老客转化QC`
    
    ,concat(round((sum(Q_commission)/sum(C_commission))*100,1),"%") as `收益QC`
    ,concat(round((sum(Q_commission_new)/sum(C_commission_new))*100,1),"%") as `新客收益QC`
    ,concat(round((sum(Q_commission_old)/sum(C_commission_old))*100,1),"%") as `老客收益QC`
    
    ,concat(round((sum(Q_room_night)/sum(C_room_night))*100,1),"%") as `间夜QC`
    ,concat(round((sum(Q_room_night_new)/sum(C_room_night_new))*100,1),"%") as `新客间夜QC`
    ,concat(round((sum(Q_room_night_old)/sum(C_room_night_old))*100,1),"%") as `老客间夜QC`

    ,concat(round(((sum(Q_GMV)/sum(Q_room_night))/(sum(C_GMV)/sum(C_room_night)))*100,1),"%") as `ADRQC`
    ,round(sum(Q_GMV)/sum(Q_room_night),0) as `adr_q`
    ,round(sum(C_GMV)/sum(C_room_night),0) as `adr_c`
    ,concat(round(((sum(Q_commission)/sum(Q_GMV))/(sum(C_commission)/sum(C_GMV)))*100,1),"%") as `佣金率QC`
    --QC佣金差
    ,concat(round((sum(Q_commission)/sum(Q_GMV)-sum(C_commission)/sum(C_GMV))*100,2),"%") as `CQ佣金差(Q)`
    ,concat(round((sum(Q_commission+`追价补`+`协议补`)/sum(Q_GMV)-sum(C_commission)/sum(C_GMV))*100,2),"%") as `CQ佣金差(C)`
    ,concat(round((sum(Q_commission_45+`追价补_45`+`协议补_45`)/sum(Q_GMV_45)-sum(C_commission_45)/sum(C_GMV_45))*100,2),"%") as `高星佣金差(2%)`
    ,concat(round((sum(Q_commission_3+`追价补_3`+`协议补_3`)/sum(Q_GMV_3)-sum(C_commission_3)/sum(C_GMV_3))*100,2),"%") as `中星佣金差(3%)`
    ,concat(round((sum(Q_commission_012+`追价补_012`+`协议补_012`)/sum(Q_GMV_012)-sum(C_commission_012)/sum(C_GMV_012))*100,2),"%") as `低星佣金差(3%)`
    --佣金差构成
    ,concat(round((sum(compare_c_commission_amount)/sum(c_sp_sum)-sum(C_commission)/sum(C_GMV))*100,2),"%") as `订单结构差`
    ,concat(round((sum(Q_commission-`变现提`+`定价补`+`追价补`+`券补`+`积分补`)/sum(Q_GMV)-sum(compare_c_commission_amount)/sum(c_sp_sum))*100,2),"%") as `定价系统差`
    ,concat(round((sum(`变现提`-`定价补`-`追价补`-`券补`-`积分补`)/sum(Q_GMV))*100,2),"%") as `定价策略差`
    --佣金率构成
    ,concat(round((sum(Q_commission)/sum(Q_GMV))*100,2),"%") as `Q佣金率`
    ,concat(round((sum(C_commission)/sum(C_GMV))*100,2),"%") as `C佣金率`

    --,concat(round((sum(sp_advantage_amount)/sum(c_sp_sum))*100,2),"%") as `卖价优势率`
    --,concat(round((sum(bp_advantage_amount)/sum(q_sp_sum))*100,2),"%") as `底价优势率`
    
    --,concat(round((sum(C_commission)/sum(C_GMV))*100,2),"%") as `C佣金率`
    --,concat(round((sum(compare_c_commission_amount)/sum(c_sp_sum))*100,2),"%") as `抓取C佣金率`
    --,concat(round((sum(pricing_c_commission_amount)/sum(pricing_c_gmv))*100,2),"%") as `定价c佣金率`
    --,concat(round((sum(Q_commission-`变现提`+`定价补`+`追价补`+`券补`)/sum(Q_GMV))*100,2),"%") as `基础佣金率`
    ,concat(round((sum(`变现提`)/sum(Q_GMV))*100,2),"%") as `变现提`
    ,concat(round((sum(-`定价补`)/sum(Q_GMV))*100,2),"%") as `定价补`
    ,concat(round((sum(-`追价补`)/sum(Q_GMV))*100,2),"%") as `追价补`
    ,concat(round((sum(-`券补`)/sum(Q_GMV))*100,2),"%") as `券补`
    ,concat(round((sum(-`积分补`)/sum(Q_GMV))*100,2),"%") as `积分补`
    ,concat(round((sum(-`多倍积分补`)/sum(Q_GMV))*100,2),"%") as `多倍积分补`
    ,concat(round((sum(-`非多倍积分补`)/sum(Q_GMV))*100,2),"%") as `非多倍积分补`
    ,concat(round((sum(-`协议补`)/sum(Q_GMV))*100,2),"%") as `协议补`
    ,round(sum(`协议补`),0) as `协议额`
    --负佣数据
    ,concat(round((sum(Q_losing_room_night)/sum(Q_room_night))*100,2),"%") as `负佣间夜比`
    ,concat(round((sum(Q_losing_commission)/sum(Q_GMV))*100,2),"%") as `负佣金率`
    ,round(sum(Q_losing_commission),0) as `负佣额`
    --单间夜数据
    
    ,round(sum(Q_commission)/sum(Q_room_night),0) as `rn收益q`
    ,round(sum(C_commission)/sum(C_room_night),0) as `rn收益c`

    ,concat(round((sum(`预售间夜量`)/sum(C_room_night))*100,2),"%") as `C预售间夜占比`
    --,concat(round((sum(non_c_bp_advantage_amount_limit20)/sum(q_sp_sum))*100,2),"%") as `非C底价优势率`
    --,concat(round((sum(`非C变现提`)/sum(Q_GMV))*100,2),"%") as `非C变现提`
    --,concat(round(((sum(`非C变现提`)/sum(Q_GMV))/(sum(non_c_bp_advantage_amount_limit20)/sum(q_sp_sum)))*100,2),"%") as `非C变现率`
    

    
    --产品力
    --  ,concat(round((sum(beat_count)/sum(all_count))*100,1),"%") as beat
    --  ,concat(round((sum(diff01heli_beat_count)/sum(all_count))*100,1),"%") as `0-1元beat`
    --  ,concat(round((sum(diff13heli_beat_count)/sum(all_count))*100,1),"%") as `1元-3%beat`
    --  ,concat(round((sum(heli_beat_count)/sum(all_count))*100,1),"%") as `3%-5%beat`
    --  ,concat(round((sum(diff57_beat_count)/sum(all_count))*100,1),"%") as `5%-7%以上beat`
    --  ,concat(round((sum(diff7_beat_count )/sum(all_count))*100,1),"%") as `7%以上beat`
    --  ,concat(round((sum(r4_overbeat_count)/sum(r4_count))*100,1),"%") as `过度beat（券前价）`
    --  ,concat(round((sum(lose_count)/sum(all_count))*100,1),"%") as lose
    --,q.region
    --,q.hotel_grade
    --,q.area_region
    --,sum(Q_room_night) as `Q间夜`
    --,sum(Q_commission) as `Q收益额`
    --,sum(Q_GMV) as `QGMV`
    --,sum(Q_commission)/sum(Q_GMV) as `Q佣金率`
    --,sum(C_room_night)as `C间夜`
    --,sum(C_commission)as `C收益额`
    --,sum(C_GMV)as `CGMV`

from q_order q
    left join c_order c on q.order_date = c.order_date 
    left join cq_compare cq_compare on q.order_date = cq_compare.crawl_date 
    left join q_uv q_uv on q.order_date = q_uv.`日期` 
    left join c_uv c_uv on q.order_date = c_uv.`日期` 
group by 1,2
order by order_date desc
limit 30
