with c_user_type as
    (select user_id
        , ubt_user_id
        , substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
    ) 

,q_user_type as 
    (select user_id 
        , min(order_date) as min_order_date
    from mdw_order_v3_international
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and terminal_channel_type in ('www','app','touch') 
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
    )

,q_uv as 
	(select dt as order_date 
		, case when province_name in ('澳门','香港') then province_name 
			when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name 
			when c.area in ('欧洲','亚太','美洲') then c.area
			else '其他' end as `目的地`
		, case when dt > b.min_order_date then '老客' else '新客' end as user_type
		, count(distinct a.user_id) as `Q_流量`
	from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
	left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
	left join q_user_type b on a.user_id = b.user_id 
	where dt >= '2024-01-01' 
		and business_type = 'hotel'
		and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
		and device_id is not null
		and device_id <> ''
	group by 1,2,3
	)

,q_order as 
	(select order_date 
		, case when order_date = b.min_order_date then '新客' else '老客' end as user_type
		, case 
            when province_name in ('澳门','香港') then province_name  
            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  
            when e.area in ('欧洲','亚太','美洲') then e.area 
            else '其他' end as `目的地`
		, count(order_no) as `Q_订单量`
		, sum(room_night) as `Q_间夜量`
		, sum(final_commission_after + nvl(ext_plat_certificate,0)) as `Q_收益额`
		, sum(final_gmv) as `Q_GMV` 
	from mdw_order_v3_international a
	left join q_user_type b on a.user_id = b.user_id 
	left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
	where dt = '%(DATE)s' 
		and terminal_channel_type = 'app' 
        and order_status not in ('CANCELLED','REJECTED')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
		and is_valid = '1'
		and order_date >= '2024-01-01' 
	group by 1,2,3
	)

,c_uv as 
	(select date as order_date
		, case when a.provincename in ('澳门','香港') then a.provincename 
			when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.countryname 
			when d.area in ('欧洲','亚太','美洲') then d.area
			else '其他' end as `目的地`
		, case when dt> b.min_order_date then '老客' else '新客' end as user_type 
		, count(distinct a.uid) as `C_流量`
	from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a 
	left join c_user_type b on a.uid=b.ubt_user_id
	left join temp.temp_yiquny_zhang_ihotel_area_region_forever d on a.countryname = d.country_name 
	where dt >= '2024-01-01'
		and device_chl = 'app'
		and page_short_domain = 'dbo'
	group by 1,2,3
	)

,c_order as
	(select substr(order_date,1,10) as order_date 
		, case when substr(order_date,1,10) = b.min_order_date then '新客' else '老客' end as user_type 
        , case 
            when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE'] 
            when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then extend_info['COUNTRY']
            when c.area in ('欧洲','亚太','美洲') then c.area
            else '其他' end as `目的地`
		, count(order_no) as `C_订单量`
		, sum(extend_info['room_night']) as `C_间夜量`
		, sum(comission) as `C_收益额`
		, sum(room_fee) as `C_GMV` 
	from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
	left join c_user_type b on o.user_id=b.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name 
	where dt = '%(FORMAT_DATE)s'
		and extend_info['IS_IBU'] = '0'
		and extend_info['book_channel'] = 'Ctrip'
		and extend_info['sub_book_channel'] = 'Direct-Ctrip'
		and order_status <> 'C' 
		-- and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
		and terminal_channel_type = 'app' 
		and substr(order_date,1,10) >= '2024-01-01'
	group by 1,2,3
	)



,qc_price as 
		(select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date

			,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
			,case 
	            when province_name in ('澳门','香港') then province_name  
	            when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯','越南') then a.country_name  
	            when c.area in ('欧洲','亚太','美洲') then c.area 
	            else '其他' end as `目的地`
			,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
			,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
			,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.03 and pay_price_diff/ctrip_pay_price <= 0 then id end)  `支付价beat0-3%次数`
			,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.04 and pay_price_diff/ctrip_pay_price <= -0.03 then id end)  `支付价beat3-4%次数`
			,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.05 and pay_price_diff/ctrip_pay_price <= -0.04 then id end)  `支付价beat4-5%次数`
			,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.06 and pay_price_diff/ctrip_pay_price <= -0.05 then id end)  `支付价beat5-6%次数`
			,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.07 and pay_price_diff/ctrip_pay_price <= -0.06 then id end)  `支付价beat6-7%次数`
            ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price>-0.08 and pay_price_diff/ctrip_pay_price <= -0.07 then id end)  `支付价beat7-8%次数`
            ,count(distinct case when  pay_price_diff<0 and pay_price_diff/ctrip_pay_price <= -0.08 then id end)  `支付价beat8%以上次数`
			,count(distinct id) as `比价次数`
		from default.dwd_hotel_cq_compare_price_result_intl_hi a
    	left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
		where dt >= '20240101' 
			and business_type = 'intl_crawl_cq_spa'
	        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
	        and room_type_cover = 'Qmeet'
	        and ctrip_room_status = 'true' 
	        and qunar_room_status = 'true'
	        and ctrip_price_info['match_mainland_before_coupons_price_result'] = 1

		group by 1,2,3
		)


select a.`目的地`
	,a.order_date as `预定日期`
	,a.user_type as `新老客`
	,`Q_流量`
	,`Q_订单量`
	,`Q_间夜量`
	,`Q_收益额`
	,`Q_GMV` 
	,`C_流量`
	,`C_订单量`
	,`C_间夜量`
	,`C_收益额`
	,`C_GMV` 
	,`支付价lose数`
	,`支付价beat数`
	,`支付价beat0-3%次数`
	,`支付价beat3-4%次数`
	,`支付价beat4-5%次数`
	,`支付价beat5-6%次数`
    ,`支付价beat6-7%次数`
    ,`支付价beat7-8%次数`
    ,`支付价beat8%以上次数`
	,`比价次数`
from q_uv a
left join q_order b on a.order_date = b.order_date and a.`目的地` = b.`目的地` and a.user_type = b.user_type
left join c_uv c on a.order_date = c.order_date and a.`目的地` = c.`目的地` and a.user_type = c.user_type 
left join c_order d on a.order_date = d.order_date and a.`目的地` = d.`目的地` and a.user_type = d.user_type 
left join qc_price e on a.order_date = e.order_date and a.`目的地` = e.`目的地` and a.user_type = e.user_type 
;






select t1.order_date
	  ,`GMV`
	  ,`佣金`
	  ,`间夜量`
	  ,`订单量`
	  ,`生单用户`
	  ,`券额`
	  ,`用券订单量`
	  ,`佣金` / `GMV`  as  `佣金率`
	  ,`订单量` / t2.dau  as  `CR`
	  ,`券额` / `GMV`  as  `券补率`
	  ,`GMV` / `间夜量`  as  `ADR`
	  ,`间夜量` / `订单量`  as  `单间夜`
	  ,`用券订单量` / `订单量`  as  `用券订单占比`
from (
	select order_date
			,sum(init_gmv) `GMV`
			,sum(case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
					then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
					else init_commission_after+coalesce(ext_plat_certificate,0) end) `佣金`
			,sum(room_night) `间夜量`
			,count(distinct order_no) `订单量`
			,count(distinct a.user_id) `生单用户`
			,sum(case when (coupon_substract_summary is null 
					or batch_series like '%23base_ZK_728810%' 
					or batch_series like '%23extra_ZK_ce6f99%') then 0
			else coalesce(coupon_substract_summary,0) end) `券额`
			,count(distinct case when coupon_id is not null 
				and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
				and batch_series not like '%23base_ZK_728810%'
				and batch_series not like '%23extra_ZK_ce6f99%' 
			then order_no end)  `用券订单量`
	from default.mdw_order_v3_international a 
	where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
		and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
		-- and terminal_channel_type = 'app'
		and terminal_channel_type in ('www','app','touch')
		and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
		and (first_rejected_time is null or date(first_rejected_time) > order_date) 
		and (refund_time is null or date(refund_time) > order_date)
		and is_valid='1'
		and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
		and order_no <> '103576132435'
	group by 1
) t1 
left join (
	select dt
	     ,sum(uv) dau 
	from (
		select  dt 
				,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
							when e.area in ('欧洲','亚太','美洲') then e.area
							else '其他' end as mdd
				,count(distinct a.user_id) uv
		from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
		left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
		where dt >= date_sub(current_date, 14)
			and dt <= date_sub(current_date, 1)
			and business_type = 'hotel'
			and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
			and (search_pv + detail_pv + booking_pv + order_pv) > 0
			and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
			and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
		group by 1,2
	) t group by 1 
) t2 on t1.order_date=t2.dt
order by 1
;

