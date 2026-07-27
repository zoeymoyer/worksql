with user_type as (--- 用于判定q新老客
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
,c_user_type as(--- 用于判定c新老客
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
,q_c_hotel_mapping as(--- 用于映射q和c的酒店
    select hotel_seq,
        partner_hotel_id
    from ihotel_default.dim_hotel_mapping_intl_v3
    where dt = '%(DATE)s'
    and partner = 'ctrip'
    group by 1,2
)
,family_rank_hotel as (-- 亲子榜单酒店  517家
    select hotel_seq
    from ihotel_default.ods_leaderboard_analysis_rankling_hotel_da
    where dt >= '2026-05-18' and dt <= '2026-06-16' and theme_tab_name='亲子乐园'
    group by 1
)
,family_hotel as (-- 亲子酒店 57521家 
    select hotel_seq
    from ihotel_default.ads_rank_tree_node_da
    where dt >= '2026-05-18' 
        and dt <= '2026-06-16'
        and tree_node_name='亲子酒店'
    group by 1
)
,uv as (-- Q流量
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,hotel_seq
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= '2026-05-18'
       and dt <= '2026-06-16'
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5,6
)
,q_order_app as (-- Q订单明细表 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after,a.hotel_seq
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
    from default.mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= '2026-05-18' and order_date <= '2026-06-16'
        and order_no <> '103576132435'
)
,c_uv as(--- C流量
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,m.hotel_seq
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    left join q_c_hotel_mapping m on a.masterhotelid = m.partner_hotel_id
    where device_chl='app'
    and  dt>= '2026-05-18' and dt<= '2026-06-16'
    group by 1,2,3,4,5
)
,c_order as (--- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
            ,extend_info['room_night'] room_night
            ,m.hotel_seq
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    left join q_c_hotel_mapping m on o.hotel_seq = m.partner_hotel_id
    where dt = date_sub(current_date, 1)
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2026-05-18'
      and substr(order_date,1,10) <= '2026-06-16'
)
,qc_price as (-- 产品力 qc比价
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
        ,case when province_name in ('澳门','香港') then '港澳'  
                when a.country_name in ('德国','英国','法国','意大利','美国','西班牙','澳大利亚','土耳其','阿联酋','俄罗斯') then '海长'
                when a.country_name in ('日本','韩国','泰国') then a.country_name 
                else '其他' end as new_mdd
        ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when c.area in ('欧洲','亚太','美洲') then c.area else '其他' end as mdd
        ,case when identity in ('R1','R1_5') then '新客' else '老客' end as user_type
        ,id,pay_price_diff,ctrip_pay_price,pay_price_compare_result
        ,hotel_seq
    from default.dwd_hotel_cq_compare_price_result_intl_hi a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on a.country_name = c.country_name 
    where dt >= '20260518' and dt <= '20260616'
        and business_type = 'intl_crawl_cq_spa'
        and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
        and room_type_cover = 'Qmeet'
        and ctrip_room_status = 'true' 
        and qunar_room_status = 'true'
)
,q_uv_base as (-- q流量分酒店类型
    select dt,hotel_type
            ,count(distinct user_id) as q_dau
            ,count(distinct case when user_type='新客' then user_id end) as q_nu
    from (
        select
            dt
            ,user_type
            ,'整体' as hotel_type
            ,user_id
            ,user_type
        from uv
        union all
        select
            a.dt
            ,a.user_type
            ,'亲子酒店' as hotel_type
            ,a.user_id
            ,user_type
        from uv a
        join family_hotel b on a.hotel_seq = b.hotel_seq
        union all
        select
            a.dt
            ,a.user_type
            ,'亲子榜单酒店' as hotel_type
            ,a.user_id
            ,user_type
        from uv a
        join family_rank_hotel b on a.hotel_seq = b.hotel_seq
    )
    group by 1,2
)
,q_order_base as (-- q订单分酒店类型
    select dt,hotel_type
        ,count(distinct order_no) as q_order_num
        ,sum(init_gmv) as q_gmv
        ,sum(room_night) as q_room_night
        ,sum(final_commission_after) as q_commission

        ,count(distinct case when user_type='新客' then order_no end) as q_new_order_num
        ,sum(case when user_type='新客' then init_gmv end) as q_new_gmv
        ,sum(case when user_type='新客' then room_night end) as q_new_room_night
        ,sum(case when user_type='新客' then final_commission_after end) as q_new_commission
    from (
        select
            order_date as dt
            ,user_type
            ,'整体' as hotel_type
            ,order_no
            ,init_gmv
            ,room_night
            ,final_commission_after
            ,user_type
        from q_order_app
        union all
        select
            a.order_date as dt
            ,a.user_type
            ,'亲子酒店' as hotel_type
            ,a.order_no
            ,a.init_gmv
            ,a.room_night
            ,a.final_commission_after
            ,a.user_type
        from q_order_app a
        join family_hotel b on a.hotel_seq = b.hotel_seq
        union all
        select
            a.order_date as dt
            ,a.user_type
            ,'亲子榜单酒店' as hotel_type
            ,a.order_no
            ,a.init_gmv
            ,a.room_night
            ,a.final_commission_after
            ,a.user_type
        from q_order_app a
        join family_rank_hotel b on a.hotel_seq = b.hotel_seq
    ) group by 1,2
)
,c_uv_base as (-- c流量分酒店类型
    select dt,hotel_type
            ,count(distinct uid) as c_dau
            ,count(distinct case when user_type='新客' then uid end) as c_nu
    from (
        select
            dt
            ,user_type
            ,'整体' as hotel_type
            ,uid
            ,user_type
        from c_uv
        union all
        select
            a.dt
            ,a.user_type
            ,'亲子酒店' as hotel_type
            ,a.uid
            ,a.user_type
        from c_uv a
        join family_hotel b on a.hotel_seq = b.hotel_seq
        union all
        select
            a.dt
            ,a.user_type
            ,'亲子榜单酒店' as hotel_type
            ,a.uid
            ,a.user_type
        from c_uv a
        join family_rank_hotel b on a.hotel_seq = b.hotel_seq
    ) group by 1,2
)
,c_order_base as (-- c订单分酒店类型
    select dt,hotel_type
        ,count(distinct order_no) as c_order_num
        ,sum(room_fee) as c_gmv
        ,sum(room_night) as c_room_night
        ,sum(comission) as c_commission

        ,count(distinct case when user_type='新客' then order_no end) as c_new_order_num
        ,sum(case when user_type='新客' then room_fee end) as c_new_gmv
        ,sum(case when user_type='新客' then room_night end) as c_new_room_night
        ,sum(case when user_type='新客' then comission end) as c_new_commission
    from (
        select
            dt
            ,user_type
            ,'整体' as hotel_type
            ,order_no
            ,room_fee
            ,room_night
            ,comission
            ,user_type
        from c_order
        union all
        select
            a.dt
            ,a.user_type
            ,'亲子酒店' as hotel_type
            ,a.order_no
            ,a.room_fee
            ,a.room_night
            ,comission
            ,user_type
        from c_order a
        join family_hotel b on a.hotel_seq = b.hotel_seq
        union all
        select
            a.dt
            ,a.user_type
            ,'亲子榜单酒店' as hotel_type
            ,a.order_no
            ,a.room_fee
            ,a.room_night
            ,comission
            ,user_type
        from c_order a
        join family_rank_hotel b on a.hotel_seq = b.hotel_seq
    ) group by 1,2
)
,price_base as (-- 产品力分酒店类型
    select dt,hotel_type
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) / count(distinct id) as `支付价lose率`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) / count(distinct id) as `支付价beat率`
        ,sum(case when  pay_price_diff > 0 then pay_price_diff end) / sum(case when pay_price_diff > 0  then ctrip_pay_price end) as `支付价lose深度`
        ,sum(case when  pay_price_diff < 0 then pay_price_diff end) / sum(case when pay_price_diff < 0  then ctrip_pay_price end) as `支付价beat深度`
        ,count(distinct id) `支付价抓取次数`
        ,count(distinct case when pay_price_compare_result = 'Qlose' then id end) as `支付价lose数`
        ,count(distinct case when pay_price_compare_result = 'Qbeat' then id end) as `支付价beat数`
    from (
        select
            order_date as dt
            ,user_type
            ,'整体' as hotel_type
            ,id
            ,pay_price_diff
            ,ctrip_pay_price
            ,pay_price_compare_result
        from qc_price
        union all
        select
            a.order_date as dt
            ,a.user_type
            ,'亲子酒店' as hotel_type
            ,a.id
            ,a.pay_price_diff
            ,a.ctrip_pay_price
            ,a.pay_price_compare_result
        from qc_price a
        join family_hotel b on a.hotel_seq = b.hotel_seq
        union all
        select
            a.order_date as dt
            ,a.user_type
            ,'亲子榜单酒店' as hotel_type
            ,a.id
            ,a.pay_price_diff
            ,a.ctrip_pay_price
            ,a.pay_price_compare_result
        from qc_price a
        join family_rank_hotel b on a.hotel_seq = b.hotel_seq
    ) group by 1,2
)

select
    a.dt
    ,a.hotel_type
    ,a.q_dau
    ,a.q_nu
    ,c.c_dau
    ,c.c_nu
    ,d.q_order_num
    ,d.q_room_night
    ,d.q_gmv
    ,d.q_commission

    ,d.q_new_order_num
    ,d.q_new_room_night
    ,d.q_new_gmv
    ,d.q_new_commission

    ,e.c_order_num
    ,e.c_room_night
    ,e.c_gmv
    ,e.c_commission

    ,e.c_new_order_num
    ,e.c_new_room_night
    ,e.c_new_gmv
    ,e.c_new_commission

    ,f.`支付价lose率`
    ,f.`支付价beat率`
    ,f.`支付价lose深度`
    ,f.`支付价beat深度`
from q_uv_base a
left join c_uv_base c on a.dt = c.dt and a.hotel_type = c.hotel_type
left join q_order_base d on a.dt = d.dt  and a.hotel_type = d.hotel_type
left join c_order_base e on a.dt = e.dt  and a.hotel_type = e.hotel_type
left join price_base f on a.dt = f.dt  and a.hotel_type = f.hotel_type
order by dt desc
    ,case
        when hotel_type = '整体' then 1
        when hotel_type = '亲子酒店' then 2
        when hotel_type = '亲子榜单酒店' then 3
    end
;

