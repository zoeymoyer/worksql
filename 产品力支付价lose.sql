select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) `日期`
                -- , `目的地`
        , count(distinct case when business_type = 'intl_crawl_cq_api_userview' then id end) as `主站抓取次数`
        , count(distinct case when business_type = 'intl_crawl_cq_api_userview' and after_chase_discount_base_price_compare_result='Qlose' then id end) as `主站折后底价Qlose次数`
        , count(distinct case when business_type = 'intl_crawl_cq_api_userview' and after_chase_discount_base_price_compare_result='Qbeat' then id end) as `主站折后底价Qbeat次数`
        , count(distinct case when business_type = 'intl_crawl_cq_api_userview' and qunar_price_info['before_coupon_before_point_price'] - ctrip_before_coupons_cashback_price > 0 then id end) as `主站券前价Qlose次数`

        , count(distinct case when business_type = 'intl_crawl_cq_spa' then id end) as `抓取抓取次数`
        , count(distinct case when business_type = 'intl_crawl_cq_spa' and before_coupons_cashback_price_compare_result = 'Qlose' then id end) as `抓取券前价Qlose次数`
        , count(distinct case when business_type = 'intl_crawl_cq_spa' and after_chase_discount_base_price_compare_result='Qlose' then id end) as `抓取折后底价Qlose次数`
        , count(distinct case when business_type = 'intl_crawl_cq_spa' and pay_price_compare_result = 'Qlose' then id end) as `抓取支付价Qlose次数`
        , count(distinct case when business_type = 'intl_crawl_cq_spa' and before_coupons_cashback_price_compare_result = 'Qbeat'  then id end) as `抓取券前价Qbeat次数`
        , count(distinct case when business_type = 'intl_crawl_cq_spa' and pay_price_compare_result = 'Qbeat' then id end) as `抓取支付价Qbeat次数`
        , sum(case when business_type = 'intl_crawl_cq_spa' then ctrip_before_coupons_cashback_price end) as `抓取C券前价`
        , sum(case when business_type = 'intl_crawl_cq_spa' then qunar_before_coupons_cashback_price end) as `抓取Q券前价`

        , -sum(case when business_type = 'intl_crawl_cq_spa' and pay_price_compare_result = 'Qbeat' then pay_price_diff else 0 end) as `抓取支付价beat金额`
        , sum(case when business_type = 'intl_crawl_cq_spa' and pay_price_compare_result = 'Qbeat' then ctrip_pay_price else 0 end) as `抓取支付价beatC支付`
        , sum(case when business_type = 'intl_crawl_cq_spa' and pay_price_compare_result = 'Qlose' then pay_price_diff else 0 end) as `抓取支付价lose金额`
        , sum(case when business_type = 'intl_crawl_cq_spa' and pay_price_compare_result = 'Qlose' then ctrip_pay_price else 0 end) as `抓取支付价loseC支付`
from (
        select crawl_time
                , id
                , dt
                , business_type
                , ctrip_price_info
                , pay_price_diff
                , ctrip_pay_price
                , qunar_product_room_id
                , pay_price_compare_result
                , qunar_discount_base_price
                , ctrip_discount_base_price
                , qunar_before_coupons_cashback_price
                , ctrip_before_coupons_cashback_price
                , c_identity_before_coupons_cashback_price
                , discount_base_price_compare_result
                , before_coupons_cashback_price_compare_result
                , chased_discount_price_compare_result as after_chase_discount_base_price_compare_result
                , qunar_price_info['traceId'] as qtrace_id
                , qunar_price_info
                , case 
                     when qunar_member_level in ('R1_5','R1') then '新客' 
                     when qunar_member_level in ('R2','R3','R4') then '老客' 
                     else '其他' end user_type
                
                , case when province_name in ('澳门','香港') then province_name when country_name in ('日本','泰国','马来西亚','韩国','阿联酋','新加坡','美国') then country_name else '其他' end as `目的地`
        from default.dwd_hotel_cq_compare_price_result_intl_hi a 
        where dt >= '20250101' and dt <= '%(DATE)s'
        -- and business_type = 'intl_crawl_cq_api_userview'
                and compare_type = 'PHYSICAL_ROOM_TYPE_LOWEST'
                and room_type_cover = 'Qmeet'
                and ctrip_room_status = 'true' 
                and qunar_room_status = 'true'
        )a               
group by 1
order by 1
;

      
-- 流量(同质化+物理)
with
spa_out as (
    select distinct 
        product_id
    from ihotel_default.dw_data_log_hour_xray_intl_spa_out
    where concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) = date_sub(current_date,1)
        and c_isagent = 'false'
)
, flow_perspective_sim as (
    select a.order_date
        ,`渠道`
        , case 
                when province_name in ('澳门','香港') then province_name 
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                when e.area in ('欧洲','亚太','美洲') then e.area 
                else '其他' 
        end as `目的地`
        , chased_discount_price_compare_result
        , count(distinct id) as `次数`
        , sum(bp_advantage_amount) as `折后底价Q-C`
        , sum(ctrip_pay_price) as `C支付价`
    from(
            select id
            , order_date
            , orderNum
            ,`渠道`
            , country_name
            , province_name
                , chased_discount_price_compare_result
                , bp_advantage_amount
                , ctrip_pay_price
            from
                (select *
                    ,concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as order_date
                    ,case
                        when qunar_wrapper_id in ('hca1faud10j','hca1fc8h40i','hca1f71a00i','hca1f7nc00j','hca1fel540i','hca1fe4100j','hca1eg3k60n','hca1f71a00j','hca1fc1250i','hca10eg3k6k','hca1fbsn50i','hca1fe4100k','hca1fbr920j',
                                                    'hca1erb900o','hca1eg3k60o','hca1erb900m','hca1fd4750i','hca1625f80l','hca1ffou40i','hca1em5m10n','hca1fcek50i','hca10eq7a8i','hca1fa7u40i','hca1fck230i','hca1fbr920i','hca908hc00p',
                                                    'hca1fbra40i','hca10175k6m','hca1fes670i','hca1fe4050i','hca1e3t010i','hca1fbr920m','hca1fdfs80i','hca1fbb170i','hca1erb900n','hca1fc2e70k','hca10e9bc3j','hca1f4om90i','hca1feo980j',
                                                    'hca1fep110i','hca1fbr920l','hca1fe4100i','hca10du058k') then '小代理同投'
                        when qunar_wrapper_id not in ('hca9008oc4l','hca10lqv90p') then '其他非C2Q'
                        when qunar_wrapper_id in ('hca9008oc4l','hca10lqv90p') and b.product_id is not null then 'DC'
                        else '其他C2Q'
                    end as `渠道`
                    , qunar_price_info['traceId'] as trace_id
                    , qunar_price_info['orderNum'] as orderNum
                    , -chased_discount_price_diff as bp_advantage_amount
                    , -chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate
                from default.dwd_hotel_cq_compare_price_result_intl_hi a
                left join spa_out as b on SPLIT(a.qunar_product_room_id, '_')[0] = b.product_id
                where concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) between date_sub(current_date,8) and date_sub(current_date,1)
                    and business_type='intl_crawl_cq_api_userview'
                    and compare_type='SIMILAR_PRODUCT_LOWEST' --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
                    and room_type_cover='Qmeet'
                    and ctrip_room_status='true'
                    and qunar_room_status='true'
                    --and qunar_price_info['order_product_similar_lowest']='1'
                    and qunar_pay_type != '现付'
                --and country_name = '日本'
                ) cq_compare_raw
    ) a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    group by 1,2,3,4
)
,flow_perspective_phy as (
    select a.order_date
        ,`渠道`
        , case 
                when province_name in ('澳门','香港') then province_name 
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                when e.area in ('欧洲','亚太','美洲') then e.area 
                else '其他' 
        end as `目的地`
        , chased_discount_price_compare_result
        , count(distinct id) as `次数`
        , sum(bp_advantage_amount) as `折后底价Q-C`
        , sum(ctrip_pay_price) as `C支付价`
    from(
            select id
            , order_date
            , orderNum
            ,`渠道`
            , country_name
            , province_name
                , chased_discount_price_compare_result
                , bp_advantage_amount
                , ctrip_pay_price
            from
                (select *
                    ,concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) as order_date
                    ,case
                        when qunar_wrapper_id in ('hca1faud10j','hca1fc8h40i','hca1f71a00i','hca1f7nc00j','hca1fel540i','hca1fe4100j','hca1eg3k60n','hca1f71a00j','hca1fc1250i','hca10eg3k6k','hca1fbsn50i','hca1fe4100k','hca1fbr920j',
                                                    'hca1erb900o','hca1eg3k60o','hca1erb900m','hca1fd4750i','hca1625f80l','hca1ffou40i','hca1em5m10n','hca1fcek50i','hca10eq7a8i','hca1fa7u40i','hca1fck230i','hca1fbr920i','hca908hc00p',
                                                    'hca1fbra40i','hca10175k6m','hca1fes670i','hca1fe4050i','hca1e3t010i','hca1fbr920m','hca1fdfs80i','hca1fbb170i','hca1erb900n','hca1fc2e70k','hca10e9bc3j','hca1f4om90i','hca1feo980j',
                                                    'hca1fep110i','hca1fbr920l','hca1fe4100i','hca10du058k') then '小代理同投'
                        when qunar_wrapper_id not in ('hca9008oc4l','hca10lqv90p') then '其他非C2Q'
                        when qunar_wrapper_id in ('hca9008oc4l','hca10lqv90p') and b.product_id is not null then 'DC'
                        else '其他C2Q'
                    end as `渠道`
                    , qunar_price_info['traceId'] as trace_id
                    , qunar_price_info['orderNum'] as orderNum
                    , -chased_discount_price_diff as bp_advantage_amount
                    , -chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate
                from default.dwd_hotel_cq_compare_price_result_intl_hi a
                left join spa_out as b on SPLIT(a.qunar_product_room_id, '_')[0] = b.product_id
                where concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) between date_sub(current_date,8) and date_sub(current_date,1)
                    and business_type='intl_crawl_cq_api_userview'
                    and compare_type='PHYSICAL_ROOM_TYPE_LOWEST' --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
                    and room_type_cover='Qmeet'
                    and ctrip_room_status='true'
                    and qunar_room_status='true'
                --and country_name = '日本'
                ) cq_compare_raw
    ) a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    group by 1,2,3,4
)
, flow_table_sim as (
    select
        order_date `日期`
        , `渠道`
        , `目的地`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `次数` else 0 end) `Beat次数-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `次数` else 0 end) `Lose次数-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `次数` else 0 end) `比价次数-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `次数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `次数` else 0 end) `Beat率-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `次数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `次数` else 0 end) `Lose率-同质化`

        , sum(case when chased_discount_price_compare_result = 'Qlose' then `折后底价Q-C` else 0 end) `lose金额-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `折后底价Q-C` else 0 end) `beat金额-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) `底价优势金额-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `C支付价-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `底价优势率-同质化`
    from flow_perspective_sim
    group by 1,2,3
)
, flow_table_phy as (
    select
        order_date `日期`
        , `渠道`
        , `目的地`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `次数` else 0 end) `Beat次数-物理`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `次数` else 0 end) `Lose次数-物理`
        , sum(case when chased_discount_price_compare_result is not null then `次数` else 0 end) `比价次数-物理`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `次数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `次数` else 0 end) `Beat率-物理`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `次数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `次数` else 0 end) `Lose率-物理`

        , sum(case when chased_discount_price_compare_result = 'Qlose' then `折后底价Q-C` else 0 end) `lose金额-物理`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `折后底价Q-C` else 0 end) `beat金额-物理`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) `底价优势金额-物理`
        , sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `C支付价-物理`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `底价优势率-物理`
    from flow_perspective_phy
    group by 1,2,3
)

select
    substr(a.`日期`,1,7) `月`
    , a.`日期`
    , a.`渠道`
    , a.`目的地`
    , `Beat次数-同质化`
    , `Lose次数-同质化`
    , `比价次数-同质化`
    ,concat(round(`Beat率-同质化`*100,2),'%') `Beat率-同质化`
    ,concat(round(`Lose率-同质化`*100,2),'%') `Lose率-同质化`
    , `lose金额-同质化`
    , `beat金额-同质化`
    , `底价优势金额-同质化`
    , `C支付价-同质化`
    ,concat(round(`底价优势率-同质化`*100,2),'%') `底价优势率-同质化`
    , `Beat次数-物理`
    , `Lose次数-物理`
    , `比价次数-物理`
    ,concat(round(`Beat率-物理`*100,2),'%') `Beat率-物理`
    ,concat(round(`Lose率-物理`*100,2),'%') `Lose率-物理`
    , `lose金额-物理`
    , `beat金额-物理`
    , `底价优势金额-物理`
    , `C支付价-物理`
    ,concat(round(`底价优势率-物理`*100,2),'%') `底价优势率-物理`
from flow_table_sim a
left join flow_table_phy b on a.`日期` = b.`日期` and a.`渠道` = b.`渠道`  and a.`目的地` = b.`目的地`
order by a.`日期` desc,`渠道`,`目的地`






-- 生单(物理+同质化)
with
-- 同质化比价表
cq_compare_raw_sim as (
    select 
                compare_type
                ,chased_discount_price_compare_result
                , country_name
                , province_name
                ,qunar_physical_room_id
                ,ctrip_pay_price
                ,qunar_price_info['order_product_similar_lowest'] as order_product_similar_lowest
                , qunar_price_info['traceId'] as trace_id
                , qunar_price_info['productId'] as productId
                , qunar_price_info['orderNum'] as orderNum
                , -chased_discount_price_diff as bp_advantage_amount
                , -chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate
    from default.dwd_hotel_cq_compare_price_result_intl_hi
    where concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) between date_sub(current_date,8) and date_sub(current_date,1)
                and business_type='intl_crawl_cq_api_order'
                and compare_type='SIMILAR_PRODUCT_LOWEST' --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
                and room_type_cover='Qmeet'
                and ctrip_room_status='true'
                and qunar_room_status='true'
                and qunar_price_info['order_product_similar_lowest']='1'
                and substr(uniq_id,1,11) = 'h_datacube_'
                --and country_name = '日本'
)
--物理房型比价表
,cq_compare_raw_phy as (
    select 
                compare_type
                ,chased_discount_price_compare_result
                , country_name
                , province_name
                ,qunar_physical_room_id
                ,ctrip_pay_price
                ,qunar_price_info['order_product_similar_lowest'] as order_product_similar_lowest
                , qunar_price_info['traceId'] as trace_id
                , qunar_price_info['productId'] as productId
                , qunar_price_info['orderNum'] as orderNum
                , -chased_discount_price_diff as bp_advantage_amount
                , -chased_discount_price_diff/qunar_before_coupons_cashback_price as bp_advantage_rate
    from default.dwd_hotel_cq_compare_price_result_intl_hi
    where concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) between date_sub(current_date,8) and date_sub(current_date,1)
                and business_type='intl_crawl_cq_api_order'
                and compare_type='PHYSICAL_ROOM_TYPE_LOWEST' --物理房型维度PHYSICAL_ROOM_TYPE_LOWEST 同质化维度SIMILAR_PRODUCT_LOWEST
                and room_type_cover='Qmeet'
                and ctrip_room_status='true'
                and qunar_room_status='true'
                and substr(uniq_id,1,11) = 'h_datacube_'
                --and country_name = '日本'

)
-- 订单表
, order_intl as (
    select order_date
            , case
                when wrapper_id in ('hca1faud10j','hca1fc8h40i','hca1f71a00i','hca1f7nc00j','hca1fel540i','hca1fe4100j','hca1eg3k60n','hca1f71a00j','hca1fc1250i','hca10eg3k6k','hca1fbsn50i','hca1fe4100k','hca1fbr920j',
                                                     'hca1erb900o','hca1eg3k60o','hca1erb900m','hca1fd4750i','hca1625f80l','hca1ffou40i','hca1em5m10n','hca1fcek50i','hca10eq7a8i','hca1fa7u40i','hca1fck230i','hca1fbr920i','hca908hc00p',
                                                     'hca1fbra40i','hca10175k6m','hca1fes670i','hca1fe4050i','hca1e3t010i','hca1fbr920m','hca1fdfs80i','hca1fbb170i','hca1erb900n','hca1fc2e70k','hca10e9bc3j','hca1f4om90i','hca1feo980j',
                                                     'hca1fep110i','hca1fbr920l','hca1fe4100i','hca10du058k') then '小代理同投'
                when qta_supplier_id not in ('1615667','800000164') then '其他非C2Q'
                when qta_supplier_id in ('1615667','800000164') and c.vendor_name = 'DC' then 'DC'
                else '其他C2Q'
            end as `渠道`
            , case 
                when province_name in ('澳门','香港') then province_name 
                when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name 
                when e.area in ('欧洲','亚太','美洲') then e.area 
                else '其他' 
            end as `目的地`
            ,a.order_no
            ,qta_supplier_id
            ,qunar_product_room_id
            ,physical_room_id
            , init_gmv
            , room_night
        from
            (select *
                , concat(qta_product_id,'_',qta_supplier_id) as qunar_product_room_id
            from default.mdw_order_v3_international
            where dt='%(DATE)s'
                and order_date between date_sub(current_date,8) and date_sub(current_date,1)
                and terminal_channel_type = 'app'
                and is_valid='1'
                and (province_name in ('台湾','澳门','香港') or country_name !='中国')
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
                and (first_rejected_time is null or date(first_rejected_time) > order_date) 
                and (refund_time is null or date(refund_time) > order_date) --非当天取消&拒单
                --and country_name = '日本'
        ) a

        left join
            (select order_no 
                , max(purchase_order_no) as purchase_order_no
            from ihotel_default.dw_purchase_order_info_v3
            where dt = '%(DATE)s'
            group by 1
            ) b 
        on a.order_no = b.order_no

        left join
            (select distinct partner_order_no
                , extend_info['vendor_name'] as vendor_name
            from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da 
            where dt = '%(FORMAT_DATE)s'
            ) c 
        on b.purchase_order_no = c.partner_order_no

        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
)
-- 同质化房型订单比价结果
, order_perpective_sim as (
        select order_intl.order_date
            , chased_discount_price_compare_result
            , `目的地`
            , `渠道`
            , count(distinct order_no) as `订单数`
            , sum(room_night) as `间夜`
            , sum(bp_advantage_amount) as `折后底价Q-C`
            , sum(ctrip_pay_price) as `C支付价`
        from order_intl order_intl 
        left join cq_compare_raw_sim b
        on order_intl.order_no = b.orderNum
    group by 1,2,3,4
)
-- 物理房型订单比价结果
, order_perpective_phy as (
        select order_intl.order_date
            , `目的地`
            , `渠道`
            , chased_discount_price_compare_result
            , count(distinct order_no) as `订单数`
            , sum(room_night) as `间夜`
            , sum(bp_advantage_amount) as `折后底价Q-C`
            , sum(ctrip_pay_price) as `C支付价`
        from order_intl order_intl
        left join cq_compare_raw_phy c
        on order_intl.order_no = c.orderNum
        and order_intl.qunar_product_room_id = c.productId
        and  order_intl.physical_room_id=c.qunar_physical_room_id
    group by 1,2,3,4
)
, order_table_sim as (
    select
        order_date `日期`
        , `渠道`
        , `目的地`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `订单数` else 0 end) `Beat订单数-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `订单数` else 0 end) `Lose订单数-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `订单数` else 0 end) `比价订单数-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `订单数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `订单数` else 0 end) `Beat率-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `订单数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `订单数` else 0 end) `Lose率-同质化`

        , sum(case when chased_discount_price_compare_result = 'Qlose' then `折后底价Q-C` else 0 end) `Lose金额-同质化`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `折后底价Q-C` else 0 end) `Beat金额-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) `底价优势金额-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `C支付价-同质化`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `底价优势率-同质化`
    from order_perpective_sim
    group by 1,2,3
)
, order_table_phy as (
    select
        order_date `日期`
        , `渠道`
        , `目的地`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `订单数` else 0 end) `Beat订单数-物理`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `订单数` else 0 end) `Lose订单数-物理`
        , sum(case when chased_discount_price_compare_result is not null then `订单数` else 0 end) `比价订单数-物理`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `订单数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `订单数` else 0 end) `Beat率-物理`
        , sum(case when chased_discount_price_compare_result = 'Qlose' then `订单数` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `订单数` else 0 end) `Lose率-物理`

        , sum(case when chased_discount_price_compare_result = 'Qlose' then `折后底价Q-C` else 0 end) `Lose金额-物理`
        , sum(case when chased_discount_price_compare_result = 'Qbeat' then `折后底价Q-C` else 0 end) `Beat金额-物理`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) `底价优势金额-物理`
        , sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `C支付价-物理`
        , sum(case when chased_discount_price_compare_result is not null then `折后底价Q-C` else 0 end) / sum(case when chased_discount_price_compare_result is not null then `C支付价` else 0 end) `底价优势率-物理`
    from order_perpective_phy
    group by 1,2,3
)

select
    substr(a.`日期`,1,7) `月`
    , a.`日期`
    , a.`渠道`
    , a.`目的地`
    , `Beat订单数-同质化`
    , `Lose订单数-同质化`
    , `比价订单数-同质化`
    ,concat(round(`Beat率-同质化`*100,2),'%') `Beat率-同质化`
    ,concat(round(`Lose率-同质化`*100,2),'%') `Lose率-同质化`
    , `lose金额-同质化`
    , `beat金额-同质化`
    , `底价优势金额-同质化`
    , `C支付价-同质化`
    ,concat(round(`底价优势率-同质化`*100,2),'%') `底价优势率-同质化`
    , `Beat订单数-物理`
    , `Lose订单数-物理`
    , `比价订单数-物理`
    ,concat(round(`Beat率-物理`*100,2),'%') `Beat率-物理`
    ,concat(round(`Lose率-物理`*100,2),'%') `Lose率-物理`
    , `lose金额-物理`
    , `beat金额-物理`
    , `底价优势金额-物理`
    , `C支付价-物理`
    ,concat(round(`底价优势率-物理`*100,2),'%') `底价优势率-物理`
from order_table_sim a
left join order_table_phy b on a.`日期` = b.`日期` and a.`渠道` = b.`渠道` and a.`目的地` = b.`目的地`
order by a.`日期` desc, a.`渠道`,`目的地`