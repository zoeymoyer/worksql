with user_type as
(
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
,hotle_score as (  --- 酒店评分
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as pt_dt 
        ,obj_seq 
        ,max(reference_score) as score
    from default.ods_qunar_review_obj_score a
    where dt = '%(DATE)s'
          and tag = '1'
    group by 1,2
)
,brand_table as (   --- 酒店集团
    select
        distinct chain_id,chain_name
    from temp.temp_yuchen_shen_chainhotel_id_list_20250702
    where chain_tag <> 'Local Chain'
)
,hotel_grade as (  --- 酒店等级商圈
    select  hi.hotel_seq,
            hi.hotel_name,
            hi.gradev2, --- 钻级
            hotel_grade, --- 星级
            hotel_group_name --- 所属集团
            ,chain_name      --- 所属集团
            ,hotel_brand_name,
            x.BizZones,  --- 商圈
            Standard_Hotel
    from (--- 国际酒店基础信息表
        select  hotel_seq
            ,hotel_name
            ,gradev2  --- 钻级
            ,hotel_grade --- 星级
            ,hotel_group_id  --- 所属集团
            ,hotel_group_name  --- 所属集团
            ,chain_name         --- 所属集团
            ,hotel_brand_name   --- 品牌
            ,case when attrs['hotelSubCategory'] in ('0','501','503','504','505','506','507','509','510','512','513','514','515','517','521','522','523','524','525','561') then '非标'
                when attrs['hotelSubCategory'] is null then '非标'
                else '标准' end as Standard_Hotel  --- 非标
        from ihotel_default.dim_hotel_info_intl_v3  a
        left join brand_table b on a.hotel_group_id = b.chain_id
        where a.dt = '%(DATE)s'
        group by 1,2,3,4,5,6,7,8,9
    ) hi
    left join (
        select distinct
            business_id AS hotel_seq,
            regexp_replace(
                regexp_replace(
                get_json_object(attrs, '$.BizZones'),
                '\\\\"', '"'  -- 处理转义双引号
                ),
                '\\\\\\\\', '\\\\'  -- 处理转义反斜杠
            ) AS BizZones,  --- 商圈
            get_json_object(attrs, 'CityCode') as citycode
        FROM hotel.ods_hotel_search_xds_entity_index_1_da
        WHERE dt = date_sub(current_date, 1)
        AND get_json_object(attrs, '$.Channel') = '1'
        AND (
            (
            get_json_object(attrs, '$.Status') IS NULL
            OR get_json_object(attrs, '$.Status') = 'on'
            )
            AND status = 0
        )
        AND get_json_object(attrs, '$.BizZones') IS NOT NULL
    )x
    on hi.hotel_seq = x.hotel_seq
)

,q_order as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,batch_series,hotel_grade,order_status
            ,coupon_id,user_name,hotel_seq
            ,init_gmv / room_night adr
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        --and order_date >= '2025-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,user_profile as (
    select distinct user_id,
            gender,     --性别
            city_name,  --常驻地
            prov_name,
            city_level,
            birth_year_month
    from pub.dim_user_profile_nd
)
,order_result as (
    select user_id,user_type,order_no,gender,city_name,prov_name,city_level,order_date,mdd,room_night,init_gmv,adr,hotel_seq,hotel_grade
           ,case when city_level in ('一线','新一线','二线')  then '高线'
                 when city_level in ('三线','四线','五线')  then '低线'
            else  '未知' end as  city_lev
           ,birth_year_month
           ,age
           ,case when age < 30 then '年轻'
                 when age >= 31 and age <= 45 then '成熟'
                 when age > 45 then '中老年'
            else '未知' end as age_level
           ,case when adr <= 100 then '100以内'
                 when adr > 100 and adr <= 300 then '(100, 300]'
                 when adr > 300 and adr <= 400 then '(300, 400]'
                 when adr > 400 and adr <= 500 then '(400, 500]'
                 when adr > 500 and adr <= 700 then '(500, 700]'
                 when adr > 700 and adr <= 900 then '(700, 900]'
                 when adr > 900 and adr <= 1000 then '(900, 1000]'
                 when adr > 1000 and adr <= 1200 then '(1000, 1200]'
                 when adr > 1200 and adr <= 1500 then '(1200, 1500]'
                 when adr > 1500  then '1500以上' end adr_type
    from (
        select o.order_no,user_type,order_date,mdd,room_night,init_gmv,adr,hotel_seq,hotel_grade
            ,o.user_id
            ,gender
            ,city_name
            ,prov_name
            ,coalesce(u.city_level, '未知')  city_level
            ,birth_year_month
            ,CASE
                WHEN birth_year_month IS NULL THEN '未知'
                ELSE CAST(SUBSTR('20251107', 1, 4) AS INT) - CAST(SUBSTR(birth_year_month, 1, 4) AS INT)
            END AS age
        from q_order o
        join user_profile u on u.user_id = o.user_id
    )
)

select coalesce(adr,'未知') adr,count(distinct order_no) orders,concat(round(count(distinct order_no) / sum(count(distinct order_no) ) over () * 100, 2),'%') rate
from order_result t1
left join hotel_grade t2 on t1.hotel_seq=t2.hotel_seq
left join hotle_score t3 on t1.hotel_seq=t3.obj_seq
where order_no in (select trim(order_no) order_no from temp.temp_zeyz_yang_urs_tiyan_chaping_order_list where is_manyi = '不满意')
group by 1
order by rate desc
;

