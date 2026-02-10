---- 房源紧张型酒店人群短信触达
with hotel_jz as (  --- 房源紧张酒店
    select hotel_seq,user_name,user_id,min(all_room_count_arr) all_room_count_arr
    from ihotel_default.dw_hotel_price_display  
    where dt = '20250916' 
        and room_ct_nerv = '1' 
        and room_ct_accu = '1'
    group by 1,2,3
)
,uv as (  ---- 近3/7天浏览D页>=2次或者B页>0
    select distinct user_id,user_name,hotel_seq
    from (
            select  case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                    ,a.user_id
                    ,a.user_name
                    ,hotel_seq
                    ,sum(detail_pv) detail_pv
                    ,sum(booking_pv) booking_pv
            from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
            left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
            where dt >= date_sub(current_date, 7)
                and dt <= date_sub(current_date, 1)
                and business_type = 'hotel'
                and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
                and (search_pv + detail_pv + booking_pv + order_pv) > 0
                and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
                and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
            group by 1,2,3,4
    )t where detail_pv >= 2 or booking_pv > 0  
) 
,q_order as (-- q订单量
    select user_name
    from default.mdw_order_v3_international a
    where dt = '%(DATE)s'
      and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
      and terminal_channel_type in ('app')
      and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
      and (first_rejected_time is null or date(first_rejected_time) > order_date)
      and (refund_time is null or date(refund_time) > order_date)
      and is_valid='1'
      and order_no <> '103576132435'
      and order_date >= date_sub(current_date, 7)  
      and order_date <= date_sub(current_date, 1)  
    group by 1
)

select count(distinct user_name) uv 
from (
    select t1.user_name
            ,t1.hotel_seq
            ,all_room_count_arr
            ,row_number() over(partition by t1.user_name order by all_room_count_arr) rn
    from uv t1
    left join q_order t2 on t1.user_name=t2.user_name
    left join hotel_jz t3 on t1.user_name=t3.user_name and t1.hotel_seq=t3.hotel_seq
    where t2.user_name is null and t3.hotel_seq is not null 

) where rn=1
;

-------- 去哪日活中近15日平台新客中有指定国酒券的用户
with platform_new as (--- 判定平台新
    select distinct dt,
                    user_pk,
                    user_id
    from pub.dwd_flow_accapp_potential_user_di
    where dt >= date_sub(current_date, 15)  and dt <= date_sub(current_date, 1)
        and dict_type = 'pncl_wl_username'
)
,qunar_app as (  --- 去哪日活
        select distinct dt,username,uid
        from pub.dws_flow_app_wechat_active_user_di a
        where dt = date_sub(current_date, 1)
)
,conpon_info as ( --- 账户有券 制定券id
    select create_time
            ,uid
            ,batch_series
            ,coupon_name
    from ihotel_default.ods_hotel_qta_coupon_di a
    where dt >= '2025-05-01'
    and batch_series in ('PTnew30_MJ_24e30a',
                        'appqxcx_ZK_4bd0b7',
                        'jgw99zzh_ZK_297d9e',
                        'jgw99zql_ZK_1a9a42')
)
,uv as ----国酒流量
(
    select distinct dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,a.user_id
            ,a.user_name
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     where dt >= date_sub(current_date, 1)
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
)


select count(distinct t1.username)  `近15日平台新客活跃用户`
        ,count(distinct case when  t3.user_id is not null then t1.username end)  `近15日平台新客国酒活跃用户`
        ,count(distinct case when  t3.uid is not null then t1.username end)  `近15日平台新客活跃且有国酒券用户`
from qunar_app t1 
left join platform_new t2 on t1.username=t2.user_pk
left join uv t3 on t1.username=t3.user_name
left join conpon_info t4 on t4.uid=t3.user_id
where t2.user_pk is not null 
;





