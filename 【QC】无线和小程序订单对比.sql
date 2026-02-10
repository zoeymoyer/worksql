with user_type as 
(
    select user_id,user_name
            ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1,2
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
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
,q_order as (----订单明细表表包含取消  分目的地、新老维度 app
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,case when terminal_channel_type = 'app' then 'app' when user_tracking_data['inner_channel'] = 'smart_app' then 'wechat' else 'else' end as channel
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        -- and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        -- and (refund_time is null or date(refund_time) > order_date)
        -- and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
        and order_date >= '2024-01-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,case when terminal_channel_type = 'app' then 'app' when extend_info['IS_WEBCHATAPP'] = 'T' then 'wechat' else 'else' end as channel
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
    --   and terminal_channel_type = 'app'
    --   and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >= '2024-01-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)

select t1.mth
       ,order_no_app_q
       ,order_no_app_c
       ,concat(round(order_no_app_q / order_no_app_c * 100, 1),'%') order_no_app_qc
       ,order_no_app_nu_q
       ,order_no_app_nu_c
       ,concat(round(order_no_app_nu_q / order_no_app_nu_c * 100, 1),'%') order_no_app_nu_qc


       ,order_no_wechat_q
       ,order_no_wechat_c
       ,concat(round(order_no_wechat_q / order_no_wechat_c * 100, 1),'%') order_no_wechat_qc
       ,order_no_wechat_nu_q
       ,order_no_wechat_nu_c
       ,concat(round(order_no_wechat_nu_q / order_no_wechat_nu_c * 100, 1),'%') order_no_wechat_nu_qc

from (
    select substr(order_date,1,7) mth
           ,round(sum(order_no_app_q) / count(1)) order_no_app_q
           ,round(sum(order_no_wechat_q) / count(1)) order_no_wechat_q
           ,round(sum(order_no_app_nu_q) / count(1)) order_no_app_nu_q
           ,round(sum(order_no_wechat_nu_q) / count(1)) order_no_wechat_nu_q
    from (
        select order_date
            ,count(distinct case when channel='app' then  order_no end) order_no_app_q
            ,count(distinct case when channel='wechat' then  order_no end) order_no_wechat_q
            ,count(distinct case when channel='app' and user_type = '新客' then  order_no end) order_no_app_nu_q
            ,count(distinct case when channel='wechat' and user_type = '新客' then  order_no end) order_no_wechat_nu_q
        from q_order
        group by 1
    )a group by 1
) t1 
left join (
    select substr(dt,1,7) mth
           ,round(sum(order_no_app_c) / count(1)) order_no_app_c
           ,round(sum(order_no_wechat_c) / count(1)) order_no_wechat_c
           ,round(sum(order_no_app_nu_c) / count(1)) order_no_app_nu_c
           ,round(sum(order_no_wechat_nu_c) / count(1)) order_no_wechat_nu_c
    from (
        select dt
            ,count(distinct case when channel='app' then  order_no end) order_no_app_c
            ,count(distinct case when channel='wechat' then  order_no end) order_no_wechat_c
            ,count(distinct case when channel='app' and user_type = '新客' then  order_no end) order_no_app_nu_c
            ,count(distinct case when channel='wechat' and user_type = '新客' then  order_no end) order_no_wechat_nu_c
        from c_order
        group by 1
    )a group by 1
) t2 on t1.mth=t2.mth 
order by 1 desc;

