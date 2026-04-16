
--- 埋点数据
with track_base as (
    select  dt
           ,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as stat_date
           ,user_name
           ,key
           ,value
           ,q_trace_id
           ,log_time
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260301' and dt <= '20260330'
      and key in (
            'ihotel/preOrderFill/preOrderFillPage/resp/orderBookingTipsButton'
           ,'ihotel/booking/bookingPage/monitor/bookingContainerShow'
      )
      and user_name is not null
)

select stat_date
       ,count(distinct case when key='ihotel/preOrderFill/preOrderFillPage/resp/orderBookingTipsButton' then q_trace_id else null end) as `变价弹窗曝光PV`
       ,count(distinct case when key='ihotel/booking/bookingPage/monitor/bookingContainerShow' then q_trace_id else null end) as `B页曝光PV`
       ,count(distinct case when key='ihotel/preOrderFill/preOrderFillPage/resp/orderBookingTipsButton' then user_name else null end) as `变价弹窗曝光UV`
       ,count(distinct case when key='ihotel/booking/bookingPage/monitor/bookingContainerShow' then user_name else null end) as `B页曝光UV`
from track_base
group by 1
order by 1 desc
;



--- D2B房态房价不一致率
select a.booking_date,
        round((b/c)*100,2) as `D2B-房态不一致率PV`,
        round((a/(c-b))*100,2) as `D2B-房价不一致率PV`,
        round((a_more_expensive/(c-b))*100,2) as `D2B-房价变贵率PV`,
        round((a_more_cheap/(c-b))*100,2) as `D2B-房价变便宜率PV`,

        round((b_uv/c_uv)*100,2) as `D2B-房态不一致率UV`,
        round((a_uv/(c_uv-b_uv))*100,2) as `D2B-房价不一致率UV`
        ,round((a_more_expensive_uv/(c_uv-b_uv))*100,2) as `D2B-房价变贵率UV`
        ,round((a_more_cheap_uv/(c_uv-b_uv))*100,2) as `D2B-房价变便宜率UV`
        ,c as `总PV`
        ,c_uv as `总UV`
from(
    select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date

            ,count(distinct case when ischange='true' and ret='true' then q_trace_id else null end) as a
            ,count(distinct case when ischange='true' and ret='true' and price_change_type='变贵' then q_trace_id else null end) as a_more_expensive
            ,count(distinct case when ischange='true' and ret='true' and price_change_type='变便宜' then q_trace_id else null end) as a_more_cheap
            ,count(distinct if((ret='false' or ret is null),q_trace_id,null)) as b
            ,count(distinct q_trace_id) as c

            ,count(distinct case when ischange='true' and ret='true' then user_id else null end) as a_uv
            ,count(distinct case when ischange='true' and ret='true' and price_change_type='变贵' then user_id else null end) as a_more_expensive_uv
            ,count(distinct case when ischange='true' and ret='true' and price_change_type='变便宜' then user_id else null end) as a_more_cheap_uv
            ,count(distinct if((ret='false' or ret is null),user_id,null)) as b_uv
            ,count(distinct user_id) as c_uv
    from(
        select dt,log_time,q_trace_id,ret,a.country_name,province_name,err_code,err_message,err_sys,ischange,a.user_id
                
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                        when e.area in ('欧洲','亚太','美洲') then e.area
                        else '其他' end as mdd
                
                ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
                ,origin_price  -- 原价
                ,curroomprice  -- 实际价格
                ,tips
                ,case when curroomprice - origin_price > 0 then '变贵' when curroomprice - origin_price < 0 then '变便宜' else '不变' end as price_change_type
        from default.view_dw_user_app_booking_qta_di a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= date_sub(current_date, 30)
                and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
                and source='app_intl'
                and platform in ('adr','ios')
                and (province_name in ('香港','澳门','台湾') or a.country_name!='中国')
                and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                and q_trace_id not like 'f_inter_autotest%'
    )a
    group by 1
)a
order by 1 desc
;

--- B2O房态房价不一致率
select booking_date
        ,round(((total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价不一致率PV`
        ,round(((total_submit_fail_uv-total_submit_coupon_uv)/total_submit_count_uv)*100,2) as `B2O-房态房价不一致率UV`
        ,total_submit_count as `总提交PV`
        ,total_submit_count_uv as `总提交UV`
from (
    select booking_date
            ,count(if((ret='false' or ret is null),user_id,null)) as total_submit_fail
            ,count(if((ret='false' or ret is null) and err_message='领券人与入住人不符',user_id,null)) as total_submit_coupon
            ,count(user_id) as total_submit_count

            ,count(distinct if((ret='false' or ret is null),user_id,null)) as total_submit_fail_uv
            ,count(distinct if((ret='false' or ret is null) and err_message='领券人与入住人不符',user_id,null)) as total_submit_coupon_uv
            ,count(distinct user_id) as total_submit_count_uv
    from(
        select to_date(log_time) as booking_date
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                    when e.area in ('欧洲','亚太','美洲') then e.area
                    else '其他' end as mdd
                ,case when datediff(checkout_date, checkin_date) >= 2 then '多晚' else '单晚' end is_more_roomnight
                ,ret,err_message,a.user_id
                ,room_price 
        from default.dw_user_app_submit_qta_di a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where  concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) >= date_sub(current_date, 30)
            and concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) <= date_sub(current_date, 1)
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (a.country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
    ) y group by 1
)a
order by  1 desc
;