--0224 李娜修改 sql
--- pv角度
select a.datas as `日期`,
    pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`,
    -- `S2D-房态一致率`,`S2D-房价一致率`,`S2D-房态房价一致率`,
    `L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
    `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
    `B2O-房态房价一致率`,
    round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `S2D-房价一致率`,
        round((1-e/a)*100,2) as `S2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `S2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price in('','0') or (low_price not in('','0') and listPrice!=low_price)) and is_hotel_full='false' then log_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and low_price  in ('','0') and is_hotel_full='false' then log_id  else null end)  as e
        from (
            select dt,
                log_id,
                cast(regexp_extract(params,'&preListPrice=([^&]*)',1) as DECIMAL) as listPrice,
                regexp_extract(params,'&orderPriceLog=([^&]*)',1) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                action_entrance_map['fromforlog'] as is_list  
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between date_sub(current_date,16) and date_sub(current_date,1)
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and (action_entrance_map['fromforlog']=0 )
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        group by 1
    ) a
) a
left join (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
        round((1-e/a)*100,2) as `L2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e
        from (
            select dt,log_id,
                ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                -- 20240927 是否符合人数条件
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                action_entrance_map['fromforlog'] as is_list  
                ,checkin_date
                checkout_date,user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between date_sub(current_date,16) and date_sub(current_date,1)
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        where match_adult != 'false' or match_adult is null
        group by 1
    ) a
) b on a.datas=b.datas
left join(
    select a.booking_date,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,
            round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then q_trace_id else null end)) as a,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as b,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as c
        from(
            select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange
            from view_dw_user_app_booking_qta_di 
            where  dt between '%(DATE_15)s' and '%(DATE)s'
                and source='app_intl'
                and platform in ('adr','ios')
                and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
        )a
        group by 1
    ) a
) c on a.datas=c.booking_date
left join(
    select booking_date,
        round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
    from(
        select to_date(log_time) as booking_date,
            count(if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),true,null)) as total_submit_fail,
            count(if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,true,null)) as total_submit_coupon,
            count(if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,true,null)) as total_submit_count
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s' 
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
        group by 1
    ) y
) d on a.datas=d.booking_date
order by `日期` desc
;

--- uv角度
select a.datas as `日期`,
    pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`,
    -- `S2D-房态一致率`,`S2D-房价一致率`,`S2D-房态房价一致率`,
    `L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
    `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
    `B2O-房态房价一致率`,
    round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `S2D-房价一致率`,
        round((1-e/a)*100,2) as `S2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `S2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then user_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price in('','0') or (low_price not in('','0') and listPrice!=low_price)) and is_hotel_full='false' then user_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and low_price  in ('','0') and is_hotel_full='false' then user_id  else null end)  as e
        from (
            select dt,
                log_id,
                cast(regexp_extract(params,'&preListPrice=([^&]*)',1) as DECIMAL) as listPrice,
                regexp_extract(params,'&orderPriceLog=([^&]*)',1) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                action_entrance_map['fromforlog'] as is_list  ,user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between '2025-10-31' and '2025-11-06'
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and (action_entrance_map['fromforlog']=0 )
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        group by 1
    ) a
) a
left join (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
        round((1-e/a)*100,2) as `L2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then user_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then user_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then user_id  else null end)  as e
        from (
            select dt,log_id,
                ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                -- 20240927 是否符合人数条件
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                action_entrance_map['fromforlog'] as is_list  
                ,checkin_date
                checkout_date,user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between '2025-10-31' and '2025-11-06'
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        where match_adult != 'false' or match_adult is null
        group by 1
    ) a
) b on a.datas=b.datas
left join(
    select a.booking_date,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,
            round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then user_id else null end)) as a,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),user_id,null)) as b,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),user_id,null)) as c
        from(
            select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id
            from view_dw_user_app_booking_qta_di 
            where  dt between '20251031' and '20251106'
                and source='app_intl'
                and platform in ('adr','ios')
                and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
        )a
        group by 1
    ) a
) c on a.datas=c.booking_date
left join(
    select booking_date,
        round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
    from(
        select to_date(log_time) as booking_date,
            count(distinct if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),user_id,null)) as total_submit_fail,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,user_id,null)) as total_submit_coupon,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,user_id,null)) as total_submit_count
        from dw_user_app_submit_qta_di 
        where dt between '20251031' and '20251106' 
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
        group by 1
    ) y
) d on a.datas=d.booking_date
order by `日期` desc
;

---- D2B不同表对比
select t1.booking_date
       ,t1.`D2B-房态一致率`,t2.`D2B-房态一致率`, t1.`D2B-房态一致率`-t2.`D2B-房态一致率` a_gap 
       ,t1.`D2B-房价一致率`,t2.`D2B-房价一致率`, t1.`D2B-房价一致率`-t2.`D2B-房价一致率` b_gap 
       ,t1.`D2B-房态房价一致率`,t2.`D2B-房态房价一致率`, t1.`D2B-房态房价一致率`-t2.`D2B-房态房价一致率` c_gap 
from (
    select a.booking_date,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
            ,count(distinct case when  p_impactorderingflag = '3' then  p_traceid else null end) a
            ,count(distinct case when p_impactorderingflag not in ('2', '3', '4') then  p_traceid else null end) b
            ,count(distinct p_traceid) c
        from (
            select dt
                ,p_impactorderingflag
                ,p_userid
                ,p_traceid
            from qlibra.h_intl_order_fail_monitor
            where event_id = '738555'
            and dt  between '%(DATE_15)s' and '%(DATE)s' 
            and (p_country !='中国' or p_province in ('香港','澳门','台湾'))
            and p_bookingchannel = 'MOBILE'
        ) a
        group by 1
    )a
) t1 left join (
    select a.booking_date,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,
            round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then q_trace_id else null end)) as a,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as b,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),q_trace_id,null)) as c
        from(
            select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange
            from view_dw_user_app_booking_qta_di 
            where  dt between '%(DATE_15)s' and '%(DATE)s'
                and source='app_intl'
                and platform in ('adr','ios')
                and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
        )a
        group by 1
     ) a
)t2 on t1.booking_date=t2.booking_date
order by 1
;

--- uv角度   弃用--------
select a.datas as `日期`,
    pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`,
    -- `S2D-房态一致率`,`S2D-房价一致率`,`S2D-房态房价一致率`,
    `L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
    `D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
    `B2O-房态房价一致率`,
    round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `S2D-房价一致率`,
        round((1-e/a)*100,2) as `S2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `S2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then user_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price in('','0') or (low_price not in('','0') and listPrice!=low_price)) and is_hotel_full='false' then user_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and low_price  in ('','0') and is_hotel_full='false' then user_id  else null end)  as e
        from (
            select dt,
                log_id,
                cast(regexp_extract(params,'&preListPrice=([^&]*)',1) as DECIMAL) as listPrice,
                regexp_extract(params,'&orderPriceLog=([^&]*)',1) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                action_entrance_map['fromforlog'] as is_list ,user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between date_sub(current_date,16) and date_sub(current_date,1)
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and (action_entrance_map['fromforlog']=0 )
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        group by 1
    ) a
) a
left join (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
        round((1-e/a)*100,2) as `L2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then user_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then user_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then user_id  else null end)  as e
        from (
            select dt,log_id,
                ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                -- 20240927 是否符合人数条件
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                action_entrance_map['fromforlog'] as is_list  
                ,checkin_date
                checkout_date,user_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between date_sub(current_date,16) and date_sub(current_date,1)
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        where match_adult != 'false' or match_adult is null
        group by 1
    ) a
) b on a.datas=b.datas
left join(
    select a.booking_date,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
            ,count(distinct case when  p_impactorderingflag = '3' then  p_userid else null end) a
            ,count(distinct case when p_impactorderingflag not in ('2', '3', '4') then  p_userid else null end) b
            ,count(distinct p_userid) c
        from (
            select dt
                ,p_impactorderingflag
                ,p_userid
                ,p_traceid
            from qlibra.h_intl_order_fail_monitor
            where event_id = '738555'
            and dt  between '%(DATE_15)s' and '%(DATE)s' 
            and (p_country !='中国' or p_province in ('香港','澳门','台湾'))
            and p_bookingchannel = 'MOBILE'
        ) a
        group by 1
    )a
) c on a.datas=c.booking_date
left join(
    select booking_date,
        round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
    from(
        select to_date(log_time) as booking_date,
            count(distinct if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),user_id,null)) as total_submit_fail,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,user_id,null)) as total_submit_coupon,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,user_id,null)) as total_submit_count
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s' 
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
        group by 1
    ) y
) d on a.datas=d.booking_date
order by `日期` desc
;




---- 三个页面不一致情况

select booking_date,count(distinct user_id) 
    
from (

    select booking_date,user_id
    from(
        select a.dt as booking_date,user_id,listPrice,low_price
        from (
            select dt,log_id,
                ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                -- 20240927 是否符合人数条件
                get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                action_entrance_map['fromforlog'] as is_list  
                ,checkin_date
                checkout_date,user_id
                ,action_time
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between '2025-10-31' and '2025-10-31'
                and source='hotel'
                and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        ) a
        where match_adult != 'false' or match_adult is null 
    ) 
    where  
       ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
      or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
    
    union all

    select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id
            
        from(
            select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id
            from view_dw_user_app_booking_qta_di 
            where  dt between '20251031' and '20251106'
                and source='app_intl'
                and platform in ('adr','ios')
                and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                and (
                    (ischange='true' and ret='true') ---房价不一致
                    and (ret!='false' and ret is not null)  ---房态不一致
                )
        )a
        
    union all
    select to_date(log_time) as booking_date,user_id
    from dw_user_app_submit_qta_di 
    where dt between '20251031' and '20251106' 
        and source='app_intl'
        and platform in ('adr','ios','AndroidPhone','iPhone')
        and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        and err_code not in( '-98','784','785')
        and (ret='false' or ret is null)
    group by 1,2

) group by 1 order by 1

;

