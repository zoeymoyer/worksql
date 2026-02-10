--- pv角度
select a.datas as `日期`,
    pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`
    ,round(100 - `L2D-房态一致率` , 2) `L2D-房态不一致率`
    ,round(100 - `L2D-房价一致率` , 2) `L2D-房价不一致率`
    ,round(100 - `L2D-房态房价一致率` , 2) `L2D-房态房价不一致率`
    
    ,round(100 - `D2B-房态一致率` , 2) `D2B-房态不一致率`
    ,round(100 - `D2B-房价一致率` , 2) `D2B-房价不一致率`
    ,round(100 - `D2B-房态房价一致率` , 2) `D2B-房态房价不一致率`
    
    ,round(100 - `B2O-房态房价一致率` , 2) `B2O-房态房价不一致率`
    ,round(`整体不一致pv` / dau_pv * 100, 2) as `整体不一致率`
    ,round(`整体不一致pv_剔除房态` / dau_pv * 100, 2) as `整体不一致率_剔除房态`
    ,round(`整体不一致pv_剔除D2B房态` / dau_pv * 100, 2) as `整体不一致率_剔除D2B房态`
    
    --,`L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`
    --,`D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`
    --,`B2O-房态房价一致率`
    --,round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
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
            where dt between date_sub(current_date,15) and date_sub(current_date,1)
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
) a
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
left join (
    select booking_date,count(distinct device_id) `整体不一致uv`,count(device_id) `整体不一致pv`
    from (---- 页面整体变价
        select booking_date,user_id,device_id   
        from(
            select a.dt as booking_date,user_id,listPrice,low_price,device_id
            from (
                select dt,log_id,
                    ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                    ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                    regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                    -- 20240927 是否符合人数条件
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                    action_entrance_map['fromforlog'] as is_list  
                    ,user_id
                    ,action_time,qtrace_id,device_id
                from ihotel_default.dw_user_app_log_detail_visit_di_v1
                where dt between date_sub(current_date, 15) and date_sub(current_date, 1)
                    and source='hotel'
                    and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                    and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                    --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                    and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                    and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            ) a
            where match_adult != 'false' or match_adult is null 
        ) a
        where  
           ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
        or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all

        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id,orig_device_id device_id
                
            from(
                select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id,orig_device_id
                from view_dw_user_app_booking_qta_di 
                where  dt between '%(DATE_15)s' and '%(DATE)s'
                    and source='app_intl'
                    and platform in ('adr','ios')
                    and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                    and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                    and (
                        (ischange='true' and ret='true') ---房价不一致
                         or (ret='false' or ret is not null)  ---房态不一致
                    )
            )a
            
        union all

        select to_date(log_time) as booking_date,user_id,orig_device_id device_id
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and (ret='false' or ret is null)
    ) t group by 1
) e on a.datas=e.booking_date
left join (
    select booking_date,count(distinct device_id) `整体不一致uv_剔除房态`,count(device_id) `整体不一致pv_剔除房态`
    from (---- 页面整体变价
        select booking_date,user_id,device_id   
        from(
            select a.dt as booking_date,user_id,listPrice,low_price,device_id
            from (
                select dt,log_id,
                    ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                    ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                    regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                    -- 20240927 是否符合人数条件
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                    action_entrance_map['fromforlog'] as is_list  
                    ,user_id
                    ,action_time,qtrace_id,device_id
                from ihotel_default.dw_user_app_log_detail_visit_di_v1
                where dt between date_sub(current_date, 15) and date_sub(current_date, 1)
                    and source='hotel'
                    and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                    and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                    --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                    and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                    and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            ) a
            where match_adult != 'false' or match_adult is null 
        ) a
        where  
          -- ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
         ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all

        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id,orig_device_id device_id
                
            from(
                select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id,orig_device_id
                from view_dw_user_app_booking_qta_di 
                where  dt between '%(DATE_15)s' and '%(DATE)s'
                    and source='app_intl'
                    and platform in ('adr','ios')
                    and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                    and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                    and (
                        (ischange='true' and ret='true') ---房价不一致
                        -- or (ret='false' or ret is not null)  ---房态不一致
                    )
            )a
            
        union all

        select to_date(log_time) as booking_date,user_id,orig_device_id device_id
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and (ret='false' or ret is null)
    ) t group by 1
) g on a.datas=g.booking_date
left join (
    select booking_date,count(distinct device_id) `整体不一致uv_剔除D2B房态`,count(device_id) `整体不一致pv_剔除D2B房态`
    from (---- 页面整体变价
        select booking_date,user_id,device_id   
        from(
            select a.dt as booking_date,user_id,listPrice,low_price,device_id
            from (
                select dt,log_id,
                    ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                    ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                    regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                    -- 20240927 是否符合人数条件
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                    action_entrance_map['fromforlog'] as is_list  
                    ,user_id
                    ,action_time,qtrace_id,device_id
                from ihotel_default.dw_user_app_log_detail_visit_di_v1
                where dt between date_sub(current_date, 15) and date_sub(current_date, 1)
                    and source='hotel'
                    and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                    and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                    --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                    and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                    and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            ) a
            where match_adult != 'false' or match_adult is null 
        ) a
        where  
           ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
        or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all

        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id,orig_device_id device_id
                
            from(
                select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id,orig_device_id
                from view_dw_user_app_booking_qta_di 
                where  dt between '%(DATE_15)s' and '%(DATE)s'
                    and source='app_intl'
                    and platform in ('adr','ios')
                    and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                    and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                    and (
                        (ischange='true' and ret='true') ---房价不一致
                         --or (ret='false' or ret is not null)  ---房态不一致
                    )
            )a
            
        union all

        select to_date(log_time) as booking_date,user_id,orig_device_id device_id
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and (ret='false' or ret is null)
    ) t group by 1
) h on a.datas=h.booking_date
left join (
    select dt
          ,sum(uv) dau 
          ,sum(pv) dau_pv
    from (
        select  dt 
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,count(distinct a.user_id) uv
                ,count(a.user_id) pv
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        group by 1,2
    ) t group by 1 
)f  on a.datas=f.dt
order by `日期` desc
;


--- uv角度
select a.datas as `日期`,
    pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`
    ,round(100 - `L2D-房态一致率` , 2) `L2D-房态不一致率`
    ,round(100 - `L2D-房价一致率` , 2) `L2D-房价不一致率`
    ,round(100 - `L2D-房态房价一致率` , 2) `L2D-房态房价不一致率`
    
    ,round(100 - `D2B-房态一致率` , 2) `D2B-房态不一致率`
    ,round(100 - `D2B-房价一致率` , 2) `D2B-房价不一致率`
    ,round(100 - `D2B-房态房价一致率` , 2) `D2B-房态房价不一致率`
    
    ,round(100 - `B2O-房态房价一致率` , 2) `B2O-房态房价不一致率`
    ,round(`整体不一致uv` / dau * 100, 2) as `整体不一致率`
    ,round(`整体不一致uv_剔除房态` / dau * 100, 2) as `整体不一致率_剔除房态`
    ,round(`整体不一致uv_剔除D2B房态` / dau * 100, 2) as `整体不一致率_剔除D2B房态`
    
    -- ,`L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`
    -- ,`D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`
    -- ,`B2O-房态房价一致率`
    -- ,round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from (
    select datas,
        round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
        round((1-e/a)*100,2) as `L2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
    from(
        select a.dt as  datas,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then device_id end) as a,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then device_id  else null end)  as b,
            count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then device_id  else null end)  as e
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
                checkout_date,user_id,device_id
            from ihotel_default.dw_user_app_log_detail_visit_di_v1
            where dt between date_sub(current_date,15) and date_sub(current_date,1)
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
) a
left join(
    select a.booking_date,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
    from(
        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,
            round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then orig_device_id else null end)) as a,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),orig_device_id,null)) as b,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),orig_device_id,null)) as c
        from(
            select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,orig_device_id
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
            count(distinct if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),orig_device_id,null)) as total_submit_fail,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,orig_device_id,null)) as total_submit_coupon,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,orig_device_id,null)) as total_submit_count
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s' 
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
        group by 1
    ) y
) d on a.datas=d.booking_date
left join (
    select booking_date,count(distinct device_id) `整体不一致uv`,count(device_id) `整体不一致pv`
    from (---- 页面整体变价
        select booking_date,user_id,device_id   
        from(
            select a.dt as booking_date,user_id,listPrice,low_price,device_id
            from (
                select dt,log_id,
                    ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                    ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                    regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                    -- 20240927 是否符合人数条件
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                    action_entrance_map['fromforlog'] as is_list  
                    ,user_id
                    ,action_time,qtrace_id,device_id
                from ihotel_default.dw_user_app_log_detail_visit_di_v1
                where dt between date_sub(current_date, 15) and date_sub(current_date, 1)
                    and source='hotel'
                    and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                    and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                    --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                    and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                    and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            ) a
            where match_adult != 'false' or match_adult is null 
        ) a
        where  
           ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
        or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all

        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id,orig_device_id device_id
                
            from(
                select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id,orig_device_id
                from view_dw_user_app_booking_qta_di 
                where  dt between '%(DATE_15)s' and '%(DATE)s'
                    and source='app_intl'
                    and platform in ('adr','ios')
                    and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                    and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                    and (
                        (ischange='true' and ret='true') ---房价不一致
                         or (ret='false' or ret is not null)  ---房态不一致
                    )
            )a
            
        union all

        select to_date(log_time) as booking_date,user_id,orig_device_id device_id
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and (ret='false' or ret is null)
    ) t group by 1
) e on a.datas=e.booking_date
left join (
    select booking_date,count(distinct device_id) `整体不一致uv_剔除房态`,count(device_id) `整体不一致pv_剔除房态`
    from (---- 页面整体变价
        select booking_date,user_id,device_id   
        from(
            select a.dt as booking_date,user_id,listPrice,low_price,device_id
            from (
                select dt,log_id,
                    ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                    ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                    regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                    -- 20240927 是否符合人数条件
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                    action_entrance_map['fromforlog'] as is_list  
                    ,user_id
                    ,action_time,qtrace_id,device_id
                from ihotel_default.dw_user_app_log_detail_visit_di_v1
                where dt between date_sub(current_date, 15) and date_sub(current_date, 1)
                    and source='hotel'
                    and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                    and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                    --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                    and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                    and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            ) a
            where match_adult != 'false' or match_adult is null 
        ) a
        where  
          -- ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
         ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all

        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id,orig_device_id device_id
                
            from(
                select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id,orig_device_id
                from view_dw_user_app_booking_qta_di 
                where  dt between '%(DATE_15)s' and '%(DATE)s'
                    and source='app_intl'
                    and platform in ('adr','ios')
                    and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                    and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                    and (
                        (ischange='true' and ret='true') ---房价不一致
                        -- or (ret='false' or ret is not null)  ---房态不一致
                    )
            )a
            
        union all

        select to_date(log_time) as booking_date,user_id,orig_device_id device_id
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and (ret='false' or ret is null)
    ) t group by 1
) g on a.datas=g.booking_date
left join (
    select booking_date,count(distinct device_id) `整体不一致uv_剔除D2B房态`,count(device_id) `整体不一致pv_剔除D2B房态`
    from (---- 页面整体变价
        select booking_date,user_id,device_id   
        from(
            select a.dt as booking_date,user_id,listPrice,low_price,device_id
            from (
                select dt,log_id,
                    ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
                    ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
                    regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
                    -- 20240927 是否符合人数条件
                    get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
                    action_entrance_map['fromforlog'] as is_list  
                    ,user_id
                    ,action_time,qtrace_id,device_id
                from ihotel_default.dw_user_app_log_detail_visit_di_v1
                where dt between date_sub(current_date, 15) and date_sub(current_date, 1)
                    and source='hotel'
                    and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                    and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                    --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                    and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                    and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            ) a
            where match_adult != 'false' or match_adult is null 
        ) a
        where  
           ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
        or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all

        select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,user_id,orig_device_id device_id
                
            from(
                select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,user_id,orig_device_id
                from view_dw_user_app_booking_qta_di 
                where  dt between '%(DATE_15)s' and '%(DATE)s'
                    and source='app_intl'
                    and platform in ('adr','ios')
                    and (province_name in ('香港','澳门','台湾') or country_name!='中国')
                    and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
                    and (
                        (ischange='true' and ret='true') ---房价不一致
                         --or (ret='false' or ret is not null)  ---房态不一致
                    )
            )a
            
        union all

        select to_date(log_time) as booking_date,user_id,orig_device_id device_id
        from dw_user_app_submit_qta_di 
        where dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios','AndroidPhone','iPhone')
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
            and err_code not in( '-98','784','785')
            and (ret='false' or ret is null)
    ) t group by 1
) h on a.datas=h.booking_date
left join (
    select dt
          ,sum(uv) dau 
          ,sum(pv) dau_pv
    from (
        select  dt 
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,count(distinct a.user_id) uv
                ,count(a.user_id) pv
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        group by 1,2
    ) t group by 1 
)f  on a.datas=f.dt
order by `日期` desc
;


--- PV角度十分内不一致
with l_log as (--- L页
    select dt
          ,user_id
          ,log_id
          ,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
          ,qtrace_id,device_id
    from ihotel_default.dw_user_app_log_search_di_v1
    where dt >= date_sub(current_date, 15)
)
,d_price as (--- D页房态房价变价
    select a.dt as booking_date,user_id,listPrice,low_price
            ,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
            ,qtrace_id,log_id,device_id,is_hotel_full
    from (
        select dt,log_id,
            ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
            ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
            regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
            get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
            get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
            action_entrance_map['fromforlog'] as is_list  
            ,user_id
            ,action_time,qtrace_id,device_id
        from ihotel_default.dw_user_app_log_detail_visit_di_v1
        where dt >= date_sub(current_date, 15)
            and source='hotel'
            and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
            and regexp_extract(params,'&fromList=([^&]*)',1)='true'
            --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
            and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
    ) a
    where match_adult != 'false' or match_adult is null 
)
,b_price as (---B页报价
    select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
        ,user_id,log_time,q_trace_id,orig_device_id
        ,ischange,ret
    from(
        select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_sys,ischange,user_id,orig_device_id
        from view_dw_user_app_booking_qta_di 
        where  dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios')
            and (province_name in ('香港','澳门','台湾') or country_name!='中国')
            and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
    )a
)
,o_price as (--- o页
    select distinct to_date(log_time) as booking_date,user_id,log_time,orig_device_id,q_trace_id,ret,err_message
    from dw_user_app_submit_qta_di 
    where dt between '%(DATE_15)s' and '%(DATE)s'
        and source='app_intl'
        and platform in ('adr','ios','AndroidPhone','iPhone')
        and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        and err_code not in( '-98','784','785')     
)
,l2d_price_10min as (--- L2D在10min内变价
    select * 
    from (
        select t2.booking_date,t2.listPrice,t2.low_price,t2.action_time,t2.qtrace_id,t2.user_id,t2.log_id,t2.device_id,is_hotel_full
            ,min(unix_timestamp(t2.action_time) - unix_timestamp(t1.action_time)) ts
        from l_log t1 
        join ( --- D页房态房价不一致
            select * 
            from d_price 
            -- where  
            --     ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            --     or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        ) t2 
        on t1.dt=t2.booking_date and t1.device_id=t2.device_id 
        where t2.action_time >= t1.action_time
        group by 1,2,3,4,5,6,7,8,9
    )t where ts <= 600
)
,d2b_price_10min as (--- D2B在10min内变价
    select booking_date,user_id,q_trace_id,orig_device_id,ischange,ret,ts,log_time
    from (
        select t2.booking_date,t2.user_id,t2.log_time,t2.q_trace_id,t2.orig_device_id,ischange,ret,min(unix_timestamp(t2.log_time) - unix_timestamp(t1.action_time)) ts
        from d_price t1 
        join (
            select booking_date ,user_id,log_time,q_trace_id,orig_device_id,ischange,ret
            from  b_price
            -- where (
            --     (ischange='true' and ret='true') ---房价不一致
            --     and (ret!='false' and ret is not null)  ---房态不一致
            -- )
        ) t2 on t1.booking_date=t2.booking_date and t1.device_id=t2.orig_device_id where t2.log_time >= t1.action_time
        group by 1,2,3,4,5,6,7
    ) t where ts <= 600
)
,b2o_price_10min as (--- B2O在10min内变价
    select *
    from (
        select t2.booking_date,t2.user_id,t2.log_time,t2.q_trace_id,t2.orig_device_id,t2.ret,t2.err_message,min(unix_timestamp(t2.log_time) - unix_timestamp(t1.log_time)) ts
        from b_price t1 
        join (
            select booking_date,user_id,log_time,q_trace_id,orig_device_id,ret,err_message
            from o_price
            -- where  (ret='false' or ret is null)
        ) t2 on t1.booking_date=t2.booking_date and t1.orig_device_id=t2.orig_device_id where t2.log_time >= t1.log_time
        group by 1,2,3,4,5,6,7
    )a where a.ts <= 600
)
,overall_price as (--- 整体变价不一致
    select booking_date
          ,count(distinct device_id) `整体不一致uv` 
          ,count(device_id) `整体不一致pv` 
    from (
        select booking_date,device_id
        from l2d_price_10min
        where  
            ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
    )t group by 1
)
,overall_price_noft as (--- 整体变价不一致剔除房态
    select booking_date
          ,count(distinct device_id) `整体不一致uv_剔除房态` 
          ,count(device_id) `整体不一致pv_剔除房态` 
    from (
        select booking_date,device_id
        from l2d_price_10min
        where  
            -- ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
             ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                -- or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
    )t group by 1
)
,overall_price_noD2Bft as (--- 整体变价不一致剔除D2B房态
    select booking_date
          ,count(distinct device_id) `整体不一致uv_剔除D2B房态` 
          ,count(device_id) `整体不一致pv_剔除D2B房态` 
    from (
        select booking_date,device_id
        from l2d_price_10min
        where  
            ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                -- or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
    )t group by 1
)
,gj_dau as (
    select dt
          ,sum(uv) dau 
          ,sum(pv) dau_pv
    from (
        select  dt 
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,count(distinct a.user_id) uv
                ,count(a.user_id) pv
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        group by 1,2
    ) t group by 1 
)

select t1.booking_date as `日期`
       ,pmod(datediff(t1.booking_date, '2018-06-25'), 7)+1  as `星期`

       ,round(100 - `L2D-房态一致率` , 2) `L2D-房态不一致率`
       ,round(100 - `L2D-房价一致率` , 2) `L2D-房价不一致率`
       ,round(100 - `L2D-房态房价一致率` , 2) `L2D-房态房价不一致率`
       
       ,round(100 - `D2B-房态一致率` , 2) `D2B-房态不一致率`
       ,round(100 - `D2B-房价一致率` , 2) `D2B-房价不一致率`
       ,round(100 - `D2B-房态房价一致率` , 2) `D2B-房态房价不一致率`
       
       ,round(100 - `B2O-房态房价一致率` , 2) `B2O-房态房价不一致率`

       ,round(`整体不一致pv` / dau_pv * 100, 2) as `整体不一致率`
       ,round(`整体不一致pv_剔除房态` / dau_pv * 100, 2) as `整体不一致率_剔除房态`
       ,round(`整体不一致pv_剔除D2B房态` / dau_pv * 100, 2) as `整体不一致率_剔除D2B房态`
      
    --   ,`L2D-房价一致率`
    --   ,`L2D-房态一致率`
    --   ,`L2D-房态房价一致率`
    --   ,`L2D-房价一致率_uv`
    --   ,`L2D-房态一致率_uv`
    --   ,`L2D-房态房价一致率_uv`
    --   ,`D2B-房态一致率`
    --   ,`D2B-房价一致率`
    --   ,`D2B-房态房价一致率`
    --   ,`D2B-房态一致率_uv`
    --   ,`D2B-房价一致率_uv`
    --   ,`D2B-房态房价一致率_uv`
    --   ,`B2O-房态房价一致率`
    --   ,`B2O-房态房价一致率_uv`
    --   ,`整体不一致uv` 
    --   ,`整体不一致pv` 
    --   ,dau
    --   ,dau_pv
from (
    select booking_date
        ,round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`
        ,round((1-e/a)*100,2) as `L2D-房态一致率`
        ,round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`

        ,round((1-(b_uv-e_uv)/(a_uv-e_uv))*100,2) as `L2D-房价一致率_uv`
        ,round((1-e_uv/a_uv)*100,2) as `L2D-房态一致率_uv`
        ,round((1-(b_uv-e_uv)/(a_uv-e_uv))*(1-e_uv/a_uv)*100,2) as `L2D-房态房价一致率_uv`
    from (
        select booking_date
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e

            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then device_id end) as a_uv
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then device_id  else null end)  as b_uv
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then device_id  else null end)  as e_uv
        from l2d_price_10min
        group by 1
    )a
) t1 
left join  (
    select a.booking_date
          ,round((1-b/c)*100,2) as `D2B-房态一致率`
          ,round((1-a/(c-b))*100,2) as `D2B-房价一致率`
          ,round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`

          ,round((1-b_uv/c_uv)*100,2) as `D2B-房态一致率_uv`
          ,round((1-a_uv/(c_uv-b_uv))*100,2) as `D2B-房价一致率_uv`
          ,round((1-b_uv/c_uv)*(1-a_uv/(c_uv-b_uv))*100,2) as `D2B-房态房价一致率_uv`
    from(
        select  booking_date
               ,round(count(distinct case when ischange='true' and ret='true' then q_trace_id else null end)) as a
               ,count(distinct if((ret='false' or ret is null),q_trace_id,null)) as b
               ,count(distinct q_trace_id) as c

               ,round(count(distinct case when ischange='true' and ret='true' then orig_device_id else null end)) as a_uv
               ,count(distinct if((ret='false' or ret is null),orig_device_id,null)) as b_uv
               ,count(distinct orig_device_id) as c_uv
        from d2b_price_10min a
        group by 1
    ) a
) t2 on t1.booking_date=t2.booking_date 
left join (
    select booking_date
          ,round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100, 2) as `B2O-房态房价一致率`
          ,round((1-(total_submit_fail_uv-total_submit_coupon_uv)/total_submit_count_uv)*100, 2) as `B2O-房态房价一致率_uv`
    from(
        select  booking_date
                ,count(if((ret='false' or ret is null) ,true,null)) as total_submit_fail
                ,count(if((ret='false' or ret is null) and err_message='领券人与入住人不符' ,true,null)) as total_submit_coupon
                ,count(true) as total_submit_count

                ,count(distinct if((ret='false' or ret is null) ,orig_device_id,null)) as total_submit_fail_uv
                ,count(distinct if((ret='false' or ret is null) and err_message='领券人与入住人不符' ,orig_device_id,null)) as total_submit_coupon_uv
                ,count(distinct orig_device_id) as total_submit_count_uv
        from b2o_price_10min 
        group by 1
    ) y
) t3 on t1.booking_date=t3.booking_date
left join overall_price t4 on t1.booking_date=t4.booking_date
left join gj_dau t5 on t1.booking_date=t5.dt
left join overall_price_noft t6 on t1.booking_date=t6.booking_date
left join overall_price_noD2Bft t7 on t1.booking_date=t7.booking_date
order by `日期` desc
;



---- UV维度十分钟内
with l_log as (--- L页
    select dt
          ,user_id
          ,log_id
          ,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
          ,qtrace_id,device_id
    from ihotel_default.dw_user_app_log_search_di_v1
    where dt >= date_sub(current_date, 15)
)
,d_price as (--- D页房态房价变价
    select a.dt as booking_date,user_id,listPrice,low_price
            ,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
            ,qtrace_id,log_id,device_id,is_hotel_full
    from (
        select dt,log_id,
            ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
            ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
            regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
            get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
            get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
            action_entrance_map['fromforlog'] as is_list  
            ,user_id
            ,action_time,qtrace_id,device_id
        from ihotel_default.dw_user_app_log_detail_visit_di_v1
        where dt >= date_sub(current_date, 15)
            and source='hotel'
            and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
            and regexp_extract(params,'&fromList=([^&]*)',1)='true'
            --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
            and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
    ) a
    where match_adult != 'false' or match_adult is null 
)
,b_price as (---B页报价
    select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
        ,user_id,log_time,q_trace_id,orig_device_id
        ,ischange,ret
    from(
        select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_sys,ischange,user_id,orig_device_id
        from view_dw_user_app_booking_qta_di 
        where  dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios')
            and (province_name in ('香港','澳门','台湾') or country_name!='中国')
            and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
    )a
)
,o_price as (--- o页
    select distinct to_date(log_time) as booking_date,user_id,log_time,orig_device_id,q_trace_id,ret,err_message
    from dw_user_app_submit_qta_di 
    where dt between '%(DATE_15)s' and '%(DATE)s'
        and source='app_intl'
        and platform in ('adr','ios','AndroidPhone','iPhone')
        and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        and err_code not in( '-98','784','785')     
)
,l2d_price_10min as (--- L2D在10min内变价
    select * 
    from (
        select t2.booking_date,t2.listPrice,t2.low_price,t2.action_time,t2.qtrace_id,t2.user_id,t2.log_id,t2.device_id,is_hotel_full
            ,min(unix_timestamp(t2.action_time) - unix_timestamp(t1.action_time)) ts
        from l_log t1 
        join ( --- D页房态房价不一致
            select * 
            from d_price 
            -- where  
            --     ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            --     or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        ) t2 
        on t1.dt=t2.booking_date and t1.device_id=t2.device_id 
        where t2.action_time >= t1.action_time
        group by 1,2,3,4,5,6,7,8,9
    )t where ts <= 600
)
,d2b_price_10min as (--- D2B在10min内变价
    select booking_date,user_id,q_trace_id,orig_device_id,ischange,ret,ts,log_time
    from (
        select t2.booking_date,t2.user_id,t2.log_time,t2.q_trace_id,t2.orig_device_id,ischange,ret,min(unix_timestamp(t2.log_time) - unix_timestamp(t1.action_time)) ts
        from d_price t1 
        join (
            select booking_date ,user_id,log_time,q_trace_id,orig_device_id,ischange,ret
            from  b_price
            -- where (
            --     (ischange='true' and ret='true') ---房价不一致
            --     and (ret!='false' and ret is not null)  ---房态不一致
            -- )
        ) t2 on t1.booking_date=t2.booking_date and t1.device_id=t2.orig_device_id where t2.log_time >= t1.action_time
        group by 1,2,3,4,5,6,7
    ) t where ts <= 600
)
,b2o_price_10min as (--- B2O在10min内变价
    select *
    from (
        select t2.booking_date,t2.user_id,t2.log_time,t2.q_trace_id,t2.orig_device_id,t2.ret,t2.err_message,min(unix_timestamp(t2.log_time) - unix_timestamp(t1.log_time)) ts
        from b_price t1 
        join (
            select booking_date,user_id,log_time,q_trace_id,orig_device_id,ret,err_message
            from o_price
            -- where  (ret='false' or ret is null)
        ) t2 on t1.booking_date=t2.booking_date and t1.orig_device_id=t2.orig_device_id where t2.log_time >= t1.log_time
        group by 1,2,3,4,5,6,7
    )a where a.ts <= 600
)
,l_price_change_raw AS (--- L页变价明细
    SELECT log_date,
        CAST(get_json_object(value, '$.operTime') AS BIGINT) AS oper_time,
        orig_device_id,
        get_json_object(value, '$.ext.qTraceId') AS trace_id
    FROM default.dw_qav_ihotel_track_info_di
    WHERE dt >= '%(DATE_15)s'
    AND dt <= '%(DATE)s'
    AND key = 'ihotel/List/HotelCell/monitor/updatePricePageL'
    AND get_json_object(value, '$.ext.priceChange') = 'true'
    group by 1,2,3,4
)
,l_update AS (---- L页10分内变价明细
    select log_date,
        orig_device_id,
        trace_id
    from (
            SELECT
                log_date,
                orig_device_id,
                trace_id,
                substr(oper_time,1,10) oper_time,
                LEAD(substr(oper_time,1,10), 1) OVER ( PARTITION BY log_date,orig_device_id ORDER BY oper_time) AS prev_oper_time
            FROM l_price_change_raw
            -- where orig_device_id = '12249ecf9e9277dd'
        ) t where cast(prev_oper_time as int) - cast(oper_time as int) <= 600
    group by 1,2,3
)
,d_price_change_raw AS (--- D页变价明细
    SELECT log_date,
        CAST(get_json_object(value, '$.operTime') AS BIGINT) AS oper_time,
        orig_device_id,
        get_json_object(value, '$.ext.traceIdPrice') AS trace_id
    FROM default.dw_qav_ihotel_track_info_di
    WHERE dt >= '%(DATE_15)s'
    AND dt <= '%(DATE)s'
    AND key = 'ihotel/detail/priceList/monitor/updatePriceNoRoom'
    group by 1,2,3,4
)
,d_update AS (--- D页10min内变价明细
    select log_date,
        orig_device_id,
        trace_id
    from (
            SELECT
                log_date,
                orig_device_id,
                trace_id,
                substr(oper_time,1,10) oper_time,
                LEAD(substr(oper_time,1,10), 1) OVER ( PARTITION BY log_date,orig_device_id ORDER BY oper_time) AS prev_oper_time
            FROM d_price_change_raw
            -- where orig_device_id = '12249ecf9e9277dd'
        ) t where cast(prev_oper_time as int) - cast(oper_time as int) <= 600
    group by 1,2,3
)
,overall_price as (--- 整体变价不一致
    select booking_date
          ,count(distinct device_id) `整体不一致uv` 
          ,count(device_id) `整体不一致pv` 
    from (
        ---- 页面间
        select booking_date,device_id
        from l2d_price_10min
        where  
            ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
    )t group by 1
)
,overall_price_all as (--- 整体变价不一致
    select booking_date
          ,count(distinct device_id) `整体不一致uv_全部` 
          ,count(device_id) `整体不一致pv_全部` 
    from (
        ---- 页面间
        select booking_date,device_id
        from l2d_price_10min
        where  
            ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
        ----- 页面内变价
        ----- L页变价
        union all
        select log_date booking_date,orig_device_id  device_id
        from l_update
        ----- D页变价
        union all
        select log_date booking_date,orig_device_id  device_id
        from d_update
    )t group by 1
)
,overall_price_noft as (--- 整体变价不一致剔除房态
    select booking_date
          ,count(distinct device_id) `整体不一致uv_剔除房态` 
          ,count(device_id) `整体不一致pv_剔除房态` 
    from (
        select booking_date,device_id
        from l2d_price_10min
        where  
            -- ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
             ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                -- or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
    )t group by 1
)
,overall_price_noD2Bft as (--- 整体变价不一致剔除D2B房态
    select booking_date
          ,count(distinct device_id) `整体不一致uv_剔除D2B房态` 
          ,count(device_id) `整体不一致pv_剔除D2B房态` 
    from (
        select booking_date,device_id
        from l2d_price_10min
        where  
            ((listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 ))   ---- 房态不一致
            or ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
                -- or (ret='false' or ret is not null)  ---房态不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
    )t group by 1
)
,gj_dau as (
    select dt
          ,sum(uv) dau 
          ,sum(pv) dau_pv
    from (
        select  dt 
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,count(distinct a.user_id) uv
                ,count(a.user_id) pv
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        group by 1,2
    ) t group by 1 
)

select t1.booking_date as `日期`
       ,pmod(datediff(t1.booking_date, '2018-06-25'), 7)+1  as `星期`

       ,round(100 - `L2D-房态一致率_uv` , 2) `L2D-房态不一致率`
       ,round(100 - `L2D-房价一致率_uv` , 2) `L2D-房价不一致率`
       ,round(100 - `L2D-房态房价一致率_uv` , 2) `L2D-房态房价不一致率`
       
       ,round(100 - `D2B-房态一致率_uv` , 2) `D2B-房态不一致率`
       ,round(100 - `D2B-房价一致率_uv` , 2) `D2B-房价不一致率`
       ,round(100 - `D2B-房态房价一致率_uv` , 2) `D2B-房态房价不一致率`
       
       ,round(100 - `B2O-房态房价一致率_uv` , 2) `B2O-房态房价不一致率`

       ,round(`整体不一致uv` / dau * 100, 2) as `整体不一致率`
       ,round(`整体不一致uv_剔除房态` / dau * 100, 2) as `整体不一致率_剔除房态`
       ,round(`整体不一致uv_剔除D2B房态` / dau * 100, 2) as `整体不一致率_剔除D2B房态`
       ,round(`整体不一致uv_全部` / dau * 100, 2) as `整体不一致率_全部`

      
    --   ,`L2D-房价一致率`
    --   ,`L2D-房态一致率`
    --   ,`L2D-房态房价一致率`
    --   ,`L2D-房价一致率_uv`
    --   ,`L2D-房态一致率_uv`
    --   ,`L2D-房态房价一致率_uv`
    --   ,`D2B-房态一致率`
    --   ,`D2B-房价一致率`
    --   ,`D2B-房态房价一致率`
    --   ,`D2B-房态一致率_uv`
    --   ,`D2B-房价一致率_uv`
    --   ,`D2B-房态房价一致率_uv`
    --   ,`B2O-房态房价一致率`
    --   ,`B2O-房态房价一致率_uv`
    --   ,`整体不一致uv` 
    --   ,`整体不一致pv` 
    --   ,dau
    --   ,dau_pv
from (
    select booking_date
        ,round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`
        ,round((1-e/a)*100,2) as `L2D-房态一致率`
        ,round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`

        ,round((1-(b_uv-e_uv)/(a_uv-e_uv))*100,2) as `L2D-房价一致率_uv`
        ,round((1-e_uv/a_uv)*100,2) as `L2D-房态一致率_uv`
        ,round((1-(b_uv-e_uv)/(a_uv-e_uv))*(1-e_uv/a_uv)*100,2) as `L2D-房态房价一致率_uv`
    from (
        select booking_date
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e

            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then device_id end) as a_uv
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then device_id  else null end)  as b_uv
            ,count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then device_id  else null end)  as e_uv
        from l2d_price_10min
        group by 1
    )a
) t1 
left join  (
    select a.booking_date
          ,round((1-b/c)*100,2) as `D2B-房态一致率`
          ,round((1-a/(c-b))*100,2) as `D2B-房价一致率`
          ,round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`

          ,round((1-b_uv/c_uv)*100,2) as `D2B-房态一致率_uv`
          ,round((1-a_uv/(c_uv-b_uv))*100,2) as `D2B-房价一致率_uv`
          ,round((1-b_uv/c_uv)*(1-a_uv/(c_uv-b_uv))*100,2) as `D2B-房态房价一致率_uv`
    from(
        select  booking_date
               ,round(count(distinct case when ischange='true' and ret='true' then q_trace_id else null end)) as a
               ,count(distinct if((ret='false' or ret is null),q_trace_id,null)) as b
               ,count(distinct q_trace_id) as c

               ,round(count(distinct case when ischange='true' and ret='true' then orig_device_id else null end)) as a_uv
               ,count(distinct if((ret='false' or ret is null),orig_device_id,null)) as b_uv
               ,count(distinct orig_device_id) as c_uv
        from d2b_price_10min a
        group by 1
    ) a
) t2 on t1.booking_date=t2.booking_date 
left join (
    select booking_date
          ,round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100, 2) as `B2O-房态房价一致率`
          ,round((1-(total_submit_fail_uv-total_submit_coupon_uv)/total_submit_count_uv)*100, 2) as `B2O-房态房价一致率_uv`
    from(
        select  booking_date
                ,count(if((ret='false' or ret is null) ,true,null)) as total_submit_fail
                ,count(if((ret='false' or ret is null) and err_message='领券人与入住人不符' ,true,null)) as total_submit_coupon
                ,count(true) as total_submit_count

                ,count(distinct if((ret='false' or ret is null) ,orig_device_id,null)) as total_submit_fail_uv
                ,count(distinct if((ret='false' or ret is null) and err_message='领券人与入住人不符' ,orig_device_id,null)) as total_submit_coupon_uv
                ,count(distinct orig_device_id) as total_submit_count_uv
        from b2o_price_10min 
        group by 1
    ) y
) t3 on t1.booking_date=t3.booking_date
left join overall_price t4 on t1.booking_date=t4.booking_date
left join gj_dau t5 on t1.booking_date=t5.dt
left join overall_price_noft t6 on t1.booking_date=t6.booking_date
left join overall_price_noD2Bft t7 on t1.booking_date=t7.booking_date
left join overall_price_all t8 on t1.booking_date=t8.booking_date
order by `日期` desc
;




--0224 李娜修改 sql

select a.datas as `日期`,
pmod(datediff(a.datas, '2018-06-25'), 7)+1  as `星期`,
-- `S2D-房态一致率`,`S2D-房价一致率`,`S2D-房态房价一致率`,
`L2D-房态一致率`,`L2D-房价一致率`,`L2D-房态房价一致率`,
`D2B-房态一致率`,`D2B-房价一致率`,`D2B-房态房价一致率`,
`B2O-房态房价一致率`,
round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) AS `预订顺畅度`
from 
(select datas,
round((1-(b-e)/(a-e))*100,2) as `S2D-房价一致率`,
round((1-e/a)*100,2) as `S2D-房态一致率`,
round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `S2D-房态房价一致率`
from
(select a.dt as  datas,
count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then device_id end) as a,
count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price in('','0') or (low_price not in('','0') and listPrice!=low_price)) and is_hotel_full='false' then device_id  else null end)  as b,
count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and low_price  in ('','0') and is_hotel_full='false' then device_id  else null end)  as e
from
(select dt,log_id,
cast(regexp_extract(params,'&preListPrice=([^&]*)',1) as DECIMAL) as listPrice,
regexp_extract(params,'&orderPriceLog=([^&]*)',1) as low_price,
regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
action_entrance_map['fromforlog'] as is_list  ,device_id
from ihotel_default.dw_user_app_log_detail_visit_di_v1
where dt between date_sub(current_date,16) and date_sub(current_date,1)
and source='hotel'
 and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
and (action_entrance_map['fromforlog']=0 )
and (country_name!='中国' or province_name in('香港','澳门','台湾'))) a
group by 1) a) a

left join
(select datas,
round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
round((1-e/a)*100,2) as `L2D-房态一致率`,
round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`
from
(select a.dt as  datas,
count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then device_id end) as a,
count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then device_id  else null end)  as b,
count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then device_id  else null end)  as e
from
(select dt,log_id,
ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
-- 20240927 是否符合人数条件
get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
action_entrance_map['fromforlog'] as is_list  
,checkin_date
checkout_date,device_id
from ihotel_default.dw_user_app_log_detail_visit_di_v1
where dt between date_sub(current_date,16) and date_sub(current_date,1)
and source='hotel'
and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
and regexp_extract(params,'&fromList=([^&]*)',1)='true'
--and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
and (country_name!='中国' or province_name in('香港','澳门','台湾'))) a
where match_adult != 'false' or match_adult is null
group by 1) a) b
on a.datas=b.datas

left join
(select a.booking_date,
round((1-b/c)*100,2) as `D2B-房态一致率`,
round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`
from
(select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,
round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门','台湾')) then orig_device_id else null end)) as a,
count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')),orig_device_id,null)) as b,
count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')),orig_device_id,null)) as c
from
(select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,orig_device_id
from view_dw_user_app_booking_qta_di 
where  dt between '%(DATE_15)s' and '%(DATE)s'
and source='app_intl'
and platform in ('adr','ios')
and (province_name in ('香港','澳门','台湾') or country_name!='中国')
and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price'))a
group by 1) a) c
on a.datas=c.booking_date

left join
(select booking_date,
round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`
from
(select to_date(log_time) as booking_date,
count(if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),orig_device_id,null)) as total_submit_fail,
count(if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,orig_device_id,null)) as total_submit_coupon,
count(if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,orig_device_id,null)) as total_submit_count
from dw_user_app_submit_qta_di 
where  dt between '%(DATE_15)s' and '%(DATE)s' 
and source='app_intl'
and platform in ('adr','ios','AndroidPhone','iPhone')
and (country_name!='中国' or province_name in('香港','澳门','台湾'))
and err_code not in( '-98','784','785')
group by 1) y) d
on a.datas=d.booking_date
order by `日期` desc
;





---- UV维度十分钟内
---- UV维度十分钟内
with l_log as (--- L页
    select dt
          ,user_id
          ,log_id
          ,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
          ,qtrace_id,device_id
    from ihotel_default.dw_user_app_log_search_di_v1
    where dt >= date_sub(current_date, 15)
)
,d_price as (--- D页房态房价变价
    select a.dt as booking_date,user_id,listPrice,low_price
            ,substr(concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2),substr(action_time,9)),1,19) action_time
            ,qtrace_id,log_id,device_id,is_hotel_full
    from (
        select dt,log_id,
            ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
            ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
            regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
            get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
            get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
            action_entrance_map['fromforlog'] as is_list  
            ,user_id
            ,action_time,qtrace_id,device_id
        from ihotel_default.dw_user_app_log_detail_visit_di_v1
        where dt >= date_sub(current_date, 15)
            and source='hotel'
            and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
            and regexp_extract(params,'&fromList=([^&]*)',1)='true'
            --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
            and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
            and (country_name!='中国' or province_name in('香港','澳门','台湾'))
    ) a
    where match_adult != 'false' or match_adult is null 
)
,b_price as (---B页报价
    select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date
        ,user_id,log_time,q_trace_id,orig_device_id
        ,ischange,ret
    from(
        select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_sys,ischange,user_id,orig_device_id
        from view_dw_user_app_booking_qta_di 
        where  dt between '%(DATE_15)s' and '%(DATE)s'
            and source='app_intl'
            and platform in ('adr','ios')
            and (province_name in ('香港','澳门','台湾') or country_name!='中国')
            and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
    )a
)
,o_price as (--- o页
    select distinct to_date(log_time) as booking_date,user_id,log_time,orig_device_id,q_trace_id,ret,err_message
    from dw_user_app_submit_qta_di 
    where dt between '%(DATE_15)s' and '%(DATE)s'
        and source='app_intl'
        and platform in ('adr','ios','AndroidPhone','iPhone')
        and (country_name!='中国' or province_name in('香港','澳门','台湾'))
        and err_code not in( '-98','784','785')     
)
,l2d_price_10min as (--- L2D在10min内变价
    select * 
    from (
        select t2.booking_date,t2.listPrice,t2.low_price,t2.action_time,t2.qtrace_id,t2.user_id,t2.log_id,t2.device_id,is_hotel_full
            ,min(unix_timestamp(t2.action_time) - unix_timestamp(t1.action_time)) ts
        from l_log t1 
        join ( --- D页房态房价不一致
            select * 
            from d_price 
        ) t2 
        on t1.dt=t2.booking_date and t1.device_id=t2.device_id 
        where t2.action_time >= t1.action_time
        group by 1,2,3,4,5,6,7,8,9
    )t where ts <= 600
)
,d2b_price_10min as (--- D2B在10min内变价
    select booking_date,user_id,q_trace_id,orig_device_id,ischange,ret,ts,log_time
    from (
        select t2.booking_date,t2.user_id,t2.log_time,t2.q_trace_id,t2.orig_device_id,ischange,ret,min(unix_timestamp(t2.log_time) - unix_timestamp(t1.action_time)) ts
        from d_price t1 
        join (
            select booking_date ,user_id,log_time,q_trace_id,orig_device_id,ischange,ret
            from  b_price
        ) t2 on t1.booking_date=t2.booking_date and t1.device_id=t2.orig_device_id where t2.log_time >= t1.action_time
        group by 1,2,3,4,5,6,7
    ) t where ts <= 600
)
,b2o_price_10min as (--- B2O在10min内变价
    select *
    from (
        select t2.booking_date,t2.user_id,t2.log_time,t2.q_trace_id,t2.orig_device_id,t2.ret,t2.err_message,min(unix_timestamp(t2.log_time) - unix_timestamp(t1.log_time)) ts
        from b_price t1 
        join (
            select booking_date,user_id,log_time,q_trace_id,orig_device_id,ret,err_message
            from o_price
        ) t2 on t1.booking_date=t2.booking_date and t1.orig_device_id=t2.orig_device_id where t2.log_time >= t1.log_time
        group by 1,2,3,4,5,6,7
    )a where a.ts <= 600
)
,l_price_change_raw AS (--- L页变价明细
    SELECT log_date,
        CAST(get_json_object(value, '$.operTime') AS BIGINT) AS oper_time,
        orig_device_id,
        get_json_object(value, '$.ext.qTraceId') AS trace_id
    FROM default.dw_qav_ihotel_track_info_di
    WHERE dt >= '%(DATE_15)s'
    AND dt <= '%(DATE)s'
    AND key = 'ihotel/List/HotelCell/monitor/updatePricePageL'
    AND get_json_object(value, '$.ext.priceChange') = 'true'
    group by 1,2,3,4
)
,l_update AS (---- L页10分内变价明细
    select log_date,
        orig_device_id,
        trace_id
    from (
            SELECT
                log_date,
                orig_device_id,
                trace_id,
                substr(oper_time,1,10) oper_time,
                LEAD(substr(oper_time,1,10), 1) OVER ( PARTITION BY log_date,orig_device_id ORDER BY oper_time) AS prev_oper_time
            FROM l_price_change_raw
            -- where orig_device_id = '12249ecf9e9277dd'
        ) t where cast(prev_oper_time as int) - cast(oper_time as int) <= 600
    group by 1,2,3
)
,d_price_change_raw AS (--- D页变价明细
    SELECT log_date,
        CAST(get_json_object(value, '$.operTime') AS BIGINT) AS oper_time,
        orig_device_id,
        get_json_object(value, '$.ext.traceIdPrice') AS trace_id
    FROM default.dw_qav_ihotel_track_info_di
    WHERE dt >= '%(DATE_15)s'
    AND dt <= '%(DATE)s'
    AND key = 'ihotel/detail/priceList/monitor/updatePriceNoRoom'
    group by 1,2,3,4
)
,d_update AS (--- D页10min内变价明细
    select log_date,
        orig_device_id,
        trace_id
    from (
            SELECT
                log_date,
                orig_device_id,
                trace_id,
                substr(oper_time,1,10) oper_time,
                LEAD(substr(oper_time,1,10), 1) OVER ( PARTITION BY log_date,orig_device_id ORDER BY oper_time) AS prev_oper_time
            FROM d_price_change_raw
            -- where orig_device_id = '12249ecf9e9277dd'
        ) t where cast(prev_oper_time as int) - cast(oper_time as int) <= 600
    group by 1,2,3
)
,l2d_res as (
    select booking_date,device_id
    from l2d_price_10min
    where  
        ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
)
,d2b_res as (
    select booking_date,orig_device_id device_id
    from d2b_price_10min
    where (
            (ischange='true' and ret='true') ---房价不一致
        )
)
,b2o_res as (
    select booking_date,orig_device_id device_id
    from b2o_price_10min
    where (ret='false' or ret is null)
)

,overall_price_all as (--- 整体变价不一致
    select booking_date
          ,count(distinct device_id) `整体不一致uv_全部` 
          ,count(device_id) `整体不一致pv_全部` 
    from (
        ---- 页面间
        select booking_date,device_id
        from l2d_price_10min
        where  
            ((low_price not in('','0') and (listPrice - low_price) NOT BETWEEN 0 AND 1))   --房价不一致
        union all
        select booking_date,orig_device_id device_id
        from d2b_price_10min
        where (
                (ischange='true' and ret='true') ---房价不一致
            )
        union all
        select booking_date,orig_device_id device_id
        from b2o_price_10min
        where (ret='false' or ret is null)
        ----- 页面内变价
        ----- L页变价
        union all
        select log_date booking_date,orig_device_id  device_id
        from l_update
        ----- D页变价
        union all
        select log_date booking_date,orig_device_id  device_id
        from d_update
    )t group by 1
)

,gj_dau as (
    select dt
          ,sum(uv) dau 
          ,sum(pv) dau_pv
    from (
        select  dt 
                ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                            when e.area in ('欧洲','亚太','美洲') then e.area
                            else '其他' end as mdd
                ,count(distinct a.user_id) uv
                ,count(a.user_id) pv
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
            and dt <= date_sub(current_date, 1)
            and business_type = 'hotel'
            and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
            and (search_pv + detail_pv + booking_pv + order_pv) > 0
            and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
            and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
        group by 1,2
    ) t group by 1 
)

select t1.dt as `日期`
       ,pmod(datediff(t1.dt, '2018-06-25'), 7)+1  as `星期`
       ,dau
       ,`L2D房价不一致UV`
       ,`D2B房价不一致UV`
       ,`B2O房态房价不一致UV`
       ,`L页不一致UV`
       ,`D页不一致UV`
       ,`整体不一致uv_全部`

from gj_dau t1 
left join  (
    select booking_date
           ,count(distinct device_id) `L2D房价不一致UV`
    from l2d_res
    group by 1
) t2 on t1.dt=t2.booking_date 
left join  (
    select booking_date
           ,count(distinct device_id) `D2B房价不一致UV`
    from d2b_res
    group by 1
) t3 on t1.dt=t3.booking_date
left join  (
    select booking_date
           ,count(distinct device_id) `B2O房态房价不一致UV`
    from b2o_res
    group by 1
) t4 on t1.dt=t4.booking_date
left join (
    select log_date booking_date,count(distinct orig_device_id)  `L页不一致UV`
    from l_update
    group by 1
) t5 on t1.dt=t5.booking_date
left join (
    select log_date booking_date,count(distinct orig_device_id)  `D页不一致UV`
    from d_update
    group by 1
) t6 on t1.dt=t6.booking_date
left join overall_price_all t8 on t1.dt=t8.booking_date
order by `日期` desc
;