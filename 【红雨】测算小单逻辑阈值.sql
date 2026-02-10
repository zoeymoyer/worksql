select 
    `日期`
    ,`小时`
    ,supplier_id
    ,hotel_name as `酒店`
    ,hotel_seq
    ,sum(`供应商进订次数（总）`) as `进订次数总数`
    ,sum(`供应商进订失败次数（总）`) as `进订失败次数`
    ,sum(`供应商变价次数（总）`) as `进订变价次数`
    ,sum(`供应商进订失败次数（总）` + `供应商变价次数（总）`) as `进订失败+变价总量`
    ,concat(round(sum((`供应商进订失败次数（总）` + `供应商变价次数（总）`)) / sum(`供应商进订次数（总）`) * 100, 2), '%') as `进订失败率`
from ( 
    select 
        t1.dt as `日期`,
        t1.hour as `小时`,
        t2.supplier_id,t2.wrapper_name,t2.hotel_name,t2.hotel_seq,
        t1.preSubmitTotal as `进订次数（总）`,
        t1.totalFail as `进订失败次数（总）`,
        t1.totalPriceChange as `变价次数（总）`,
        t2.supplierTotalPreSubmit as `供应商进订次数（总）`,
        t2.supplierTotalFail as `供应商进订失败次数（总）`,
        t2.supplierTotalPriceChange as `供应商变价次数（总）`,
        round((t2.supplierTotalFail/t1.totalFail)*100,2) as `失败占比`,
        round((t2.supplierTotalFail/t1.preSubmitTotal)*100,2) as `失败贡献`,
        round((t2.supplierTotalPriceChange/t1.totalPriceChange)*100,2) as `变价占比`,
        round((t2.supplierTotalPriceChange/t1.preSubmitTotal)*100,2) as `变价贡献`
    from (
        select 
            dt,
            hour(log_time) as hour,
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')), q_trace_id, null)) as totalFail,
            round(count(distinct case when ischange='true' and (ret!='false' and ret is not null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) then q_trace_id else null end)) as totalPriceChange,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')), q_trace_id, null)) as preSubmitTotal
        from default.view_dw_user_app_booking_qta_di
        where  
            dt = '20250723'
            and source='app_intl'
            and platform in ('adr','ios')
            and (province_name in ('香港','澳门','台湾') or country_name!='中国')
            and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
        group by dt, hour(log_time)  
    ) t1
    left join (
        select 
            dt,
            hour(log_time) as hour,
            supplier_id,
            wrapper_name, -- 供应商名称
            b.hotel_name, 
            a.hotel_seq,  -- 添加hotel_seq字段
            count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')), q_trace_id, null)) as supplierTotalFail,
            round(count(distinct case when ischange='true' and (ret!='false' and ret is not null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) then q_trace_id else null end)) as supplierTotalPriceChange,
            count(distinct if((country_name!='中国' or province_name in('香港','澳门','台湾')), q_trace_id, null)) as supplierTotalPreSubmit
        from default.view_dw_user_app_booking_qta_di a
        left join (select hotel_seq,hotel_name from default.dim_hotel_info_intl_v3 where dt='%(DATE)s') b on a.hotel_seq = b.hotel_seq
        where  
            dt = '20250723'
            and source='app_intl'
            and platform in ('adr','ios')
            and (province_name in ('香港','澳门','台湾') or country_name!='中国')
            and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price')
        group by 1,2,3,4,5,6
    ) t2 on t1.dt = t2.dt and t1.hour = t2.hour  
) total
group by `日期`, `小时`, supplier_id,hotel_name,hotel_seq
order by `日期`, `小时`, supplier_id;
