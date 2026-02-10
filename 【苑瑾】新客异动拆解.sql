with user_type as (--- 用于判定Q新老客
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
,c_user_type as (--- 用于判定c新老客
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
,uv as (----Q流量
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,c_uv as (--- C流量
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid user_id
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt >= date_sub(current_date, 15) and dt <= date_sub(current_date, 1)
    group by 1,2,3,4
)
,q_channel_uv as (--- 近8日Q分渠道明细
    select channel,user_name,dt,row_number() over(partition by user_name order by dt desc) rn
    from ihotel_default.dwd_flow_ug_channel_di
    where dt >= date_sub(current_date, 8) and dt <= date_sub(current_date, 1)
    group by 1,2,3
)
,re7d_data as (--- T-8日的七日留存
    select t1.mdd
            ,t1.user_type
            ,t1.channel
            ,t1.uv_7d
            ,t2.uv_7d_c
            ,re_uv_7d
            ,t2.re_uv_7d_c
    from (--- Q流量T-8日的7日留存
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,if(grouping(channel)=1,'ALL', channel) as  channel
                ,count(t1.user_name) uv_7d
                ,count(case when t3.user_name is not null then t1.user_name end) re_uv_7d
        from (-- T-8日流量
            select mdd,user_type,user_name 
            from  uv  
            where dt = date_sub(current_date, 8)
            group by 1,2,3
        )t1
        left join (--- T-8日流量渠道
            select channel,user_name 
            from q_channel_uv 
            where dt = date_sub(current_date, 8)
            group by 1,2
        ) t2 on t1.user_name=t2.user_name
        left join  (  --- T-1~T-7流量
            select user_name
            from uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 1) 
            group by 1
        ) t3 on t1.user_name=t3.user_name
        group by cube(t1.mdd,t1.user_type,t2.channel)
    ) t1 
    left join (--- C流量T-8日的7日留存
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,count(t1.user_id) uv_7d_c
                ,count(case when t2.user_id is not null then t1.user_id end) re_uv_7d_c
        from (
            select mdd,user_type,user_id
            from c_uv
            where dt = date_sub(current_date, 8)
        )t1
        left join  (  --- T-1~T-7流量
                select user_id
                from c_uv 
                where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 1) 
                group by 1
        ) t2 on t1.user_id=t2.user_id
        group by cube(t1.mdd,t1.user_type)
    )t2 on t1.mdd=t2.mdd and t1.user_type=t2.user_type
)
,act_frequency_data as (--- 人均活跃频次、7日内首访、次留
    select t1.mdd
            ,t1.user_type
            ,t1.channel
            ,t1.uv
            ,t2.uv_c
            ,first_act_uv_7d
            ,re_uv_1d

            ,act_days
            ,mact_days
            ,pact_days
            ,act_cnt_1
            ,act_cnt_2
            ,act_cnt_3
            ,act_cnt_4
            ,act_cnt_5
            ,act_cnt_6
            ,act_cnt_7


            ,first_act_uv_7d_c
            ,re_uv_1d_c
            ,act_days_c
            ,mact_days_c
            ,pact_days_c
            ,act_cnt_1_c
            ,act_cnt_2_c
            ,act_cnt_3_c
            ,act_cnt_4_c
            ,act_cnt_5_c
            ,act_cnt_6_c
            ,act_cnt_7_c
            
    from (--- Q流量
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,if(grouping(channel)=1,'ALL', channel) as  channel
                ,count(t1.user_name) uv
                ,count(case when t3.user_name is null then t1.user_name end) first_act_uv_7d
                ,count(case when t4.user_name is not null then t1.user_name end) re_uv_1d
                ,sum(act_days) act_days
                ,max(act_days) mact_days
                ,sum(act_days) / count(t1.user_name) pact_days
                ,count(case when act_days = 1 then t1.user_name end) act_cnt_1
                ,count(case when act_days = 2 then t1.user_name end) act_cnt_2
                ,count(case when act_days = 3 then t1.user_name end) act_cnt_3
                ,count(case when act_days = 4 then t1.user_name end) act_cnt_4
                ,count(case when act_days = 5 then t1.user_name end) act_cnt_5
                ,count(case when act_days = 6 then t1.user_name end) act_cnt_6
                ,count(case when act_days = 7 then t1.user_name end) act_cnt_7
        from (--- 当日活跃用户
            select t1.mdd,t1.user_type,t2.channel,t1.user_name
            from (select mdd,user_type,user_name from uv where dt = date_sub(current_date, 1) group by 1,2,3) t1
            left join (select channel,user_name from q_channel_uv where dt = date_sub(current_date, 1) group by 1,2) t2 
            on t1.user_name=t2.user_name
        )t1
        left join (--- 近7日用户活跃天数
            select user_name,count(distinct dt) act_days 
            from uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 1) 
            group by 1
        )t2 on t1.user_name=t2.user_name
        left join (--- 近7日首访不含当日
            select user_name
            from uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 2) 
            group by 1
        )t3 on t1.user_name=t3.user_name
        left join (--- 次日留存
            select user_name
            from uv 
            where dt = date_sub(current_date, 2) 
            group by 1
        )t4 on t1.user_name=t4.user_name
        group by cube(t1.mdd,t1.user_type,channel)
    ) t1 
    left join (--- C流量
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,count(t1.user_id) uv_c
                ,sum(act_days) act_days_c
                ,sum(act_days) / count(t1.user_id) pact_days_c
                ,max(act_days) mact_days_c
                ,count(case when t3.user_id is null then t1.user_id end) first_act_uv_7d_c
                ,count(case when t4.user_id is not null then t1.user_id end) re_uv_1d_c

                ,count(case when act_days = 1 then t1.user_id end) act_cnt_1_c
                ,count(case when act_days = 2 then t1.user_id end) act_cnt_2_c
                ,count(case when act_days = 3 then t1.user_id end) act_cnt_3_c
                ,count(case when act_days = 4 then t1.user_id end) act_cnt_4_c
                ,count(case when act_days = 5 then t1.user_id end) act_cnt_5_c
                ,count(case when act_days = 6 then t1.user_id end) act_cnt_6_c
                ,count(case when act_days = 7 then t1.user_id end) act_cnt_7_c
        from (--- 当日活跃用户
            select mdd,user_type,t1.user_id
            from c_uv t1
            where t1.dt = date_sub(current_date, 1)
        )t1
        left join (--- 近7日用户活跃天数
            select user_id,count(distinct dt) act_days 
            from c_uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 1) 
            group by 1
        )t2 on t1.user_id=t2.user_id
        left join (--- 近7日首访不含当日
            select user_id
            from c_uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 2)  
            group by 1
        )t3 on t1.user_id=t3.user_id
        left join (--- 次日留存
            select user_id
            from c_uv 
            where dt = date_sub(current_date, 2) 
            group by 1
        )t4 on t1.user_id=t4.user_id
        group by cube(t1.mdd,t1.user_type)
    )t2 on t1.mdd=t2.mdd and t1.user_type=t2.user_type
)
,wau_data as (--- WAU 数据
    select t1.mdd
            ,t1.user_type
            ,t1.channel
            ,t1.wau
            ,t2.wau_c
    from (--- Q流量T-8日的7日留存
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,if(grouping(channel)=1,'ALL', channel) as  channel
                ,count(distinct t1.user_name) wau
        from (--- Q近一周流量
            select mdd,user_type,user_name
            from uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 1) 
            group by 1,2,3
        )t1
        left join (--- 取用户最新一天的渠道
            select user_name,channel 
            from q_channel_uv
            where rn = 1
            group by 1,2
        ) t2 on t1.user_name=t2.user_name
        group by cube(t1.mdd,t1.user_type,t2.channel)
    ) t1 
    left join (--- C流量T-8日的7日留存
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,count(t1.user_id) wau_c
        from (--- C近一周流量
            select mdd,user_type,user_id
            from c_uv 
            where dt >= date_sub(current_date, 7) and dt <= date_sub(current_date, 1) 
            group by 1,2,3
        )t1
        group by cube(t1.mdd,t1.user_type)
    )t2 on t1.mdd=t2.mdd and t1.user_type=t2.user_type
)



select t1.mdd         --- 目的地
       ,t1.user_type  --- 用户类型
       ,t1.channel    --- Q渠道
       ,uv            --- QUV
       ,wau           --- QWAU username去重
       ,uv_7d         --- QT-8日活跃用户
       ,first_act_uv_7d ---Q近7日首访用户
       ,re_uv_1d      ---- QT-2日次留用户
       ,re_uv_7d      ---- QT-8日7日内留用户
       ,act_days      ---- Q近7日用户累计活跃天数
       ,mact_days     ---- Q近7日最大连续活跃天数 7天
       ,pact_days     ---- Q人均活跃天数
       ,act_cnt_1     ---- Q近7日活跃1天用户
       ,act_cnt_2     ---- Q近7日活跃2天用户
       ,act_cnt_3
       ,act_cnt_4
       ,act_cnt_5
       ,act_cnt_6
       ,act_cnt_7  
        
       ,uv_c     --- CUV
       ,wau_c    --- CWAU user_id去重
       ,uv_7d_c  --- CT-8日活跃用户
       ,first_act_uv_7d_c
       ,re_uv_1d_c
       ,re_uv_7d_c
       ,act_days_c
       ,mact_days_c
       ,pact_days_c
       ,act_cnt_1_c
       ,act_cnt_2_c
       ,act_cnt_3_c
       ,act_cnt_4_c
       ,act_cnt_5_c
       ,act_cnt_6_c
       ,act_cnt_7_c

from act_frequency_data t1
left join re7d_data t2
on t1.mdd=t2.mdd and t1.user_type=t2.user_type and t1.channel=t2.channel
left join wau_data t3
on t1.mdd=t3.mdd and t1.user_type=t3.user_type and t1.channel=t3.channel
order by case when mdd = '香港'  then 1
           when mdd = '澳门'  then 2
           when mdd = '日本'  then 3
           when mdd = '韩国'  then 4
           when mdd = '泰国'  then 5
           when mdd = '马来西亚'  then 6
           when mdd = '新加坡'  then 7
           when mdd = '美国'  then 8
           when mdd = '印度尼西亚'  then 9
           when mdd = '俄罗斯'  then 10
           when mdd = '欧洲'  then 11
           when mdd = '亚太'  then 12
           when mdd = '美洲'  then 13
           when mdd = '其他'  then 14
           when mdd = 'ALL'  then 0
      end asc
      ,case when user_type = 'ALL' then 1 
            when user_type = '新客' then 2 
            when  user_type = '老客' then 3 
      end asc
      ,case when channel = 'ALL' then 1 
            when channel = '机酒交叉' then 2 
            when  channel = '小红书' then 3 
            when  channel = '内容交叉' then 3 
            when  channel = '营销活动' then 3 
            when  channel = '国内交叉' then 3 
            when  channel = '自然流量' then 3 
      end  asc
;


    select '${zdt.addDay(-1).format("yyyy-MM-dd")}'  t1.dt
        ,if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
        ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
        ,sum(act_days) act_days
        ,count(user_id) uv
        ,sum(act_days) / count(user_id) per_act_days
    from (
        select user_id,mdd,user_type
            ,count(distinct t1.dt) act_days
        from  uv t1 where dt >= '${zdt.addDay(-7).format("yyyy-MM-dd")}' and dt <= '${zdt.addDay(-1).format("yyyy-MM-dd")}' 
        group by 1,2,3
    )t1 
    group by cube(t1.mdd,t1.user_type)




with user_type as (--- 用于判定Q新老客
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
,c_user_type as (--- 用于判定c新老客
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
,uv as (----分日去重活跃用户
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
    from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    left join user_type b on a.user_id = b.user_id 
    where dt >= date_sub(current_date, 30)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and (search_pv + detail_pv + booking_pv + order_pv) > 0
        and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
        and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,c_uv as (--- C 流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid user_id
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>= date_sub(current_date, 30) and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,q_channel_uv as (--- Q分渠道明细
    select channel,user_name
    from ihotel_default.dwd_flow_ug_channel_di
    where dt = date_sub(current_date, 1)
    group by 1,2
)
,q_channel_uv_8d as (--- Q分渠道明细8天前
    select channel,user_name
    from ihotel_default.dwd_flow_ug_channel_di
    where dt = date_sub(current_date, 8)
    group by 1,2
)


   select t1.mdd
            ,t1.user_type
            ,t1.channel
            ,t1.uv
            ,t2.uv_c
            ,t2.uv_c / t1.uv  uv_qc
            ,act_days
            ,pact_days
            ,mact_days
            ,first_act_uv_7d
            ,uv_1d
            ,act_days_c
            ,pact_days_c
            ,mact_days_c
            ,first_act_uv_7d_c
            ,uv_1d_c
    from (--- Q流量
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,if(grouping(channel)=1,'ALL', channel) as  channel
                ,count(t1.user_name) uv
                ,sum(act_days) act_days
                ,sum(act_days) / count(t1.user_name) pact_days
                ,max(act_days) mact_days
                ,count(case when t3.user_name is null then t1.user_name end) first_act_uv_7d
                ,count(case when t4.user_name is not null then t1.user_name end) uv_1d
        from (--- 当日活跃用户
            select mdd,user_type,t2.channel,t1.user_name
            from uv t1
            left join q_channel_uv t2 on t1.user_name=t2.user_name
            where t1.dt = date_sub(current_date, 1)
        )t1
        left join (--- 近7日用户活跃天数
            select user_name,count(distinct dt) act_days 
            from uv 
            where dt between date_sub(current_date, 8) and date_sub(current_date, 1) 
            group by 1
        )t2 on t1.user_name=t2.user_name
        left join (--- 近7日首访不含当日
            select user_name
            from uv 
            where dt between date_sub(current_date, 8) and date_sub(current_date, 2) 
            group by 1
        )t3 on t1.user_name=t3.user_name
        left join (--- 次日留存
            select user_name
            from uv 
            where dt = date_sub(current_date, 2) 
            group by 1
        )t4 on t1.user_name=t4.user_name
        group by cube(t1.mdd,t1.user_type,channel)
    ) t1 
    left join (--- C流量
        select  if(grouping(t1.mdd)=1,'ALL', t1.mdd) as  mdd
                ,if(grouping(t1.user_type)=1,'ALL', t1.user_type) as  user_type
                ,count(t1.user_id) uv_c
                ,sum(act_days) act_days_c
                ,sum(act_days) / count(t1.user_id) pact_days_c
                ,max(act_days) mact_days_c
                ,count(case when t3.user_id is null then t1.user_id end) first_act_uv_7d_c
                ,count(case when t4.user_id is not null then t1.user_id end) uv_1d_c
        from (--- 当日活跃用户
            select mdd,user_type,t1.user_id
            from c_uv t1
            where t1.dt = date_sub(current_date, 1)
        )t1
        left join (--- 近7日用户活跃天数
            select user_id,count(distinct dt) act_days 
            from c_uv 
            where dt between date_sub(current_date, 8) and date_sub(current_date, 1) 
            group by 1
        )t2 on t1.user_id=t2.user_id
        left join (--- 近7日首访不含当日
            select user_id
            from c_uv 
            where dt between date_sub(current_date, 8) and date_sub(current_date, 2) 
            group by 1
        )t3 on t1.user_id=t3.user_id
        left join (--- 次日留存
            select user_id
            from c_uv 
            where dt = date_sub(current_date, 2) 
            group by 1
        )t4 on t1.user_id=t4.user_id
        group by cube(t1.mdd,t1.user_type)
    )t2 on t1.mdd=t2.mdd and t1.user_type=t2.user_type
    
    order by mdd,user_type,channel








CREATE TABLE IF NOT EXISTS ads_flow_activity_mult_dim_qc_di (
    -- 基础维度
    mdd                 STRING          COMMENT '目的地',
    user_type           STRING          COMMENT '用户类型',
    channel             STRING          COMMENT '渠道',

    -- Q侧指标 (Q Metrics)
    uv                  BIGINT          COMMENT 'Q侧_UV',
    wau                 BIGINT          COMMENT 'Q侧_WAU(username去重)',
    uv_7d               BIGINT          COMMENT 'Q侧_T-8日活跃用户',
    first_act_uv_7d     BIGINT          COMMENT 'Q侧_近7日首访用户',
    re_uv_1d            BIGINT          COMMENT 'Q侧_T-2日次留用户数',
    re_uv_7d            BIGINT          COMMENT 'Q侧_T-8日7日留存用户数',
    act_days            BIGINT          COMMENT 'Q侧_近7日用户累计活跃总天数',
    mact_days           BIGINT          COMMENT 'Q侧_近7日最大连续活跃天数',
    pact_days           DECIMAL(38,4)   COMMENT 'Q侧_人均活跃天数',
    
    -- Q侧活跃频次分布
    act_cnt_1           BIGINT          COMMENT 'Q侧_近7日活跃1天用户数',
    act_cnt_2           BIGINT          COMMENT 'Q侧_近7日活跃2天用户数',
    act_cnt_3           BIGINT          COMMENT 'Q侧_近7日活跃3天用户数',
    act_cnt_4           BIGINT          COMMENT 'Q侧_近7日活跃4天用户数',
    act_cnt_5           BIGINT          COMMENT 'Q侧_近7日活跃5天用户数',
    act_cnt_6           BIGINT          COMMENT 'Q侧_近7日活跃6天用户数',
    act_cnt_7           BIGINT          COMMENT 'Q侧_近7日活跃7天用户数',

    -- C侧指标 (C Metrics)
    uv_c                BIGINT          COMMENT 'C侧_UV',
    wau_c               BIGINT          COMMENT 'C侧_WAU(userid去重)',
    uv_7d_c             BIGINT          COMMENT 'C侧_T-8日活跃用户',
    first_act_uv_7d_c   BIGINT          COMMENT 'C侧_近7日首访用户',
    re_uv_1d_c          BIGINT          COMMENT 'C侧_T-2日次留用户数',
    re_uv_7d_c          BIGINT          COMMENT 'C侧_T-8日7日留存用户数',
    act_days_c          BIGINT          COMMENT 'C侧_近7日用户累计活跃总天数',
    mact_days_c         BIGINT          COMMENT 'C侧_近7日最大连续活跃天数',
    pact_days_c         DECIMAL(38,4)   COMMENT 'C侧_人均活跃天数',
    
    -- C侧活跃频次分布
    act_cnt_1_c         BIGINT          COMMENT 'C侧_近7日活跃1天用户数',
    act_cnt_2_c         BIGINT          COMMENT 'C侧_近7日活跃2天用户数',
    act_cnt_3_c         BIGINT          COMMENT 'C侧_近7日活跃3天用户数',
    act_cnt_4_c         BIGINT          COMMENT 'C侧_近7日活跃4天用户数',
    act_cnt_5_c         BIGINT          COMMENT 'C侧_近7日活跃5天用户数',
    act_cnt_6_c         BIGINT          COMMENT 'C侧_近7日活跃6天用户数',
    act_cnt_7_c         BIGINT          COMMENT 'C侧_近7日活跃7天用户数'
)
COMMENT '国酒流量活跃频次与留存QC对比监控日报（多维度）'
PARTITIONED BY (dt STRING COMMENT '统计日期, yyyy-MM-dd')
STORED AS ORC;




with data_info  as (
    select 
        dt
        ,mdd
        ,user_type
        ,channel
        ,uv
        ,wau
        ,uv_7d
        ,first_act_uv_7d
        ,re_uv_1d
        ,re_uv_7d
        ,act_days
        ,mact_days
        ,pact_days
        ,act_cnt_1
        ,act_cnt_2
        ,act_cnt_3
        ,act_cnt_4
        ,act_cnt_5
        ,act_cnt_6
        ,act_cnt_7
        ,uv_c
        ,wau_c
        ,uv_7d_c
        ,first_act_uv_7d_c
        ,re_uv_1d_c
        ,re_uv_7d_c
        ,act_days_c
        ,mact_days_c
        ,pact_days_c
        ,act_cnt_1_c
        ,act_cnt_2_c
        ,act_cnt_3_c
        ,act_cnt_4_c
        ,act_cnt_5_c
        ,act_cnt_6_c
        ,act_cnt_7_c
    from ihotel_default.ads_flow_activity_mult_dim_qc_di
)

select t1.dt
        ,t1.mdd
        ,t1.user_type
        ,t1.channel
        ,uv  `Q_UV`
        ,uv_c `C_UV`
        ,uv / uv_c   `流量QC`
        ,first_act_uv_7d / uv `Q_近7天首访占比`
        ,first_act_uv_7d_c / uv_c `C_近7天首访占比`
        ,pact_days `Q_人均访问频次`
        ,pact_days_c `C_人均访问频次`
        ,t2.re_uv_1d / uv `Q_次留`
        ,t3.re_uv_7d / uv `Q_7留`

        ,t2.re_uv_1d_c  / uv_c  `C_次留`
        ,t3.re_uv_7d_c  / uv_c  `C_7留`
from data_info t1
left join ( --- 次留
    select dt,mdd,user_type,channel
          ,re_uv_1d
          ,re_uv_1d_c
    from data_info
) t2 on datediff(t2.dt,t1.dt) = 1 and t1.mdd=t2.mdd and t1.user_type=t2.user_type and t1.channel=t2.channel
left join ( --- 7留
    select dt,mdd,user_type,channel
          ,re_uv_7d
          ,re_uv_7d_c
    from data_info
) t3 on datediff(t3.dt,t1.dt) = 7 and t1.mdd=t3.mdd and t1.user_type=t3.user_type and t1.channel=t3.channel
order by 1 desc
;