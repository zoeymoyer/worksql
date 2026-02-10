select dt,
       key, 
       count(distinct user_name) as uv 
       ,count(user_name) pv
from ihotel_default.dw_qav_hotel_track_info_di
where dt >= '20251024'
    and key in ('ihotel/Home/Page/show/pageShow'              --- H页曝光
                ,'hotel/home/searchCard/click/searchClick'     --- 搜索按钮点击 
                ,'hotel/home/bottomEntrance/show/entranceShow' --- 金刚位曝光 bottomDesc
                ,'hotel/home/bottomEntrance/click/entrance'    --- 金刚位点击 bottomDesc
                ,'hotel/global/home/recommendHotelInter/show'  --- 精选推荐曝光
                ,'hotel/global/home/recommendHotelInter/click' --- 精选推荐点击
                ,'hotel/global/home/recommendCityInter/show'   --- 热门目的地曝光
                ,'hotel/global/home/recommendCityInter/click'  --- 热门目的地点击
                ,'hotel/global/travelList/showInfo'            --- 达人精选
                ,'hotel/global/travelList/click'               --- 达人精选点击
                ,'hotel/home/searchCard/show/quickFilter'      --- 快搜词曝光
                ,'hotel/home/searchCard/click/quickFilter'     --- 快搜词点击
        )
group by 1,2
;

--- 金刚位曝光点击
select dt
       ,key
       ,count(distinct user_name) as uv 
       ,count(user_name) pv
from default.dw_qav_hotel_track_info_di
where dt >= '20251024'
    and key in ('hotel/home/bottomEntrance/show/entranceShow' --- 金刚位曝光 bottomDesc
                ,'hotel/home/bottomEntrance/click/entrance'    --- 金刚位点击 bottomDesc   
    )
    and get_json_object(value,'$.ext.isForeignCity') = 'true'
group by 1,2
;



--- H页流量表
--- default.dw_qav_ihotel_track_info_di 或者
--- ihotel_default.dw_qav_hotel_track_info_di
--- H页分页
with h_module_exp as (--- H页分模块曝光
    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) dt
        ,case 
            when key = 'hotel/global/home/recommendHotelInter/show' then '精选推荐'
            when key = 'hotel/global/travelList/showInfo' then '达人精选'
            when key = 'hotel/global/home/recommendCityInter/show' then '热门目的地'
        end key_type
        ,count(distinct user_name) exp_uv
        ,count(1) exp_pv
    from ihotel_default.dw_qav_hotel_track_info_di
    where dt >= '%(DATE_14)s' and dt <= '%(DATE)s'
        and key in ('hotel/global/home/recommendHotelInter/show'   --- 精选推荐曝光
                    ,'hotel/global/travelList/showInfo'            --- 达人精选
                    ,'hotel/global/home/recommendCityInter/show'   --- 热门目的地曝光
        )
    group by 1,2
)
,h_module_clk as (--- 模块点击  取末次点击时间
    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) dt
            ,case 
                when key = 'hotel/global/home/recommendHotelInter/click' then '精选推荐'
                when key = 'hotel/global/travelList/click' then '达人精选'
                when key = 'hotel/global/home/recommendCityInter/click' then '热门目的地'
            end key_type
            ,user_name
            ,max(concat(log_date,' ',log_time)) log_time
            ,min(concat(log_date,' ',log_time)) log_time_min
            ,count(1) clk_pv
    from ihotel_default.dw_qav_hotel_track_info_di
    where dt >= '%(DATE_14)s' and dt <= '%(DATE)s'
        and key in ('hotel/global/home/recommendHotelInter/click'    --- 精选推荐点击 
                    ,'hotel/global/travelList/click'               --- 达人精选点击 
                    ,'hotel/global/home/recommendCityInter/click'  --- 热门目的地点击 
        )
    group by 1,2,3
)
,q_order_app as (  ---- q订单
    select order_date
        ,a.user_name
        ,min(order_time)   order_time
        ,count(distinct order_no) order_no
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435' 
    group by 1,2
)
,q_ldbo_uv as (--- q流量 ldbo
    select dt,user_name,concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2), ' ',substr(action_time,10,8)) action_time
    from (
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    ) group by 1,2,3
)

 
select t1.dt,t1.key_type,exp_uv,exp_pv
        ,t2.clk_uv
        ,t2.clk_pv
        ,t2.order_uv
        ,t2.order_no
        ,t3.act_uv
from h_module_exp t1 
left join (---- 订单  最后一次点击时间之后24h有生单
    select t1.dt
           ,t1.key_type
           ,count(distinct t1.user_name) clk_uv
           ,sum(clk_pv) clk_pv
           ,count(distinct case when order_no is not null then t1.user_name end) order_uv
           ,sum(order_no) order_no
    from (
        select t1.dt,t1.key_type,t1.user_name,t1.log_time,clk_pv,sum(order_no) order_no
        from h_module_clk t1 
        left join q_order_app t2 on t1.user_name=t2.user_name and unix_timestamp(order_time) - unix_timestamp(log_time) between 1 and 86400 and t2.order_date >= t1.dt  --- 限定生单时间在最后一次点击时间后24h之内
        group by 1,2,3,4,5
    )t1
    group by 1,2
) t2 on t1.dt=t2.dt and t1.key_type=t2.key_type
left join (--- 流量 首次点击或者最后一次点击之后当天有进ldbo主流程
    select t1.dt
            ,t1.key_type
            ,count(distinct t1.user_name) clk_uv
            ,count(distinct t2.user_name) act_uv
    from h_module_clk t1  
    left join q_ldbo_uv t2 on t1.user_name=t2.user_name and t1.dt=t2.dt and (unix_timestamp(t2.action_time) > unix_timestamp(t1.log_time) or unix_timestamp(t2.action_time) > unix_timestamp(t1.log_time_min))
    group by 1,2
) t3 on t1.dt=t3.dt and t1.key_type=t3.key_type
order by 1 desc, 2 asc
;


---- H页整体
with h_exp as ( --- H页曝光
    select concat(substr(dt, 1, 4), '-', substr(dt, 5, 2), '-', substr(dt, 7, 2)) dt
          ,user_name
          ,max(concat(log_date,' ',log_time)) log_time
          ,min(concat(log_date,' ',log_time)) log_time_min
          ,count(1) exp_pv
    from default.dw_qav_ihotel_track_info_di
    where dt >= '%(DATE_14)s' and dt <= '%(DATE)s'
    and key = 'ihotel/home/preload/monitor/homePreFetch'
    group by 1,2
)
,q_order_app as (  ---- q订单
    select order_date
        ,a.user_name
        ,min(order_time)   order_time
        ,count(distinct order_no) order_no
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 15) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435' 
    group by 1,2
)
,q_ldbo_uv as (--- q流量 ldbo
    select dt,user_name,concat(substr(action_time,1,4),'-',substr(action_time,5,2),'-',substr(action_time,7,2), ' ',substr(action_time,10,8)) action_time
    from (
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_search_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_detail_visit_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_booking_di_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        union
        select distinct dt 
                ,a.user_name
                ,action_time
        from ihotel_default.dw_user_app_log_order_submit_hi_v1 a
        left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
        where dt >= date_sub(current_date, 15)
        and dt <= date_sub(current_date, 1)
        and business_type = 'hotel'
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
    ) group by 1,2,3
)



select t1.dt,t1.exp_uv,t1.exp_pv,t1.order_uv,order_no,act_uv
from (---- 订单  最后一次点击时间之后24h有生单
    select t1.dt
           ,count(distinct t1.user_name) exp_uv
           ,sum(t1.exp_pv) exp_pv
           ,count(distinct case when t1.order_no is not null then t1.user_name end) order_uv
           ,sum(t1.order_no) order_no
    from (
        select t1.dt,t1.user_name,exp_pv,sum(order_no) order_no
        from h_exp t1 
        left join q_order_app t2 on t1.user_name=t2.user_name and unix_timestamp(order_time) - unix_timestamp(log_time) between 1 and 86400 and t2.order_date >= t1.dt  --- 限定生单时间在最后一次点击时间后24h之内
        group by 1,2,3
    )t1
    group by 1
) t1
left join (--- 流量 首次点击或者最后一次点击之后当天有进ldbo主流程
    select t1.dt
           ,count(distinct user_name) exp_uv
           ,count(distinct act_user_name) act_uv
    from (
        select t1.dt,t1.user_name,t2.user_name  act_user_name
        from h_exp t1 
        left join q_ldbo_uv t2 on t1.user_name=t2.user_name and t1.dt=t2.dt and (unix_timestamp(t2.action_time) > unix_timestamp(t1.log_time) or unix_timestamp(t2.action_time) > unix_timestamp(t1.log_time_min))
        group by 1,2,3
    )t1
    group by 1
) t2 on t1.dt=t2.dt
order by t1.dt desc
;





