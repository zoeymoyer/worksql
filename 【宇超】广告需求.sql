--- 阿塞拜疆营业酒店明细
SELECT
    country_name
    ,hotel_name
    ,attrs['enName'] AS hotel_enname
    ,count(1) over() cnt
FROM default.dim_hotel_info_intl_v3 
WHERE dt = '%(DATE)s' 
    and country_name = '阿塞拜疆' 
    and hotel_operating_status = '营业中'
  ;

--- 国际酒店用户年龄及提前预定酒店时间情况
with q_order as (----订单明细表表包含取消  分目的地、新老维度
    select order_date,checkin_date,advance_booking_days
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id
    from mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = '%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type in ('www','app','touch')  -- 用户终端类型
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_no <> '103576132435'
        and order_date >= date_sub(current_date,60) and order_date <= date_sub(current_date,1)

)

--- 广告数据需求-雅诗兰黛客户-国际酒店list页搜索查询需求
select substr(dt,1,6) mth
     ,country_name
     ,count(distinct orig_device_id) 
from default.dwd_ihotel_flow_app_searchlist_di
where dt >= '20240101' and dt<='20250228'
  and orig_device_id is not null
  and orig_device_id != ''
  and search_type in (0,16,17)
  and is_display=1
  and country_name in ('日本','韩国','泰国','新加坡')
group by 1,2
order by 1
;


select substr(dt,1,6) mth
        ,country_name
        ,sum(`搜索次数`) as `搜索次数`
from
    (select dt
        ,country_name
        ,hotel_seq
        ,orig_device_id
        ,user_id
        ,count(distinct search_request_uid) as `搜索次数`
    from default.dwd_ihotel_flow_app_searchlist_di
    where dt >= '20240101' and dt<='20250228'
        and orig_device_id is not null
        and orig_device_id != ''
        and search_type in (0,16,17)
        and is_display=1
        and country_name in ('日本','韩国','泰国','新加坡')
    group by 1,2,3,4,5
    ) z
group by 1,2