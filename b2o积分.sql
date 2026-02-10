with q_sugar_order as ( --全量积分订单
    select distinct order_no
    from default.mdw_order_v3_international
    lateral view explode(supplier_promotion_code) bb as promotion_ids
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and is_valid='1'
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        --and order_date>='2025-02-21' and order_date<='2025-03-06'
        and qta_supplier_id='1615667' 
        -- and supplier_promotion_code like '%2913%'
        and promotion_ids='2913'
)
,q_sugar_hotel as ( --积分酒店
    select distinct hotel_seq, order_date
    from default.mdw_order_v3_international
    lateral view explode(supplier_promotion_code) bb as promotion_ids
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and is_valid='1'
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        --and order_date>='2025-02-21' and order_date<='2025-03-06'
        and qta_supplier_id='1615667' 
        -- and supplier_promotion_code like '%2913%'
        and promotion_ids='2913'
)

,q_sugar_uv as (  --- 积分酒店流量
   select 
        concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as order_date
        -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
        ,user_id
        ,sum(search_pv) search_pv
        ,sum(detail_pv) detail_pv
        ,sum(booking_pv) booking_pv
        ,sum(order_pv) order_pv
   from default.mdw_user_app_sdbo_di_v3 a  -- 用户流量表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   join q_sugar_hotel b on concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))=b.order_date and a.hotel_seq=b.hotel_seq   --- 积分酒店
   where  dt>='20250817' and dt<=from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
          and business_type = 'hotel'
          and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
          and (search_pv+detail_pv+booking_pv+order_pv)>0
   group by 1,2
)
,q_order as (  --- 积分酒店订单
   select distinct 
          a.order_date 
        --  ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,user_id
         ,a.order_no
   from default.mdw_order_v3_international a   --- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   join q_sugar_hotel b on a.order_date=b.order_date and a.hotel_seq=b.hotel_seq  --- 积分酒店
   where dt =from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch') 
        and terminal_channel_type='app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
        and (refund_time is null or date(refund_time) > a.order_date)
        and a.order_date >= date_sub(current_date,15) and a.order_date <= date_sub(current_date,1)
)
,q_sugar_hotel_order as (  --- 积分酒店积分订单
   select distinct 
          a.order_date 
        --  ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,user_id
         ,a.order_no
   from default.mdw_order_v3_international a   --- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   join q_sugar_hotel b on a.order_date=b.order_date and a.hotel_seq=b.hotel_seq  --- 积分酒店
   where dt =from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch') 
        and terminal_channel_type='app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
        and (refund_time is null or date(refund_time) > a.order_date)
        and a.order_date >= date_sub(current_date,15) and a.order_date <= date_sub(current_date,1)
        and a.order_no in (select order_no from q_sugar_order)
)
,q_uv as (  --- 流量
   select 
        concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as order_date
        -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
        ,user_id
        ,sum(search_pv) search_pv
        ,sum(detail_pv) detail_pv
        ,sum(booking_pv) booking_pv
        ,sum(order_pv) order_pv
   from default.mdw_user_app_sdbo_di_v3 a  -- 用户流量表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   where  dt>='20250817' and dt<=from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
    and business_type = 'hotel'
    and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    and (search_pv+detail_pv+booking_pv+order_pv)>0
   group by 1,2
)
,q_order_all as (  --- 整体订单
   select distinct 
          order_date 
        --  ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,user_id
         ,order_no
   from default.mdw_order_v3_international a   --- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
       where dt =from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch') 
        and terminal_channel_type='app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and order_date >= date_sub(current_date,15) and order_date <= date_sub(current_date,1)
)


select t1.order_date
        ,d_s_UV
        ,b_ds_UV
        ,o_ds_order
        ,q_uv
        ,d_s_sugar_UV
        ,b_ds_sugar_UV
        ,o_ds_sugar_order
        ,o_ds_sugar_hotel_order
        ,q_sugar_uv
from ( --- 整体
    select  a.order_date
            ,count(distinct case when detail_pv >0 and search_pv >0 then  a.user_id else null end )d_s_UV
            ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end )b_ds_UV
            ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order
            ,count(distinct case when search_pv+detail_pv+booking_pv+order_pv>0 then a.user_id else null end ) q_uv
    from q_uv a  -- 流量表
    left join  q_order_all b on a.order_date=b.order_date and a.user_id=b.user_id    -- 订单表
    group by 1
) t1 left join ( --- 积分酒店
    select  a.order_date
            ,count(distinct case when detail_pv >0 then  a.user_id else null end )d_s_sugar_UV
            ,count(distinct case when booking_pv >0 and detail_pv >0  then  a.user_id else null end )b_ds_sugar_UV
            ,count(distinct case when b.user_id is not null and detail_pv >0 and booking_pv >0 then b.order_no else null end )o_ds_sugar_order
            ,count(distinct case when c.user_id is not null and detail_pv >0 and booking_pv >0 then c.order_no else null end )o_ds_sugar_hotel_order
            ,count(distinct case when search_pv+detail_pv+booking_pv+order_pv>0 then a.user_id else null end ) q_sugar_uv
    from q_sugar_uv a  -- 积分酒店流量
    left join  q_order b on a.order_date=b.order_date and a.user_id=b.user_id  -- 积分酒店订单
    left join  q_sugar_hotel_order c on a.order_date=c.order_date and a.user_id=c.user_id  -- 积分酒店积分订单
    group by 1

)t2 on t1.order_date=t2.order_date

  ;



---- new

with q_sugar_order as ( --全量积分订单
    select distinct order_no
    from default.mdw_order_v3_international
    lateral view explode(supplier_promotion_code) bb as promotion_ids
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国') 
        and is_valid='1'
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        --and order_date>='2025-02-21' and order_date<='2025-03-06'
        and qta_supplier_id='1615667' 
        -- and supplier_promotion_code like '%2913%'
        and promotion_ids='2913'
)
,q_sugar_hotel as ( --积分酒店
        select b.hotel_seq
        from(
        SELECT * FROM temp.temp_jiahao_yang_suger_hotellist
        ) a
        join (
        select hotel_seq,partner_hotel_id
        from ihotel_default.dim_hotel_mapping_intl_v3
        where dt= '20250831'
        ) b on a.property_id=b.partner_hotel_id
)

,q_sugar_uv as (  --- 积分酒店流量
   select 
        concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as order_date
        -- ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
        ,user_id
        ,sum(search_pv) search_pv
        ,sum(detail_pv) detail_pv
        ,sum(booking_pv) booking_pv
        ,sum(order_pv) order_pv
   from default.mdw_user_app_sdbo_di_v3 a  -- 用户流量表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   join q_sugar_hotel b on  a.hotel_seq=b.hotel_seq   --- 积分酒店
   where  dt >='20250714' and dt<=from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
          and business_type = 'hotel'
          and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
          and (search_pv+detail_pv+booking_pv+order_pv)>0
   group by 1,2
)
,q_order as (  --- 积分酒店订单
   select distinct 
          a.order_date 
        --  ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,user_id
         ,a.order_no
   from default.mdw_order_v3_international a   --- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   join q_sugar_hotel b on a.hotel_seq=b.hotel_seq  --- 积分酒店
   where dt =from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch') 
        and terminal_channel_type='app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
        and (refund_time is null or date(refund_time) > a.order_date)
        and a.order_date >= date_sub(current_date,56) and a.order_date <= date_sub(current_date,1)
)
,q_sugar_hotel_order as (  --- 积分酒店积分订单
   select distinct 
          a.order_date 
        --  ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as `目的地`
         ,user_id
         ,a.order_no
   from default.mdw_order_v3_international a   --- 海外订单表
   left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
   join q_sugar_hotel b on  a.hotel_seq=b.hotel_seq  --- 积分酒店
   where dt =from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        -- and terminal_channel_type in ('www','app','touch') 
        and terminal_channel_type='app'
        and is_valid='1'
        and (first_cancelled_time is null or date(first_cancelled_time) > a.order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > a.order_date) 
        and (refund_time is null or date(refund_time) > a.order_date)
        and a.order_date >= date_sub(current_date,56) and a.order_date <= date_sub(current_date,1)
        and a.order_no in (select order_no from q_sugar_order)
)



select order_date
       ,`D页uv_积分酒店`
       ,`B页uv_积分酒店`
       ,`订单量_积分酒店`
       ,`积分订单量_积分酒店`
       ,concat(round(`B页uv_积分酒店`/`D页uv_积分酒店`*100,2),'%') as `D2B_积分酒店`
       ,concat(round(`订单量_积分酒店`/`B页uv_积分酒店`*100,2),'%') as `B2O_积分酒店`
       ,concat(round(`积分订单量_积分酒店`/`B页uv_积分酒店`*100,2),'%') as `B2O_积分订单积分酒店`
       ,concat(round(`积分订单量_积分酒店`/`订单量_积分酒店`*100,2),'%') as `积分订单占比`
from (
    select  a.order_date
            ,count(distinct case when detail_pv >0 then  a.user_id else null end )`D页uv_积分酒店`
            ,count(distinct case when booking_pv >0 and detail_pv >0  then  a.user_id else null end ) `B页uv_积分酒店`
            ,count(distinct case when b.user_id is not null and detail_pv >0 then b.order_no else null end ) `订单量_积分酒店`
            ,count(distinct case when c.user_id is not null and detail_pv >0 then c.order_no else null end ) `积分订单量_积分酒店`
            
    from q_sugar_uv a  -- 积分酒店流量
    left join  q_order b on a.order_date=b.order_date and a.user_id=b.user_id  -- 积分酒店订单
    left join  q_sugar_hotel_order c on a.order_date=c.order_date and a.user_id=c.user_id  -- 积分酒店积分订单
    group by 1
) t

;





