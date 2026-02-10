WITH q_user_type as
(
    select  user_id
           ,min(order_date) as min_order_date
    from mdw_order_v3_international
    where dt = '%(DATE)s'
          and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
          and terminal_channel_type in ('www', 'app', 'touch')
          and order_status not in ('CANCELLED', 'REJECTED')
          and is_valid = '1'
    group by  1
)


select  substr(order_date,1,4) as data_year
       ,order_date,user_type
       ,sum(s_all_uv) as `列表页UV`
       ,sum(d_s_uv) / sum(s_all_uv) as `z-S2D`  -- L2D
       ,sum(d_s_uv) as `详情页UV`
       ,sum(b_ds_uv) / sum(d_s_uv) as `z-D2B`
       ,sum(b_ds_uv) as `填写页UV`
       ,sum(o_ds_order)/ sum(b_ds_uv) as `z-B2O`
       ,sum(o_UV) as `o_UV`
       ,sum(o_ds_order) as `订单数`
       ,sum(order_user_cnt) as `下单人数`
from
(
    select  a.order_date
            ,user_type
           ,sd_UV
           ,s_all_UV
           ,d_all_UV
           ,d_z_UV
           ,d_s_UV
           ,b_all_UV
           ,b_dz_UV
           ,b_ds_UV
           ,o_UV
           ,o_dz_order
           ,o_ds_order
           ,order_user_cnt
    from
    (
        select  order_date
                ,user_type
               ,sum(sd_UV)sd_UV
               ,sum(s_all_UV)s_all_UV
               ,sum(d_all_UV)d_all_UV
               ,sum(d_z_UV)d_z_UV
               ,sum(d_s_UV)d_s_UV
               ,sum(b_all_UV)b_all_UV
               ,sum(b_dz_UV)b_dz_UV
               ,sum(b_ds_UV)b_ds_UV
               ,sum(o_UV)o_UV
               ,sum(o_dz_order)o_dz_order
               ,sum(o_ds_order)o_ds_order
               ,sum(order_user_cnt) order_user_cnt
        from
        (
            select  a.order_date
                   ,a.`目的地`
                ,user_type
                   ,count(distinct case when search_pv > 0 or detail_pv > 0 then a.user_id else null end )sd_UV
                   ,count(distinct case when search_pv > 0 then a.user_id else null end )s_all_UV
                   ,count(distinct case when detail_pv > 0 then a.user_id else null end )d_all_UV
                   ,count(distinct case when detail_pv > 0 and search_pv <= 0 then a.user_id else null end )d_z_UV
                   ,count(distinct case when detail_pv > 0 and search_pv > 0 then a.user_id else null end )d_s_UV
                   ,count(distinct case when booking_pv > 0 then a.user_id else null end )b_all_UV
                   ,count(distinct case when booking_pv > 0 and detail_pv > 0 and search_pv <= 0 then a.user_id else null end )b_dz_UV
                   ,count(distinct case when booking_pv > 0 and detail_pv > 0 and search_pv > 0 then a.user_id else null end )b_ds_UV
                   ,count(distinct case when order_pv > 0 then a.user_id else null end )o_UV
                   ,count(distinct case when b.user_id is not null and detail_pv > 0 and search_pv <= 0 then order_no else null end )o_dz_order
                   ,count(distinct case when b.user_id is not null and detail_pv > 0 and search_pv > 0 then order_no else null end )o_ds_order
                   ,count(distinct b.user_id) as order_user_cnt
            from
            (
                select  dt as order_date
                       ,case when province_name in ('澳门','香港') then province_name
                             when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                             when c.area in ('欧洲','亚太','美洲') then c.area  else '其他' end as `目的地`
                       ,user_id
                       ,sum(search_pv) search_pv
                       ,sum(detail_pv) detail_pv
                       ,sum(booking_pv) booking_pv
                       ,sum(order_pv) order_pv
                from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
                left join temp.temp_yiquny_zhang_ihotel_area_region_forever c
                    on a.country_name = c.country_name
                where dt between date_sub(current_date, 7) and date_sub(current_date, 1)
                and business_type = 'hotel'
                and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
                and (a.search_pv+a.detail_pv+a.booking_pv+a.order_pv) > 0
                group by  1,2,3
            )a
            left join
            (
                select  distinct order_date
                       ,case when province_name in ('澳门','香港') then province_name
                             when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                             when c.area in ('欧洲','亚太','美洲') then c.area  else '其他' end as `目的地`
                       ,case when order_date > b.min_order_date then '老客'  else '新客' end as user_type
                       ,a.user_id
                       ,order_no
                from default.mdw_order_v3_international a
                left join temp.temp_yiquny_zhang_ihotel_area_region_forever c
                on a.country_name = c.country_name
                                left join q_user_type b
                    on a.user_id = b.user_id
                where dt = '%(DATE)s'
                and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
                and terminal_channel_type = 'app'
                and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
                and (first_rejected_time is null or date(first_rejected_time) > order_date)
                and (refund_time is null or date(refund_time) > order_date)
                and is_valid = '1'
                and order_date between '2025-06-11' and '2025-06-24'
            ) b
            on a.order_date = b. order_date and a.user_id = b.user_id and a.`目的地` = b.`目的地`
            group by  1,2,3
        )a
        group by  1,2
    )a
) T
group by  1,2,3;