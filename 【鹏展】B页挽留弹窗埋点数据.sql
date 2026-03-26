select 
     dt
    -- 1. 曝光UV
    ,show_kl_uv as `可领优惠-弹窗曝光uv`
    ,show_sf_uv as `身份优惠-弹窗曝光uv`
    ,show_yx_uv as `已选优惠-弹窗曝光uv`
    
    -- 2. 点击行为UV（用于辅助查阅明细）
    -- ,click_kl_succ_uv as `可领优惠-继续预订uv`
    -- ,click_kl_fail_uv as `可领优惠-放弃预订uv`
    -- ,click_sf_succ_uv as `身份优惠-继续预订uv`
    -- ,click_sf_fail_uv as `身份优惠-放弃预订uv`
    -- ,click_yx_succ_uv as `已选优惠-继续预订uv`
    -- ,click_yx_fail_uv as `已选优惠-放弃预订uv`

    -- 3. 核心转化率指标 (挽留成功/失败率 = 点击UV / 曝光UV)
    ,concat(round(click_kl_succ_uv / show_kl_uv * 100, 2), '%') as `可领优惠-挽留成功率`
    ,concat(round(click_kl_fail_uv / show_kl_uv * 100, 2), '%') as `可领优惠-挽留失败率`
    
    ,concat(round(click_sf_succ_uv / show_sf_uv * 100, 2), '%') as `身份优惠-挽留成功率`
    ,concat(round(click_sf_fail_uv / show_sf_uv * 100, 2), '%') as `身份优惠-挽留失败率`
    
    ,concat(round(click_yx_succ_uv / show_yx_uv * 100, 2), '%') as `已选优惠-挽留成功率`
    ,concat(round(click_yx_fail_uv / show_yx_uv * 100, 2), '%') as `已选优惠-挽留失败率`

from (
    select concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as dt
        --------------------------------- 曝光UV统计 ---------------------------------
        -- 可领优惠-弹窗曝光uv
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '可领优惠' 
                             then user_name end) as show_kl_uv
        -- 身份优惠-弹窗曝光uv
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '身份优惠' 
                             then user_name end) as show_sf_uv
        -- 已选优惠-弹窗曝光uv（旧弹窗）
        ,count(distinct case when key = 'ihotel/Booking/Footer/show/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '已选优惠' 
                             then user_name end) as show_yx_uv
                             
        --------------------------------- 点击UV统计 ---------------------------------
        -- 可领优惠-挽留成功uv (buttonClicked in 1,3)
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '可领优惠' 
                              and get_json_object(value, '$.ext.buttonClicked') in ('1','3') 
                             then user_name end) as click_kl_succ_uv
        -- 可领优惠-挽留失败uv (buttonClicked = 2)
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '可领优惠' 
                              and get_json_object(value, '$.ext.buttonClicked') = '2' 
                             then user_name end) as click_kl_fail_uv
                             
        -- 身份优惠-挽留成功uv (buttonClicked in 1,3)
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '身份优惠' 
                              and get_json_object(value, '$.ext.buttonClicked') in ('1','3') 
                             then user_name end) as click_sf_succ_uv
        -- 身份优惠-挽留失败uv (buttonClicked = 2)
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '身份优惠' 
                              and get_json_object(value, '$.ext.buttonClicked') = '2' 
                             then user_name end) as click_sf_fail_uv

        -- 已选优惠-挽留成功uv（旧弹窗） (buttonClicked in 1,3)
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '已选优惠' 
                              and get_json_object(value, '$.ext.buttonClicked') in ('1','3') 
                             then user_name end) as click_yx_succ_uv
        -- 已选优惠-挽留失败uv（旧弹窗） (buttonClicked = 2)
        ,count(distinct case when key = 'ihotel/Booking/Footer/click/retentionModal' 
                              and get_json_object(value, '$.ext.modalType') = '已选优惠' 
                              and get_json_object(value, '$.ext.buttonClicked') = '2' 
                             then user_name end) as click_yx_fail_uv
                             
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260227' and dt <= '20260311'
      and key in (
          'ihotel/Booking/Footer/show/retentionModal',   ---- 挽留弹窗曝光
          'ihotel/Booking/Footer/click/retentionModal'   ---- 挽留弹窗点击
      )
    group by 1
) t
order by 1;



with track_base as (
    select  dt
           ,concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as stat_date
           ,user_name
           ,key
           ,value
           ,get_json_object(value, '$.ext.modalType') as modal_type
           ,get_json_object(value, '$.ext.buttonClicked') as button_clicked
           ,log_time
    from default.dw_qav_ihotel_track_info_di
    where dt >= '20260227' and dt <= '20260311'
      and key in (
            'ihotel/Booking/Footer/show/retentionModal'
           ,'ihotel/Booking/Footer/click/retentionModal'
      )
      and user_name is not null
)
,show_uv as (
    select  stat_date
           ,modal_type
           ,count(distinct user_name) as show_uv
    from track_base
    where key = 'ihotel/Booking/Footer/show/retentionModal'
      and modal_type in ('可领优惠','身份优惠','已选优惠')
    group by 1,2
)
,click_uv as (
    select  stat_date
           ,modal_type
           ,count(distinct case when button_clicked in ('1','3') then user_name end) as success_click_uv
           ,count(distinct case when button_clicked = '2' then user_name end) as fail_click_uv
    from track_base
    where key = 'ihotel/Booking/Footer/click/retentionModal'
      and modal_type in ('可领优惠','身份优惠','已选优惠')
    group by 1,2
)

,success_click_detail as (
    select  stat_date
           ,modal_type
           ,user_name
           ,max(log_time) as click_time
    from track_base
    where key = 'ihotel/Booking/Footer/click/retentionModal'
      and button_clicked in ('1','3')
      and modal_type in ('可领优惠','身份优惠','已选优惠')
    group by 1,2,3
)
,q_order_app as (
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  
                  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  
                  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,a.user_id,user_name,init_gmv,order_no,room_night,order_time
            ,batch_series,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+coalesce(split(coupon_info['23base_ZK_728810'],'_')[1],0)+coalesce(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+coalesce(ext_plat_certificate,0))
                  else init_commission_after+coalesce(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from default.mdw_order_v3_international a 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >= date_sub(current_date, 30) and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)

,b2o_uv as (
    select  c.stat_date
           ,c.modal_type
           ,count(distinct c.user_name) as b2o_uv
    from success_click_detail c
    join q_order_app o
      on c.user_name = o.user_name
     and o.order_time > c.click_time
    group by 1,2
)

select  s.stat_date as dt

    ,max(case when s.modal_type = '可领优惠' then s.show_uv end) as `可领优惠-弹窗曝光uv`
    ,max(case when s.modal_type = '身份优惠' then s.show_uv end) as `身份优惠-弹窗曝光uv`
    ,max(case when s.modal_type = '已选优惠' then s.show_uv end) as `已选优惠-弹窗曝光uv（旧弹窗）`

    ,concat(
        round(
            100.0 * coalesce(max(case when c.modal_type = '可领优惠' then c.success_click_uv end),0)
            / nullif(max(case when s.modal_type = '可领优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `可领优惠-挽留成功率`

    ,concat(
        round(
            100.0 * coalesce(max(case when c.modal_type = '可领优惠' then c.fail_click_uv end),0)
            / nullif(max(case when s.modal_type = '可领优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `可领优惠-挽留失败率`

    ,concat(
        round(
            100.0 * coalesce(max(case when c.modal_type = '身份优惠' then c.success_click_uv end),0)
            / nullif(max(case when s.modal_type = '身份优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `身份优惠-挽留成功率`

    ,concat(
        round(
            100.0 * coalesce(max(case when c.modal_type = '身份优惠' then c.fail_click_uv end),0)
            / nullif(max(case when s.modal_type = '身份优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `身份优惠-挽留失败率`

    ,concat(
        round(
            100.0 * coalesce(max(case when c.modal_type = '已选优惠' then c.success_click_uv end),0)
            / nullif(max(case when s.modal_type = '已选优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `已选优惠-挽留成功率（旧弹窗）`

    ,concat(
        round(
            100.0 * coalesce(max(case when c.modal_type = '已选优惠' then c.fail_click_uv end),0)
            / nullif(max(case when s.modal_type = '已选优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `已选优惠-挽留失败率（旧弹窗）`

    ,concat(
        round(
            100.0 * coalesce(max(case when b.modal_type = '可领优惠' then b.b2o_uv end),0)
            / nullif(max(case when s.modal_type = '可领优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `可领优惠-挽留后b2o`

    ,concat(
        round(
            100.0 * coalesce(max(case when b.modal_type = '身份优惠' then b.b2o_uv end),0)
            / nullif(max(case when s.modal_type = '身份优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `身份优惠-挽留后b2o`

    ,concat(
        round(
            100.0 * coalesce(max(case when b.modal_type = '已选优惠' then b.b2o_uv end),0)
            / nullif(max(case when s.modal_type = '已选优惠' then s.show_uv end),0)
        ,2)
    ,'%') as `已选优惠-挽留后b2o（旧弹窗）`

from show_uv s
left join click_uv c
  on s.stat_date = c.stat_date
 and s.modal_type = c.modal_type
left join b2o_uv b
  on s.stat_date = b.stat_date
 and s.modal_type = b.modal_type
group by s.stat_date
order by s.stat_date
;