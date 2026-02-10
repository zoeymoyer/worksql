---小红书报表SQL2--小红书发帖数据漏斗-窄口径
with q_user_type as (select user_id
                          , user_name
                          , min(order_date) as min_order_date
                     from default.mdw_order_v3_international
                     where dt = '%(DATE)s'
                       and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
                       and terminal_channel_type in ('www', 'app', 'touch')
                       and order_status not in ('CANCELLED', 'REJECTED')
                       and is_valid = '1'
                     group by 1, 2)
   , q_app_order_new as (select order_date                as `日期`
                              , a.user_id
                              , a.user_name
                              , sum(init_gmv)             as `Q_GMV`
                              , count(order_no)           as `Q_订单量`
                              , count(distinct a.user_id) as `Q_下单用户`
                              , sum(room_night)           as `Q_间夜量`
                         from default.mdw_order_v3_international a
                                  left join q_user_type b on a.user_id = b.user_id
                                  left join temp.temp_yiquny_zhang_ihotel_area_region_forever e
                                            on a.country_name = e.country_name
                         where dt = '%(DATE)s'
                           and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
                           --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
                           and terminal_channel_type = 'app'
                           and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
                           and (first_rejected_time is null or date(first_rejected_time) > order_date)
                           and (refund_time is null or date(refund_time) > order_date)
                           and is_valid = '1'
                           and order_date >= '2025-01-01'        
                           and order_date <= date_sub(current_date, 1)               -- 改为前一天
                           and order_no <> '103576132435'
                           and case when order_date = b.min_order_date then '新客' else '老客' end = '新客'
                         group by 1, 2, 3)
   , q_app_order_ttl as (select order_date                as `日期`
                              , a.user_id
                              , a.user_name
                              , sum(init_gmv)             as `Q_GMV`
                              , count(order_no)           as `Q_订单量`
                              , count(distinct a.user_id) as `Q_下单用户`
                              , sum(room_night)           as `Q_间夜量`
                         from default.mdw_order_v3_international a
                         where dt = '%(DATE)s'
                           and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
                           --  and (terminal_channel_type = 'app' or user_tracking_data['inner_channel'] = 'smart_app')
                           and terminal_channel_type = 'app'
                           and (first_cancelled_time is null or date(first_cancelled_time) > order_date)
                           and (first_rejected_time is null or date(first_rejected_time) > order_date)
                           and (refund_time is null or date(refund_time) > order_date)
                           and is_valid = '1'
                           and order_date >= '2025-01-01' -- 改为当月第一天
                           and order_date <= date_sub(current_date, 1)               -- 改为前一天
                           and order_no <> '103576132435'
                         group by 1, 2, 3)
   , redbook_notes as
    (select substr(note_post_time, 1, 10) as `发帖日期`
          , count(distinct note_id)          `发帖量`
     from pp_pub.dwd_redbook_notes_detail_nd ----小红书帖子相关明细
     where dt = '%(FORMAT_DATE)s' --全量快照,取最新一天T-1数据
       and query is not null
       and note_id is not null
       and note_busi = 'hotel-inter'
       and substr(note_post_time, 1, 10) >= '2025-01-01'
       and substr(note_post_time, 1, 10) <= date_sub(current_date, 1)
     group by 1)
   , post_amount as
    (select `投放日期`
          , sum(cost) / 100                                                           as `投放金额`
          , sum(cost) * 1.000 / 100 * (1 - avg(cost_rate)) / 1.06                     as `实际消耗金额`
          , count(distinct case when cost <> 0 then note_id else null end)            as `帖子在线量`
          , sum(view_count)                                                           as `帖子曝光量`
          , sum(valid_click_count)                                                    as `帖子点击量`
          , sum(case when ad_name like '%应用%' then valid_click_count else null end) as `唤端点击量` --(待定)
     from (select distinct `投放日期`, a.note_id, cost, ad_name, view_count, valid_click_count, cost_rate
           from (select cost_dt as `投放日期`, note_id, cost, ad_name, view_count, valid_click_count, cost_rate
                 from pp_pub.dwd_redbook_spotlight_creative_cost_info_da --小红书-投流日消耗
                 where dt = '%(FORMAT_DATE)s' --全量快照,取最新一天T-1数据
                   and cost_dt >= '2025-01-01'
                   and cost_dt <= date_sub(current_date, 1)) a
                    --关联国酒帖子
                    join
                (select distinct note_id
                 from pp_pub.dwd_redbook_notes_detail_nd ----小红书帖子相关明细
                 where dt = '%(FORMAT_DATE)s' --全量快照,取最新一天T-1数据
                   and query is not null
                   and note_id is not null
                   and note_busi = 'hotel-inter') b on a.note_id = b.note_id) cost_distinct
     group by 1)
   , action as
    (select log_date
          , count(distinct uv.user_name)                                                      as `活动页UV`
          , count(distinct case when a.user_name is not null then uv.user_name else null end) as `大盘贡献UV`
          , count(distinct t.user_name)                                                       as `下单人数`
          , sum(t.`Q_订单量`)                                                                 as `ttl_Q_订单量`
     from (select distinct substr(log_time, 1, 10) as log_date
                         , d.user_name
           from hotel.dwd_flow_qav_htl_qmark_di d
           where dt >= '2025-01-01'
             and dt <= date_sub(current_date, 1)
             and page_cid in ('36186','36190','36189','36162','36142','17163','19439','20079','20399','20171','26847','29442','29443','29617','29616','29671','29679','29680','29681','29662','28334','28634','28337','28326','28339','28463','28716','31677','32228','32793','32792','32753','33017','32879','32936','32937','32968','32693','32700','33020','33017','33168','33197','34162','34157','34163','34045','34082','34176','34693','34695','34185','34366','36186','36190','36189','36162','36142')
             and page_url like '%/shark/active%'
             and d.user_name not like '0000%') uv
              left join
          (select distinct dt as `日期`
                         , user_id
                         , user_name
           from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
           where dt >= '2025-01-01'
             and dt <= date_sub(current_date, 1)
             and business_type = 'hotel'
             and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
             and (search_pv + detail_pv + booking_pv + order_pv) > 0
             and user_name is not null
             and user_name not in ('null', 'NULL', '', ' ')
             and user_id is not null
             and user_id not in ('null', 'NULL', '', ' ')) a on uv.user_name = a.user_name and uv.log_date = a.`日期`
              left join q_app_order_ttl t on t.`日期` = uv.log_date and t.user_name = uv.user_name
     group by 1)
   , action_new as
    (select log_date
          , count(distinct uv.user_name)                                                      as `活动页UV-新客`
          , count(distinct case when a.user_name is not null then uv.user_name else null end) as `大盘贡献UV-新客`
          , count(distinct t1.user_name)                                                      as `下单人数-新`
          , sum(t1.`Q_订单量`)                                                                as `Q_订单量_新`
     from (select distinct substr(log_time, 1, 10)                                       as log_date
                         , case when d.dt > q.min_order_date then '老客' else '新客' end as user_type
                         , d.user_name user_name
           from hotel.dwd_flow_qav_htl_qmark_di d
                    left join q_user_type q on d.user_name = q.user_name
           where d.dt >= '2025-01-01'
             and d.dt <= date_sub(current_date, 1)
             and page_cid in ('36186','36190','36189','36162','36142','17163','19439','20079','20399','20171','26847','29442','29443','29617','29616','29671','29679','29680','29681','29662','28334','28634','28337','28326','28339','28463','28716','31677','32228','32793','32792','32753','33017','32879','32936','32937','32968','32693','32700','33020','33017','33168','33197','34162','34157','34163','34045','34082','34176','34693','34695','34185','34366','36186','36190','36189','36162','36142')
             and page_url like '%/shark/active%'
             and d.user_name not like '0000%') uv
              left join
          (select distinct dt                                                          as `日期`
                         , case when dt > q.min_order_date then '老客' else '新客' end as user_type
                         , a.user_id
                         , a.user_name
           from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
                    left join q_user_type q on q.user_id = a.user_id
           where dt >= '2025-01-01'
             and dt <= date_sub(current_date, 1)
             and business_type = 'hotel'
             and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
             and (search_pv + detail_pv + booking_pv + order_pv) > 0
             and a.user_name is not null
             and a.user_name not in ('null', 'NULL', '', ' ')
             and a.user_id is not null
             and a.user_id not in ('null', 'NULL', '', ' ')) a
          on uv.user_name = a.user_name and uv.log_date = a.`日期` and a.user_type = uv.user_type
              left join q_app_order_new t1 on t1.`日期` = uv.log_date  and t1.user_name = uv.user_name
     where uv.user_type = '新客'
     group by 1)
   , change_show as
    (select dt
          , count(distinct uid) as `唤端页面曝光uv_未安装APP`
     from pp_pub.dwd_redbook_call_item_ad_qmark_view_click_detail_di
     where dt >= '2025-01-01'
       and dt <= date_sub(current_date, 1) --每日增量
       and action_type = 'view'
     group by 1)
   , change_uv as
    (select substr(create_time, 1, 10) as dt
          , count(distinct client_uid) as `唤端UV`
     from pp_pub.dwd_redbook_call_item_ad_click_pub_di a
              join
          (select distinct note_id
           from pp_pub.dwd_redbook_notes_detail_nd
           where dt = '%(FORMAT_DATE)s' --全量快照,取最新一天T-1数据
             and query is not null
             and note_id is not null
             and note_busi = 'hotel-inter') b on a.note_id = b.note_id
     where dt >= '2024-05-01' --每日增量
       and substr(create_time, 1, 10) = substr(click_time, 1, 10)
       and substr(create_time, 1, 10) >= '2025-01-01'
       and substr(create_time, 1, 10) <= date_sub(current_date, 1)
     group by 1)

select `投放日期`
     , date_format(`投放日期`, 'u')                                `星期`
     , nvl(`发帖量`, 0)                                            `发帖量`
     , round(`投放金额`, 0)                                        `投放金额`
     , round(`实际消耗金额`, 0)                                    `实际消耗金额`
     , `帖子在线量`                                             as `在投帖量`
     , `帖子曝光量`
     , `帖子点击量`
     , `唤端页面曝光uv_未安装APP`
     , `唤端UV`
     , `活动页UV`
     , `大盘贡献UV`
     , `下单人数`
     , `ttl_Q_订单量` as `订单量`
     , concat(round(`ttl_Q_订单量` / `大盘贡献UV` * 100, 2), '%') as CR
     , `活动页uv-新客`
     , `大盘贡献UV-新客`
     , `下单人数-新` as  `下单人数-新客`
     , `Q_订单量_新` as `订单量_新客`
     , concat(round(`Q_订单量_新` / `大盘贡献UV-新客` * 100, 2), '%') as `CR-新客`
     , concat(round(`帖子点击量` / `帖子曝光量` * 100, 2), '%') as `点击/曝光`
     , concat(round(`大盘贡献UV` / `帖子点击量` * 100, 2), '%') as `活跃/点击`
     , concat(round(`活动页UV` / `帖子点击量` * 100, 2), '%')   as `活动页/点击`
     , concat(round(`大盘贡献UV` / `活动页UV` * 100, 2), '%')   as `活跃/活动页`
     , round(`帖子曝光量` / `帖子在线量`, 0)                    as `单帖曝光`
from post_amount a
         left join redbook_notes b on a.`投放日期` = b.`发帖日期`
         left join change_show c on a.`投放日期` = c.dt
         left join change_uv e on a.`投放日期` = e.dt
         left join action d on a.`投放日期` = d.log_date
         left join action_new n on n.log_date = d.log_date
order by `投放日期` desc;