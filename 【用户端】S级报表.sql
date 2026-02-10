with user_type as (-----新老客
    select user_id
          ,min(order_date) as min_order_date
    from mdw_order_v3_international   --- 海外订单表
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and order_status not in ('CANCELLED', 'REJECTED')
        and is_valid = '1'
    group by 1
)
,uv as (   
    select  dt 
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name
                       when e.area in ('欧洲','亚太','美洲') then e.area
                       else '其他' end as mdd
            ,case when dt > b.min_order_date then '老客' else '新客' end as user_type
            ,a.user_id
            ,a.user_name
            ,sum(search_pv) search_pv
            ,sum(detail_pv) detail_pv
            ,sum(booking_pv) booking_pv
            ,sum(order_pv) order_pv
     from ihotel_default.mdw_user_app_log_sdbo_di_v1 a
     left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
     left join user_type b on a.user_id = b.user_id 
     where dt >='2024-10-01'
       and dt <= date_sub(current_date, 1)
       and business_type = 'hotel'
       and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
       and (search_pv + detail_pv + booking_pv + order_pv) > 0
       and a.user_name is not null and a.user_name not in ('null', 'NULL', '', ' ')
       and a.user_id is not null and a.user_id not in ('null', 'NULL', '', ' ')
    group by 1,2,3,4,5
)
,q_app_order as (----订单明细表表包含取消  分目的地、新老维度 APP端
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night
            ,hotel_grade,coupon_id
            ,init_commission_after
            ,case when coupon_id is not null 
                and batch_series not in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') 
                and batch_series not like '%23base_ZK_728810%'
                and batch_series not like '%23extra_ZK_ce6f99%' 
            then 'Y' else 'N' end is_user_conpon   --- 是否用券
            ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                  then (init_commission_after+COALESCE(split(coupon_info['23base_ZK_728810'],'_')[1],0)+COALESCE(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+COALESCE(ext_plat_certificate,0))
                  else init_commission_after+COALESCE(ext_plat_certificate,0) end as final_commission_after  --- Q佣金
            ,case when (coupon_substract_summary is null 
                  or batch_series like '%23base_ZK_728810%' 
                  or batch_series like '%23extra_ZK_ce6f99%') then 0
            else coalesce(coupon_substract_summary,0) end as coupon_substract_summary
    from mdw_order_v3_international a 
    left join user_type b on a.user_id = b.user_id 
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name 
    where dt = from_unixtime(unix_timestamp() -86400, 'yyyyMMdd')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and terminal_channel_type = 'app'
        -- and terminal_channel_type in ('www','app','touch')
        and (first_cancelled_time is null or date(first_cancelled_time) > order_date) 
        and (first_rejected_time is null or date(first_rejected_time) > order_date) 
        and (refund_time is null or date(refund_time) > order_date)
        and is_valid='1'
        and order_date >='2024-10-01'  and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,c_user_type as
(   --- 用于判定c新老客
    select user_id,
            ubt_user_id,
            substr(min(order_date),1,10) as min_order_date
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da
    where dt = '%(FORMAT_DATE)s'
        and extend_info['IS_IBU'] = '0'
        and extend_info['book_channel'] = 'Ctrip'
        and extend_info['sub_book_channel'] = 'Direct-Ctrip'
        and order_status <> 'C'
    group by 1,2
)
,c_uv as
(   --- C流量 目的地加和
    select dt 
        ,case when dt> b.min_order_date then '老客' else '新客' end as user_type
        ,case when provincename in ('澳门','香港') then provincename  when a.countryname in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.countryname  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
        ,uid
        ,count(distinct case when page_short_domain='list' then uid else null end) search_pv
        ,count(distinct case when page_short_domain='dbo' then uid else null end) detail_pv
        ,count(distinct case when page_short_domain='dbo' and detail_dingclick_cnt> 0 then uid else null end) booking_pv
        ,count(distinct case when page_short_domain='dbo' and order_sumbit_cnt>0 then uid else null end) o_uv
    from ihotel_default.ods_traf_browse_sdbo_details_fromc_intl_di a
    left join c_user_type b on a.uid=b.ubt_user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.countryname = e.country_name 
    where device_chl='app'
    and  dt>='2024-10-01'  and dt<= date_sub(current_date, 1)
    group by 1,2,3,4
)
,c_order as (  --- c订单明细
    select substr(order_date,1,10) as dt
           ,case when extend_info['PROVINCE'] in ('澳门','香港') then extend_info['PROVINCE']
               when extend_info['COUNTRY'] in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then extend_info['COUNTRY']
               when c.area in ('欧洲','亚太','美洲') then c.area
               else '其他' end as mdd
            ,case when min_order_date=substr(o.order_date,1,10) then '新客' else '老客' end user_type
            ,o.user_id,order_no,room_fee,comission,o.ubt_user_id
            ,extend_info['room_night'] room_night
            ,extend_info['STAR'] star
            ,get_json_object(json_path_array(discount_detail, '$.detail')[1],'$.amount') cqe  -- C_券额
    from ihotel_default.ceq_three_sync_pull_edw_trade_tripart_qunar_oversea_hotelorder_reconfig_da o
    left join c_user_type u on o.user_id=u.user_id
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever c on extend_info['COUNTRY'] = c.country_name
    where dt = '%(FORMAT_DATE)s'
      and extend_info['IS_IBU'] = '0'
      and extend_info['book_channel'] = 'Ctrip'
      and extend_info['sub_book_channel'] = 'Direct-Ctrip'
      --   and (terminal_channel_type = 'app' or extend_info['IS_WEBCHATAPP'] = 'T')
      and terminal_channel_type = 'app'
      and (extend_info['CANCEL_TIME'] is null or extend_info['CANCEL_TIME']='NULL' or substr((extend_info['CANCEL_TIME']),1,10)>substr(order_date,1,10))
      and substr(order_date,1,10) >='2024-10-01'
      and substr(order_date,1,10) <= date_sub(current_date, 1)
)
,q_order_data as(
    select order_date
           ,sum(room_night) room_night
           ,count(distinct order_no) order_no
           ,count(distinct case when user_type = '新客' then order_no end) order_no_nu
           ,count(distinct case when user_type = '老客' then order_no end) order_no_old
    from q_app_order
    -- where order_date >= date_sub(current_date, 30)
    group by 1
)
,c_order_data as(
    select dt
           ,sum(room_night) room_night_c
           ,count(distinct order_no) order_no_c
           ,count(distinct case when user_type = '新客' then order_no end) order_no_nu_c
           ,count(distinct case when user_type = '老客' then order_no end) order_no_old_c
    from c_order
    -- where dt >= date_sub(current_date, 30)
    group by 1
)
,q_flow_data as (
    select dt
           ,count(user_id) dau
           ,count(case when user_type = '新客' then user_id end) nu
           ,count(case when user_type = '老客' then user_id end) dau_old
    from uv
    -- where dt >= date_sub(current_date, 30)
    group by 1
)
,c_flow_data as (
    select dt
           ,count(uid) dau_c
           ,count(case when user_type = '新客' then uid end) nu_c
           ,count(case when user_type = '老客' then uid end) dau_old_c
    from c_uv
    -- where dt >= date_sub(current_date, 30)
    group by 1
)
,qc_data as (--- qc数据
    select t1.dt
        ,room_night / room_night_c  as `间夜QC`
        ,(order_no / dau) / (order_no_c / dau_c)  as `转化QC`
        ,(order_no_nu / nu) / (order_no_nu_c / nu_c) as `新客转化QC`
        ,(order_no_old / dau_old) / (order_no_old_c / dau_old_c)  as `老客转化QC`
        ,order_no / dau as `Q-CR`
    from q_flow_data t1   
    left join q_order_data t2 on t1.dt=t2.order_date 
    left join c_flow_data t3 on t1.dt=t3.dt 
    left join c_order_data t4 on t1.dt=t4.dt 
)
,q_sdbo as (
    select dt
         ,sum(s_all_UV) s_all_UV
         ,sum(d_all_UV) d_all_UV
         ,sum(b_all_UV) b_all_UV
         ,sum(d_s_UV) d_s_UV
         ,sum(b_ds_UV) b_ds_UV
         ,sum(o_ds_order) o_ds_order
         ,sum(q_uv) q_uv
         ,sum(order_user_cnt) order_user_cnt
    from (---- Q得DBO转化
        select 
            a.dt
            ,a.mdd
            ,a.user_type
            ,count(distinct case when search_pv >0 then  a.user_id else null end )s_all_UV
            ,count(distinct case when detail_pv >0 then  a.user_id else null end )d_all_UV
            ,count(distinct case when booking_pv >0 then a.user_id else null end )b_all_UV
            ,count(distinct case when order_pv >0 then   a.user_id else null end )o_UV
            ,count(distinct case when search_pv >0 or detail_pv>0 then  a.user_id else null end )sd_UV

            ,count(distinct case when detail_pv >0 and search_pv >0 then a.user_id else null end) d_s_UV
            ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  a.user_id else null end ) b_ds_UV
            ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end ) o_ds_order

            ,count(distinct case when detail_pv >0 and search_pv <=0 then  a.user_id else null end )  d_z_UV
            ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv <=0 then  a.user_id else null end )b_dz_UV
            ,count(distinct case when b.user_id is not null and detail_pv >0 and search_pv <=0 then order_no else null end )o_dz_order
            ,count(distinct a.user_id) q_uv
            ,count(distinct b.user_id) order_user_cnt
        from  uv a  -- 流量表
        left join q_app_order b on a.dt=b.order_date and a.user_id=b.user_id and a.mdd=b.mdd   -- 订单表
        group by 1,2,3
    ) a 
    group by 1
)
,c_sdbo as (
    select dt
          ,sum(s_all_UV_c) s_all_UV_c
          ,sum(d_s_UV_c) d_s_UV_c
          ,sum(b_ds_UV_c) b_ds_UV_c
          ,sum(o_ds_order_c) o_ds_order_c
          ,sum(c_uv) c_uv
          ,sum(order_user_cnt_c) order_user_cnt_c
    from (
        select t1.dt
            ,t1.mdd
            ,t1.user_type
            ,count(distinct case when search_pv >0 then  t1.uid else null end) as s_all_UV_c
            ,count(distinct case when detail_pv >0 and search_pv >0 then  t1.uid else null end)  d_s_UV_c
            ,count(distinct case when booking_pv >0 and detail_pv >0 and search_pv >0 then  t1.uid else null end )b_ds_UV_c
            ,count(distinct case when t2.ubt_user_id is not null and detail_pv >0 and search_pv >0 then order_no else null end )o_ds_order_c
            ,count(distinct uid) as c_uv
            ,count(distinct t2.ubt_user_id) as order_user_cnt_c
        from c_uv t1 
        left join c_order t2 on t1.dt=t2.dt and t1.uid=t2.ubt_user_id and t1.mdd=t2.mdd
        group by 1,2,3
    ) t 
    group by 1
)
,qc_sdbo as (--- QC sdbo数据
    select a.dt
          ,(d_s_UV / s_all_UV) s2d
          ,(b_ds_UV / d_s_UV)  d2b
          ,(o_ds_order / b_ds_UV) b2o
          ,(d_s_UV_c / s_all_UV_c) s2d_c
          ,(b_ds_UV_c / d_s_UV_c) d2b_c
          ,(o_ds_order_c / b_ds_UV_c) b2o_c

          ,(d_s_UV / s_all_UV) / (d_s_UV_c / s_all_UV_c) s2d_qc
          ,(b_ds_UV / d_s_UV) / (b_ds_UV_c / d_s_UV_c)   d2b_qc
          ,(o_ds_order / b_ds_UV) / (o_ds_order_c / b_ds_UV_c) b2o_qc
    from q_sdbo a
    left join c_sdbo b
    on a.dt=b.dt
)

select a.dt,`星期`
      ,a.`间夜QC`
      ,a.`转化QC`
      ,concat(round((a.`转化QC-T` / c.`转化QC` -1) * 100, 1), '%') as   `转化QC-YOY`
      ,a.`新客转化QC`
      ,concat(round((a.`新客转化QC-T` / c.`新客转化QC` -1)  * 100, 1), '%') as   `新客转化QC-YOY`
      ,a.`老客转化QC`
      ,concat(round((a.`老客转化QC-T` / c.`老客转化QC` -1)  * 100, 1), '%') as   `老客转化QC-YOY`
      ,a.`Q-CR`
      ,`S2D-QC`
      ,concat(round((`S2D-T` / s2d_qc -1) * 100, 1), '%') as   `S2D-YOY`
      ,`D2B-QC`
      ,concat(round((`D2B-T` / d2b_qc -1)  * 100, 1), '%') as  `D2B-YOY`
      ,`B2O-QC`
      ,concat(round((`B2O-T` / b2o_qc -1)  * 100, 1), '%') as  `B2O-YOY`
from (
    ---- 整体
    select t1.dt
        ,date_format(t1.dt,'u') `星期`
        ,concat(round(`间夜QC` * 100, 1), '%') as `间夜QC`
        ,concat(round(`转化QC` * 100, 1), '%') as `转化QC`
        ,concat(round(`新客转化QC`  * 100, 1), '%') as `新客转化QC`
        ,concat(round(`老客转化QC`  * 100, 1), '%') as `老客转化QC`
        ,concat(round(`Q-CR`  * 100, 1), '%') as `Q-CR`
        ,concat(round(s2d_qc * 100, 1), '%')  as   `S2D-QC`
        ,concat(round(d2b_qc * 100, 1), '%')  as  `D2B-QC`
        ,concat(round(b2o_qc * 100, 1), '%') as  `B2O-QC`
        ,`转化QC` as `转化QC-T`
        ,`新客转化QC` as `新客转化QC-T`
        ,`老客转化QC` as `老客转化QC-T`
        ,s2d_qc  as   `S2D-T`
        ,d2b_qc   as  `D2B-T`
        ,b2o_qc as  `B2O-T`
    from qc_data t1   
    left join qc_sdbo t5 on t1.dt=t5.dt 
    where t1.dt >= date_sub(current_date, 30)
) a
left join qc_sdbo b on add_months(a.dt, -12) = b.dt
left join qc_data c on add_months(a.dt, -12) = c.dt
order by 1 desc
;


/*****
<h3 face="微软雅黑">用户端核心报表</h3>
|**|
<tr>
<th face="微软雅黑">日期</th>
<th face="微软雅黑">星期</th>
<th face="微软雅黑">间夜QC</th>
<th face="微软雅黑">转化QC</th>
<th style="background-color:#FFAA33" face="微软雅黑">转化-YOY</th>
<th face="微软雅黑">新客转化QC</th>
<th style="background-color:#FFAA33" face="微软雅黑">新客转化-YOY</th>
<th face="微软雅黑">老客转化QC</th>
<th style="background-color:#FFAA33" face="微软雅黑">老客转化-YOY</th>
<th face="微软雅黑">Q-CR</th>
<th face="微软雅黑">S2D-QC</th>
<th style="background-color:#FFAA33" face="微软雅黑">S2D-YOY</th>
<th face="微软雅黑">D2B-QC</th>
<th style="background-color:#FFAA33" face="微软雅黑">D2B-YOY</th>
<th face="微软雅黑">B2O-QC</th>
<th style="background-color:#FFAA33" face="微软雅黑">B2O-YOY</th>
</tr>
|**|
*****/