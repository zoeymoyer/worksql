---- 1、国酒窄口径
with user_type as
(
    select user_id,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,date_range as (   ---- 生成要看的时间序列  1101~0115
    select date_sub(current_date, n) startdate
    from (
        SELECT explode(sequence(1, 79)) as n
    )
)
,query_url_info as (---- 短视频码贴映射
    select split(query,'_')[0] query,url,real_post_date dt
    from pp_pub.dim_smm_video_callback_mapping_da
    where dt = date_sub(current_date, 2)   --- 更新时间晚t-2
        and real_post_date between date_sub(current_date, 120) and date_sub(current_date, 2)
        and (organization_name in ('北京团队国际酒店合作组') or member_name in ('王利津'))--- 国酒
        --and organization_name not in ('北京团队国际酒店合作组') and query_business_name = '国际酒店' --- 公共
        and platform = '抖音'  --- 只看抖音
)
,video_url_30d as (--- 最近30天新帖
    select t1.startdate,url,query
    from date_range t1
    left join query_url_info t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3
)
,video as   -- 换成短视频数据
(
    select  t1.dt,user_name,split(t1.query,'_')[0] query
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-10-01' and '%(FORMAT_DATE)s' and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin', 'vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-10-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹', '樊庆曦')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    where t3.query is null
    group by 1,2,3
)
,video_user_30d as (--- 最近30天短视频用户明细
    select t1.startdate,user_name,query
    from date_range t1
    left join video t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3 
)
,order_a as(  ---- 订单数据
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
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
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_30d as ( ---- 最近30天生单用户明细-新客
    select t1.startdate,user_name
    from date_range t1
    left join order_a t2 ON t2.order_date >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.order_date <= t1.startdate 
    where t2.user_type = '新客'
    group by 1,2
)

select `日期`,`生单UV`,`帖量`
      ,round(`生单UV` / `帖量`,2)   `单帖拉新`
      ,`有产帖量`
      ,round(`生单UV` / `有产帖量`,2)   `有产单帖拉新`
      ,concat(round(`有产帖量`/`帖量` * 100, 2), '%') `有产率`
from (
    select t1.startdate `日期` ,count(distinct t3.user_name) `生单UV`
        ,count(distinct case when t3.user_name is not null then url end) `有产帖量`
    from video_user_30d t1 
    left join video_url_30d t2 on t1.startdate=t2.startdate and t1.query=t2.query 
    left join order_30d t3 on t1.startdate=t3.startdate and t1.user_name=t3.user_name
    where t2.query is not null
    group by 1 
)t1
left join (select startdate,count(distinct url) `帖量` from video_url_30d group by 1) t2 on t1.`日期`=t2.startdate 
order by 1 desc
;

---- 2、公共口径
with user_type as
(
    select user_id,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,date_range as (   ---- 生成要看的时间序列  1101~0115
    select date_sub(current_date, n) startdate
    from (
        SELECT explode(sequence(1, 79)) as n
    )
)
,query_url_info as (---- 短视频码贴映射
    select split(query,'_')[0] query,url,real_post_date dt
    from pp_pub.dim_smm_video_callback_mapping_da
    where dt = date_sub(current_date, 2)   --- 更新时间晚t-2
        and real_post_date between date_sub(current_date, 120) and date_sub(current_date, 2)
         and organization_name not in ('北京团队国际酒店合作组') and query_business_name = '国际酒店' and member_name not in ('王利津') --- 公共
        and platform = '抖音'  --- 只看抖音
)
,video_url_30d as (--- 最近30天新帖
    select t1.startdate,url,query
    from date_range t1
    left join query_url_info t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3
)
,video as   -- 换成短视频数据
(
    select  t1.dt,user_name,split(t1.query,'_')[0] query
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-10-01' and '%(FORMAT_DATE)s' and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin', 'vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-10-01'
        -- and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹', '樊庆曦')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    -- left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    -- where t3.query is null
    group by 1,2,3
)
,video_user_30d as (--- 最近30天短视频用户明细
    select t1.startdate,user_name,query
    from date_range t1
    left join video t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3 
)
,order_a as(  ---- 订单数据
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
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
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_30d as ( ---- 最近30天生单用户明细-新客
    select t1.startdate,user_name
    from date_range t1
    left join order_a t2 ON t2.order_date >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.order_date <= t1.startdate 
    where t2.user_type = '新客'
    group by 1,2
)


select `日期`,`生单UV`,`帖量`
      ,round(`生单UV` / `帖量`,2)   `单帖拉新`
      ,`有产帖量`
      ,round(`生单UV` / `有产帖量`,2)   `有产单帖拉新`
      ,concat(round(`有产帖量`/`帖量` * 100, 2), '%') `有产率`
from (
    select t1.startdate `日期` ,count(distinct t3.user_name) `生单UV`
        ,count(distinct case when t3.user_name is not null then url end) `有产帖量`
    from video_user_30d t1 
    left join video_url_30d t2 on t1.startdate=t2.startdate and t1.query=t2.query 
    left join order_30d t3 on t1.startdate=t3.startdate and t1.user_name=t3.user_name
    where t2.query is not null
    group by 1 
)t1
left join (select startdate,count(distinct url) `帖量` from video_url_30d group by 1) t2 on t1.`日期`=t2.startdate 
order by 1 desc
;

---- 3、国酒窄口径分人
with user_type as
(
    select user_id,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,date_range as (   ---- 生成要看的时间序列  1101~0115
    select date_sub(current_date, n) startdate
    from (
        SELECT explode(sequence(1, 79)) as n
    )
)
,query_url_info as (---- 短视频码贴映射
    select split(query,'_')[0] query,url,real_post_date dt,member_name
    from pp_pub.dim_smm_video_callback_mapping_da
    where dt = date_sub(current_date, 2)   --- 更新时间晚t-2
        and real_post_date between date_sub(current_date, 120) and date_sub(current_date, 2)
        and organization_name in ('北京团队国际酒店合作组') --- 国酒
        --and organization_name not in ('北京团队国际酒店合作组') and query_business_name = '国际酒店' --- 公共
        and platform = '抖音'  --- 只看抖音
)
,video_url_30d as (--- 最近30天新帖
    select t1.startdate,url,query,member_name
    from date_range t1
    left join query_url_info t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3,4
)
,video as   -- 换成短视频数据
(
    select  t1.dt,user_name,split(t1.query,'_')[0] query,member_name
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-10-01' and '%(FORMAT_DATE)s' and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin', 'vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-10-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹', '樊庆曦')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    where t3.query is null
    group by 1,2,3,4
)
,video_user_30d as (--- 最近30天短视频用户明细
    select t1.startdate,user_name,query,member_name
    from date_range t1
    left join video t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3,4
)
,order_a as(  ---- 订单数据
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
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
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_30d as ( ---- 最近30天生单用户明细-新客
    select t1.startdate,user_name
    from date_range t1
    left join order_a t2 ON t2.order_date >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.order_date <= t1.startdate 
    where t2.user_type = '新客'
    group by 1,2
)


select `日期`,t1.member_name,`生单UV`,`帖量`
      ,round(`生单UV` / `帖量`,2)   `单帖拉新`
      ,`有产帖量`
      ,round(`生单UV` / `有产帖量`,2)   `有产单帖拉新`
      ,concat(round(`有产帖量`/`帖量` * 100, 2), '%') `有产率`
from (
    select startdate
           ,member_name
           ,count(distinct url) `帖量` 
    from video_url_30d 
    group by 1,2
)t1
left join (
    select t1.startdate `日期` 
         ,t1.member_name
         ,count(distinct t3.user_name) `生单UV`
         ,count(distinct case when t3.user_name is not null then url end) `有产帖量`
    from video_user_30d t1 
    left join video_url_30d t2 on t1.startdate=t2.startdate and t1.query=t2.query 
    left join order_30d t3 on t1.startdate=t3.startdate and t1.user_name=t3.user_name
    where t2.query is not null
    group by 1,2
) t2 on t1.startdate=t2.`日期`  and t1.member_name=t2.member_name
order by 1 desc,`生单UV` desc
;



with user_type as
(
    select user_id,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt='%(DATE)s'
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch') and is_valid='1'
        and order_status not in ('CANCELLED','REJECTED')
    group by 1
)
,date_range as (   ---- 生成要看的时间序列  1101~0115
    select date_sub(current_date, n) startdate
    from (
        SELECT explode(sequence(1, 79)) as n
    )
)
,query_url_info as (---- 短视频码贴映射
    select split(query,'_')[0] query,url,real_post_date dt
    from pp_pub.dim_smm_video_callback_mapping_da
    where dt = date_sub(current_date, 2)   --- 更新时间晚t-2
        and real_post_date between date_sub(current_date, 120) and date_sub(current_date, 2)
        -- and organization_name in ('北京团队国际酒店合作组') --- 国酒
        and organization_name not in ('北京团队国际酒店合作组') and query_business_name = '国际酒店' --- 公共
        and platform = '抖音'  --- 只看抖音
)
,video_url_30d as (--- 最近30天新帖
    select t1.startdate,url,query
    from date_range t1
    left join query_url_info t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3
)
,video as   -- 换成短视频数据
(
    select  t1.dt,user_name,split(t1.query,'_')[0] query,member_name
    from (
        SELECT  query
                ,user_name
                ,dt
                ,potential_new_flag
        FROM pp_pub.dwd_redbook_global_flow_detail_di t1
        WHERE dt between '2025-10-01' and '%(FORMAT_DATE)s' and nvl(t1.user_name ,'')<>'' and t1.user_name is not null and lower(t1.user_name)<>'null'
    ) t1 
    inner join 
    (
        select 
            t1.dt
            ,t1.query
            ,member_name
            -- ,second_group_level_desc as member_group
            ,page
        FROM pp_pub.dim_video_query_mapping_da t1 
        left join (
            select query,page,url from pp_pub.dim_video_query_url_cid_mapping_nd
            where platform in ('douyin', 'vedio')
            ) t2 
        on t1.query_ori = t2.query
        where dt >= '2025-10-01'
        and member_name in ('吴卓奇','梅开砚','林梦雨','梁一佳','郭锦芳','王利津','方霁雪', '朱贝贝', '王斯佳wsj', '李雪莹', '樊庆曦')
    ) t2 
    on t1.query = t2.query 
    and t1.dt = t2.dt
    left join (select distinct query from temp.temp_zeyz_yang_hotel_intel_ug_vedio_query_info) t3 on split(t1.query,'_')[0]  = t3.query
    where t3.query is null
    group by 1,2,3,4
)
,video_user_30d as (--- 最近30天短视频用户明细
    select t1.startdate,user_name,query,member_name
    from date_range t1
    left join video t2 ON t2.dt >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.dt <= t1.startdate 
    group by 1,2,3,4
)
,order_a as(  ---- 订单数据
    select order_date
            ,case when province_name in ('澳门','香港') then province_name  when a.country_name in ('泰国','日本','韩国','新加坡','马来西亚','美国','印度尼西亚','俄罗斯') then a.country_name  when e.area in ('欧洲','亚太','美洲') then e.area else '其他' end as mdd
            ,case when order_date = b.min_order_date then '新客' else '老客' end as user_type 
            ,a.user_id,init_gmv,order_no,room_night,a.user_name
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
        and order_date >= '2025-10-01' and order_date <= date_sub(current_date, 1)
        and order_no <> '103576132435'
)
,order_30d as ( ---- 最近30天生单用户明细-新客
    select t1.startdate,user_name
    from date_range t1
    left join order_a t2 ON t2.order_date >= date_sub(t1.startdate, 29)  --- 取最近30天数据
    AND t2.order_date <= t1.startdate 
    where t2.user_type = '新客'
    group by 1,2
)


select t1.startdate,member_name,count(distinct t3.user_name) order_uv,count(distinct url) urls,count(distinct t3.user_name) / count(distinct url) per_order_uv
       ,count(distinct case when t3.user_name is not null then url end) yc_urls
from video_user_30d t1 
left join video_url_30d t2 on t1.startdate=t2.startdate and t1.query=t2.query 
left join order_30d t3 on t1.startdate=t3.startdate and t1.user_name=t3.user_name
where t2.query is not null
group by 1,2 order by 1
;


