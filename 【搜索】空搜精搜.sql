with q_search as 
(SELECT
  dt 
 ,concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as `日期`
-- ,country_name
-- ,hotel_seq
, user_id 
FROM
  default.dwd_ihotel_flow_app_searchlist_di a
where  dt >= '%(DATE_7)s' and dt<= '%(DATE)s'
and is_query = '1' 
 and is_filter='0'
 and (intention = 'brand' or (suggest_type = 'brand' and intention is null) 
                       or (suggest_type = 'group' and intention is null) or intention = 'group'
                       or (intention = 'hotelName' or (suggest_type = 'hotelName' and intention is null))
 and ( province_name in ('台湾', '澳门', '香港') or country_name != '中国' ))
and orig_device_id is not null
and orig_device_id != ''
and search_type in (0, 16, 17)
      group by 1,2,3
)--（精搜-包含 部分匹配 酒店名）+酒店品牌
)
,q_uv as
(        select a.dt as `日期`
--     ,case when province_name in ('澳门','香港') then province_name when a.country_name in ('日本','泰国','马来西亚','韩国','阿联酋','新加坡','美国') then a.country_name else '其他' end as `目的地`
 -- ,case      when a.hotel_grade in (3) then '中星'
 --     when a.hotel_grade in (4, 5) then '高星'
 --     else '低星' end as hotel_grade
--        ,case when concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) > b.min_order_date then '老客' else '新客' end as user_type 
 ,case when b.user_id is not null then '精搜' else '非精搜' end as search_type
        ,count(distinct if((search_pv + detail_pv + booking_pv + order_pv)>0,a.user_id,null)) as q_uv
        from ihotel_default.mdw_user_app_log_sdbo_di_v1 a 
        left join q_search b on a.user_id = b.user_id and a.dt=b. `日期`  --and a.hotel_seq=b.hotel_seq
        where a.dt >= date_sub(current_date,8) and a.dt<= date_sub(current_date,1)
                and business_type = 'hotel'
                and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
                and (search_pv + detail_pv + booking_pv + order_pv)>0
        group by 1,2
)
,q_order as (
  select
    order_date,
--  case when a.hotel_grade in (3) then '中星'
 --     when a.hotel_grade in (4, 5) then '高星'
 --     else '低星' end as hotel_grade, 
--  case when province_name in ('澳门','香港') then province_name when a.country_name in ('日本','泰国','马来西亚','韩国','阿联酋','新加坡','美国') then a.country_name else '其他' end as `目的地`,
  case when b.user_id is not null then '精搜' else '非精搜' end as search_type,
  -- 统计预订间隔及停留时长
count(distinct a.user_id) as `Q_下单客户数`,
  count(distinct order_no) as `Q_订单数`,
  sum(init_gmv) as `Q_GMV`,
    sum(room_night) as `Q_间夜量`,
    sum(case   when (    batch_series like '%23base_ZK_728810%'  or batch_series like '%23extra_ZK_ce6f99%'  ) then (   init_commission_after + nvl(   split(coupon_info ['23base_ZK_728810'], '_') [1],    0  ) + nvl(  split(coupon_info ['23extra_ZK_ce6f99'], '_') [1],0 ) + nvl(ext_plat_certificate, 0)  )     else init_commission_after + nvl(ext_plat_certificate, 0) end  ) `Q_佣金额`,
    sum( case  when (    coupon_substract is null      or batch_series like '%23base_ZK_728810%'     or batch_series like '%23extra_ZK_ce6f99%' ) then 0  else nvl(coupon_substract, 0)  end ) `Q_优惠金额`
  from   mdw_order_v3_international a
  left join q_search b on a.order_date=b.`日期` and a.user_id=b.user_id --and a.hotel_seq=b.hotel_seq
  where  a.dt = '%(DATE)s' ---分区日期为当前日期的前一天
 and order_date>=date_sub(current_date,8) and order_date<=date_sub(current_date,1)
    and (    province_name in ('台湾', '澳门', '香港')   or a.country_name != '中国'   ) ----港澳台&海外
   and order_status not in ('CANCELLED', 'REJECTED') ----非取消&拒单 用于观察CR变化趋势而获取一段时间的订单时应替换为下方代码
    -- and (first_cancelled_time is null or date(first_cancelled_time) > order_date) and (first_rejected_time is null or date(first_rejected_time) > order_date) and (refund_time is null or date(refund_time) > order_date)---非当天取消&拒单
  and terminal_channel_type= 'app'
--    and terminal_channel_type in ('www', 'app', 'touch') ---用户终端类型 只筛选app数据可替换为 terminal_channel_type= 'app'
    and is_valid = '1' ---有效单
  group by 1,2
)

  select 
  a.search_type,
-- a.hotel_grade,
  sum(q_uv) as q_uv,
  sum(`Q_订单数`) `Q_订单数`,
  sum(`Q_间夜量`) `Q_间夜量`,
  sum(`Q_GMV`) `Q_GMV`,
  sum(`Q_优惠金额`) `Q_优惠金额`
  from q_uv a
  left join q_order b on a.search_type=b.search_type and a.`日期`=b.order_date --and a.`目的地`=b.`目的地` and a.hotel_grade=b.hotel_grade
  group by 1--,2

;



----空搜
    select
        distinct  orig_device_id,
                 dt,
                 search_request_uid,
                 hotel_seq,
                 qpayprice,user_id
    from
        default.dwd_ihotel_flow_app_searchlist_di
    where
        dt>='20250615'
      and orig_device_id is not null
      and orig_device_id != ''
      and (province_name in ('台湾','澳门','香港') or country_name !='中国')
      and search_type in (0, 16, 17)
      and is_display = 1
      and is_filter=0
      and is_query=0
;
--- 非空搜
select
    distinct orig_device_id,
                dt,
                search_request_uid,
                hotel_seq,
                qpayprice,user_id
from
    default.dwd_ihotel_flow_app_searchlist_di
where
    dt>='20250627'
    and orig_device_id is not null
    and orig_device_id != ''
    and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    and search_type in (0, 16, 17)
    and is_display = 1
    and (is_filter !=0 or is_query!=0)