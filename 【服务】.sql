with servicequality as (
    select order_no
          ,hotel_id
          ,complain_type
          ,checkin_date
          ,checkout_date
          ,country
          ,province
          ,balance_type
          ,is_guarantee
          ,pay_status
          ,case
               when defect_type is null then complain_type
               else defect_type
           end as complain_type_new
          ,dt
          ,amount
          ,country
          ,province
          ,hotel_name
          ,substr(defect_type_create_date, 1, 10) as defect_type_create_date
    from fuwu.dwd_ord_htl_servicequality_di
    where dt between date_sub(current_date, 365) and date_sub(current_date, 1)
        and sale_channel = 'Q2Q'   -- 勿动
        and is_international = '1'  -- 勿动
        and order_status <> 'DELETE'
        and (((balance_type = 'PROXY' or is_guarantee = 1) and pay_status not in ('PAY', 'PAY_FAILED')) or (balance_type = 'CASH' and is_guarantee = '0'))  -- 勿动
        and checkout_date >= date_sub(current_date, 30)
        and checkout_date <= date_sub(current_date, 1)
        -- and substr(defect_type_create_date,1,10) >= date_sub(current_date,30)
)
,q_order as ( -- 订单明细表
    select order_date
          ,case when province_name in ('澳门', '香港') then province_name
                when a.country_name in ('泰国', '日本', '韩国', '新加坡', '马来西亚', '美国', '印度尼西亚', '俄罗斯') then a.country_name
                when e.area in ('欧洲', '亚太', '美洲') then e.area
                else '其他'
           end as mdd
          ,a.user_id
          ,init_gmv
          ,order_no
          ,room_night
          ,hotel_grade
          ,hotel_seq
    from default.mdw_order_v3_international a
    left join temp.temp_yiquny_zhang_ihotel_area_region_forever e on a.country_name = e.country_name
    where dt = from_unixtime(unix_timestamp() - 86400, 'yyyyMMdd')
        and (province_name in ('台湾', '澳门', '香港') or a.country_name != '中国')
        and terminal_channel_type in ('www', 'app', 'touch')
        and is_valid = '1'
        and order_no <> '103576132435'
)

select t1.defect_type_create_date
      ,t1.checkin_date
      ,t1.checkout_date
      ,datediff(t1.checkin_date, t1.defect_type_create_date) as diff_days
      ,t2.mdd
      ,t1.country
      ,t1.hotel_name
      ,t2.hotel_seq
      ,t2.init_gmv
      ,t1.order_no
from servicequality t1
left join q_order t2 on t1.order_no = t2.order_no
where t1.complain_type_new in ('确认后满房', '确认后涨价')
order by 1 desc
;



with service_defect_rate_new as (
    select checkin_date as `入住日期`
          ,total as `产单量`
          ,round(((a / total * 3.5) + (b / total * 0.2) + (c / total) + (d / total) + (e / total * 0.3) + (f / total * 0.3)) * 100, 2) as `加权缺陷率`
          ,round(((a / total * 3.5) + (b / total * 0.2) + (c / total) + (d / total) + (e / total * 0.3)) * 100, 2) as `s加权缺陷率`
          ,round(a / total * 3.5 * 100, 2) as `到店无房率`
          ,round(b / total * 0.2 * 100, 2) as `到店无预订率`
          ,round((e + f) / total * 0.3 * 100, 2) as `确认前推翻率`
          ,round((c + d) / total * 100, 2) as `确认后推翻率`
          ,a as `到店无房`
          ,b as `到店无预订`
          ,(e + f) as `确认前推翻`
          ,(c + d) as `确认后推翻`
          ,total - a - b - c - d - e - f as `无拒单`
    from (
        select checkin_date
              ,count(distinct case when complain_type_new = '到店无房' then order_no else null end) as a
              ,count(distinct case when complain_type_new = '到店无预订' then order_no else null end) as b
              ,count(distinct case when complain_type_new = '确认后满房' then order_no else null end) as c
              ,count(distinct case when complain_type_new = '确认后涨价' then order_no else null end) as d
              ,count(distinct case when complain_type_new = '确认前满房' then order_no else null end) as e
              ,count(distinct case when complain_type_new = '确认前涨价' then order_no else null end) as f
              ,count(distinct case when complain_type_new in ('无拒单', '非缺陷') then order_no else null end) as i
              ,count(distinct order_no) as total
        from (
            select order_no
                  ,hotel_id
                  ,complain_type
                  ,checkin_date
                  ,country
                  ,province
                  ,balance_type
                  ,is_guarantee
                  ,pay_status
                  ,case when defect_type is null then complain_type else defect_type end as complain_type_new
            from fuwu.dwd_ord_htl_servicequality_di
            where dt between '%(FORMAT_DATE_365)s' and '%(FORMAT_DATE)s'
                and sale_channel = 'Q2Q'   -- 勿动
                and is_international = '1'  -- 勿动
                and order_status <> 'DELETE'
                and (((balance_type = 'PROXY' or is_guarantee = 1) and pay_status not in ('PAY', 'PAY_FAILED')) or (balance_type = 'CASH' and is_guarantee = '0'))  -- 勿动
                and checkin_date >= date_sub(current_date, 14)
                and checkin_date <= date_sub(current_date, 1)
        ) aa
        group by checkin_date
    ) bb
)

select a1.`入住日期`
      -- ,a1.`产单量`
      ,a1.`加权缺陷率` as `服务缺陷率`
      ,a1.`s加权缺陷率` as `s加权缺陷率`
      ,a1.`确认前推翻率`
      ,a1.`确认后推翻率`
      ,a1.`到店无预订率`
      ,a1.`到店无房率`
      ,a1.`确认前推翻`
      ,a1.`确认后推翻`
      ,a1.`到店无预订`
      ,a1.`到店无房`
      ,a1.`无拒单`
from service_defect_rate_new a1
;


with service_defect_rate_new as (
    select checkin_date as `入住日期`
          ,total as `产单量`
          ,round(((a / total * 3.5) + (b / total * 0.2) + (c / total) + (d / total) + (e / total * 0.3) + (f / total * 0.3)) * 100, 2) as `加权缺陷率`
          ,round(((a / total * 3.5) + (b / total * 0.2) + (c / total) + (d / total) + (e / total * 0.3)) * 100, 2) as `s加权缺陷率`
          ,round(a / total * 3.5 * 100, 2) as `到店无房率`
          ,round(b / total * 0.2 * 100, 2) as `到店无预订率`
          ,round((e + f) / total * 0.3 * 100, 2) as `确认前推翻率`
          ,round((c + d) / total * 100, 2) as `确认后推翻率`
          ,a as `到店无房`
          ,b as `到店无预订`
          ,(e + f) as `确认前推翻`
          ,(c + d) as `确认后推翻`
          ,total - a - b - c - d - e - f as `无拒单`
    from (
        select checkin_date
              ,count(distinct case when complain_type_new = '到店无房' then order_no else null end) as a
              ,count(distinct case when complain_type_new = '到店无预订' then order_no else null end) as b
              ,count(distinct case when complain_type_new = '确认后满房' then order_no else null end) as c
              ,count(distinct case when complain_type_new = '确认后涨价' then order_no else null end) as d
              ,count(distinct case when complain_type_new = '确认前满房' then order_no else null end) as e
              ,count(distinct case when complain_type_new = '确认前涨价' then order_no else null end) as f
              ,count(distinct case when complain_type_new in ('无拒单', '非缺陷') then order_no else null end) as i
              ,count(distinct order_no) as total
        from (
            select order_no
                  ,hotel_id
                  ,complain_type
                  ,checkin_date
                  ,country
                  ,province
                  ,balance_type
                  ,is_guarantee
                  ,pay_status
                  ,case when defect_type is null then complain_type else defect_type end as complain_type_new
            from fuwu.dwd_ord_htl_servicequality_di
            where dt between '%(FORMAT_DATE_365)s' and '%(FORMAT_DATE)s'
                and sale_channel = 'Q2Q'   -- 勿动
                and is_international = '1'  -- 勿动
                and order_status <> 'DELETE'
                and (((balance_type = 'PROXY' or is_guarantee = 1) and pay_status not in ('PAY', 'PAY_FAILED')) or (balance_type = 'CASH' and is_guarantee = '0'))  -- 勿动
                and checkin_date >= date_sub(current_date, 14)
                and checkin_date <= date_sub(current_date, 1)
        ) aa
        group by 1
    ) bb
)
,res1 as (
    select a1.`入住日期`
          -- ,a1.`产单量`
          ,a1.`加权缺陷率` as `服务缺陷率`
          ,a1.`s加权缺陷率` as `s加权缺陷率`
    from service_defect_rate_new a1
)
,order_data as (
    select a.order_date
          ,count(distinct case when a.order_status not in ('REJECTED') then a.order_no else null end) as `非拒绝单量`
          ,count(distinct case when a.order_status in ('REJECTED') then a.order_no else null end) as `拒绝单量`
          ,count(distinct a.order_no) as `大盘预定订单`
    from mdw_order_v3_international a
    where a.order_date >= date_sub(current_date, 15)
        and a.order_date <= date_sub(current_date, 1)
        and a.dt = '%(DATE)s'
        and a.terminal_channel_type in ('www', 'app', 'touch')
        and a.pay_status not in (0, 'false', 'pay_failed')
        and a.pay_status is not null
        and order_status <> 'DELETE'
    -- and a.is_valid='1'
    group by 1
)
,fuwu_orders as (
    select a.order_date
          ,count(distinct a.order_no) as `拒单量`
          ,count(distinct case when b.complain_type in ('确认后满房', '确认后涨价') then a.order_no else null end) as `确认后拒单`
          ,count(distinct case when b.complain_type in ('确认前满房', '确认前涨价') then a.order_no else null end) as `确认前拒单`
          ,count(distinct case when b.complain_type in ('确认前满房', '确认前涨价', '确认后满房', '确认后涨价') then a.order_no else null end) as `拒单&确认前和确认后推翻量`
          ,count(distinct case when b.complain_type not in ('确认前满房', '确认前涨价', '确认后满房', '确认后涨价') then a.order_no else null end) as `拒单&非确认前和确认后推翻量`
    from mdw_order_v3_international a
    left join fuwu.dwd_ord_htl_servicequality_di b on a.order_no = b.order_no and b.dt >= date_sub(current_date, 15)
    where a.order_date >= date_sub(current_date, 15)
        and a.order_date <= date_sub(current_date, 1)
        and a.dt = '%(DATE)s'
        and a.terminal_channel_type in ('www', 'app', 'touch')
        and a.order_status = 'REJECTED'
    -- and a.is_valid='1'
    group by 1
)
,res3 as (
    select a.order_date as `统计日期`
          ,concat(round((`非拒绝单量` + `确认后拒单`) / `大盘预定订单` * 100, 2), '%') as `发单成功率`
    from order_data a
    left join fuwu_orders b on a.order_date = b.order_date
    order by `统计日期` desc
)
,peifu_detail as (
    select distinct r1.id
                    ,r1.`打款时间`
                    ,r1.`订单号`
                    ,r1.`总金额`
                    ,r1.`退款金额`
                    ,(r1.`总金额` - r1.`退款金额`) as `实赔金额`
                    ,r1.`一级类型`
                    ,r2.complain_type as `校准前缺陷类型`
                    ,case when nvl(r2.complain_type, '') = '无拒单' then '非缺陷'
                          when nvl(r2.complain_type, '') = '非缺陷' then '非缺陷'
                          when r2.complain_type is null then '非缺陷'
                          else '缺陷'
                     end as `校准前是否为缺陷`
    from (
        select aa.id
              ,substr(aa.pay_time, 1, 10) as `打款时间`
              ,aa.order_no as `订单号`
              ,aa.total_amount as `总金额`
              ,aa.refund_amount as `退款金额`
              ,aa.problem1_name as `一级类型`
        from fuwu.dwd_compensate_htl_finish_to_flight_di aa
        where aa.dt between '2023-01-01' and '%(FORMAT_DATE)s'
            and substr(aa.pay_time, 1, 10) between '%(FORMAT_DATE_13)s' and '%(FORMAT_DATE)s'
    ) r1
    left join (
        select order_no
              ,complain_type
        from fuwu.dwd_ord_htl_servicequality_di
        where dt >= '%(FORMAT_DATE_365)s'
            and order_status <> 'DELETE'
    ) r2 on r1.`订单号` = r2.order_no
    inner join (
        select order_no
        from default.mdw_order_v3_international
        where dt = '%(DATE)s'
    ) r3 on r1.`订单号` = r3.order_no
)
,order_detail as (
    select checkout_date as dt
          ,count(distinct order_no) as `离店订单量`
          ,sum(room_night) as `离店间夜量`
          ,sum(final_gmv) as `qGMV`
    from default.mdw_order_v3_international
    where dt = '%(DATE)s'
        and is_valid = '1'
        and checkout_date between '%(FORMAT_DATE_13)s' and '%(FORMAT_DATE)s'
        and order_status not in ('CANCELLED', 'REJECTED', 'DELETE')
    group by 1
)
,peifu_agg as (
    select `打款时间` as dt
          ,sum(`总金额`) as amt_all
          ,sum(case when `校准前是否为缺陷` = '缺陷' then `实赔金额` end) as real_amt_defect
          ,sum(case when `校准前是否为缺陷` <> '缺陷' and `一级类型` = '页面不符' then `实赔金额` end) as real_amt_info
          ,sum(case when `校准前是否为缺陷` <> '缺陷' and `一级类型` = '价格竞争力' then `实赔金额` end) as real_amt_price
          ,sum(case when `校准前是否为缺陷` <> '缺陷' and `一级类型` not in ('价格竞争力', '页面不符') then `实赔金额` end) as real_amt_qing
          ,sum(case when `退款金额` <> 0 then `退款金额` end) as refund_amt_tui
    from peifu_detail
    group by 1
)
,zhuipei_q_agg as (
    select substr(pay_time, 1, 10) as dt
          ,sum(chase_amount) as q_refund_recovery   -- q责退款追回
    from ihotel_default.ods_ihotel_record_chase_task_info_da
    where dt = '%(FORMAT_DATE)s'
        and substr(pay_time, 1, 10) between '%(FORMAT_SUB_29)s' and '%(FORMAT_DATE)s'
        and chase_deduct_status = 2        -- 扣款成功
        and payment_responsible_party = 2  -- q责
        and wrapper_id not in (
            'hca9008oc4l', 'hca10lqv90p',
            'hca908oh60s', 'hca908oh60t',
            'hca9008pb7m', 'hca9008pb7k', 'hca908pb70q', 'hca908pb70s', 'hca908pb70r',
            'hca908lp9aj', 'hca908lp9ag', 'hca908lp9ai', 'hca908lp9ah', 'hca9008lp9v',
            'hca908lp9ak', 'hca908lp9al', 'hca908lp9am', 'hca908lp9an',
            'hca2000021p', 'hca2000021l', 'hca2000210r',
            'hca1fceu50i', 'hca1fceu50j',
            'hca1fel540i'
        )
    group by 1
)
,zhuipei_no_q_agg as (
    select substr(pay_time, 1, 10) as dt
          ,sum(pending_refund) as agent_refund_recovery       -- 代理责退款待追回
          ,sum(pending_compensation) as agent_comp_recovery   -- 代理责赔付待追回
    from ihotel_default.ods_ihotel_record_chase_task_info_da
    where dt = '%(FORMAT_DATE)s'
        and substr(pay_time, 1, 10) between '%(FORMAT_SUB_29)s' and '%(FORMAT_DATE)s'
        and chase_deduct_status = 2        -- 扣款成功
        and payment_responsible_party = 1  -- 代理责
        and wrapper_id not in (
            'hca9008oc4l', 'hca10lqv90p',
            'hca908oh60s', 'hca908oh60t',
            'hca9008pb7m', 'hca9008pb7k', 'hca908pb70q', 'hca908pb70s', 'hca908pb70r',
            'hca908lp9aj', 'hca908lp9ag', 'hca908lp9ai', 'hca908lp9ah', 'hca9008lp9v',
            'hca908lp9ak', 'hca908lp9al', 'hca908lp9am', 'hca908lp9an',
            'hca2000021p', 'hca2000021l', 'hca2000210r',
            'hca1fceu50i', 'hca1fceu50j',
            'hca1fel540i'
        )
    group by 1
)
,res4 as (
    select o.dt as `日期`
          ,round(nvl(p.amt_all, 0) / o.`离店间夜量`, 2) as `整体_单间夜赔付`
          ,round(nvl(p.real_amt_defect, 0) / o.`离店间夜量`, 2) as `库存缺陷_单间夜赔付`
          ,round(nvl(p.real_amt_info, 0) / o.`离店间夜量`, 2) as `信息缺陷_单间夜赔付`
          ,round(nvl(p.real_amt_qing, 0) / o.`离店间夜量`, 2) as `情感补偿_单间夜赔付`
          ,round(nvl(p.refund_amt_tui, 0) / o.`离店间夜量`, 2) as `线下退款_单间夜赔付`
          ,case when nvl(p.real_amt_defect, 0) + nvl(p.real_amt_info, 0) + nvl(p.refund_amt_tui, 0) = 0 then null
                else concat(round((nvl(zq.q_refund_recovery, 0) + nvl(zn.agent_refund_recovery, 0) + nvl(zn.agent_comp_recovery, 0)) / (nvl(p.real_amt_defect, 0) + nvl(p.real_amt_info, 0) + nvl(p.refund_amt_tui, 0)) * 100, 2), '%')
           end as `赔付追回率`
    from order_detail o
    left join peifu_agg p on o.dt = p.dt
    left join zhuipei_q_agg zq on o.dt = zq.dt
    left join zhuipei_no_q_agg zn on o.dt = zn.dt
)
,res5 as (
    select t1.dt
          ,concat(round(sum(case when income_type_cq = 1 then callid_cnt else 0 end) / sum(fenxiao_order_count) * 100, 2), '%') as `spo`
          ,concat(round((sum(case when income_type_cq = 1 then callid_cnt else 0 end) - sum(chat_hotel_link_cnt)) / sum(fenxiao_order_count) * 100, 2), '%') as `spo-剔除酒店im`
    from (
        select *
              ,case when income_type in ('ivr', 'chat', 'itphone') then 1 else 0 end as income_type_cq   -- 不对齐qc
        from fuwu.ads_income_crt_biz_line_di t1
        where t1.dt >= date_sub(current_date, 14)
            and t1.dt <= date_sub(current_date, 1)
            and biz_line in ('hotel')                 -- 酒店业务线
            and t1.biz_line = 'hotel'
            and t1.biz_line_full in ('hotel_inter')   -- 国际酒店业务线
    ) t1
    group by 1
)

select a.`入住日期` as `日期`
      ,date_format(a.`入住日期`, 'u') as `星期`
      ,c.`整体_单间夜赔付` as `单间夜预赔付`
      ,concat(`服务缺陷率`, '%') as `服务缺陷率`
      ,`发单成功率`
      ,concat(`s加权缺陷率`, '%') as `s加权缺陷率`
      ,c.`库存缺陷_单间夜赔付`
      ,c.`信息缺陷_单间夜赔付`
      ,c.`线下退款_单间夜赔付`
      ,c.`赔付追回率`
      ,c.`整体_单间夜赔付` - c.`库存缺陷_单间夜赔付` - c.`信息缺陷_单间夜赔付` - c.`线下退款_单间夜赔付` as `情感补偿_单间夜赔付`
      ,d.spo
      ,d.`spo-剔除酒店im`
from res1 a
left join res3 b on a.`入住日期` = b.`统计日期`
left join res4 c on c.`日期` = a.`入住日期`
left join res5 d on d.dt = a.`入住日期`
order by `日期` desc
;