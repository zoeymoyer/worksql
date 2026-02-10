select * from (
select `国家`,`城市`,`酒店星级`,`酒店名称`,hotel_seq
        ,round((`销量得分`*5.5+`产品力得分`*2.5+`评分得分`*0.5+`评论数得分`*0.5+`房间数得分`*0.5+`缺陷率得分`*0.5),2) as `总得分`
        ,row_number() over(partition by `国家`,`城市` order by round((`销量得分`*5.5+`产品力得分`*2.5+`评分得分`*0.5+`评论数得分`*0.5+`房间数得分`*0.5+`缺陷率得分`*0.5),2) desc) as rn
        ,`间夜量` ,`城市间夜排名` ,`销量得分`
        ,`酒店评分` ,`评分得分`
        ,`酒店评论数` ,`评论数得分`
        ,`抓取次数`,`券前价beat率`,`单间夜券前价价差`,`beat价差深度`,`产品力得分`
        ,`房间数`,`房间数得分`
        ,`加权缺陷率`,`缺陷率得分`
        ,`券前支付价`,`单间夜房费`
from (
    select `国家`,`城市`,`酒店星级`,`酒店名称`,hotel.hotel_seq
            ,`间夜量`,rk as `城市间夜排名`
            ,case when rk<=20 then '1'
                  when rk>20  and rk<=50 then '0.8'
                  when rk>50  and rk<=100 then '0.6'
                  when rk>100 and rk<=150 then '0.4'
                  when rk>150 and rk<=200 then '0.2'
                  when rk>200 then '0' else '0' end as `销量得分`
            ,`酒店评分`
            ,case when `酒店评分`>=4.7 then '1'
                  when `酒店评分`>=4.4 and `酒店评分`<4.7 then '0.8'
                  when `酒店评分`>=4.1 and `酒店评分`<4.4 then '0.6'
                  when `酒店评分`<4.1  then '0' else '0' end as `评分得分`
            ,round(comm_count,0) as `酒店评论数`
            ,case when comm_count>=500 then '1'
                  when comm_count>=100 and comm_count<500 then '0.8'
                  when comm_count>=50 and comm_count<100 then '0.6'
                  when comm_count>=20 and comm_count<50 then '0.4'
                  when comm_count<20 then '0.2' else '0' end as `评论数得分`
            ,`抓取次数`
            ,concat(round(`券前价beat率`*100,2),'%') as `券前价beat率`
            ,round(`单间夜券前价价差`,2) as `单间夜券前价价差`
            ,concat(round(`beat价差深度`*100,2),'%') as `beat价差深度`
            ,case when `券前价beat率`>0.9 and ((0-`beat价差深度`)>=0.1 or (0-`单间夜券前价价差`)>=200) then '1'
                  when `券前价beat率`>0.9 and (((0-`beat价差深度`)>=0.05 and (0-`beat价差深度`)<0.1) or ((0-`单间夜券前价价差`)>=100 and (0-`单间夜券前价价差`)<200)) then '0.8'
                  when `券前价beat率`>0.9 and (((0-`beat价差深度`)>=0.03 and (0-`beat价差深度`)<0.05) or ((0-`单间夜券前价价差`)>=20 and (0-`单间夜券前价价差`)<100)) then '0.6'
                  when `券前价beat率`>0.9 and (((0-`beat价差深度`)>=0.03 and (0-`beat价差深度`)<0.05) or (0-`单间夜券前价价差`)<20) then '0.4' else '0' end as `产品力得分`
            ,hotel_room_count as `房间数`
            ,case when hotel_room_count>=500 then '1'
                  when hotel_room_count>=200 and hotel_room_count<500 then '0.8'
                  when hotel_room_count>=100 and hotel_room_count<200 then '0.6'
                  when hotel_room_count<100 then '0.4' else '0' end as `房间数得分`
            ,round(`加权缺陷率`,4) as `加权缺陷率`
            ,case when `加权缺陷率`=0   then '1'
                  when `加权缺陷率`>0   and hotel_room_count<=0.5 then '0.6'
                  when `加权缺陷率`>0.5 and hotel_room_count<=1 then '0.4'
                  when `加权缺陷率`>1   then '0' else '0' end as `缺陷率得分`
            ,`券前支付价`,`单间夜房费`
    from(-- 酒店信息
        select distinct country_name as `国家` ,city_name as `城市` ,hotel_grade as `酒店星级` ,hotel_seq ,hotel_name as `酒店名称` ,cast(hotel_room_count as DOUBLE) as hotel_room_count
        from default.dim_hotel_info_intl_v3
        where dt='%(DATE)s'
        and hotel_operating_status='营业中' 
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
    )hotel
    left join(-- 销量排名
        select country_name ,city_name ,hotel_seq ,`间夜量` ,cast(rank() over (partition by city_name order by `间夜量` desc) as DOUBLE) as rk
        from (
            select country_name ,city_name ,hotel_seq ,sum(room_night) as `间夜量`
            from default.mdw_order_v3_international
            where dt='%(DATE)s'
                and (province_name in ('台湾','澳门','香港') or country_name !='中国')
                and terminal_channel_type in ('www','app','touch') and is_valid='1'
                and order_status not in ('CANCELLED','REJECTED')
                and order_date>='2026-01-01'
            group by 1,2,3
        )biao
    )night on hotel.hotel_seq=night.hotel_seq
    left join(-- 评分&评论数
        select hotel_seq ,round(nvl(global_score,global_reference_score),1) as `酒店评分` ,cast(comm_count['total'] as DOUBLE) as comm_count
        from default.dim_hotel_score
        where dt='%(DATE)s'
    )score on hotel.hotel_seq=score.hotel_seq
    left join(-- 酒店最低价比价结果
        SELECT hotel_seq ,count(DISTINCT uniq_id) AS `抓取次数`,
            -- 酒店最低价
            cast((count(case when room_type_cover="Qmeet" and ctrip_room_status="true" and qunar_room_status="true" and before_coupons_cashback_price_compare_result = "Qbeat" and compare_type="HOTEL_LOWEST" then 1 end)
                / count(case when room_type_cover="Qmeet" and ctrip_room_status="true" and qunar_room_status="true" and compare_type="HOTEL_LOWEST" then 1 end)) as DOUBLE) AS `券前价beat率`,
            cast(avg(case when room_type_cover="Qmeet" and ctrip_room_status="true" and qunar_room_status="true" and before_coupons_cashback_price_compare_result = "Qbeat" and compare_type="HOTEL_LOWEST" then before_coupons_cashback_price_diff/datediff end) as DOUBLE) as `单间夜券前价价差`,
            cast((avg(case when room_type_cover="Qmeet" and ctrip_room_status="true" and qunar_room_status="true" and before_coupons_cashback_price_compare_result = "Qbeat" and compare_type="HOTEL_LOWEST" then before_coupons_cashback_price_diff/datediff end)
                /avg(case when room_type_cover="Qmeet" and ctrip_room_status="true" and qunar_room_status="true" and before_coupons_cashback_price_compare_result = "Qbeat" and compare_type="HOTEL_LOWEST" then ctrip_before_coupons_cashback_price/datediff end)) as DOUBLE) as `beat价差深度`
        FROM(
            select a.dt, a.uniq_id, a.crawl_time, a.id,
                    a.compare_type,
                    a.hotel_seq, a.qunar_physical_room_id,
                    a.country_name, a.province_name,
                    a.check_in ,a.check_out ,datediff(a.check_out,a.check_in) as datediff ,
                    a.room_type_cover, a.ctrip_room_status, a.qunar_room_status,
                    a.before_coupons_cashback_price_compare_result,
                    a.qunar_before_coupons_cashback_price, a.ctrip_before_coupons_cashback_price ,a.before_coupons_cashback_price_diff
            from default.dwd_hotel_cq_compare_price_result_intl_hi a
            where dt>='20260101'
            and substr(crawl_time,1,10)>='2026-01-01' 
        ) biao
        Group by 1
    )CQ on hotel.hotel_seq=CQ.hotel_seq
    left join(-- 国际缺陷更改
        select hotel_seq ,h as `产单量`,
            cast(((a/h*3.5)+(b/h*0.2)+(c/h)+(d/h)+(e/h*0.3)+(f/h*0.3)) as DOUBLE) as `加权缺陷率`
        from (
            select hotel_seq ,
                count(distinct case when complain_type='到店无房' then a.order_no else null end) as a,
                count(distinct case when complain_type='到店无预订' then a.order_no else null end) as b,
                count(distinct case when complain_type='确认后满房' then a.order_no else null end) as c,
                count(distinct case when complain_type='确认后涨价' then a.order_no else null end) as d,
                count(distinct case when complain_type='确认前满房' then a.order_no else null end) as e,
                count(distinct case when complain_type='确认前涨价' then a.order_no else null end) as f,
                count(distinct case when defect_type='无拒单' then a.order_no else null end) as g,
                count(distinct a.order_no) as h
            from default.dw_order_servicequality_info a
            left join (
                select order_no,hotel_seq 
                from default.mdw_order_v3_international
                where dt='%(DATE)s'
            )b on a.order_no=b.order_no
            where a.dt='%(DATE)s'
                and (a.country!='中国' or a.province in ('台湾','澳门','香港'))
                and ( ( (a.balance_type='PROXY' OR a.is_guarantee=1) and (a.pay_status NOT IN ('PAY','PAY_FAILED')) ) OR (a.balance_type='CASH' and a.is_guarantee='0') )
            group by 1
        ) aa
    )complain on hotel.hotel_seq=complain.hotel_seq
    left join(
        select hotel_seq
            ,round((sum(init_payamount_price)+sum(nvl(`用券金额`,0)))/sum(room_night),0) as `券前支付价`
            ,round(sum(init_room_fee)/sum(room_night),0) as `单间夜房费`
        from(
            select country_name ,city_name ,hotel_seq ,init_room_fee ,room_night ,init_payamount_price
                ,case when (coupon_substract is null or batch_series in ('MacaoDisco_ZK_5e27de','2night_ZK_952825','3night_ZK_ad8c83') or batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%') then 0 else nvl(coupon_substract,0) end as `用券金额`
            from default.mdw_order_v3_international
            where dt='%(DATE)s'
            and (province_name in ('台湾','澳门','香港') or country_name !='中国')
            and terminal_channel_type in ('www','app','touch') and is_valid='1'
            and order_status not in ('CANCELLED','REJECTED')
            and order_date >= '2026-01-01' 
        )biao
        group by 1
    )fee on hotel.hotel_seq=fee.hotel_seq
) t
) t
where rn <= 100
order by `国家`,`城市`,`总得分` desc