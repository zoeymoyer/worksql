with q_order_max as (
    select 
       user_id,
       count(distinct order_no) as order_cnt,
       max(order_date) as max_order_date
    from default.mdw_order_v3_international a 
        where dt = regexp_replace(date_sub(current_date,1) ,'-','')
        and (province_name in ('台湾','澳门','香港') or a.country_name !='中国') 
        and is_valid='1'
        and terminal_channel_type in ('www','app','touch')
        and order_date<='${FDATE}'
        and order_status not in ('CANCELLED','REJECTED')
        and user_id is not null
        group by 1 
)
,user_type as (
    select user_id
        ,min(order_date) as min_order_date
    from default.mdw_order_v3_international
    where dt = regexp_replace(date_sub(current_date,1) ,'-','')
        and (province_name in ('台湾','澳门','香港') or country_name !='中国')
        and terminal_channel_type in ('www','app','touch')
        and order_status not in ('CANCELLED','REJECTED')
        and is_valid='1'
    group by 1
)
,order_a as (
    select distinct a.user_id
    from user_type a
    where min_order_date<>date_sub(current_date,1) 
        and min_order_date<>current_date      -- 剔除昨日新用户
)
,order_b as (
    select user_id
        ,order_date
        ,order_no
        ,final_payamount_price as user_real_pay_amount
        ,case when (batch_series like '%23base_ZK_728810%' or batch_series like '%23extra_ZK_ce6f99%')
                        then (init_commission_after+nvl(split(coupon_info['23base_ZK_728810'],'_')[1],0)+nvl(split(coupon_info['23extra_ZK_ce6f99'],'_')[1],0)+nvl(ext_plat_certificate,0))
                        else init_commission_after+nvl(ext_plat_certificate,0) end as q_commission
    from default.mdw_order_v3_international a
    where dt = regexp_replace(date_sub(current_date,1) ,'-','')
    and (province_name in ('台湾','澳门','香港') or a.country_name !='中国')
    and terminal_channel_type in ('www','app','touch')
    and is_valid='1'
    and order_status not in ('CANCELLED','REJECTED')
    and order_date<=date_sub(current_date,1)
)
 ,user_data as
 (
   select
   a.user_id
   ,datediff(current_date,max(order_date)) r
   ,count(distinct order_no) order_cnt
   ,sum(user_real_pay_amount)user_real_pay_amount
   ,sum(q_commission) as q_commission
   ,sum(user_real_pay_amount)/count(distinct order_no) avg_order_amount
   ,sum(q_commission)/count(distinct order_no) avg_arpu
   from order_a a
   left join order_b b on a.user_id=b.user_id
   group by 1
 )

,ave as
(
  select
   avg( r) avg_r
  ,avg( order_cnt) avg_order_cnt
  ,avg(avg_order_amount) avg_pay_amount
  ,avg(avg_arpu) avg_arpu_amount
  ,avg(q_commission) avg_q_commission
  from user_data
)
 
,user_group as
(
  select user_id
  ,case when last_r_score =1 and last_f_score=1 and last_m_score=1  then '重要价值用户'
  when last_r_score =1 and last_f_score=0 and last_m_score=1 then '重要发展用户'
  when last_r_score =0 and last_f_score=1 and last_m_score=1 then '重要召回用户'
  when last_r_score =0 and last_f_score=0 and last_m_score=1 then '重要挽留用户'
  when last_r_score =1 and last_f_score=1 and last_m_score=0 then '一般价值用户'
  when last_r_score =1 and last_f_score=0 and last_m_score=0 then '一般发展用户'  
  when last_r_score =0 and last_f_score=1 and last_m_score=0 then '一般召回用户'
  when last_r_score =0 and last_f_score=0 and last_m_score=0 then '一般挽留用户'
  else null end as user_rfm_value
  from
  (
    select
    a.user_id
    ,case when  r<=avg_r then 1 else 0 end last_r_score
    ,case when  order_cnt>=avg_order_cnt then 1 else 0 end last_f_score
    ,case when q_commission>= avg_q_commission then 1 else 0 end last_m_score
    from user_data a
    left join ave b
    )a
)

insert overwrite table ihotel_default.ads_ihotel_user_group_all partition(dt = '${FDATE}')
select distinct '${FDATE}' as data_date
        ,case when datediff('${FDATE}',t1.max_order_date) between 0 and 243 then '活跃'
             when datediff('${FDATE}',t1.max_order_date) between 244 and 424 then '沉默'
             when datediff('${FDATE}',t1.max_order_date)>424 then '流失' else '活跃'
            end as cycle_label
            ,case when t1.order_cnt>=1 and t1.order_cnt<5 then '次新' 
                  when t1.order_cnt>=5 then '老客' else '新客' end as user_type
            ,nvl(case when datediff('${FDATE}',t1.max_order_date) between 0 and 243 then 
               case 
                    when t1.order_cnt>=1 and t1.order_cnt<5 then '活跃-次新'
                    else '活跃-老客' end 
             when datediff('${FDATE}',t1.max_order_date) between 244 and 424 then 
                 case 
                    when t1.order_cnt>=1 and t1.order_cnt<5 then '沉默-次新'
                    else '沉默-老客' end 
             when datediff('${FDATE}',t1.max_order_date)>424 then       
               case 
                    when t1.order_cnt>=1 and t1.order_cnt<5 then '流失-次新'
                    else '流失-老客' end 
            end,'活跃-新客') as user_label
            ,t1.user_id
            ,t2.user_rfm_value
       from q_order_max t1 
       left join user_group t2 on t1.user_id=t2.user_id
;