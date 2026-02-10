select t1.dt
      ,t1.note_id
      ,t1.note_post_time
      ,t2.seq_id
      ,t2.product_name
      ,count(1) over()
      ,count(case when t2.activity_code is not null then 1 end) over()
      ,count(case when seq_id is not null then 1 end) over()
from (
    select dt
           ,note_id
           ,note_post_time
           ,activity_code 
    from pp_pub.dwd_redbook_notes_detail_nd 
    where dt = '2025-10-16' 
        and note_busi = 'hotel-inter' 
        -- and activity_code = '202306031404'
    group by 1,2,3,4
) t1 left join (  --- 全量维表
    select seq_id,product_name,activity_code
    from smm.dim_redbook_query_info_stat_apply_org_da 
    where dt = date_sub(current_date, 1)  
        and  business_type = 'hotel-inter'
    group by 1,2,3
)t2 on t1.activity_code=t2.activity_code
order by dt, note_post_time desc
;