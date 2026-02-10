with datainfo as (
        select * 
        from 
        (
            select t1.*
            ,row_number() over (partition by url order by dt desc) as rn --因为视频带的码有多码对应多人问题,所以要去重
            from    smm.dws_smm_video_own_product_note_level_statistic_da t1 
            where dt >= date_sub(current_date,2)
            and post_date between '2025-08-15' and '2025-09-11'
            and team_name in ('高玺组','姝冶组','达人IP1组','达人IP2组','达人IP3组','达人IP8组','北京团队国际机票合作组','北京团队国际酒店合作组')
            and video_busi_type in ('机票','酒店','火车','门票','国际机票','国际酒店')
        ) t1 
        where rn = 1
)
,datainfo2 as (
        select * 
        from 
        (
            select t1.*
            ,row_number() over (partition by url order by dt desc) as rn --因为视频带的码有多码对应多人问题,所以要去重
            from    smm.dws_smm_video_own_product_note_level_statistic_da t1 
            where dt >= date_sub(current_date,2)
            and post_date between '2025-08-15' and '2025-09-11'
            and team_name in ('北京团队国际机票合作组','北京团队国际酒店合作组')
            and video_busi_type in ('国际机票','国际酒店')
        ) t1 
        where rn = 1
)
,query_data as (
        select distinct post_date,url
        from (
                select explode(split(regexp_replace(query_series, '\\[|\\]', ''),',')) query,post_date,url
                from 
                (
                    select t1.*
                        ,row_number() over (partition by url order by dt desc) as rn --因为视频带的码有多码对应多人问题,所以要去重
                    from   smm.dws_smm_video_own_product_note_level_statistic_da t1 
                    where dt >= date_sub(current_date,2)
                    and post_date between '2025-08-15' and '2025-09-11'
                ) t1 
                where rn = 1
        ) 
        where query in ('722140','748213','384377','537338','745746','237852','917514','278622','717753','735979','213974','242696','369356','842843','441362','247931','905370','556187','861677','352021','757411','873092','893711','225498','475179','513277','227213','846563','226892','812273','652683','809498','593411','429089','730621','233775','616865','419354','402124','481029','253786','242595','986817','407338','556931','830674','251799','963135','251371','349123','428750','256924','963845','684914','259367','259965','348867','436262','263166','329780','559801','821806','923838','667821','324580','611627','754181','910999','967115','702844')
)

select 
video_busi_type as `视频属性/组属性`
,video_platform as `平台`
,count(distinct member_name) as `发帖人数`
,count(distinct url) as `发帖量`
,count(distinct url)/count(distinct member_name) as `人均产贴`
-- ,sum(read_cnt) as `曝光量`
,sum(read_cnt)/count(distinct url) as `单帖曝光`
,count(distinct case when read_cnt >= 10000 then url end) as `万播帖量`
,count(distinct case when read_cnt >= 10000 then url end)/count(distinct url) as `万播率`
-- ,count(distinct case when new_uv >= 1 then url end) as `有产帖量`
,count(distinct case when new_uv >= 1 then url end)/count(distinct url) as `有产帖率`
,sum(new_uv)/count(distinct case when new_uv >= 1 then url end) as `有产单帖拉新`
,count(distinct case when ((video_busi_type in ('机票','火车') and new_uv >= 30) or (video_busi_type in ('国内酒店') and new_uv >= 15)) then url end) as `爆帖量`
,count(distinct case when ((video_busi_type in ('机票','火车') and new_uv >= 30) or (video_busi_type in ('国内酒店') and new_uv >= 15)) then url end)/count(distinct url) as `爆帖率`
,sum(query_uv)/sum(read_cnt) as `曝光引流比`
,sum(query_new_uv)/sum(query_uv) as `引流潜新占比`
,sum(new_uv)/sum(query_new_uv) as `新客转化率`
,sum(new_uv) as `新客量`
,sum(inter_flight_order_new_uv) as `国际机票业务新`
,sum(inter_flight_order_cnt) as `国际机票订单量`
from 
(
        select t1.*
        from  datainfo t1
        join query_data t2 on t1.post_date=t2.post_date and t1.url=t2.url
) t1 
group by 1,2

union all 

select 
team_name as `视频属性/组属性`
,video_platform as `平台`
,count(distinct member_name) as `发帖人数`
,count(distinct url) as `发帖量`
,count(distinct url)/count(distinct member_name) as `人均产贴`
-- ,sum(read_cnt) as `曝光量`
,sum(read_cnt)/count(distinct url) as `单帖曝光`
,count(distinct case when read_cnt >= 10000 then url end) as `万播帖量`
,count(distinct case when read_cnt >= 10000 then url end)/count(distinct url) as `万播率`
-- ,count(distinct case when new_uv >= 1 then url end) as `有产帖量`
,count(distinct case when new_uv >= 1 then url end)/count(distinct url) as `有产帖率`
,sum(new_uv)/count(distinct case when new_uv >= 1 then url end) as `有产单帖拉新`
,count(distinct case when ((video_busi_type in ('机票','火车') and new_uv >= 30) or (video_busi_type in ('国内酒店') and new_uv >= 15)) then url end) as `爆帖量`
,count(distinct case when ((video_busi_type in ('机票','火车') and new_uv >= 30) or (video_busi_type in ('国内酒店') and new_uv >= 15)) then url end)/count(distinct url) as `爆帖率`
,sum(query_uv)/sum(read_cnt) as `曝光引流比`
,sum(query_new_uv)/sum(query_uv) as `引流潜新占比`
,sum(new_uv)/sum(query_new_uv) as `新客转化率`
,sum(new_uv) as `新客量`
,sum(inter_flight_order_new_uv) as `国际机票业务新`
,sum(inter_flight_order_cnt) as `国际机票订单量`
from 
(
        select t1.*
        from  datainfo2 t1
        join query_data t2 on t1.post_date=t2.post_date and t1.url=t2.url
) t1 
group by 1,2