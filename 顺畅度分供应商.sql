with temp_l2d_detail as (
    select dt ,log_id,root_city_code,root_city_name,
           ceil(regexp_extract(params,'&preListPrice=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as listPrice,
           ceil(regexp_extract(params,'&orderPriceLog=([^&]*)',1)/if( datediff(checkout_date,checkin_date)<=0,1,datediff(checkout_date,checkin_date) )) as low_price,
           regexp_extract(params,'&orderAll=([^&]*)',1) as is_hotel_full,
           get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.makeUp') as is_make_up,
           split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] as supplier_id,
           get_json_object(regexp_extract(params,'extra=([^&]+)',1),'$.matchAdult') as match_adult,
           action_entrance_map['fromforlog'] as is_list
    from ihotel_default.dw_user_app_log_detail_visit_di_v1
    where dt between date_sub(current_date,16) and date_sub(current_date,1)
      and source='hotel'
      and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
      and regexp_extract(params,'&fromList=([^&]*)',1)='true'
      --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
      and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
      and (country_name!='中国' or province_name in('香港','澳门','台湾'))
      and user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874')

),
     top_20_supplier as (

         select
             split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] as supplier_id
              ,count(distinct qtrace_id) as cnt
         from
             ihotel_default.dw_user_app_log_detail_visit_di_v1
         where dt =date_sub(current_date,1)
           and source='hotel'
           and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
           and regexp_extract(params,'&fromList=([^&]*)',1)='true'
           --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
           and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
           and (country_name!='中国' or province_name in('香港','澳门','台湾'))
           and split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] is not null
           and user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874')

         group by 1
         order by cnt desc
         limit 20
     ),
     top_other_supplier as (

         select
             '0' as supplier_id
              ,count(distinct qtrace_id) as cnt

         from
             (

                 select
                     split(get_json_object(regexp_extract(params,'&lowestPriceInfo=([^&]*)',1),'$.roomId'),'\\_')[1] as supplier_id
                      ,qtrace_id
                 from
                     ihotel_default.dw_user_app_log_detail_visit_di_v1
                 where dt =date_sub(current_date,1)
                   and source='hotel'
                   and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
                   and regexp_extract(params,'&fromList=([^&]*)',1)='true'
                   --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
                   and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
                   and (country_name!='中国' or province_name in('香港','澳门','台湾'))
                   and user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874')

             ) t
         where t.supplier_id not in (select distinct supplier_id from top_20_supplier where supplier_id is not null and supplier_id <> '' and supplier_id <> 'NULL')

     ),
     top_all_supplier as (
         select
             supplier_id,
             cnt
         from
             (
                 select
                     *
                 from
                     top_20_supplier
                 union all
                 select
                     *
                 from
                     top_other_supplier
             ) t

     ),
     all_pv as (
         select
             count(distinct qtrace_id) as allpv
         from
             ihotel_default.dw_user_app_log_detail_visit_di_v1
         where dt =date_sub(current_date,1)
           and source='hotel'
           and ((platform='ios' and  app_version>80011172) or (platform='adr' and  app_version>60001255))
           and regexp_extract(params,'&fromList=([^&]*)',1)='true'
           --and (action_entrance_map['fromforlog']=0 or action_entrance_map['fromforlog']=131)
           and regexp_extract(params,'&fromDetail=([^&]*)',1)='false'
           and (country_name!='中国' or province_name in('香港','澳门','台湾'))
           and user_id not in ('150822769','338486393','324973057','200516815','192614594','265324698','323552428','264279849','160831394','209885579','270425361','257187213','161356781','270439318','301721923','175764702','241068766','282485301','300014995','712426070','7937418','440572550','235860052','237045427','291310481','296104243','157611717','290876522','238909252','249201114','264361211','439440377','281977582','311048741','283176527','156762707','161752520','367222878','8723086','142240948','175795418','202156311','241484198','1324216348','156903351','178005856','193923149','235084473','1415490823','171501312','234444616','202918199','232233133','283291887','284354209','196106160','198349768','208916989','263966569','295570060','1535166244','157386454','159793424','256116607','785380','124106302','300277966','319364993','1249066','159455315','168120066','230477857','134484152','156840991','160287204','232078784','275538127','408453812','261771591','191516817','9749800','11438368','1501932601','1532018526','136605158','379492272','308729850','414832481','271792257','315915487','158693788','260959689','997888414','156491104','244919952','127791314','156706079','223152307','262441763','289880942','915019667','1424308429','208278240','318493485','152259749','123638512','143634113','167628843','160387255','268331746','906764390','135391922','1522916797','233623890','247007700','314967684','140333830','6793206','281901855','452828174','236467651','121747848','170675567','318156641','377339262','296476061','363519624','229859551','256717793','197085704','278575089','227117','253066590','1561113894','140140286','307108223','635523920','271151604','271417189','170919301','212633976','230804322','255548595','364890042','135987974','146523467','151101117','158381541','158842269','282184223','319576993','121100892','122353704','212356265','247918722','373077843','207656359','196586566','213122676','253049047','277006428','6638420','136662328','255670674','1324501966','144866925','166302812','182274336','230506848','235003407','268080910','272741724','313725970','674481596','868662605','8921670','141442372','173123470','5526354','940705106','9424496','131312358','176455032','187579298','198325780','245872058','256045551','260201545','295123420','311768573','126836254','129863660','207351063','301268237','322882674','6601732','123577110','127393856','128157982','152700988','154390305','1590730982','242582053','268518833','2991110','1076488780','149507814','151249812','172524846','9751908','207863048','229376072','256382194','268330373','310075889','400302327','133501280','193047005','232385065','269347602','282016870','285443056','311937041','425085746','436566626','215618293','239308294','261420135','287275977','299162394','225250470','248183965','285011137','291025564','314310340','402483552','878998469','9790582','1453820893','206204268','220474988','248229220','272166899','409485500','6496584','200447110','248794607','253489910','309886440','262597874')

     )



-- 1615667、800001361、800000191、800000188、800000650、1625282、1617599、800000224、1625519、800001370、800000731、800000665、800000533、800000227、1623242、800000194、800000539、800000518、800000677、800000863

select
    concat(nvl(name,''),'【',case when t.supplier_id = '0' then '其他代理商' else t.supplier_id end,'】') as `代理商`,
    concat(round(supplier_flow.cnt/allpv * 100, 2),'%') as `流量占比`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE)s' THEN bad_case_rate ELSE NULL END) `bad_case占比`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE)s' THEN all_case_rate ELSE NULL END) `case占比`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE)s' THEN `L2D-代理商影响大盘-房态房价一致率` ELSE NULL END) AS `L2D-影响大盘一致率`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE)s' THEN `D2B-代理商影响大盘-房态房价一致率` ELSE NULL END) AS `D2B-影响大盘一致率`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE)s' THEN `B2O-代理商影响大盘-房态房价一致率` ELSE NULL END) AS `B2O-影响大盘一致率`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_1)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_1)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_3)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_3)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_4)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_4)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_5)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_5)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_6)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_6)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_7)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_7)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_7)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_7)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_8)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_8)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_9)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_9)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_11)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_11)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_12)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_12)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_SUB_13)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_SUB_13)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_13)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_13)s`,
    MAX(CASE WHEN datas = '%(FORMAT_DATE_14)s' THEN smoothness ELSE NULL END) AS `%(FORMAT_DATE_14)s`

from
    (
        select
            coalesce(l2d.datas,d2b.booking_date,b2o.booking_date) as datas,
            coalesce(l2d.supplier_id, d2b.supplier_id, b2o.supplier_id) as supplier_id,
            round(nvl((`L2D-房态房价一致率`/100),1)*nvl((`D2B-房态房价一致率`/100),1)*nvl((`B2O-房态房价一致率`/100),1)*100,2) as smoothness,
            `L2D-代理商影响大盘-房态房价一致率`,
            `D2B-代理商影响大盘-房态房价一致率`,
            `B2O-代理商影响大盘-房态房价一致率`,
            concat(round((`L2D-代理商badcase数`+`D2B-代理商badcase数`+`B2O-代理商badcase数`)/(`L2D-大盘badcase数`+`D2B-大盘badcase数`+`B2O-大盘badcase数`)*100,2),'%') bad_case_rate,
            concat(round((`L2D-代理商case数`+`D2B-代理商case数`+`B2O-代理商case数`)/(`L2D-大盘case数`+`D2B-大盘case数`+`B2O-大盘case数`)*100,2),'%') all_case_rate
        from
-- L2D
(select datas,
        supplier_id,
        round((1-(b-e)/(a-e))*100,2) as `L2D-房价一致率`,
        round((1-e/a)*100,2) as `L2D-房态一致率`,
        round((1-(b-e)/(a-e))*(1-e/a)*100,2) as `L2D-房态房价一致率`,
        round((b/total_a) *100,3) as `L2D-代理商影响大盘-房态房价一致率`,
        b as `L2D-代理商badcase数`,
        total_b as `L2D-大盘badcase数`,
        a as `L2D-代理商case数`,
        total_a as `L2D-大盘case数`

 from
     (
         select datas ,supplier_id,a,b,e,SUM(a) OVER (PARTITION BY datas) AS total_a,SUM(b) OVER (PARTITION BY datas) AS total_b from
             (select a.dt as  datas,
                     case when supplier.supplier_id is not null then supplier.supplier_id else '0' end as supplier_id,
                     count(distinct case when (listPrice is not null and listPrice not in (-1,0)) then log_id end) as a,
                     count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0 or (low_price is not null and low_price != 0 and listPrice!=low_price and (listPrice - low_price) NOT BETWEEN 0 AND 1)) and is_hotel_full='false' then log_id  else null end)  as b,
                     count(distinct case when (listPrice is not null and listPrice not in (-1,0)) and (low_price is null or low_price =0) and is_hotel_full='false' then log_id  else null end)  as e

              from
                  temp_l2d_detail a
                      left join
                  top_20_supplier supplier on a.supplier_id = supplier.supplier_id
              where match_adult != 'false' or match_adult is null
              group by 1,2) temp) a) l2d
-- D2B
    left join
(select a.booking_date,
        supplier_id,
        round((1-b/c)*100,2) as `D2B-房态一致率`,
        round((1-a/(c-b))*100,2) as `D2B-房价一致率`,
        round((1-b/c)*(1-a/(c-b))*100,2) as `D2B-房态房价一致率`,
        round((state_price_case/total_all_case) *100,3) as `D2B-代理商影响大盘-房态房价一致率`,
        state_price_case as `D2B-代理商badcase数`,
        total_state_price_case as `D2B-大盘badcase数`,
        all_case as `D2B-代理商case数`,
        total_all_case as `D2B-大盘case数`
 from
     (select concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2)) as  booking_date,
             case when supplier.supplier_id is not null then supplier.supplier_id else '0' end as supplier_id,

             round(count(distinct case when ischange='true' and ret='true' and (country_name!='中国' or province_name in('香港','澳门')) then q_trace_id else null end)) as a,
             count(distinct if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门')),q_trace_id,null)) as b,
             count(distinct if((country_name!='中国' or province_name in('香港','澳门')),q_trace_id,null)) as c,
             count(distinct q_trace_id) as all_case,
             SUM(count(distinct q_trace_id)) OVER (PARTITION BY concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) AS total_all_case,
             round(count(distinct case when ((ischange='true' and ret='true') or (ret='false' or ret is null) ) then q_trace_id else null end)) as state_price_case,
             SUM(COUNT(distinct case when ((ischange='true' and ret='true') or (ret='false' or ret is null) ) then q_trace_id else null end)) OVER (PARTITION BY concat(substr(a.dt,1,4),'-',substr(a.dt,5,2),'-',substr(a.dt,7,2))) AS total_state_price_case

      from
          (select dt,log_time,q_trace_id,ret,country_name,province_name,err_code,err_message,err_sys,ischange,city_name,supplier_id
           from view_dw_user_app_booking_qta_di
           where dt between '%(DATE_15)s' and '%(DATE)s'
             and source='app_intl'
             and platform in ('adr','ios')
             and (province_name in ('香港','澳门','台湾') or country_name!='中国')
             and q_trace_id not like 'f_inter_autotest%'
             and get_json_object(regexp_extract(params,'roomExtraInfo=([^&]+)',1),'$.crawStrategy') not in ('Interrupt','Lightly_Price'))a
              left join
          top_20_supplier supplier on a.supplier_id = supplier.supplier_id
      group by 1,2) a) d2b
on l2d.datas=d2b.booking_date and l2d.supplier_id=d2b.supplier_id
-- B2O
    left join
(select booking_date,
        supplier_id,
        round((1-(total_submit_fail-total_submit_coupon)/total_submit_count)*100,2) as `B2O-房态房价一致率`,
        round((state_price_case/total_all_case) *100,3) as `B2O-代理商影响大盘-房态房价一致率`,
        state_price_case as `B2O-代理商badcase数`,
        total_state_price_case as `B2O-大盘badcase数`,
        all_case as `B2O-代理商case数`,
        total_all_case as `B2O-大盘case数`
 from
     (select to_date(log_time) as booking_date,
             case when supplier.supplier_id is not null then supplier.supplier_id else '0' end as supplier_id,

             count(if((ret='false' or ret is null)  and (country_name!='中国' or province_name in('香港','澳门','台湾')),true,null)) as total_submit_fail,
             count(if((ret='false' or ret is null) and (country_name!='中国' or province_name in('香港','澳门','台湾')) and err_message='领券人与入住人不符' ,true,null)) as total_submit_coupon,
             count(if((country_name!='中国' or province_name in('香港','澳门','台湾')) ,true,null)) as total_submit_count,

             count(*) as all_case,
             SUM(COUNT(*)) OVER (PARTITION BY to_date(log_time)) AS total_all_case,
             count(if((ret='false' or ret is null) and err_message!='领券人与入住人不符' ,true,null)) as state_price_case,
             SUM(COUNT(IF((ret = 'false' OR ret IS NULL) AND err_message != '领券人与入住人不符', TRUE, NULL))) OVER (PARTITION BY to_date(log_time)) AS total_state_price_case

      from dw_user_app_submit_qta_di order_t
               left join
           top_20_supplier supplier on order_t.supplier_id = supplier.supplier_id
      where dt between '%(DATE_15)s' and '%(DATE)s'
        and source='app_intl'
        and platform in ('adr','ios','AndroidPhone','iPhone')
-- 排除风控
        and err_code not in( '-98','784','785')
        and (country_name!='中国' or province_name in('香港','澳门','台湾'))
      group by 1,2) y) b2o
on l2d.datas=b2o.booking_date and  l2d.supplier_id=b2o.supplier_id
    ) t

        left join

    ihotel_default.ods_qta_supplier supplier

    on t.supplier_id = supplier.id and supplier.dt = '%(DATE)s'

        left join top_all_supplier supplier_flow
                  on t.supplier_id = supplier_flow.supplier_id
        left join all_pv pv

group by 1,cnt,allpv
order by cast(split(`流量占比`,'%')[0] as double) desc