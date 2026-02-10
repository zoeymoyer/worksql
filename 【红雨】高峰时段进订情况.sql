SELECT 
    t2.supplier_id,
    LPAD(t2.booking_hour, 2, '0') booking_hour, 
    t2.gt,
    t2.`B2O-房态房价一致率`,
    t1.preSubmitTotal AS `进订次数（总）`,
    t1.totalFail AS `进订失败次数（总）`,
    t1.totalPriceChange AS `变价次数（总）`,
    t2.supplierTotalPreSubmit AS `供应商进订次数（总）`,
    t2.supplierTotalFail AS `供应商进订失败次数（总）`,
    t2.supplierTotalPriceChange AS `供应商变价次数（总）`,
    ROUND((t2.supplierTotalFail / t1.totalFail) * 100, 2) AS `失败占比`,
    ROUND((t2.supplierTotalFail / t1.preSubmitTotal) * 100, 2) AS `失败贡献`,
    ROUND((t2.supplierTotalPriceChange / t1.totalPriceChange) * 100, 2) AS `变价占比`,
    ROUND((t2.supplierTotalPriceChange / t1.preSubmitTotal) * 100, 2) AS `变价贡献`
FROM  (
    SELECT 
          hour(log_time) AS booking_hour,  
          gt,
          NVL(COUNT(IF((country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), TRUE, NULL)), 0) AS preSubmitTotal,
          NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                      AND err_message != '领券人与入住人不符', TRUE, NULL)), 0) AS totalFail,
          NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                      AND err_message LIKE '%价格变动%', TRUE, NULL)), 0) AS totalPriceChange
      FROM  (
          SELECT 
              log_time,
              ret,
              country_name,
              province_name,
              SPLIT(room_id, '\\_')[1] AS supplier_id,
              err_message
              ,minute(log_time) booking_min
          FROM 
              dw_user_app_submit_qta_di
          WHERE 
              dt BETWEEN '20250723' AND '20250723'
              AND source = 'app_intl'
              AND platform IN ('adr', 'ios', 'AndroidPhone', 'iPhone')
      ) AS base
      left join (
          select  id 
                  ,ntile(6) over(order by id) group
                  ,case when ntile(6) over(order by id) = 1 then '[00-10]'
                        when ntile(6) over(order by id) = 2 then '[10-20]'
                        when ntile(6) over(order by id) = 3 then '[20-30]'
                        when ntile(6) over(order by id) = 4 then '[30-40]'
                        when ntile(6) over(order by id) = 5 then '[40-50]'
                        when ntile(6) over(order by id) = 6 then '[50-60]'
                    end gt
          from  (select  split(space(59), ' ') as arr ) t 
          lateral view posexplode(arr) pe as id, val
      ) t on base.booking_min=id
      GROUP BY 1,2
) AS t1 
JOIN (
    SELECT 
        supplier_id,
        hour(log_time) AS booking_hour, 
        gt, 
        ROUND((1 - (total_submit_fail - total_submit_coupon) / total_submit_count) * 100, 2) AS `B2O-房态房价一致率`,
        NVL(COUNT(IF((country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), TRUE, NULL)), 0) AS supplierTotalPreSubmit,
        NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                    AND err_message != '领券人与入住人不符', TRUE, NULL)), 0) AS supplierTotalFail,
        NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                    AND err_message LIKE '%价格变动%', TRUE, NULL)), 0) AS supplierTotalPriceChange
    FROM  (
        SELECT 
            log_time,
            minute(log_time) AS booking_min, 
            ret,
            country_name,
            province_name,
            SPLIT(room_id, '\\_')[1] AS supplier_id,
            err_message,
          
            NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), 
                        TRUE, NULL)) OVER (PARTITION BY SPLIT(room_id, '\\_')[1], date_format(log_time, 'H:00')), 0) AS total_submit_fail,
            NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                        AND err_message = '领券人与入住人不符', TRUE, NULL)) OVER (PARTITION BY SPLIT(room_id, '\\_')[1], date_format(log_time, 'H:00')), 0) AS total_submit_coupon,
            NVL(COUNT(IF((country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), TRUE, NULL)) OVER (PARTITION BY SPLIT(room_id, '\\_')[1], date_format(log_time, 'H:00')), 0) AS total_submit_count
        FROM 
            dw_user_app_submit_qta_di
        WHERE 
            dt BETWEEN '20250723' AND '20250723'
            AND source = 'app_intl'
            AND platform IN ('adr', 'ios', 'AndroidPhone', 'iPhone')
    ) AS base
    left join (
        select  id 
                ,ntile(6) over(order by id) group
                ,case when ntile(6) over(order by id) = 1 then '[00-10]'
                      when ntile(6) over(order by id) = 2 then '[10-20]'
                      when ntile(6) over(order by id) = 3 then '[20-30]'
                      when ntile(6) over(order by id) = 4 then '[30-40]'
                      when ntile(6) over(order by id) = 5 then '[40-50]'
                      when ntile(6) over(order by id) = 6 then '[50-60]'
                  end gt
        from  (select  split(space(59), ' ') as arr ) t 
        lateral view posexplode(arr) pe as id, val
      )t on base.booking_min=t.id
    GROUP BY supplier_id,hour(log_time),gt,total_submit_fail,total_submit_coupon,total_submit_count
) AS t2  ON   t1.booking_hour = t2.booking_hour  and t1.gt=t2.gt

;



select booking_10min
       ,count(distinct hotel_seq) `酒店数量`
       ,count(distinct supplier_id) `代理商数量`
       ,count(distinct case when `提交通过率` < 50 then hotel_seq end) `酒店数量(<49%)`
       ,count(distinct case when `提交通过率` < 50 then supplier_id end) `代理商数量(<49%)`

       ,count(distinct case when `提交通过率` < 60 then hotel_seq end) `酒店数量(50%-59%)`
       ,count(distinct case when `提交通过率` < 60 then supplier_id end) `代理商数量(50%-59%)`

       ,count(distinct case when `提交通过率` < 70 then hotel_seq end) `酒店数量(60%-69%)`
       ,count(distinct case when `提交通过率` < 70 then supplier_id end) `代理商数量(60%-69%)`

       ,count(distinct case when `提交通过率` < 80 then hotel_seq end) `酒店数量(70%-79%)`
       ,count(distinct case when `提交通过率` < 80 then supplier_id end) `代理商数量(70%-79%)`

       ,count(distinct case when `提交通过率` < 90 then hotel_seq end) `酒店数量(80%-89%)`
       ,count(distinct case when `提交通过率` < 90 then supplier_id end) `代理商数量(80%-89%)`

       ,count(distinct case when `提交通过率` < 100 then hotel_seq end) `酒店数量(90%-99%)`
       ,count(distinct case when `提交通过率` < 100 then supplier_id end) `代理商数量(90%-99%)`

       ,count(distinct case when `提交通过率` = 100 then hotel_seq end) `酒店数量(100%)`
       ,count(distinct case when `提交通过率` = 100 then supplier_id end) `代理商数量(100%)`
from (

SELECT 
    t2.supplier_id,
    t2.hotel_seq,
    t2.country_name,
    t2.booking_10min,  -- 10分钟级时间
    t2.`B2O-房态房价一致率`,
    t1.preSubmitTotal AS `进订次数（总）`,
    t1.totalFail AS `进订失败次数（总）`,
    t1.totalPriceChange AS `变价次数（总）`,
    t2.supplierTotalPreSubmit AS `供应商酒店进订次数（总）`,
    t2.supplierTotalFail AS `供应商酒店进订失败次数（总）`,
    t2.supplierTotalPriceChange AS `供应商酒店变价次数（总）`,
    ROUND((t2.supplierTotalFail / t1.totalFail) * 100, 2) AS `失败占比`,
    ROUND((t2.supplierTotalFail / t1.preSubmitTotal) * 100, 2) AS `失败贡献`,
    ROUND((t2.supplierTotalPriceChange / t1.totalPriceChange) * 100, 2) AS `变价占比`,
    ROUND((t2.supplierTotalPriceChange / t1.preSubmitTotal) * 100, 2) AS `变价贡献`
    ,ROUND((1- totalFail / preSubmitTotal) * 100, 2) `提交通过率`
FROM 
    -- 总指标汇总表（按国家和10分钟）
    (SELECT 
        country_name,
        -- 计算10分钟时间段
        CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10) AS booking_10min,
        NVL(COUNT(IF((country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), TRUE, NULL)), 0) AS preSubmitTotal,
        NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                    AND err_message != '领券人与入住人不符', TRUE, NULL)), 0) AS totalFail,
        NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                    AND err_message LIKE '%价格变动%', TRUE, NULL)), 0) AS totalPriceChange
    FROM 
        (SELECT 
            log_time,
            ret,
            country_name,
            province_name,
            SPLIT(room_id, '\\_')[1] AS supplier_id,
            hotel_seq,  -- 添加hotel_seq字段
            err_message
        FROM 
            dw_user_app_submit_qta_di
        WHERE 
            dt BETWEEN '20250723' AND '20250723'
            AND source = 'app_intl'
            AND platform IN ('adr', 'ios', 'AndroidPhone', 'iPhone')) AS base
    GROUP BY 
        country_name,
        CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10)) AS t1  -- 按国家和10分钟分组
JOIN 
    -- 供应商+酒店维度指标表（按国家和10分钟）
    (SELECT 
        supplier_id,
        hotel_seq,
        country_name,
        CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10) AS booking_10min,  -- 10分钟级时间
        HOUR(log_time) booking_hour,
        ROUND((1 - (total_submit_fail - total_submit_coupon) / total_submit_count) * 100, 2) AS `B2O-房态房价一致率`,
        NVL(COUNT(IF((country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), TRUE, NULL)), 0) AS supplierTotalPreSubmit,
        NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                    AND err_message != '领券人与入住人不符', TRUE, NULL)), 0) AS supplierTotalFail,
        NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                    AND err_message LIKE '%价格变动%', TRUE, NULL)), 0) AS supplierTotalPriceChange
    FROM 
        (SELECT 
            log_time,
            ret,
            country_name,
            province_name,
            SPLIT(room_id, '\\_')[1] AS supplier_id,
            hotel_seq,  -- 添加hotel_seq字段
            err_message,
            -- 计算失败相关指标（按国家、供应商、酒店和10分钟）
            NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), 
                        TRUE, NULL)) OVER (PARTITION BY country_name, SPLIT(room_id, '\\_')[1], hotel_seq, 
                                          CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10)), 0) AS total_submit_fail,
            NVL(COUNT(IF((ret = 'false' OR ret IS NULL) AND (country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')) 
                        AND err_message = '领券人与入住人不符', TRUE, NULL)) OVER (PARTITION BY country_name, SPLIT(room_id, '\\_')[1], hotel_seq,
                                                                                 CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10)), 0) AS total_submit_coupon,
            NVL(COUNT(IF((country_name != '中国' OR province_name IN ('香港', '澳门', '台湾')), TRUE, NULL)) OVER (PARTITION BY country_name, SPLIT(room_id, '\\_')[1], hotel_seq,
                                                                                                             CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10)), 0) AS total_submit_count
        FROM 
            dw_user_app_submit_qta_di
        WHERE 
            dt BETWEEN '20250723' AND '20250723'
            AND source = 'app_intl'
            AND platform IN ('adr', 'ios', 'AndroidPhone', 'iPhone')) AS base
    GROUP BY 
        supplier_id,
        hotel_seq,
        country_name,
        HOUR(log_time),
        CONCAT(HOUR(log_time), ':', FLOOR(MINUTE(log_time)/10)*10),  -- 按国家、供应商、酒店和10分钟分组
        total_submit_fail,
        total_submit_coupon,
        total_submit_count) AS t2
ON 
    t1.country_name = t2.country_name AND t1.booking_10min = t2.booking_10min  -- 按国家和10分钟关联
    where t2.booking_hour in ('08','09','10','18')  --- 高峰时段
ORDER BY 
    t2.booking_10min, 
    t2.country_name,
    t2.supplier_id,
    t2.hotel_seq
) group by 1 order by 1
;