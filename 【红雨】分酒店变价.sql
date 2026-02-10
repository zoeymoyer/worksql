WITH ttl_uv AS (
    SELECT dt
         , orig_device_id
         , hotel_seq AS destination
         , COUNT(DISTINCT IF((search_pv + detail_pv + order_pv + booking_pv) > 0, a.user_id, NULL)) AS q_uv
    FROM ihotel_default.mdw_user_app_log_sdbo_di_v1 a
    WHERE dt BETWEEN date_sub(current_date,7) AND date_sub(current_date,1)
      AND business_type = 'hotel'
      AND (province_name IN ('台湾','澳门','香港') OR a.country_name <> '中国')
      AND (search_pv + detail_pv + booking_pv + order_pv) > 0
    GROUP BY 1,2,3
),

-- 近7天按“目的地国家/地区”汇总UV
     uv_res AS (
         SELECT hotel_seq AS destination,
                COUNT(DISTINCT IF((search_pv + detail_pv + booking_pv + order_pv) > 0, a.user_id, NULL)) AS q_uv
         FROM ihotel_default.mdw_user_app_log_sdbo_di_v1 a
         WHERE dt BETWEEN date_sub(current_date,7) AND date_sub(current_date,1)
           AND business_type = 'hotel'
           AND (province_name IN ('台湾','澳门','香港') OR a.country_name <> '中国')
           AND (search_pv + detail_pv + booking_pv + order_pv) > 0
         GROUP BY 1
     ),

-- 取UV前20的目的地
     top20 AS (
         SELECT destination,q_uv
         FROM uv_res
         ORDER BY q_uv DESC
       
     ),

-- 目的地 -> 分组名（TOP20保留原名，其余映射为“其他国家”）
     dest_tag AS (
         SELECT u.destination,
                CASE WHEN t.destination IS NOT NULL THEN u.destination ELSE '未知' END AS destination_group
         FROM (SELECT DISTINCT destination FROM uv_res) u
                  LEFT JOIN top20 t
                            ON u.destination = t.destination
     ),

-- L页曝光：酒店卡片曝光（traceId）
     D1 AS (
         SELECT
             log_date,
             orig_device_id,
             get_json_object(value, '$.ext.traceId') AS trace_id
         FROM default.dw_qav_ihotel_track_info_di
         WHERE dt >= '%(DATE_7)s'
           and dt <= '%(DATE)s'
           AND key = 'ihotel/GDetail/priceList/show/priceCardShow'
         GROUP BY 1,2,3
     ),

-- L页变价事件（qTraceId）
     D2 AS (
         SELECT
             log_date,
             orig_device_id,
             get_json_object(value, '$.ext.traceIdPrice') AS trace_id
         FROM default.dw_qav_ihotel_track_info_di
         WHERE dt >= '%(DATE_7)s'
           and dt <= '%(DATE)s'
           AND key = 'ihotel/detail/priceList/monitor/updatePriceNoRoom'
         GROUP BY 1,2,3
     ),

-- 先得到含“目的地”的基础表（来自ttl_uv）
     L_c0 AS (
         SELECT DISTINCT a.log_date, a.orig_device_id, c.destination
         FROM D1 a
                  INNER JOIN D2 b
                             ON a.log_date = b.log_date
                                 AND a.orig_device_id = b.orig_device_id
                                 AND a.trace_id = b.trace_id
                  LEFT JOIN ttl_uv c
                            ON a.log_date = c.dt
                                AND a.orig_device_id = c.orig_device_id
     ),

-- L页总体曝光（入口曝光）+ 目的地
     L_uv0 AS (
         SELECT a.log_date,
                a.orig_device_id,
                b.destination
         FROM ihotel_default.dw_qav_hotel_track_info_di a
                  LEFT JOIN ttl_uv b
                            ON a.log_date = b.dt
                                AND a.orig_device_id = b.orig_device_id
         WHERE a.dt BETWEEN '%(DATE_7)s' AND '%(DATE)s'
           AND key = 'ihotel/GDetail/GDetailPage/monitor/GDetailContainerShow'
         GROUP BY 1,2,3
     ),

-- 把目的地映射为分组（TOP20或“其他国家”）
     L_c AS (
         SELECT lc.log_date,
                lc.orig_device_id,
                COALESCE(t.destination_group, '其他国家') AS destination_group
         FROM L_c0 lc
                  LEFT JOIN dest_tag t ON lc.destination = t.destination
     ),

     L_uv AS (
         SELECT lu.log_date,
                lu.orig_device_id,
                COALESCE(t.destination_group, '其他国家') AS destination_group
         FROM L_uv0 lu
                  LEFT JOIN dest_tag t ON lu.destination = t.destination
     ),

-- 分组维度下的变价率
     price_diff AS (
         SELECT a.log_date,
                a.destination_group,
                COUNT(1)                             AS l_uv,
                COUNT(b.orig_device_id)              AS l_var_uv,
                COUNT(b.orig_device_id) / COUNT(1.0) AS uv_per
         FROM L_uv a
                  LEFT JOIN L_c b
                            ON a.log_date = b.log_date
                                AND a.orig_device_id = b.orig_device_id
                                AND a.destination_group = b.destination_group
         GROUP BY a.log_date, a.destination_group
     ),

-- 近7天日期参数
     params AS (
         SELECT date_sub(current_date,1) AS d0,
                date_sub(current_date,2) AS d1,
                date_sub(current_date,3) AS d2,
                date_sub(current_date,4) AS d3,
                date_sub(current_date,5) AS d4,
                date_sub(current_date,6) AS d5,
                date_sub(current_date,7) AS d6
     ),

-- 把逐日变价率横向展开（列名继续用你的宏）
     pivot_rates AS (
         SELECT p.destination_group AS destination,
                MAX(CASE WHEN p.log_date = ps.d0 THEN p.uv_per END) AS `%(DATE)s`,
                MAX(CASE WHEN p.log_date = ps.d1 THEN p.uv_per END) AS `%(DATE_1)s`,
                MAX(CASE WHEN p.log_date = ps.d2 THEN p.uv_per END) AS `%(DATE_2)s`,
                MAX(CASE WHEN p.log_date = ps.d3 THEN p.uv_per END) AS `%(DATE_3)s`,
                MAX(CASE WHEN p.log_date = ps.d4 THEN p.uv_per END) AS `%(DATE_4)s`,
                MAX(CASE WHEN p.log_date = ps.d5 THEN p.uv_per END) AS `%(DATE_5)s`,
                MAX(CASE WHEN p.log_date = ps.d6 THEN p.uv_per END) AS `%(DATE_6)s`
         FROM price_diff p
                  CROSS JOIN params ps
         GROUP BY p.destination_group
     ),

-- 把UV按分组汇总，计算“近7天流量占比”
     uv_group AS (
         SELECT t.destination_group AS destination,
                SUM(u.q_uv) AS q_uv
         FROM uv_res u
                  JOIN dest_tag t ON u.destination = t.destination
         GROUP BY t.destination_group
     ),

     uv_result AS (
         SELECT destination,
                q_uv,
                ROUND(100.0 * q_uv / SUM(q_uv) OVER (), 2) AS flow_share_7d_num,
                CONCAT(ROUND(100.0 * q_uv / SUM(q_uv) OVER (), 2), '%') AS `近7天流量占比`
         FROM uv_group
     )

-- 最终输出
SELECT pr.destination AS `hotel_seq`,
       ur.`近7天流量占比`,
       concat(round(100 * pr.`%(DATE)s`,2),'%')      AS `%(DATE)s`,
       concat(round(100 * pr.`%(DATE_1)s`,2),'%')     AS `%(DATE_1)s`,
       concat(round(100 * pr.`%(DATE_2)s`,2),'%')      AS `%(DATE_2)s`,
       concat(round(100 * pr.`%(DATE_3)s`,2),'%')      AS `%(DATE_3)s`,
       concat(round(100 * pr.`%(DATE_4)s`,2),'%')     AS `%(DATE_4)s`,
       concat(round(100 * pr.`%(DATE_5)s`,2),'%')     AS `%(DATE_5)s`,
       concat(round(100 * pr.`%(DATE_6)s`,2),'%')      AS `%(DATE_6)s`
FROM pivot_rates pr
         LEFT JOIN uv_result ur
                   ON pr.destination = ur.destination
ORDER BY
    CAST(REPLACE(ur.`近7天流量占比`, '%', '') AS DOUBLE) DESC NULLS LAST;