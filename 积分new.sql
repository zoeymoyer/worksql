select
    a.log_date,
    a.user_name,
    b.order_no
from
(
        select
            log_date,
            user_name --get_json_object(value,'$.ext') AS ext_content--使用Hive推荐的JSON函数
        from
            default.dw_qav_ihotel_track_info_di
        where
            dt between '20250702'
            and '20250708' --and hour = 11
            and(
                province_name in('台湾', '香港')
                or country_name != '中国'
            )
            and key = 'ihotel/Booking/points/show/usePoints' -- and key = 'ihotel/Booking/points/click/usePoints'
    )
    left join (
        select
            order_date,
            user_name,
            order_no
        from
            default.mdw_order_v3_international a
        where
            dt = from_unixtime(unix_timestamp() -86400, 'yyyMMdd')
            and(
                province_name in('台湾', '香港')
                or country_name != '中国'
            )
            and order_date >= '2025-07-02'
            and order_date <= '2025-07-08'
            and points_deduction_benefit > 0
    ) b on a.log_date = b.order_date
    and a.user_name = b.user_name