with user_income as (
    select
        income_mth,
        user_name,
        sum(orders) as orders,
        sum(income) as income,
        sum(num) as num,
        sum(pay_price) as pay_price,
        sum(
            case
                when order_type_class in ('国机') then orders
                else 0
            end
        ) as orders_flight_inter,
        sum(
            case
                when order_type_class in ('国酒') then orders
                else 0
            end
        ) as orders_hotel_inter,
        sum(
            case
                when order_type_class in ('机票') then orders
                else 0
            end
        ) as orders_flight_inner,
        sum(
            case
                when order_type_class in ('酒店') then orders
                else 0
            end
        ) as orders_hotel,
        sum(
            case
                when order_type_class in ('火车') then orders
                else 0
            end
        ) as orders_train,
        sum(
            case
                when order_type_class in ('门票') then orders
                else 0
            end
        ) as orders_ticket,
        sum(
            case
                when order_type_class in ('其他') then orders
                else 0
            end
        ) as orders_others,
        sum(
            case
                when order_type_class in ('国机') then income
                else 0
            end
        ) as income_flight_inter,
        sum(
            case
                when order_type_class in ('国酒') then income
                else 0
            end
        ) as income_hotel_inter,
        sum(
            case
                when order_type_class in ('机票') then income
                else 0
            end
        ) as income_flight_inner,
        sum(
            case
                when order_type_class in ('酒店') then income
                else 0
            end
        ) as income_hotel,
        sum(
            case
                when order_type_class in ('火车') then income
                else 0
            end
        ) as income_train,
        sum(
            case
                when order_type_class in ('门票') then income
                else 0
            end
        ) as income_ticket,
        sum(
            case
                when order_type_class in ('其他') then income
                else 0
            end
        ) as income_others,
        sum(
            case
                when order_type_class in ('国机') then num
                else 0
            end
        ) as num_flight_inter,
        sum(
            case
                when order_type_class in ('国酒') then num
                else 0
            end
        ) as num_hotel_inter,
        sum(
            case
                when order_type_class in ('机票') then num
                else 0
            end
        ) as num_flight_inner,
        sum(
            case
                when order_type_class in ('酒店') then num
                else 0
            end
        ) as num_hotel,
        sum(
            case
                when order_type_class in ('火车') then num
                else 0
            end
        ) as num_train,
        sum(
            case
                when order_type_class in ('门票') then num
                else 0
            end
        ) as num_ticket,
        sum(
            case
                when order_type_class in ('其他') then num
                else 0
            end
        ) as num_others,
        sum(
            case
                when order_type_class in ('国机') then pay_price
                else 0
            end
        ) as pay_price_flight_inter,
        sum(
            case
                when order_type_class in ('国酒') then pay_price
                else 0
            end
        ) as pay_price_hotel_inter,
        sum(
            case
                when order_type_class in ('机票') then pay_price
                else 0
            end
        ) as pay_price_flight_inner,
        sum(
            case
                when order_type_class in ('酒店') then pay_price
                else 0
            end
        ) as pay_price_hotel,
        sum(
            case
                when order_type_class in ('火车') then pay_price
                else 0
            end
        ) as pay_price_train,
        sum(
            case
                when order_type_class in ('门票') then pay_price
                else 0
            end
        ) as pay_price_ticket,
        sum(
            case
                when order_type_class in ('其他') then pay_price
                else 0
            end
        ) as pay_price_others
    from
        (
            select
                substr (order_time, 1, 7) as income_mth,
                username as user_name,
                case
                    when order_type_class in ('flight-inter') then '国机'
                    when order_type_class in ('hotel-inter') then '国酒'
                    when order_type_class in ('flight-inner') then '机票'
                    when order_type_class in ('hotel') then '酒店'
                    when order_type_class in ('train') then '火车'
                    when order_type_class in ('piao') then '门票'
                    else '其他'
                end as order_type_class,
                count(distinct order_no) orders,
                sum(COALESCE(income_after_month, 0)) as income,
                sum(COALESCE(number, 0)) as num,
                sum(COALESCE(pay_price, 0)) as pay_price
            from
                pub.dwd_ord_order_income_mi
            where
                month >= '2025-07'
                and platform = 'app'
                and closedloop = '0'
            group by
                1,
                2,
                3
        ) t
    group by
        1,
        2
),
new_loss_and_dim_info as (
    select
        distinct substr (a.dt, 1, 7) as order_mth,
        a.touch_key,
        a.touch_type,
        a.dict_type,
        a.order_flag_qunar,
        case
            when order_type_class in ('flight-inter') then '国机'
            when order_type_class in ('hotel-inter') then '国酒'
            when order_type_class in ('flight-inner') then '机票'
            when order_type_class in ('hotel') then '酒店'
            when order_type_class in ('train') then '火车'
            when order_type_class in ('piao') then '门票'
            else '其他'
        end as order_type,
        a.username,
        a.dt,
        a.smm_channel_name,
        a.smm_team_name,
        a.business_name,
        a.position,
        COALESCE(a.note_id, c.note_id) as note_id
    from
        pub.dwd_ord_user_first_order_media_attribution_di a
        left join (
            select
                distinct query,
                note_id,
                row_number() over (
                    partition by query
                    order by
                        query_begin_dt desc
                ) rn
            from
                smm.dim_redbook_query_info_stat_apply_org_da
            where
                dt = date_sub (current_date, 1)
        ) c on a.query = c.query
        and c.rn = 1
    where
        a.dt >= '2025-07-01'
        and (
            dict_type in ('new_qncl_wl_username')
            or (
                dict_type in ('lost_pncl_wl_username')
                and order_flag_qunar = 1
            )
        )
        and smm_channel_name in ('小红书业务')
        and smm_team_name in ('投流团队')
),
note_type as (
    select
        distinct a.note_id,
        scene_second_v2,
        query_begin_dt,
        query_end_dt,
        row_number() over (
            partition by query
            order by
                query_begin_dt desc
        ) rn
    from
        smm.dim_redbook_query_info_stat_apply_org_da a
    where
        dt = date_format (
            date_add ('day', -1, current_timestamp),
            '%Y-%m-%d'
        )
)
select
    order_mth,
    business_name,
    a.order_type as first_order_type,
    c.scene_second_v2 as note_type,
    count(
        distinct case
            when dict_type = 'new_qncl_wl_username' then a.username
        end
    ) users_new,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then income
            else 0
        end
    ) income_new,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_flight_inter
            else 0
        end
    ) income_new_flight_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_hotel_inter
            else 0
        end
    ) income_new_hotel_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_flight_inner
            else 0
        end
    ) income_new_flight_inner,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_hotel
            else 0
        end
    ) income_new_hotel,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_ticket
            else 0
        end
    ) income_new_ticket,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_train
            else 0
        end
    ) income_new_train,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.income_others
            else 0
        end
    ) income_new_other,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then orders
            else 0
        end
    ) ords_new,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_flight_inter
            else 0
        end
    ) orders_new_flight_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_hotel_inter
            else 0
        end
    ) orders_new_hotel_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_flight_inner
            else 0
        end
    ) orders_new_flight_inner,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_hotel
            else 0
        end
    ) orders_new_hotel,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_ticket
            else 0
        end
    ) orders_new_ticket,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_train
            else 0
        end
    ) orders_new_train,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.orders_others
            else 0
        end
    ) orders_new_other,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then num
            else 0
        end
    ) num_new,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_flight_inter
            else 0
        end
    ) num_new_flight_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_hotel_inter
            else 0
        end
    ) num_new_hotel_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_flight_inner
            else 0
        end
    ) num_new_flight_inner,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_hotel
            else 0
        end
    ) num_new_hotel,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_ticket
            else 0
        end
    ) num_new_ticket,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_train
            else 0
        end
    ) num_new_train,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.num_others
            else 0
        end
    ) num_new_other,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then pay_price
            else 0
        end
    ) pay_price_new,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_flight_inter
            else 0
        end
    ) pay_price_new_flight_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_hotel_inter
            else 0
        end
    ) pay_price_new_hotel_inter,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_flight_inner
            else 0
        end
    ) pay_price_new_flight_inner,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_hotel
            else 0
        end
    ) pay_price_new_hotel,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_ticket
            else 0
        end
    ) pay_price_new_ticket,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_train
            else 0
        end
    ) pay_price_new_train,
    sum(
        case
            when dict_type = 'new_qncl_wl_username' then b.pay_price_others
            else 0
        end
    ) pay_price_new_other,
    count(
        distinct case
            when dict_type = 'lost_pncl_wl_username' then a.username
        end
    ) users_lost,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then income
            else 0
        end
    ) income_lost,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_flight_inter
            else 0
        end
    ) income_lost_flight_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_hotel_inter
            else 0
        end
    ) income_lost_hotel_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_flight_inner
            else 0
        end
    ) income_lost_flight_inner,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_hotel
            else 0
        end
    ) income_lost_hotel,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_ticket
            else 0
        end
    ) income_lost_ticket,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_train
            else 0
        end
    ) income_lost_train,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.income_others
            else 0
        end
    ) income_lost_other,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then orders
            else 0
        end
    ) ords_lost,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_flight_inter
            else 0
        end
    ) orders_lost_flight_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_hotel_inter
            else 0
        end
    ) orders_lost_hotel_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_flight_inner
            else 0
        end
    ) orders_lost_flight_inner,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_hotel
            else 0
        end
    ) orders_lost_hotel,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_ticket
            else 0
        end
    ) orders_lost_ticket,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_train
            else 0
        end
    ) orders_lost_train,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.orders_others
            else 0
        end
    ) orders_lost_other,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then num
            else 0
        end
    ) num_lost,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_flight_inter
            else 0
        end
    ) num_lost_flight_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_hotel_inter
            else 0
        end
    ) num_lost_hotel_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_flight_inner
            else 0
        end
    ) num_lost_flight_inner,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_hotel
            else 0
        end
    ) num_lost_hotel,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_ticket
            else 0
        end
    ) num_lost_ticket,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_train
            else 0
        end
    ) num_lost_train,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.num_others
            else 0
        end
    ) num_lost_other,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then pay_price
            else 0
        end
    ) pay_price_lost,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_flight_inter
            else 0
        end
    ) pay_price_lost_flight_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_hotel_inter
            else 0
        end
    ) pay_price_lost_hotel_inter,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_flight_inner
            else 0
        end
    ) pay_price_lost_flight_inner,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_hotel
            else 0
        end
    ) pay_price_lost_hotel,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_ticket
            else 0
        end
    ) pay_price_lost_ticket,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_train
            else 0
        end
    ) pay_price_lost_train,
    sum(
        case
            when dict_type = 'lost_pncl_wl_username' then b.pay_price_others
            else 0
        end
    ) pay_price_lost_other
from
    new_loss_and_dim_info a
    left join note_type c on a.note_id = c.note_id
    and a.dt >= c.query_begin_dt
    and a.dt <= c.query_end_dt
    and c.rn = 1
    left join user_income b on a.username = b.user_name
    and a.order_mth = b.income_mth
group by
    1,
    2,
    3,
    4