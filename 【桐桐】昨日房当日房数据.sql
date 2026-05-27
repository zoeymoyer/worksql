with base as (
    select
        a.supplier_code,
        a.order_no,
        a.order_status,
        case
            when datediff(a.checkin_date, a.order_date) = -1 then '昨日房'
            when datediff(a.checkin_date, a.order_date) = 0 then '当日房'
            else '其他'
        end as room_type
        ,case
            when datediff(a.checkout_date,a.checkin_date)=2 then '2晚'
            when datediff(a.checkout_date,a.checkin_date)=3 then '3晚'
            when datediff(a.checkout_date,a.checkin_date)=4 then '4晚'
            when datediff(a.checkout_date,a.checkin_date)>=5 then '5晚+'
            else 'OTHER'
        end as stay_dim
        ,case
            when a.base_price_new<=1000 then '0-1000'
            when a.base_price_new<=3000 then '1001-3000'
            when a.base_price_new<=5000 then '3001-5000'
            when a.base_price_new<=8000 then '5001-8000'
            when a.base_price_new<=10000 then '8001-10000'
            when a.base_price_new>10000 then '10000+'
            else 'NULL' end as price_dim
    from default.mdw_order_v3_international a
    where a.order_date between '2026-04-01' and '2026-04-22'
      and a.dt between '20260401' and '20260422'
      and a.terminal_channel_type in ('www','app','touch')
      and a.pay_status not in (0,'false','pay_failed')
      and a.pay_status is not null
      and a.order_status <> 'DELETE'
       in ('hca1f62c70i', 'hca1f62c70j', 'hca1fa7u40i', 'hca1fa7u40j', 'hca1f95n50i', 'hca1f95n50j', 'hca10eg3k6k', 'hca1eg3k60o', 'hca1ffq250i', 'hca1fbsn50i', 'hca1eg3k60n', 'hca1f71a00j', 'hca1f71a00i', 'hca1fdkh10i', 'hca1fepr10i', 'hca1fepr10j', 'hca1fc8h40i', 'hca1fc8h40j', 'hca1erb900m', 'hca1erb900n', 'hca1erb900k', 'hca1erb900o', 'hca1faud10j', 'hca10eq7a8i', 'hca1fck230i', 'hca1eq7a80j', 'hca1fc1250i', 'hca1fep110i', 'hca1fceu50i', 'hca1fceu50j', 'hca1fdda60l', 'hca1fctj50i', 'hca1ffh920k', 'hca1fbra40i', 'hca1ffh920i', 'hca908hc00p', 'hca1em5m10n', 'hca1f4om90i', 'hca123i850k', 'hca123i850l', 'hca1ff0h80i', 'hca1faro30o', 'hca1ffp970j', 'hca1fe4100j', 'hca1faro30i', 'hca1fevc00i', 'hca1fevb80l', 'hca1fevb80i', 'hca1fevc00k', 'hca1fgc00i', 'hca1ffp970k', 'hca1dueb7l', 'hca1fc2e70i', 'hca1fc2e70j', 'hca1fc2e70k', 'hca1fes670i', 'hca1fes670j')
),

order_data as (
    select
        supplier_code,
        '整体' as dim_type,
        count(distinct order_no) as order_cnt,
        count(distinct case when order_status <> 'REJECTED' then order_no end) as non_rejected_cnt,
        count(distinct case when order_status = 'REJECTED' then order_no end) as rejected_cnt
    from base
    group by supplier_code

    union all

    select
        supplier_code,
        '昨日房' as dim_type,
        count(distinct order_no) as order_cnt,
        count(distinct case when order_status <> 'REJECTED' then order_no end) as non_rejected_cnt,
        count(distinct case when order_status = 'REJECTED' then order_no end) as rejected_cnt
    from base
    where room_type = '昨日房'
    group by supplier_code

    union all

    select
        supplier_code,
        '当日房' as dim_type,
        count(distinct order_no) as order_cnt,
        count(distinct case when order_status <> 'REJECTED' then order_no end) as non_rejected_cnt,
        count(distinct case when order_status = 'REJECTED' then order_no end) as rejected_cnt
    from base
    where room_type = '当日房'
    group by supplier_code
),

fuwu_base as (
    select
        a.supplier_code,
        a.order_no,
        case
            when datediff(a.checkin_date, a.order_date) = -1 then '昨日房'
            when datediff(a.checkin_date, a.order_date) = 0 then '当日房'
            else '其他'
        end as room_type,
        b.complain_type
    from default.mdw_order_v3_international a
    left join fuwu.dwd_ord_htl_servicequality_di b
        on a.order_no = b.order_no
       and b.dt >= '20260407'
    where a.order_date between '2026-04-01' and '2026-04-22'
      and a.dt between '20260401' and '20260422'
      and a.terminal_channel_type in ('www','app','touch')
      and a.order_status = 'REJECTED'
),

-- 这里彻底修复，严格 group by 所有常量字段，Hive 100% 识别
fuwu_orders as (
    select
        supplier_code,
        '整体' as dim_type,
        count(distinct case when complain_type in ('确认后满房','确认后涨价') then order_no end) as after_reject_cnt,
        count(distinct case when complain_type in ('确认前满房','确认前涨价') then order_no end) as before_reject_cnt
    from fuwu_base
    group by supplier_code

    union all

    select
        supplier_code,
        '昨日房' as dim_type,
        count(distinct case when complain_type in ('确认后满房','确认后涨价') then order_no end) as after_reject_cnt,
        count(distinct case when complain_type in ('确认前满房','确认前涨价') then order_no end) as before_reject_cnt
    from fuwu_base
    where room_type = '昨日房'
    group by supplier_code

    union all

    select
        supplier_code,
        '当日房' as dim_type,
        count(distinct case when complain_type in ('确认后满房','确认后涨价') then order_no end) as after_reject_cnt,
        count(distinct case when complain_type in ('确认前满房','确认前涨价') then order_no end) as before_reject_cnt
    from fuwu_base
    where room_type = '当日房'
    group by supplier_code
)

select
    a.supplier_code,
    a.dim_type as `维度`,
    a.order_cnt as `订单量`,
    a.rejected_cnt as `拒绝单量`,
    nvl(b.after_reject_cnt, 0) as `确认后拒单`,
    nvl(b.before_reject_cnt, 0) as `确认前拒单`,
    concat(
        round(
            (a.non_rejected_cnt + nvl(b.after_reject_cnt, 0)) / nullif(a.order_cnt, 0) * 100,
            2
        ),
        '%'
    ) as `发单成功率`
from order_data a
left join fuwu_orders b
    on a.supplier_code = b.supplier_code
   and a.dim_type = b.dim_type
order by
    a.supplier_code,
    case
        when a.dim_type = '整体' then 1
        when a.dim_type = '昨日房' then 2
        when a.dim_type = '当日房' then 3
        else 4
    end;


case
            when datediff(a.checkin_date, a.order_date) = -1 then '昨日房'
            when datediff(a.checkin_date, a.order_date) = 0 then '当日房'
            else '其他'
        end as room_type

with base as (--- 昨日房
    select
        a.supplier_code,
        a.order_no,
        a.order_status,
        a.base_price_new,
        case
            when datediff(a.checkout_date,a.checkin_date)=2 then '2晚'
            when datediff(a.checkout_date,a.checkin_date)=3 then '3晚'
            when datediff(a.checkout_date,a.checkin_date)=4 then '4晚'
            when datediff(a.checkout_date,a.checkin_date)>=5 then '5晚+'
        end as stay_dim,
        case
            when a.base_price_new <= 1000  then '0-1000'
            when a.base_price_new <= 3000  then '1001-3000'
            when a.base_price_new <= 5000  then '3001-5000'
            when a.base_price_new <= 8000  then '5001-8000'
            when a.base_price_new <= 12000 then '8001-12000'
            when a.base_price_new > 12000  then '12001+'
            else '未知底价'
        end as fee_interval
    from default.mdw_order_v3_international a
    where a.order_date between '2026-04-01' and '2026-04-22'
        and a.dt = '20260422'
        and a.terminal_channel_type in ('www','app','touch')
        and a.pay_status not in (0,'false','pay_failed')
        and a.pay_status is not null
        and a.order_status <> 'DELETE'
        and datediff(a.checkin_date, a.order_date) = -1  --- 昨日房
        -- and datediff(a.checkin_date, a.order_date) = 0   --- 当日房
        and a.supplier_code in ('hca1f62c70i', 'hca1f62c70j', 'hca1fa7u40i', 'hca1fa7u40j', 'hca1f95n50i', 'hca1f95n50j', 'hca10eg3k6k', 'hca1eg3k60o', 'hca1ffq250i', 'hca1fbsn50i', 'hca1eg3k60n', 'hca1f71a00j', 'hca1f71a00i', 'hca1fdkh10i', 'hca1fepr10i', 'hca1fepr10j', 'hca1fc8h40i', 'hca1fc8h40j', 'hca1erb900m', 'hca1erb900n', 'hca1erb900k', 'hca1erb900o', 'hca1faud10j', 'hca10eq7a8i', 'hca1fck230i', 'hca1eq7a80j', 'hca1fc1250i', 'hca1fep110i', 'hca1fceu50i', 'hca1fceu50j', 'hca1fdda60l', 'hca1fctj50i', 'hca1ffh920k', 'hca1fbra40i', 'hca1ffh920i', 'hca908hc00p', 'hca1em5m10n', 'hca1f4om90i', 'hca123i850k', 'hca123i850l', 'hca1ff0h80i', 'hca1faro30o', 'hca1ffp970j', 'hca1fe4100j', 'hca1faro30i', 'hca1fevc00i', 'hca1fevb80l', 'hca1fevb80i', 'hca1fevc00k', 'hca1fgc00i', 'hca1ffp970k', 'hca1dueb7l', 'hca1fc2e70i', 'hca1fc2e70j', 'hca1fc2e70k', 'hca1fes670i', 'hca1fes670j')
        and datediff(a.checkout_date,a.checkin_date) >=2
),

order_data as (
    select
        supplier_code,
        stay_dim,
        fee_interval,
        count(distinct order_no) as order_cnt,
        count(distinct case when order_status <> 'REJECTED' then order_no end) as non_rejected_cnt,
        count(distinct case when order_status = 'REJECTED' then order_no end) as rejected_cnt
    from base
    group by supplier_code, stay_dim, fee_interval
),

fuwu_base_all as (
    select
        a.supplier_code,
        a.order_no,
        case
            when datediff(a.checkout_date,a.checkin_date)=2 then '2晚'
            when datediff(a.checkout_date,a.checkin_date)=3 then '3晚'
            when datediff(a.checkout_date,a.checkin_date)=4 then '4晚'
            when datediff(a.checkout_date,a.checkin_date)>=5 then '5晚+'
        end as stay_dim,
        case
            when a.base_price_new <= 1000  then '0-1000'
            when a.base_price_new <= 3000  then '1001-3000'
            when a.base_price_new <= 5000  then '3001-5000'
            when a.base_price_new <= 8000  then '5001-8000'
            when a.base_price_new <= 12000 then '8001-12000'
            when a.base_price_new > 12000  then '12001+'
            else '未知底价'
        end as fee_interval,
        b.complain_type
    from default.mdw_order_v3_international a
    left join fuwu.dwd_ord_htl_servicequality_di b
        on a.order_no = b.order_no
       and b.dt between '20260401' and '20260422'
    where a.order_date between '2026-04-01' and '2026-04-22'
      and a.dt = '20260422'
      and a.terminal_channel_type in ('www','app','touch')
      and a.pay_status not in (0,'false','pay_failed')
      and a.pay_status is not null
      and a.order_status <> 'DELETE'
      and datediff(a.checkin_date, a.order_date) = -1   --- 昨日房
        -- and datediff(a.checkin_date, a.order_date) = 0   --- 当日房
      and a.supplier_code in ('hca1f62c70i', 'hca1f62c70j', 'hca1fa7u40i', 'hca1fa7u40j', 'hca1f95n50i', 'hca1f95n50j', 'hca10eg3k6k', 'hca1eg3k60o', 'hca1ffq250i', 'hca1fbsn50i', 'hca1eg3k60n', 'hca1f71a00j', 'hca1f71a00i', 'hca1fdkh10i', 'hca1fepr10i', 'hca1fepr10j', 'hca1fc8h40i', 'hca1fc8h40j', 'hca1erb900m', 'hca1erb900n', 'hca1erb900k', 'hca1erb900o', 'hca1faud10j', 'hca10eq7a8i', 'hca1fck230i', 'hca1eq7a80j', 'hca1fc1250i', 'hca1fep110i', 'hca1fceu50i', 'hca1fceu50j', 'hca1fdda60l', 'hca1fctj50i', 'hca1ffh920k', 'hca1fbra40i', 'hca1ffh920i', 'hca908hc00p', 'hca1em5m10n', 'hca1f4om90i', 'hca123i850k', 'hca123i850l', 'hca1ff0h80i', 'hca1faro30o', 'hca1ffp970j', 'hca1fe4100j', 'hca1faro30i', 'hca1fevc00i', 'hca1fevb80l', 'hca1fevb80i', 'hca1fevc00k', 'hca1fgc00i', 'hca1ffp970k', 'hca1dueb7l', 'hca1fc2e70i', 'hca1fc2e70j', 'hca1fc2e70k', 'hca1fes670i', 'hca1fes670j')
      and datediff(a.checkout_date,a.checkin_date) >=2
),

fuwu_orders as (
    select
        supplier_code,
        stay_dim,
        fee_interval,
        count(distinct case when complain_type in ('确认后满房','确认后涨价') then order_no end) as after_reject_cnt,
        count(distinct case when complain_type in ('确认前满房','确认前涨价') then order_no end) as before_reject_cnt
    from fuwu_base_all
    group by supplier_code, stay_dim, fee_interval
)

select
    a.supplier_code as `KA代理商`,
    a.stay_dim as `连住晚数`,
    a.fee_interval as `底价区间`,
    a.order_cnt as `订单量`,
    a.rejected_cnt as `拒单量`,
    nvl(b.after_reject_cnt,0) as `确认后拒单`,
    nvl(b.before_reject_cnt,0) as `确认前拒单`,
    concat(
        round(
            (a.non_rejected_cnt + nvl(b.after_reject_cnt, 0)) / nullif(a.order_cnt, 0) * 100,
            2
        ),
        '%'
    ) as `发单成功率`
from order_data a
left join fuwu_orders b
    on a.supplier_code = b.supplier_code
    and a.stay_dim = b.stay_dim
    and a.fee_interval = b.fee_interval
order by
    a.supplier_code,
    case a.stay_dim when '2晚' then 1 when '3晚' then 2 when '4晚' then 3 when '5晚+' then 4 end,
    case a.fee_interval 
        when '0-1000' then 1 
        when '1001-3000' then 2 
        when '3001-5000' then 3 
        when '5001-8000' then 4 
        when '8001-12000' then 5 
        when '12001+' then 6 
    end;