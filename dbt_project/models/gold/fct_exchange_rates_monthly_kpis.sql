{{ config(
    materialized = 'table',
    tags         = ['gold', 'exchange_rates', 'kpi']
) }}

with silver as (

    select * from {{ ref('int_exchange_rates_daily') }}

),

monthly_kpis as (

    select
        DATE_TRUNC('month', date)                           as month,
        base_currency,
        target_currency,

        round(avg(rate), 6)                                 as avg_rate,
        round(min(rate), 6)                                 as min_rate,
        round(max(rate), 6)                                 as max_rate,
        round(max(rate) - min(rate), 6)                     as rate_range,

        round(avg(abs(rate_pct_change)), 4)                 as avg_abs_pct_change,
        round(max(abs(rate_pct_change)), 4)                 as max_abs_pct_change,

        count(*)                                            as trading_days_with_data,

        -- Taxa de abertura e fechamento do mês
        first(rate ORDER BY date ASC)                       as month_open_rate,
        last(rate  ORDER BY date ASC)                       as month_close_rate,

        round(
            (last(rate ORDER BY date ASC) - first(rate ORDER BY date ASC))
            / NULLIF(first(rate ORDER BY date ASC), 0) * 100,
            4
        )                                                   as month_pct_change,

        CURRENT_TIMESTAMP                                   as _dbt_loaded_at

    from silver
    group by 1, 2, 3

)

select * from monthly_kpis