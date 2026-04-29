{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'base_currency', 'target_currency'],
    incremental_strategy = 'delete+insert',
    tags                 = ['silver', 'exchange_rates']
) }}

with source as (

    select * from {{ ref('stg_exchange_rates_daily') }}

    {% if is_incremental() %}
        where date >= CURRENT_DATE - INTERVAL 3 DAY
    {% endif %}

),

deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by date, base_currency, target_currency
                order by fetched_at desc
            ) as rn
        from source
    ) ranked
    where rn = 1

),

with_lag as (

    select
        {{ safe_cast('date',            'DATE',    'date') }}            as date,
        {{ safe_cast('base_currency',   'VARCHAR', 'base_currency') }}   as base_currency,
        {{ safe_cast('target_currency', 'VARCHAR', 'target_currency') }} as target_currency,
        {{ safe_cast('rate',            'DOUBLE',  'rate') }}            as rate,

        -- Inverso da taxa: ex. se USD→BRL = 5.1, então BRL→USD = 0.196
        rate / NULLIF({{ safe_cast('rate', 'DOUBLE', 'rate') }}, 0)      as inverse_rate,

        lag({{ safe_cast('rate', 'DOUBLE', 'rate') }}) over (
            partition by base_currency, target_currency
            order by date
        ) as prev_rate,

        fetched_at,
        _dbt_loaded_at

    from deduplicated
    where date is not null

),

final as (

    select
        date,
        base_currency,
        target_currency,
        rate,
        1.0 / NULLIF(rate, 0)                              as inverse_rate,
        prev_rate,
        round(
            (rate - prev_rate) / NULLIF(prev_rate, 0) * 100,
            4
        )                                                   as rate_pct_change,
        fetched_at,
        _dbt_loaded_at

    from with_lag

)

select * from final