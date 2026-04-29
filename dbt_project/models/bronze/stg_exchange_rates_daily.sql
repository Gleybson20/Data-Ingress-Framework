{{ config(
    materialized = 'view',
    tags         = ['bronze', 'exchange_rates']
) }}

select
    date,
    base_currency,
    target_currency,
    rate,
    fetched_at,
    current_timestamp()     as _dbt_loaded_at,
    '{{ invocation_id }}'   as _dbt_invocation_id

from {{ source('bronze', 'exchange_rates_daily') }}
