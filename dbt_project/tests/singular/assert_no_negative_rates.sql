select
    date,
    base_currency,
    target_currency,
    rate,
    'non_positive_rate' as failure_reason
from {{ ref('int_exchange_rates_daily') }}
where rate is not null
  and rate <= 0