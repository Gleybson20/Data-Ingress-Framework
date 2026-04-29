select
    date,
    location_id,
    'future_date' as failure_reason
from {{ ref('int_weather_daily') }}
where date > current_date()