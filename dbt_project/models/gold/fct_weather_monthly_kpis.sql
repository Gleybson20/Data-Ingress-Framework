{{ config(
    materialized = 'table',
    tags         = ['gold', 'weather', 'kpi']
) }}

with silver as (

    select * from {{ ref('int_weather_daily') }}

),

monthly_kpis as (

    select
        DATE_TRUNC('month', date)             as month,
        location_id,
        location_name,

        round(avg(temp_mean_c),  2)           as avg_temp_c,
        round(max(temp_max_c),   2)           as max_temp_c,
        round(min(temp_min_c),   2)           as min_temp_c,
        round(avg(temp_amplitude_c), 2)       as avg_daily_amplitude_c,

        round(sum(precipitation_mm), 2)       as total_precipitation_mm,
        COUNT(*) FILTER (WHERE had_rain)      as rainy_days,
        count(*)                              as days_with_data,
        round(
            COUNT(*) FILTER (WHERE had_rain) * 100.0 / NULLIF(count(*), 0),
            1
        )                                     as rain_day_pct,

        round(max(wind_speed_max_kmh), 2)     as max_wind_speed_kmh,
        round(avg(wind_speed_max_kmh), 2)     as avg_wind_speed_kmh,

        -- Categoria de vento mais frequente no mês
        MODE() WITHIN GROUP (ORDER BY wind_category) as dominant_wind_category,

        CURRENT_TIMESTAMP                     as _dbt_loaded_at

    from silver
    group by 1, 2, 3

)

select * from monthly_kpis