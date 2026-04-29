{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'location_id'],
    incremental_strategy = 'delete+insert',
    tags                 = ['silver', 'weather']
) }}

with source as (

    select * from {{ ref('stg_weather_daily') }}

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
                partition by date, location_id
                order by fetched_at desc
            ) as rn
        from source
    ) ranked
    where rn = 1

),

transformed as (

    select
        {{ safe_cast('date',        'DATE',    'date') }}           as date,
        {{ safe_cast('location_id', 'VARCHAR', 'location_id') }}    as location_id,
        location_name,
        {{ safe_cast('latitude',    'DOUBLE',  'latitude') }}       as latitude,
        {{ safe_cast('longitude',   'DOUBLE',  'longitude') }}      as longitude,
        {{ safe_cast('temp_max_c',         'DOUBLE', 'temp_max_c') }}         as temp_max_c,
        {{ safe_cast('temp_min_c',         'DOUBLE', 'temp_min_c') }}         as temp_min_c,
        {{ safe_cast('temp_mean_c',        'DOUBLE', 'temp_mean_c') }}        as temp_mean_c,
        {{ safe_cast('precipitation_mm',   'DOUBLE', 'precipitation_mm') }}   as precipitation_mm,
        {{ safe_cast('wind_speed_max_kmh', 'DOUBLE', 'wind_speed_max_kmh') }} as wind_speed_max_kmh,
        {{ safe_cast('wind_direction_deg', 'DOUBLE', 'wind_direction_deg') }} as wind_direction_deg,

        round(
            {{ safe_cast('temp_max_c', 'DOUBLE', 'temp_max_c') }}
            - {{ safe_cast('temp_min_c', 'DOUBLE', 'temp_min_c') }},
            2
        ) as temp_amplitude_c,

        case
            when {{ safe_cast('precipitation_mm', 'DOUBLE', 'precipitation_mm') }} > 0 then true
            else false
        end as had_rain,

        case
            when {{ safe_cast('wind_speed_max_kmh', 'DOUBLE', 'wind_speed_max_kmh') }} is null then 'unknown'
            when wind_speed_max_kmh < 20  then 'calm'
            when wind_speed_max_kmh < 50  then 'moderate'
            when wind_speed_max_kmh < 90  then 'strong'
            else 'storm'
        end as wind_category,

        fetched_at,
        _dbt_loaded_at

    from deduplicated
    where date is not null
      and location_id is not null

)

select * from transformed