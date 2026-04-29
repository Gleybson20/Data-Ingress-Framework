{% macro date_spine(start_date, end_date, datepart="day") %}

with date_spine as (
    select
        CAST(unnest(
            generate_series(
                CAST({{ start_date }} AS DATE),
                CAST({{ end_date }}   AS DATE),
                INTERVAL 1 {{ datepart }}
            )
        ) AS DATE) as date_day
)

select date_day from date_spine

{% endmacro %}