
WITH spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2000-01-01' AS DATE)",
        end_date="CAST('2026-01-01' AS DATE)"
    ) }}

),

flattened AS (

    SELECT
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))  AS date_key,
        date_day::DATE                             AS date,
        YEAR(date_day)                             AS year,
        QUARTER(date_day)                          AS quarter,
        MONTH(date_day)                            AS month,
        MONTHNAME(date_day)                        AS month_name,
        DAY(date_day)                              AS day,
        DAYOFWEEK(date_day)                        AS day_of_week,
        DAYNAME(date_day)                          AS day_name,
        (DAYOFWEEK(date_day) IN (0, 6))            AS is_weekend
    FROM spine

)

SELECT * FROM flattened
