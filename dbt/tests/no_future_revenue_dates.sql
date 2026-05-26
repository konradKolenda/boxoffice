
SELECT
    source_row_id,
    revenue_date,
    revenue,
    theaters
FROM {{ ref('fact_daily_revenue') }}
WHERE revenue_date > CURRENT_DATE()
