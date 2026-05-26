
WITH base AS (

    SELECT
        distributor,
        COUNT(DISTINCT title)  AS films_distributed_n,
        SUM(revenue)           AS lifetime_revenue
    FROM {{ ref('stg_revenues') }}
    WHERE distributor IS NOT NULL
    GROUP BY distributor

)

SELECT
    MD5(distributor)  AS distributor_key,
    distributor       AS distributor_name,
    films_distributed_n,
    lifetime_revenue
FROM base
