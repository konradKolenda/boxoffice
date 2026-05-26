
WITH src AS (

    SELECT
        id,
        revenue_date,
        REGEXP_REPLACE(TRIM(title), '\\s+', ' ')  AS title,
        revenue::NUMBER(12, 0)                    AS revenue,
        theaters::NUMBER(6, 0)                    AS theaters,
        distributor
    FROM {{ source('raw', 'revenues') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _loaded_at DESC) = 1

),

with_release_year AS (

    SELECT
        *,
        MIN(YEAR(revenue_date)) OVER (PARTITION BY title)  AS release_year
    FROM src

)

SELECT
    id,
    revenue_date,
    title,
    release_year,
    revenue,
    theaters,
    distributor
FROM with_release_year
