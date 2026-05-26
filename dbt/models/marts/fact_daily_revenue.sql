
WITH revenues AS (

    SELECT
        id,
        revenue_date,
        title,
        release_year,
        revenue,
        theaters,
        distributor
    FROM {{ ref('stg_revenues') }}

),

with_keys AS (

    SELECT
        r.id                                                AS source_row_id,
        m.imdb_id                                           AS movie_key,
        TO_NUMBER(TO_CHAR(r.revenue_date, 'YYYYMMDD'))      AS date_key,
        d.distributor_key,

        r.revenue,
        r.theaters,
        r.revenue_date

    FROM revenues r
    INNER JOIN {{ ref('dim_movie') }} m
        ON r.title         = m.lookup_title
        AND r.release_year = m.release_year
    INNER JOIN {{ ref('dim_distributor') }} d
        ON r.distributor   = d.distributor_name

)

SELECT * FROM with_keys
