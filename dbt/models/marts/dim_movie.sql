
WITH revenues_agg AS (

    SELECT
        title,
        release_year,
        MIN(revenue_date)  AS first_revenue_date,
        MAX(revenue_date)  AS last_revenue_date,
        SUM(revenue)       AS lifetime_revenue
    FROM {{ ref('stg_revenues') }}
    GROUP BY title, release_year

),

joined AS (

    SELECT
        o.imdb_id,

        COALESCE(o.title, r.title)  AS title,

        r.title                     AS lookup_title,

        r.release_year,

        o.runtime_min,
        o.genres,
        o.director,
        o.writer,
        o.actors,
        o.plot,
        o.languages,
        o.country,
        o.rated,
        o.awards,
        o.poster_url,

        o.imdb_rating,
        o.metascore,
        o.rotten_tomatoes_score,
        o.imdb_votes,
        o.box_office_omdb,

        r.first_revenue_date,
        r.last_revenue_date,
        r.lifetime_revenue,

        o._fetched_at  AS omdb_fetched_at

    FROM revenues_agg r
    INNER JOIN {{ ref('stg_omdb_movies') }} o
        ON r.title        = o.lookup_title
        AND r.release_year = o.lookup_year

)

SELECT * FROM joined
