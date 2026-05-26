
WITH filtered AS (

    SELECT
        imdb_id,
        lookup_title,
        lookup_year,
        response,
        _fetched_at
    FROM {{ source('raw', 'omdb') }}
    WHERE status = 'found'
      AND response:Type::VARCHAR = 'movie'

),

ratings_unpacked AS (

    SELECT
        f.imdb_id,
        MAX(CASE WHEN r.value:Source::VARCHAR = 'Rotten Tomatoes'
                 THEN TRY_TO_NUMBER(REGEXP_SUBSTR(r.value:Value::VARCHAR, '\\d+'))
            END)  AS rotten_tomatoes_score
    FROM filtered f,
         LATERAL FLATTEN(input => f.response:Ratings) r
    GROUP BY f.imdb_id

),

flat AS (

    SELECT
        f.imdb_id,
        f.lookup_title,
        f.lookup_year,

        NULLIF(f.response:Title::VARCHAR,    'N/A')                                  AS title,
        NULLIF(f.response:Rated::VARCHAR,    'N/A')                                  AS rated,
        NULLIF(f.response:Released::VARCHAR, 'N/A')                                  AS released_raw,

        TRY_TO_NUMBER(REGEXP_SUBSTR(NULLIF(f.response:Runtime::VARCHAR, 'N/A'), '\\d+'))  AS runtime_min,

        NULLIF(f.response:Genre::VARCHAR,    'N/A')                                  AS genres,
        NULLIF(f.response:Director::VARCHAR, 'N/A')                                  AS director,
        NULLIF(f.response:Writer::VARCHAR,   'N/A')                                  AS writer,
        NULLIF(f.response:Actors::VARCHAR,   'N/A')                                  AS actors,
        NULLIF(f.response:Plot::VARCHAR,     'N/A')                                  AS plot,
        NULLIF(f.response:Language::VARCHAR, 'N/A')                                  AS languages,
        NULLIF(f.response:Country::VARCHAR,  'N/A')                                  AS country,
        NULLIF(f.response:Awards::VARCHAR,   'N/A')                                  AS awards,
        NULLIF(f.response:Poster::VARCHAR,   'N/A')                                  AS poster_url,

        TRY_TO_NUMBER(NULLIF(f.response:imdbRating::VARCHAR, 'N/A'), 4, 1)           AS imdb_rating,
        TRY_TO_NUMBER(NULLIF(f.response:Metascore::VARCHAR,  'N/A'))                 AS metascore,

        TRY_TO_NUMBER(REPLACE(NULLIF(f.response:imdbVotes::VARCHAR, 'N/A'), ',', ''))  AS imdb_votes,

        TRY_TO_NUMBER(REPLACE(REPLACE(NULLIF(f.response:BoxOffice::VARCHAR, 'N/A'), '$', ''), ',', '')) AS box_office_omdb,

        r.rotten_tomatoes_score,

        f._fetched_at
    FROM filtered f
    LEFT JOIN ratings_unpacked r USING (imdb_id)

)

SELECT * FROM flat
