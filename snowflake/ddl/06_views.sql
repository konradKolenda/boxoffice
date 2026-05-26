-- Boxoffice pipeline — operational views (deliverable beyond BRIEF's 8 core).
-- Run as ACCOUNTADMIN. Prereqs: 01-05 (schemas, raw tables, external table, log table).
--
-- Views here are "derived state" - read-only projections over RAW. Pure SQL, no data.
-- Re-runnable freely via CREATE OR REPLACE (no LOAD_HISTORY / no External Table dependency).
--
-- Creates:
--   BOXOFFICE.RAW.OMDB_FETCH_QUEUE  — anti-join: films in REVENUES NOT in OMDB cache AND NOT errored today.
--                                     Consumer (omdb_fetch.py) reads with ORDER BY lifetime_rev DESC + LIMIT N.

USE ROLE ACCOUNTADMIN;
USE DATABASE BOXOFFICE;
USE SCHEMA RAW;

CREATE OR REPLACE VIEW BOXOFFICE.RAW.OMDB_FETCH_QUEUE AS
WITH film_keys AS (
  SELECT
    title,
    MIN(YEAR(revenue_date)) AS release_year,
    SUM(revenue)            AS lifetime_rev
  FROM BOXOFFICE.RAW.REVENUES
  GROUP BY title
),
errored_today AS (
  SELECT
    lookup_title,
    lookup_year
  FROM BOXOFFICE.RAW.OMDB_FETCH_LOG
  WHERE outcome LIKE 'error%'
    AND call_at::DATE = CURRENT_DATE()
  GROUP BY lookup_title, lookup_year
)
SELECT
  f.title,
  f.release_year,
  f.lifetime_rev
FROM film_keys f
LEFT JOIN BOXOFFICE.RAW.OMDB c
  ON  f.title        = c.lookup_title
  AND f.release_year = c.lookup_year
LEFT JOIN errored_today e
  ON  f.title        = e.lookup_title
  AND f.release_year = e.lookup_year
WHERE c.lookup_title IS NULL
  AND e.lookup_title IS NULL
;

COMMENT ON VIEW BOXOFFICE.RAW.OMDB_FETCH_QUEUE IS
  'Films from RAW.REVENUES that still need OMDb enrichment AND have not errored today. Consumer should ORDER BY lifetime_rev DESC NULLS LAST + LIMIT N (revenue-priority queue capped by quota).';

GRANT SELECT ON VIEW BOXOFFICE.RAW.OMDB_FETCH_QUEUE TO ROLE BOXOFFICE_AIRFLOW;
