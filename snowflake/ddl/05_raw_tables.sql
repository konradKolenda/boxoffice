-- Boxoffice pipeline — raw landing tables (deliverable 5 of 8).
-- Run as ACCOUNTADMIN. Prereqs: 01 (schemas), 02 (BOXOFFICE_AIRFLOW role), 04 (file format + stages).
--
-- Three objects, three patterns:
--   REVENUES         — regular table. Loaded by Airflow via COPY INTO from @S3_BOX_OFFICE. Snowflake stores rows.
--   OMDB             — EXTERNAL TABLE. Data lives only in S3 (@S3_OMDB); Snowflake queries in-place. No COPY step.
--   OMDB_FETCH_LOG   — regular table. One row per OMDb API call (success/not_found/error). Drives quota counter and same-day error skip.
-- Why asymmetric: REVENUES is 337k+ structured rows from regular CSV drops (standard EL pattern).
-- OMDB is ~6.5k tiny JSON files with append-only writes — External Table cuts a step from the DAG and makes replay trivial.
-- OMDB_FETCH_LOG sits OUTSIDE the External Table because errors never reach S3 — log captures the full call history including failures.

USE ROLE ACCOUNTADMIN;
USE DATABASE BOXOFFICE;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS BOXOFFICE.RAW.REVENUES (
  ID VARCHAR NOT NULL,
  REVENUE_DATE DATE NOT NULL,
  TITLE VARCHAR NOT NULL,
  REVENUE NUMBER(12,0),
  THEATERS NUMBER(6,0),
  DISTRIBUTOR VARCHAR,
  _SOURCE_FILE VARCHAR NOT NULL,
  _LOADED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw landing for daily box-office records. One row per (movie, day). Loaded by Airflow via COPY INTO from @BOXOFFICE.UTIL.S3_BOX_OFFICE; bootstrap loads once from @S3_SEED.';

CREATE OR REPLACE EXTERNAL TABLE BOXOFFICE.RAW.OMDB (
  LOOKUP_TITLE  VARCHAR     AS (VALUE:_lookup:title::VARCHAR),
  LOOKUP_YEAR   NUMBER(4,0) AS (VALUE:_lookup:year::NUMBER(4,0)),
  STATUS        VARCHAR     AS (VALUE:_status::VARCHAR),
  IMDB_ID       VARCHAR     AS (VALUE:imdbID::VARCHAR),
  RESPONSE      VARIANT     AS (VALUE),
  _FETCHED_AT   TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ(VALUE:_fetched_at::VARCHAR))
)
LOCATION = @BOXOFFICE.UTIL.S3_OMDB
FILE_FORMAT = (FORMAT_NAME = BOXOFFICE.UTIL.JSON_STANDARD)
AUTO_REFRESH = FALSE
COMMENT = 'OMDb wrapped JSON responses. Data lives in s3://kk-demo-pipeline/raw/omdb/ (one file per API call). Logical PK: (LOOKUP_TITLE, LOOKUP_YEAR). Airflow appends files + calls ALTER EXTERNAL TABLE ... REFRESH.';

CREATE TABLE IF NOT EXISTS BOXOFFICE.RAW.OMDB_FETCH_LOG (
  CALL_AT       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  LOOKUP_TITLE  VARCHAR       NOT NULL,
  LOOKUP_YEAR   NUMBER(4,0),
  OUTCOME       VARCHAR       NOT NULL,
  HTTP_STATUS   NUMBER(3,0),
  ERROR_MESSAGE VARCHAR
)
COMMENT = 'Per-call OMDb API log. One row per call regardless of outcome. OUTCOME in (found, not_found, error_5xx, error_429, error_timeout, error_other). Drives quota counter and same-day error skip.';

GRANT SELECT ON TABLE BOXOFFICE.RAW.REVENUES TO ROLE BOXOFFICE_AIRFLOW;

GRANT SELECT ON TABLE BOXOFFICE.RAW.OMDB_FETCH_LOG TO ROLE BOXOFFICE_AIRFLOW;

GRANT SELECT ON EXTERNAL TABLE BOXOFFICE.RAW.OMDB TO ROLE BOXOFFICE_AIRFLOW;

GRANT SELECT ON TABLE BOXOFFICE.RAW.REVENUES TO ROLE BOXOFFICE_DBT;
GRANT SELECT ON EXTERNAL TABLE BOXOFFICE.RAW.OMDB TO ROLE BOXOFFICE_DBT;

GRANT SELECT ON FUTURE TABLES IN SCHEMA BOXOFFICE.RAW TO ROLE BOXOFFICE_AIRFLOW;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BOXOFFICE.RAW TO ROLE BOXOFFICE_DBT;
