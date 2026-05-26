-- Boxoffice pipeline — file format + external stages (deliverable 4 of 8).
-- Run as ACCOUNTADMIN. Prereqs: 01 (schemas), 02 (roles), 03 (storage integration KK_DEMO_PIPELINE_S3 + matching AWS IAM role).
--
-- Creates:
--   BOXOFFICE.UTIL.CSV_STANDARD           — CSV file format reused by all CSV-shaped sources
--   BOXOFFICE.UTIL.JSON_STANDARD          — JSON file format for OMDb wrapped responses (one object per file)
--   BOXOFFICE.UTIL.S3_BOX_OFFICE          — read stage: raw/box_office/   (Snowflake reads CSVs placed by Airflow)
--   BOXOFFICE.UTIL.S3_OMDB                — read stage: raw/omdb/         (backs the EXTERNAL TABLE RAW.OMDB; in-place reads, no COPY)
--   BOXOFFICE.UTIL.S3_INBOX               — inbox/box_office/ (bootstrap UNLOAD writes here; pickup script reads via boto3 + LIST)
--   BOXOFFICE.UTIL.S3_SEED                — read stage: _seed/box_office/  (one-off bootstrap source; can be dropped after seed load)
--
-- Why one file format reused by every stage: when the unload step writes back to S3, the on-write format must match the
-- on-read format byte-for-byte (header row, quoting, NULL marker), otherwise the re-ingest of the produced files breaks.
-- One named object means we change the rules in one place.

USE ROLE ACCOUNTADMIN;
USE DATABASE BOXOFFICE;
USE SCHEMA UTIL;

CREATE OR REPLACE FILE FORMAT BOXOFFICE.UTIL.CSV_STANDARD
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', '-')
  EMPTY_FIELD_AS_NULL = TRUE
  COMPRESSION = AUTO
  COMMENT = 'Standard CSV: header + double-quote-optional + NULL markers. Used by every CSV stage in this DB.';

CREATE FILE FORMAT IF NOT EXISTS BOXOFFICE.UTIL.JSON_STANDARD
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE
  COMPRESSION = AUTO
  COMMENT = 'Standard JSON: one object per file, no array stripping. Used by S3_OMDB stage and the RAW.OMDB external table.';


CREATE OR REPLACE STAGE BOXOFFICE.UTIL.S3_BOX_OFFICE
  STORAGE_INTEGRATION = KK_DEMO_PIPELINE_S3
  URL = 's3://kk-demo-pipeline/raw/box_office/'
  FILE_FORMAT = BOXOFFICE.UTIL.CSV_STANDARD
  COMMENT = 'Snowflake-read landing for CSV files moved here by Airflow from inbox/box_office/. Drives COPY INTO BOXOFFICE.RAW.REVENUES.';

CREATE STAGE IF NOT EXISTS BOXOFFICE.UTIL.S3_OMDB
  STORAGE_INTEGRATION = KK_DEMO_PIPELINE_S3
  URL = 's3://kk-demo-pipeline/raw/omdb/'
  FILE_FORMAT = BOXOFFICE.UTIL.JSON_STANDARD
  COMMENT = 'Read stage for OMDb JSON cache. Backs EXTERNAL TABLE BOXOFFICE.RAW.OMDB - data lives only in S3, Snowflake queries in-place.';

CREATE OR REPLACE STAGE BOXOFFICE.UTIL.S3_INBOX
  STORAGE_INTEGRATION = KK_DEMO_PIPELINE_S3
  URL = 's3://kk-demo-pipeline/inbox/box_office/'
  FILE_FORMAT = BOXOFFICE.UTIL.CSV_STANDARD
  COMMENT = 'Pointer to s3://kk-demo-pipeline/inbox/box_office/. Bootstrap UNLOAD writes 24 yearly CSVs here; downstream pickup logic moves them one at a time to raw/box_office/.';

CREATE OR REPLACE STAGE BOXOFFICE.UTIL.S3_SEED
  STORAGE_INTEGRATION = KK_DEMO_PIPELINE_S3
  URL = 's3://kk-demo-pipeline/_seed/box_office/'
  FILE_FORMAT = BOXOFFICE.UTIL.CSV_STANDARD
  COMMENT = 'One-off read stage for the original full-history CSV. Used once by bootstrap/01_load_seed_to_raw.sql; safe to drop after seed load.';


GRANT USAGE ON FILE FORMAT BOXOFFICE.UTIL.CSV_STANDARD TO ROLE BOXOFFICE_AIRFLOW;
GRANT USAGE ON FILE FORMAT BOXOFFICE.UTIL.JSON_STANDARD TO ROLE BOXOFFICE_AIRFLOW;

GRANT USAGE ON STAGE BOXOFFICE.UTIL.S3_BOX_OFFICE TO ROLE BOXOFFICE_AIRFLOW;
GRANT USAGE ON STAGE BOXOFFICE.UTIL.S3_OMDB TO ROLE BOXOFFICE_AIRFLOW;

SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'KK_DEMO_PIPELINE_S3',
  's3://kk-demo-pipeline/_seed/box_office/',
  'list',
  'all'
);
