-- Boxoffice pipeline — database + schemas (deliverable 1 of 8).
-- Run as ACCOUNTADMIN. Idempotent: re-running this script is a no-op once objects exist.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS BOXOFFICE
  COMMENT = 'Boxoffice recruitment-task pipeline: raw S3 landing -> dbt models -> PBI marts.';

DROP SCHEMA IF EXISTS BOXOFFICE.PUBLIC;


CREATE SCHEMA IF NOT EXISTS BOXOFFICE.RAW
  COMMENT = 'Raw landing tables loaded by Airflow EL from s3://kk-demo-pipeline/raw/.';

CREATE SCHEMA IF NOT EXISTS BOXOFFICE.STAGING
  COMMENT = 'dbt staging models (cleaned, typed, deduplicated).';

CREATE SCHEMA IF NOT EXISTS BOXOFFICE.MARTS
  COMMENT = 'dbt facts and dims served to Power BI.';

CREATE SCHEMA IF NOT EXISTS BOXOFFICE.UTIL
  COMMENT = 'Helper objects: storage integration, file format, external stages.';
