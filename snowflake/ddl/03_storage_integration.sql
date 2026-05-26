-- Boxoffice pipeline — storage integration to S3 (deliverable 3 of 8).
-- Run as ACCOUNTADMIN. Creates the Snowflake side of the trust bridge to s3://kk-demo-pipeline/.
--
-- Flow:
--   1. This script: CREATE STORAGE INTEGRATION (points at an AWS role that doesn't exist yet)
--   2. DESC INTEGRATION exposes STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID
--   3. aws/setup_iam_role.sh uses those values to build the trust policy of the AWS IAM role
--   4. SYSTEM$VALIDATE_STORAGE_INTEGRATION at the bottom confirms the bridge works end-to-end

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS KK_DEMO_PIPELINE_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::567119266888:role/snowflake-kk-demo-pipeline'
  STORAGE_ALLOWED_LOCATIONS = ('s3://kk-demo-pipeline/')
  COMMENT = 'Trust bridge to s3://kk-demo-pipeline/ for COPY INTO from raw/ and COPY UNLOAD to demo/.';

GRANT USAGE ON INTEGRATION KK_DEMO_PIPELINE_S3 TO ROLE BOXOFFICE_AIRFLOW;

DESC INTEGRATION KK_DEMO_PIPELINE_S3;
