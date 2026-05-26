#!/usr/bin/env bash
# Create (or update) the AWS IAM role that Snowflake assumes to read/write s3://kk-demo-pipeline/.
#
# Prerequisites:
#   - snowflake/ddl/03_storage_integration.sql executed (storage integration KK_DEMO_PIPELINE_S3 exists)
#   - AWS CLI v2 configured with rights to create IAM roles + put role policies (kk-admin has AdministratorAccess)
#   - snow CLI configured at ~/.config/snowflake/config.toml with connection 'default'
#
# Flow:
#   1. Read STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION KK_DEMO_PIPELINE_S3
#   2. Build trust policy (only that Snowflake user, gated by that external id)
#   3. Build bucket policy (R/W on kk-demo-pipeline scoped to _seed/ + raw/ + inbox/ + archive/ — see docs/data-flow.md)
#   4. Create role or update its trust policy if it already exists; put the bucket policy as inline
#
# Idempotent: re-running updates the trust policy + inline policy in place.

set -euo pipefail

ROLE_NAME="snowflake-kk-demo-pipeline"
BUCKET="kk-demo-pipeline"
INTEGRATION="KK_DEMO_PIPELINE_S3"
SNOW_CONFIG="$HOME/.config/snowflake/config.toml"

ACTUAL_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
echo "AWS account: $ACTUAL_ACCOUNT_ID"
echo "IAM role:    $ROLE_NAME"
echo "S3 bucket:   $BUCKET"
echo


echo "reading STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID from Snowflake..."

desc_json="$(snow --config-file "$SNOW_CONFIG" sql -c default --format json -q "DESC INTEGRATION ${INTEGRATION};" 2>/dev/null)"

SNOWFLAKE_IAM_USER_ARN="$(echo "$desc_json" | jq -r '.[] | select(.property=="STORAGE_AWS_IAM_USER_ARN") | .property_value')"
SNOWFLAKE_EXTERNAL_ID="$(echo "$desc_json" | jq -r '.[] | select(.property=="STORAGE_AWS_EXTERNAL_ID") | .property_value')"

if [[ -z "$SNOWFLAKE_IAM_USER_ARN" || -z "$SNOWFLAKE_EXTERNAL_ID" ]]; then
  echo "ERROR: could not extract trust values from DESC INTEGRATION ${INTEGRATION}" >&2
  echo "       Make sure 03_storage_integration.sql ran successfully." >&2
  exit 1
fi

echo "  IAM_USER_ARN: $SNOWFLAKE_IAM_USER_ARN"
echo "  EXTERNAL_ID:  $SNOWFLAKE_EXTERNAL_ID"
echo


trust_policy=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "${SNOWFLAKE_IAM_USER_ARN}" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "${SNOWFLAKE_EXTERNAL_ID}" }
      }
    }
  ]
}
EOF
)

bucket_policy=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${BUCKET}/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET}",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "_seed/*", "_seed",
            "raw/*", "raw",
            "inbox/*", "inbox",
            "archive/*", "archive"
          ]
        }
      }
    }
  ]
}
EOF
)


if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "role $ROLE_NAME exists - updating trust policy"
  aws iam update-assume-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-document "$trust_policy"
else
  echo "creating role $ROLE_NAME"
  aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$trust_policy" \
    --description "Snowflake storage integration ${INTEGRATION} - read/write s3://${BUCKET}/" \
    >/dev/null
fi

echo "attaching inline policy boxoffice-s3-rw"
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name boxoffice-s3-rw \
  --policy-document "$bucket_policy"

echo
ROLE_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)"
echo "done. Role ARN: $ROLE_ARN"
echo
echo "next step - validate the bridge from Snowflake:"
echo "  snow sql -c default -q \"SELECT SYSTEM\$VALIDATE_STORAGE_INTEGRATION('${INTEGRATION}', 's3://${BUCKET}/raw/', 'validate_all', TRUE);\""
