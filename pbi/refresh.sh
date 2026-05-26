#!/usr/bin/env bash
# Trigger a Power BI enhanced refresh on a dataset. Optionally wait for it.
#
# Reads credentials from environment (pbi/.env for local dev, GHA secrets in CI).
# Output: machine-parseable `key=value` lines on stdout; errors on stderr.
# Exit codes: 0 = refresh accepted (or already running), 1 = refresh ended badly, 2 = bad input.

# set -e         = exit on first non-zero return (otherwise the script keeps going and silently "succeeds")
# set -u         = treat use of an undefined variable as an error (catches typos in variable names)
# set -o pipefail = in a pipe (foo | bar | baz), the whole pipe fails if any command fails (without this only the last one counts)
# Together: the script stops at the first problem instead of silently continuing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

POLL_INTERVAL_SEC=15        # how often we poll PBI for refresh status (used only with --wait)
WAIT_TIMEOUT_SEC=1800       # 30-minute hard cap — bail after this so CI never hangs forever

if [[ -n "${1:-}" && "$1" != "--wait" ]]; then
  echo "unknown argument: $1 (only --wait is supported)" >&2   # >&2 = write to stderr, not stdout
  exit 2                                                       # exit 2 = "user error" (convention: 0 ok, 1 runtime error, 2 bad input)
fi
WAIT=0
if [[ "${1:-}" == "--wait" ]]; then
  WAIT=1
fi

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  source "$SCRIPT_DIR/.env"
fi

die() { echo "$*" >&2; exit 1; }

if [[ -z "${PBI_TENANT_ID:-}" ]];     then echo "missing PBI_TENANT_ID"     >&2; exit 2; fi
if [[ -z "${PBI_CLIENT_ID:-}" ]];     then echo "missing PBI_CLIENT_ID"     >&2; exit 2; fi
if [[ -z "${PBI_CLIENT_SECRET:-}" ]]; then echo "missing PBI_CLIENT_SECRET" >&2; exit 2; fi
if [[ -z "${PBI_WORKSPACE_ID:-}" ]];  then echo "missing PBI_WORKSPACE_ID"  >&2; exit 2; fi
if [[ -z "${PBI_DATASET_ID:-}" ]];    then echo "missing PBI_DATASET_ID"    >&2; exit 2; fi

TOKEN_URL="https://login.microsoftonline.com/${PBI_TENANT_ID}/oauth2/v2.0/token"

token_resp="$(curl -sS -X POST "$TOKEN_URL" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "client_id=${PBI_CLIENT_ID}" \
  --data-urlencode "client_secret=${PBI_CLIENT_SECRET}" \
  --data-urlencode 'grant_type=client_credentials' \
  --data-urlencode 'scope=https://analysis.windows.net/powerbi/api/.default')"

access_token="$(echo "$token_resp" | jq -r '.access_token // empty')"
if [[ -z "$access_token" ]]; then
  err="$(echo "$token_resp" | jq -r '.error_description // .error // "no detail"')"
  die "token request failed: $err"
fi

REFRESH_URL="https://api.powerbi.com/v1.0/myorg/groups/${PBI_WORKSPACE_ID}/datasets/${PBI_DATASET_ID}/refreshes"

hdrs_file="$(mktemp)"
body_file="$(mktemp)"
trap 'rm -f "$hdrs_file" "$body_file"' EXIT

http_code="$(curl -sS -X POST "$REFRESH_URL" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  -D "$hdrs_file" \
  -o "$body_file" \
  -w '%{http_code}' \
  --data '{"type":"Full","commitMode":"transactional","retryCount":1}')"

if [[ "$http_code" == "202" ]]; then
  request_id="$(grep -i '^location:' "$hdrs_file" | tr -d '\r' | sed -E 's|.*/refreshes/||')"
  echo "request_id=${request_id}"
  echo "status=Accepted"
elif [[ "$http_code" == "400" || "$http_code" == "409" ]]; then
  err_code="$(jq -r '.error.code // empty' "$body_file")"
  if [[ "$err_code" == "OperationCurrentlyInProgress" ]]; then
    echo "status=AlreadyInProgress"
    echo "warning: a refresh is already running on this dataset; not starting a new one" >&2
    exit 0
  fi
  die "refresh failed (http $http_code, code=${err_code:-none}): $(cat "$body_file")"
else
  die "refresh failed (http $http_code): $(cat "$body_file")"
fi

if [[ "$WAIT" -eq 1 ]]; then
  STATUS_URL="${REFRESH_URL}/${request_id}"

  start=$SECONDS

  while true; do
    sleep "$POLL_INTERVAL_SEC"     # wait before polling again (don't hammer the API every few ms)

    elapsed=$(( SECONDS - start )) # $(( ... )) = bash arithmetic (different from $(...) which is command substitution)
    if [[ "$elapsed" -gt "$WAIT_TIMEOUT_SEC" ]]; then
      die "timeout after ${WAIT_TIMEOUT_SEC}s waiting for refresh ${request_id}"
    fi

    poll="$(curl -sS -X GET "$STATUS_URL" \
      -H "Authorization: Bearer $access_token")"

    status="$(echo "$poll" | jq -r '.status // "Unknown"')"

    if [[ "$status" == "Completed" ]]; then
      echo "status=Completed"
      exit 0
    elif [[ "$status" == "Failed" || "$status" == "Disabled" || "$status" == "Cancelled" ]]; then
      err="$(echo "$poll" | jq -r '.serviceExceptionJson // .error // "no detail"')"
      echo "status=${status}"
      echo "error: $err" >&2
      exit 1
    fi
  done
fi
