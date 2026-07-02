#!/bin/bash
# Precompute churn + refund cohorts and store them in Firestore.
# Called by the daily dashboard routine (and can be run manually to seed/refresh).
#
# For each monitored app × each plan (all/weekly/annual), calls the deployed
# churnCohorts / refundCohorts Cloud Functions with the owner's gcloud OAuth
# access token, then writes the raw JSON payload into Firestore documents:
#   churn_cohorts/{appId}__{plan}
#   refund_cohorts/{appId}__{plan}
# The dashboard reads these docs directly (no function/ClickHouse on page load).
#
# Firestore stores each payload under a single JSON string field `payload`
# (the client JSON.parse()s it). Simpler + cheaper than mirroring the nested
# cohort structure into Firestore's typed-value REST format.
set -euo pipefail

PROJECT=tangent-transactions-dashboard
BASE="https://us-central1-tangent-transactions-dashboard.cloudfunctions.net"
FS="https://firestore.googleapis.com/v1/projects/$PROJECT/databases/(default)/documents"

# Monitored apps (must match the dashboard's CHURN_APPS / REFUND_APPS).
APPS=("32830" "22372" "35269")
PLANS=("all" "weekly" "annual")

TOKEN=$(gcloud auth print-access-token)
if [ -z "$TOKEN" ]; then echo "ERROR: no gcloud token (run: gcloud auth login)"; exit 1; fi
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

ok=0; fail=0
sync_one() { # $1=fn (churnCohorts|refundCohorts) $2=collection $3=appId $4=plan
  local fn="$1" coll="$2" app="$3" plan="$4"
  local json http
  json=$(/usr/bin/curl -s "$BASE/$fn?appId=$app&plan=$plan" -H "Authorization: Bearer $TOKEN")
  # sanity: must contain cohorts
  if ! echo "$json" | jq -e '.cohorts' >/dev/null 2>&1; then
    echo "  FAIL $fn $app/$plan: $(echo "$json" | head -c 120)"; fail=$((fail+1)); return
  fi
  # wrap payload as a string field + generatedAt
  local body
  body=$(jq -n --arg p "$json" --arg g "$NOW" \
    '{fields:{payload:{stringValue:$p}, generatedAt:{stringValue:$g}}}')
  http=$(/usr/bin/curl -s -o /dev/null -w "%{http_code}" -X PATCH \
    "$FS/$coll/${app}__${plan}" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$body")
  if [ "$http" = "200" ]; then ok=$((ok+1)); else echo "  FAIL PATCH $coll/${app}__${plan}: HTTP $http"; fail=$((fail+1)); fi
}

for app in "${APPS[@]}"; do
  for plan in "${PLANS[@]}"; do
    sync_one churnCohorts  churn_cohorts  "$app" "$plan"
    sync_one refundCohorts refund_cohorts "$app" "$plan"
  done
done

echo "cohort sync done: $ok written, $fail failed"
[ "$fail" -eq 0 ]
