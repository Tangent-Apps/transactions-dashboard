# ROAS Cohort Tracker

Powers the **ROAS** tab of the dashboard. "Cohort-vaulted" ROAS: each spend-day's
return = ad spend that day ÷ **all future proceeds from the users first acquired
that day**. Renewals fold back onto their cohort's spend-day, so old days keep
maturing (D0 → D7 → D30 → D90).

## Data sources
- **Spend:** Adjust Report Service API. Metric = `network_cost` (NOT `cost` — cost
  returns almost nothing). `date_period` relative format = `-14d:-0d` (the API
  rejects `today`/`0d`). Token in the `ADJUST_API_TOKEN` env var on the functions.
- **Revenue:** Superwall ClickHouse `open_revenue.attributed_events_by_ts_rep`,
  numerator = `proceeds` (net of store cut). Same `SUPERWALL_API_KEY` secret the
  churn/refund functions use, mounted via `--set-secrets`.

## How it refreshes
The existing desktop **Claude Code scheduled task** that refreshes churn/refund
cohorts calls `dailyCohortSync`. That function now also pulls spend (`adjustSpendSync`)
**first**, then writes the ROAS rollup per app. No separate cron.

## Collections (Cloud-Function-write only; dashboard reads with dashboard claim)
- `ad_spend/{appId}__{YYYY-MM-DD}` — daily spend per app.
- `roas_cohorts/{appId}` — precomputed **raw-dollar** daily rows (180d window, JSON
  `payload`). Each row = spend + proceeds cumulative at D0/D7/D30/D90 + lifetime + age.
  The dashboard filters by range (30/60/90/180/All), buckets into day/week/month, and
  computes ROAS % = Σproceeds/Σspend×100 per bucket. Ratios never come from the backend.

To seed more history after changing the window, backfill spend once:
`curl "$BASE/adjustSpendSync?period=-180d:-0d" -H "Authorization: Bearer $(gcloud auth print-access-token)"`
then run `dailyCohortSync`.

## App mapping (Adjust label → Superwall applicationId)
- `Girl Walk` → 32830 (GirlWalk)
- `Solo Girlies` → 22372 (GirlTalk)  ← note the label
- `Poly Ai` → 35269 (Poly AI)
`adjustSpendSync` logs any unmapped Adjust app it skips. Add new paid-UA apps to
`resolveAdjustAppId` and `ROAS_APPS` in functions/index.js.

## Logic notes
- Day boundaries use tz **America/New_York** on both spend and revenue so they align.
- Cohort day = first paid day per `originalTransactionId` (`min(purchasedAt)`).
- D0/D7/D30/D90 are cumulative; a column is `—` until the spend-day ages past that
  milestone, or if that day has no ad spend yet (e.g. today, before Adjust reports).
- Refunds subtract from their **original cohort day**, so mature rows can drop.

## Verify / troubleshoot
Dashboard → **ROAS** tab → pick an app. Table fills with spend/buyers/ROAS per day.
- Tab says "no data yet" → `dailyCohortSync` hasn't run since deploy.
- Spend column all `—` for older days → Adjust pull failed; check `ADJUST_API_TOKEN`
  and that app labels still map (run `adjustSpendSync`, read its logs for skips).
- ClickHouse 401 in `roasCohorts` → `SUPERWALL_API_KEY` secret expired or unmounted.
