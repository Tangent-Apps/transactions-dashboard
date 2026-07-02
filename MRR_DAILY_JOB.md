# Daily MRR Snapshot Job

Refreshes the portfolio MRR/ARR snapshots powering the **Portfolio** tab of the
transactions dashboard. Writes one Firestore doc per day to collection `mrr_snapshots`.

## How it runs
A **Claude Code scheduled task** on this desktop: `daily-mrr-snapshot`, daily at 10:08 AM
local. Stored at `~/.claude/scheduled-tasks/daily-mrr-snapshot/SKILL.md`.
(Chosen over the Mac mini because the mini isn't logged into Google creds; this desktop's
`gcloud` is authenticated as a project owner.)

Runs only while the Claude Code desktop app is open. If it's closed when due, it runs on
next launch. Manage it from the **Scheduled** section in the sidebar.

## What it does each run
1. **Discovers apps dynamically** via `list_projects` (org 9618) — keeps every app where
   `platform != web` and `archived_at` is null. New launches auto-included; no hardcoded
   id list to maintain.
2. Pulls Superwall's own MRR timeseries (last 7 days) for each app via the Superwall MCP.
3. Sums per-day across all apps → portfolio MRR. ARR = MRR × 12.
4. Upserts `mrr_snapshots/{YYYY-MM-DD}` for the last 7 days (recent days re-pulled every
   run because MRR is retroactive — refunds/renewals shift past days).

## Prerequisites (this machine)
- Superwall MCP server connected (tool `mcp__…__get_chart_data`).
- `gcloud auth print-access-token` returns a token (owner of `tangent-transactions-dashboard`).
  If it errors, run `gcloud auth login`.
- `curl` + `jq`.

## Data notes
- Source is Superwall's native `mrr` metric — the exact number on their MRR chart.
  We do NOT reconstruct MRR from raw ClickHouse events (that diverged badly).
- ARR = MRR × 12 exactly (Superwall's definition).
- 90 days of history (from 2026-04-04) already backfilled.
- Currently 12 apps contribute nonzero MRR; GirlWalk + GirlTalk dominate. Stripe Web
  is not yet included (low volume) — add later if needed.

## Verify
Dashboard → **Portfolio** tab. Latest date should be today/yesterday. A
"⚠ snapshot N days behind" banner means the job hasn't run — check the app was open,
gcloud auth, and the MCP connection.
