# Plan: Direct Stripe revenue → Activity feed

Scope (this pass): **Activity feed only.** MRR / Portfolio deferred.

## What's true today

- Activity feed = Firestore collection `transactions`, read live (snapshot, last 60) +
  History (pagination). Query filters: `is_sandbox == false`, `transaction_type in
  {new_subscription, trial_to_paid, one_time_purchase, renewal, refund}`, order
  `received_at desc`. (index.html ~L2029–2046, renderTx ~L1870.)
- Those docs are written by the `swWebhook` Cloud Function from **Superwall** events only.
- Stripe sales that go **through** a Superwall paywall already arrive via swWebhook and
  show as app_name **"Stripe Web"** (functions/index.js L59–61). Those subs carry
  `_sw_application_id` metadata in Stripe.
- Stripe sales that **don't** go through Superwall (e.g. **Poly iMessage**, $39.99/yr,
  charges seen 17–20 Jul) never hit swWebhook → **absent from Activity.** This is the gap.

## Read side = zero changes

Activity renders any `transactions` doc with the right shape. So the entire task is:
**write direct-Stripe events into `transactions` with matching fields.** No index.html edit.

## Proposed approach

### 1. New Cloud Function `stripeWebhook` (mirror of swWebhook)
Stripe calls it on each event (real-time, same "Live" behaviour as Superwall). Verify
Stripe signature (`stripe-signature` header + webhook signing secret in Secret Manager).

### 2. Events → transaction_type
- `invoice.paid` (recurring), `billing_reason`:
  - `subscription_create` → `new_subscription` (or `trial_to_paid` if a trial preceded)
  - `subscription_cycle` / `subscription_update` → `renewal`
- one-time payment (no subscription) → `one_time_purchase`
- `charge.refunded` → `refund` (price negated)
- trial start (no money) → skip (swWebhook skips TRIAL too)

### 3. Field mapping (doc shape swWebhook writes)
| field | Stripe source |
|-------|---------------|
| `transaction_type` | from event mapping above |
| `app_name` | resolve from Stripe **product name** (e.g. "Poly iMessage"); new resolver |
| `product_id` | Stripe price id (or product id) |
| `price` | invoice/charge amount ÷ 100 (USD) |
| `proceeds` | amount − Stripe fee (from balance_transaction) — or = price if we skip fees |
| `price_local` / `currency` | charge currency + amount |
| `country_code` | charge/customer billing country |
| `store` | `"STRIPE"` |
| `received_at` | serverTimestamp |
| `purchased_at_ms` | event/charge created × 1000 |
| `app_user_id` | subscription `customer` id (or metadata `gw_uid` if present) |
| `original_transaction_id` | Stripe **subscription** id (enables dedupe + LTV rollup) |
| `is_sandbox` | `livemode == false` |

### 4. Dedupe
Stripe retries webhooks. Dedupe on Stripe **invoice id** (stable per payment) or
`(subscription_id, period_start, tt)`. Cleaner than Superwall's dedupe.

### 5. Backfill (optional, phase 2)
Existing Stripe history isn't in `transactions`. One-off script: list invoices since
date X → write docs via the same mapping. Kept separate from the webhook.

## Locked decisions

**A. Stripe becomes the SOLE web-revenue source.**
- `stripeWebhook` ingests **all** Stripe events (Poly iMessage + the current
  GirlWalk/PolyAI web-checkout subs that carry `_sw_application_id`).
- `swWebhook` **stops** writing Stripe-originated docs — delete the "Stripe Web" bucket
  branch (functions/index.js L59–61) and skip any Superwall event where `store == "STRIPE"`.
- Net effect:
  - **Poly iMessage → newly appears** in Activity.
  - Existing "Stripe Web" rows now arrive via stripeWebhook with their **real app_name**
    (GirlWalk / Poly AI) — an upgrade over the lumped "Stripe Web" label.
  - No double counting: exactly one webhook owns each Stripe event.

**B. Direct-only app today = Poly iMessage.** But sole-source routing sends *all* Stripe
through the new webhook regardless, so this only affects the app_name resolver coverage.

**C. app_name resolver (Stripe → clean name).** Map by Stripe **product**:
- product "Poly iMessage" → `"Poly iMessage"`
- GW* products / girlwalk prices → `"GirlWalk"`
- Poly AI products (All-Access / Annual*) → `"Poly AI"`
- fallback → product name as-is (never the raw price_id).

**D. app_user_id.** Prefer sub metadata `gw_uid`/`_sw_app_user_id` when present; else
Stripe `customer` id. `original_transaction_id` = Stripe **subscription** id.

**E. Assumptions (flag if wrong).** test-mode (`livemode==false`) → `is_sandbox=true`
(hidden, matches feed filter). Refunds shown negative like Apple.

## STATUS: DONE (2026-07-20) — all live + verified

1. ✅ `stripeWebhook` Cloud Function (verify signature, map events, write docs, dedupe on invoice/charge id). Live at `…cloudfunctions.net/stripeWebhook`.
2. ✅ `swWebhook` skips STRIPE-store events + "Stripe Web" branch removed. Redeployed.
3. ✅ Deployed both. Stripe endpoint created (`invoice.paid`, `charge.refunded`). Secrets `STRIPE_SECRET_KEY` (restricted rk key) + `STRIPE_WEBHOOK_SECRET` in Secret Manager, accessor granted to compute SA.
4. ✅ Backfill run (since 2026-06-01) + Poly-iMessage-only re-backfill.
5. ✅ Verified via direct Firestore query (dashboard is password-gated so no UI login):
   17 visible Stripe rows, clean labels (GirlWalk, Poly iMessage), correct types/amounts.
   **Poly iMessage now appears — the original goal met.**

### Legacy cleanup (emerged during verification — wasn't in the original plan)
The `transactions` collection already had **193 production STRIPE docs** from the OLD
swWebhook path, labelled with raw price strings (`live:price_…:3days-free`) or the lumped
`"Stripe Web"`. The first full backfill duplicated some of them (different dedupe keys).
Fixed with `scripts/relabel-legacy-stripe.js`:
- **Relabelled 169** legacy docs → real app via embedded price id
  (`price_1TThE4…`/`price_1TThDS…`→GirlWalk, `price_1TXRB3…`→Poly AI).
- **Deleted 20** backfill duplicates (`backfilled:true`).
- **Left 4** docs on the ambiguous "Test" product as `"Stripe Web"` (couldn't safely attribute).
- Then re-added the 2 genuine Poly iMessage sales (+2 refunds) via `--only-app`.

### Fix: standalone charges (no invoice) — added after first ship
Poly iMessage yearly ($39.99) and some Poly weekly sales bill via a **custom
PaymentIntent with NO invoice** — the paired invoice is $0, the money is in a
`charge.succeeded` whose `invoice` is null. The original code only read `invoice.paid`
so these were invisible (the "yesterday/today transactions I don't see" report).
Fixed by handling `charge.succeeded` for charges without an invoice (gated on
`!charge.invoice` to avoid double-counting invoice-backed charges), in both the webhook
and the backfill, plus `stripeChargeToTxType` in stripeMap.js. `charge.succeeded` added
to the endpoint's enabled_events. app_name/app_user_id fall back to charge
description / `metadata.handle`.

### Known cosmetic (pre-existing, NOT touched)
Legacy GirlWalk rows carry float-noisy prices (`$5.9959…`) because the old path stored
proceeds-as-price. Dashboard `formatCurrency` rounds to `$5.99` on display. Harmless.

### Files
- `functions/index.js` — stripeWebhook + swWebhook skip
- `functions/stripeMap.js` — shared mapping (used by webhook + scripts)
- `scripts/backfill-stripe.js` — history import (`--since`, `--only-app`, `--dry-run`)
- `scripts/relabel-legacy-stripe.js` — one-off legacy cleanup (already run)
- helper debug scripts in scripts/ can be deleted.

### Deploy runbook (for reference / future redeploys)

## Deploy runbook (needs `gcloud auth login` — current token expired; SA key lacks
## cloudfunctions perms, so this must run as corentin@tangent-app.com interactively)

Two secrets required (Secret Manager, mounted via `--set-secrets`):
- `STRIPE_SECRET_KEY` — live secret key `sk_live_…` (used for products/invoices lookups).
- `STRIPE_WEBHOOK_SECRET` — the endpoint signing secret `whsec_…` (from step 2 below).

```bash
cd ~/transactions-dashboard/functions
PROJECT=tangent-transactions-dashboard
REGION=us-central1

# 0. auth (interactive)
gcloud auth login
gcloud config set project $PROJECT

# 1. create the two secrets (paste values when prompted)
printf '%s' 'sk_live_XXXXXXXX'   | gcloud secrets create STRIPE_SECRET_KEY   --data-file=- --project=$PROJECT 2>/dev/null || \
printf '%s' 'sk_live_XXXXXXXX'   | gcloud secrets versions add STRIPE_SECRET_KEY --data-file=- --project=$PROJECT
# STRIPE_WEBHOOK_SECRET is created AFTER the endpoint exists (step 3). Placeholder first:
printf '%s' 'whsec_PLACEHOLDER'  | gcloud secrets create STRIPE_WEBHOOK_SECRET --data-file=- --project=$PROJECT 2>/dev/null || true

# 2. first deploy (gets the function URL). Match swWebhook's runtime/trigger.
gcloud functions deploy stripeWebhook \
  --gen2 --region=$REGION --runtime=nodejs20 \
  --trigger-http --allow-unauthenticated \
  --entry-point=stripeWebhook \
  --max-instances=3 \
  --set-secrets='STRIPE_SECRET_KEY=STRIPE_SECRET_KEY:latest,STRIPE_WEBHOOK_SECRET=STRIPE_WEBHOOK_SECRET:latest' \
  --source=.
# URL → https://us-central1-tangent-transactions-dashboard.cloudfunctions.net/stripeWebhook
```

3. **Create the Stripe endpoint** (I can do this via the Stripe MCP `stripe_api_write`
   on your approval, or you do it in the Stripe dashboard → Developers → Webhooks):
   - URL = the deployed function URL
   - Events: `invoice.paid`, `charge.refunded`
   - Copy the endpoint's signing secret `whsec_…`.

4. **Store the real signing secret + redeploy** so the function picks it up:
```bash
printf '%s' 'whsec_REAL' | gcloud secrets versions add STRIPE_WEBHOOK_SECRET --data-file=- --project=$PROJECT
gcloud functions deploy stripeWebhook --gen2 --region=$REGION --source=. \
  --set-secrets='STRIPE_SECRET_KEY=STRIPE_SECRET_KEY:latest,STRIPE_WEBHOOK_SECRET=STRIPE_WEBHOOK_SECRET:latest'
# also redeploy swWebhook so the Stripe-skip goes live:
gcloud functions deploy swWebhook --gen2 --region=$REGION --source=. --entry-point=swWebhook
```

5. **Verify:** Stripe dashboard → the endpoint → "Send test event" (`invoice.paid`), or wait
   for a live Poly iMessage renewal. Then open the dashboard Activity feed → the row shows
   (Poly iMessage, or GirlWalk/Poly AI for the web checkouts). Check function logs:
   `gcloud functions logs read stripeWebhook --region=$REGION --gen2 --limit=20`.

### Notes / gotchas
- `swWebhook` must be **redeployed too** (step 4) or it keeps writing "Stripe Web" duplicates.
- Signature verification uses `req.rawBody` — confirmed available on GCF/functions-framework.
- Existing Stripe history is NOT backfilled; only events from endpoint-creation onward appear
  (phase 2 script handles history if wanted).
