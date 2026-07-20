#!/usr/bin/env node
/**
 * One-off backfill: pull historical Stripe revenue into the `transactions`
 * collection so past Stripe sales (Poly iMessage + web checkouts) show in the
 * dashboard Activity feed. Uses the SAME mapping as the live stripeWebhook
 * (../functions/stripeMap.js) so classification never diverges.
 *
 * What it writes: one doc per PAID invoice (new_subscription / trial_to_paid /
 * renewal / one_time_purchase) and one per REFUND (charge.refunded equivalent).
 * Dedupes on `stripe_dedupe_key` (invoice id / charge id) so it is safe to re-run
 * and safe to run alongside the live webhook.
 *
 * Run (from repo root):
 *   cd functions && npm install            # ensure stripe + firebase-admin present
 *   GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/tangent-dashboard-sa.json \
 *   STRIPE_SECRET_KEY=rk_live_xxx \
 *   node ../scripts/backfill-stripe.js --since=2026-01-01 [--dry-run]
 *
 * Flags:
 *   --since=YYYY-MM-DD   earliest invoice/refund date to import (default: 180d ago)
 *   --dry-run            log what would be written, write nothing
 *
 * Notes:
 *  - SA key path mirrors the MRR job (never-expiring service account). The SA can
 *    write Firestore; it does NOT need Cloud Functions perms.
 *  - STRIPE_SECRET_KEY is passed at runtime and never persisted here.
 *  - Product names are fetched + cached to name each app.
 */

const path = require("path");
const admin = require(path.join(__dirname, "..", "functions", "node_modules", "firebase-admin"));
const Stripe = require(path.join(__dirname, "..", "functions", "node_modules", "stripe"));
const { resolveStripeAppName, stripeInvoiceToTxType } = require(
  path.join(__dirname, "..", "functions", "stripeMap"));

// ---- args ----
const args = process.argv.slice(2);
const DRY = args.includes("--dry-run");
// Optional: restrict to one app (matches resolveStripeAppName output, case-insensitive).
// Use when legacy docs already cover other apps and you only want to add a missing one
// (e.g. --only-app="Poly iMessage") without re-duplicating the rest.
const onlyApp = ((args.find(a => a.startsWith("--only-app=")) || "").split("=")[1] || "").toLowerCase();
const sinceArg = (args.find(a => a.startsWith("--since=")) || "").split("=")[1];
const sinceMs = sinceArg ? Date.parse(sinceArg) : Date.now() - 180 * 86400 * 1000;
if (Number.isNaN(sinceMs)) { console.error("Bad --since date"); process.exit(1); }
const sinceUnix = Math.floor(sinceMs / 1000);

if (!process.env.STRIPE_SECRET_KEY) { console.error("Missing STRIPE_SECRET_KEY env"); process.exit(1); }
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

admin.initializeApp({ projectId: "tangent-transactions-dashboard" });
const db = admin.firestore();

const productNameCache = new Map();
async function productName(productId) {
  if (!productId) return "";
  if (productNameCache.has(productId)) return productNameCache.get(productId);
  try {
    const p = await stripe.products.retrieve(productId);
    const n = p && p.name ? p.name : "";
    productNameCache.set(productId, n);
    return n;
  } catch (e) { console.warn("product fetch failed", productId, e.message); return ""; }
}

async function alreadyStored(dedupeKey, tt) {
  const snap = await db.collection("transactions")
    .where("stripe_dedupe_key", "==", dedupeKey)
    .where("transaction_type", "==", tt)
    .limit(1).get();
  return !snap.empty;
}

function firstLine(inv) { return (inv.lines && inv.lines.data && inv.lines.data[0]) || {}; }

async function docFromInvoice(inv) {
  const tt = stripeInvoiceToTxType(inv);
  if (!tt) return null;
  const line = firstLine(inv);
  const priceId = (line.price && line.price.id) || (line.plan && line.plan.id) || "";
  const productId = (line.price && line.price.product) || (line.plan && line.plan.product) || "";
  const name = await productName(productId);
  const md = (inv.subscription_details && inv.subscription_details.metadata) || inv.metadata || {};
  const swAppId = md._sw_initiating_application_id || md._sw_application_id || "";
  const priceUSD = (inv.amount_paid || 0) / 100;
  const subId = typeof inv.subscription === "string" ? inv.subscription : "";
  const customerId = typeof inv.customer === "string" ? inv.customer : "";
  const otid = subId || customerId || inv.id;
  return {
    event_type: "invoice.paid",
    transaction_type: tt,
    app_name: resolveStripeAppName(swAppId, name),
    product_id: priceId || name || "",
    price: priceUSD,
    proceeds: priceUSD,
    price_local: priceUSD,
    currency: (inv.currency || "usd").toUpperCase(),
    store: "STRIPE",
    environment: inv.livemode === false ? "SANDBOX" : "PRODUCTION",
    country_code: (inv.account_country || "").toUpperCase(),
    received_at: admin.firestore.Timestamp.fromMillis((inv.created || 0) * 1000),
    purchased_at_ms: (inv.created || 0) * 1000,
    is_trial_conversion: tt === "trial_to_paid",
    app_user_id: md.gw_uid || md._sw_app_user_id || customerId || "",
    original_transaction_id: otid,
    is_sandbox: inv.livemode === false,
    recovered_from_billing: false,
    lifetime_spend: null,          // backfill leaves LTV null; live webhook computes going forward
    lifetime_payments: null,
    stripe_dedupe_key: inv.id,
    backfilled: true,
  };
}

async function docFromRefund(charge) {
  const priceUSD = -Math.abs((charge.amount_refunded || charge.amount || 0) / 100);
  let subId = "", priceId = "", name = "", md = charge.metadata || {};
  if (charge.invoice) {
    try {
      const inv = await stripe.invoices.retrieve(charge.invoice);
      subId = typeof inv.subscription === "string" ? inv.subscription : "";
      const line = firstLine(inv);
      priceId = (line.price && line.price.id) || (line.plan && line.plan.id) || "";
      const productId = (line.price && line.price.product) || (line.plan && line.plan.product) || "";
      md = (inv.subscription_details && inv.subscription_details.metadata) || md;
      name = await productName(productId);
    } catch (e) { console.warn("refund invoice fetch failed", e.message); }
  }
  const swAppId = md._sw_initiating_application_id || md._sw_application_id || "";
  const customerId = typeof charge.customer === "string" ? charge.customer : "";
  const otid = subId || customerId || charge.id;
  return {
    event_type: "charge.refunded",
    transaction_type: "refund",
    app_name: resolveStripeAppName(swAppId, name),
    product_id: priceId || name || "",
    price: priceUSD,
    proceeds: priceUSD,
    price_local: priceUSD,
    currency: (charge.currency || "usd").toUpperCase(),
    store: "STRIPE",
    environment: charge.livemode === false ? "SANDBOX" : "PRODUCTION",
    country_code: ((charge.billing_details && charge.billing_details.address && charge.billing_details.address.country) || "").toUpperCase(),
    received_at: admin.firestore.Timestamp.fromMillis((charge.created || 0) * 1000),
    purchased_at_ms: (charge.created || 0) * 1000,
    is_trial_conversion: false,
    app_user_id: md.gw_uid || md._sw_app_user_id || customerId || "",
    original_transaction_id: otid,
    is_sandbox: charge.livemode === false,
    recovered_from_billing: false,
    lifetime_spend: null,
    lifetime_payments: null,
    stripe_dedupe_key: charge.id,
    backfilled: true,
  };
}

async function run() {
  console.log(`Backfill Stripe → transactions since ${new Date(sinceMs).toISOString().slice(0,10)}` +
    (DRY ? " [DRY RUN]" : ""));
  let written = 0, skippedDup = 0, skippedZero = 0, refunds = 0;

  // 1. Paid invoices (subscriptions + one-time). Expand line item price for product id.
  for await (const inv of stripe.invoices.list({
    status: "paid",
    created: { gte: sinceUnix },
    limit: 100,
    expand: ["data.lines.data.price"],
  })) {
    const doc = await docFromInvoice(inv);
    if (!doc) { skippedZero++; continue; }
    if (onlyApp && (doc.app_name || "").toLowerCase() !== onlyApp) continue;
    if (await alreadyStored(doc.stripe_dedupe_key, doc.transaction_type)) { skippedDup++; continue; }
    if (DRY) { console.log("WOULD WRITE", doc.transaction_type, doc.app_name, "$" + doc.price, inv.id); }
    else { await db.collection("transactions").add(doc); }
    written++;
  }

  // 2. Refunds — list charges that were refunded.
  for await (const charge of stripe.charges.list({
    created: { gte: sinceUnix },
    limit: 100,
  })) {
    if (!charge.refunded && (charge.amount_refunded || 0) === 0) continue;
    const doc = await docFromRefund(charge);
    if (onlyApp && (doc.app_name || "").toLowerCase() !== onlyApp) continue;
    if (await alreadyStored(doc.stripe_dedupe_key, "refund")) { skippedDup++; continue; }
    if (DRY) { console.log("WOULD WRITE refund", doc.app_name, "$" + doc.price, charge.id); }
    else { await db.collection("transactions").add(doc); }
    written++; refunds++;
  }

  console.log(`Done. written=${written} (refunds=${refunds}) skippedDup=${skippedDup} skippedZeroInvoice=${skippedZero}`);
  process.exit(0);
}

run().catch(e => { console.error("Backfill failed:", e); process.exit(1); });
