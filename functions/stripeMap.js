// Shared Stripe → dashboard mapping helpers.
// Used by both the live stripeWebhook (index.js) and the one-off backfill script
// (scripts/backfill-stripe.js) so the two never drift in how they classify events
// or resolve app names.

// Superwall applicationId → canonical app_name. Mirror of the dashboard APP_NAMES
// map (index.html) + resolveAppName in index.js; folds Stripe web checkouts that
// carry `_sw_initiating_application_id` into their real app bucket.
const SW_APP_ID_TO_NAME = {
  "22372": "GirlTalk", "22373": "GirlTalk", "23873": "GirlTalk", "23954": "Spicy Stories",
  "24574": "Hola", "28209": "Christian Daily Task", "29053": "Christian Music",
  "38342": "Christian Music", "31709": "Speak & Learn Spanish: Dora", "32829": "Better Stretch",
  "32830": "GirlWalk", "35269": "Poly AI", "41169": "Better Breath", "42734": "Quit Alcohol",
  "43349": "Yarn Ai", "44197": "Baby Food Scan", "45833": "Sync: Women's Health",
  "46586": "Woman Faith Tasks", "47318": "LCKR", "47554": "PollyChat", "48516": "GirlTasks",
};

// Resolve app_name for a Stripe transaction. Prefer the Superwall initiating app id
// (exact, present on SW web checkouts); else match the Stripe product NAME by keyword
// (Stripe price ids carry no app hint, but product names do: "GW Weekly 7.99",
// "Poly iMessage — yearly", "Annual Open"). Falls back to the product name itself.
function resolveStripeAppName(swInitiatingAppId, productName) {
  if (swInitiatingAppId && SW_APP_ID_TO_NAME[String(swInitiatingAppId)]) {
    return SW_APP_ID_TO_NAME[String(swInitiatingAppId)];
  }
  const n = (productName || "").toLowerCase();
  if (n.includes("imessage")) return "Poly iMessage";
  if (n.startsWith("gw ") || n.includes("girlwalk") || n.includes("girl walk")) return "GirlWalk";
  if (n.includes("girltalk") || n.includes("girl talk")) return "GirlTalk";
  // Poly AI's Stripe products are named "All-Access …", "Annual …" (no "poly" token).
  if (n.includes("poly") || n.includes("all-access") || n.startsWith("annual") ||
      n.includes("chat credit")) return "Poly AI";
  return productName || "Stripe";
}

// Map a Stripe invoice onto the dashboard's transaction_type vocabulary.
// Returns null for events we don't surface (e.g. $0 trial-start invoices).
function stripeInvoiceToTxType(invoice) {
  const reason = invoice.billing_reason || "";
  if ((invoice.amount_paid || 0) <= 0) return null; // trial start / $0 — skip like swWebhook
  if (reason === "subscription_create") {
    const line = (invoice.lines && invoice.lines.data && invoice.lines.data[0]) || {};
    const hadTrial = !!(line.period && line.plan && line.plan.trial_period_days) ||
      (invoice.subscription_details && invoice.subscription_details.metadata &&
        invoice.subscription_details.metadata._sw_offer_kind === "trial");
    return hadTrial ? "trial_to_paid" : "new_subscription";
  }
  if (reason === "subscription_cycle" || reason === "subscription_update") return "renewal";
  if (reason === "manual" || reason === "subscription_threshold") return "one_time_purchase";
  return "renewal"; // unknown recurring reason — still real revenue
}

module.exports = { SW_APP_ID_TO_NAME, resolveStripeAppName, stripeInvoiceToTxType };
