const functions = require("@google-cloud/functions-framework");
const admin = require("firebase-admin");

admin.initializeApp();
const db = admin.firestore();

const CONVERSION_EVENTS = new Set([
  "INITIAL_PURCHASE",
  "RENEWAL",
  "NON_RENEWING_PURCHASE"
]);

function resolveAppName(appId, productId) {
  const id = (appId + productId).toLowerCase();
  if (id.includes("girltalk") || id.includes("girl_talk")) return "GirlTalk";
  if (id.includes("girlwalk") || id.includes("girl_walk")) return "GirlWalk";
  if (id.includes("christian") || id.includes("cdt")) return "Christian Daily Task";
  if (id.includes("spicy")) return "Spicy Stories";
  if (id.includes("hola")) return "Hola";
  if (id.includes("stretch")) return "Better Stretch";
  if (id.includes("girlies") || id.includes("therapy")) return "Girlies Therapy";
  if (id.includes("prayer")) return "Prayer";
  if (id.includes("mew")) return "Mew";
  if (id.includes("crime") || id.includes("noir")) return "Crime Novels";
  if (id.includes("murder") || id.includes("mystery")) return "Murder Mystery";
  if (id.includes("reel") || id.includes("short_stories")) return "Reel Short Stories";
  if (id.includes("daily") && id.includes("prayer")) return "Daily Prayers";
  if (productId) {
    const s = productId.split(".");
    return s.length > 2 ? s[2] : productId;
  }
  return appId || "Unknown";
}

functions.http("rcWebhook", async (req, res) => {
  if (req.method !== "POST") return res.status(405).send("Not allowed");
  try {
    const e = req.body.event;
    if (!e) return res.status(400).send("No event");

    const t = e.type;
    if (!CONVERSION_EVENTS.has(t)) return res.status(200).send("Ignored");

    if (t === "INITIAL_PURCHASE" && e.period_type === "TRIAL")
      return res.status(200).send("Skip trial");

    const itc = t === "RENEWAL" && e.period_type === "NORMAL" &&
      (e.is_trial_conversion === true || e.renewal_number === 1);
    if (t === "RENEWAL" && !itc) return res.status(200).send("Skip renewal");

    let tt;
    if (t === "NON_RENEWING_PURCHASE") tt = "one_time_purchase";
    else if (t === "INITIAL_PURCHASE") tt = "new_subscription";
    else if (itc) tt = "trial_to_paid";
    else tt = "unknown";

    const tx = {
      event_type: t,
      transaction_type: tt,
      app_id: e.app_id || "",
      app_name: resolveAppName(e.app_id || "", e.product_id || ""),
      product_id: e.product_id || "",
      price: e.price || e.price_in_purchased_currency || 0,
      currency: e.currency || "USD",
      store: e.store || "UNKNOWN",
      environment: e.environment || "PRODUCTION",
      country_code: e.country_code || "",
      event_timestamp_ms: e.event_timestamp_ms || Date.now(),
      purchased_at_ms: e.purchased_at_ms || null,
      received_at: admin.firestore.FieldValue.serverTimestamp(),
      period_type: e.period_type || null,
      is_trial_conversion: itc || false
    };

    if (e.environment === "SANDBOX") tx.is_sandbox = true;

    await db.collection("transactions").add(tx);
    return res.status(200).send("OK");
  } catch (err) {
    console.error("Webhook error:", err.message, err.stack);
    return res.status(500).send("Error: " + err.message);
  }
});
