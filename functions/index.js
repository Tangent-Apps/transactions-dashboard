const functions = require("@google-cloud/functions-framework");
const admin = require("firebase-admin");
const { Webhook } = require("svix");

admin.initializeApp();
const db = admin.firestore();

function resolveAppName(appId, productId) {
  const id = (appId + productId).toLowerCase();
  if (id.includes("girltalk") || id.includes("girl_talk")) return "GirlTalk";
  if (id.includes("girlwalk") || id.includes("girl_walk")) return "GirlWalk";
  if (id.includes("music")) return "Christian Music";
  if (id.includes("christian") || id.includes("cdt")) return "Christian Daily Task";
  if (id.includes("spicy")) return "Spicy Stories";
  if (id.includes("hola")) return "Hola";
  if (id.includes("stretch")) return "Better Stretch";
  if (id.includes("poly")) return "Poly AI";
  if (id.includes("girlies") || id.includes("therapy")) return "GirlTalk";
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

functions.http("swWebhook", async (req, res) => {
  if (req.method !== "POST") return res.status(405).send("Not allowed");
  try {
    // Verify Superwall signature using Svix
    const secret = process.env.SUPERWALL_WEBHOOK_SECRET;
    if (!secret) return res.status(500).send("Missing webhook secret");

    const wh = new Webhook(secret);
    let payload;
    try {
      payload = wh.verify(req.rawBody, {
        "svix-id": req.headers["svix-id"],
        "svix-timestamp": req.headers["svix-timestamp"],
        "svix-signature": req.headers["svix-signature"],
      });
    } catch (verifyErr) {
      console.warn("Signature verification failed:", verifyErr.message);
      return res.status(401).send("Invalid signature");
    }

    const type = payload.type;
    const data = payload.data || {};

    // Determine transaction type — skip events we don't care about
    let tt;
    if (type === "initial_purchase") {
      if (data.periodType === "TRIAL") return res.status(200).send("Skip trial start");
      tt = "new_subscription";
    } else if (type === "renewal") {
      if (!data.isTrialConversion) return res.status(200).send("Skip renewal");
      tt = "trial_to_paid";
    } else if (type === "non_renewing_purchase") {
      tt = "one_time_purchase";
    } else {
      return res.status(200).send("Ignored");
    }

    const tx = {
      event_type: type,
      transaction_type: tt,
      app_name: resolveAppName(data.bundleId || "", data.productId || ""),
      product_id: data.productId || "",
      price: data.price || 0,
      currency: data.currencyCode || "USD",
      store: data.store || "UNKNOWN",
      environment: data.environment || "PRODUCTION",
      country_code: data.countryCode || "",
      received_at: admin.firestore.FieldValue.serverTimestamp(),
      purchased_at_ms: data.purchasedAt ? new Date(data.purchasedAt).getTime() : null,
      is_trial_conversion: data.isTrialConversion || false,
      app_user_id: data.originalAppUserId || "",
      original_transaction_id: data.originalTransactionId || "",
      is_sandbox: data.environment === "SANDBOX",
    };

    await db.collection("transactions").add(tx);
    return res.status(200).send("OK");
  } catch (err) {
    console.error("Webhook error:", err.message, err.stack);
    return res.status(500).send("Error: " + err.message);
  }
});
