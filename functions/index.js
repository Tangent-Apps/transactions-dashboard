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
    // Refund check FIRST (negative price overrides event type)
    if (typeof data.price === "number" && data.price < 0) {
      tt = "refund";
    } else if (type === "initial_purchase") {
      if (data.periodType === "TRIAL") return res.status(200).send("Skip trial start");
      tt = "new_subscription";
    } else if (type === "renewal") {
      if (data.isTrialConversion || data.periodType === "TRIAL" || data.periodType === "INTRO") {
        tt = "trial_to_paid";
      } else {
        // Normal paid renewal — includes recoveries from billing retry / grace period
        tt = "renewal";
      }
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
      // NOTE: Superwall's `price` and `proceeds` are always in USD.
      // `priceInPurchasedCurrency` is in the user's local currency (currencyCode).
      price: data.price || 0,                                         // USD
      proceeds: data.proceeds || 0,                                   // USD
      price_local: data.priceInPurchasedCurrency || 0,                // local currency
      currency: data.currencyCode || "USD",                           // local currency code (for price_local)
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

// Dashboard login: POST { password } → { token } (Firebase custom token)
functions.http("dashboardAuth", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "http://localhost:5173",
    "http://localhost:8000",
  ];
  if (allowedOrigins.includes(origin)) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
  }
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST") return res.status(405).send("Not allowed");

  try {
    const expected = process.env.DASHBOARD_PASSWORD;
    if (!expected) return res.status(500).send("Server misconfigured");

    const submitted = (req.body && req.body.password) || "";
    if (typeof submitted !== "string" || submitted.length > 256) {
      return res.status(400).send("Bad request");
    }
    if (submitted !== expected) {
      return res.status(401).json({ error: "Invalid password" });
    }

    const uid = "dashboard-viewer";
    const token = await admin.auth().createCustomToken(uid, { dashboard: true });
    return res.status(200).json({ token });
  } catch (err) {
    console.error("dashboardAuth error:", err.message);
    return res.status(500).send("Error");
  }
});

// Superwall KPI stats: GET ?appId=22372 (optional) → { trialConv, initialConv, cancelRate, billingIssueRate } x { today, yest }
const SW_ORG_ID = 9618;
const SW_API = "https://api.superwall.com/v2/organizations/" + SW_ORG_ID + "/query";
const CACHE_TTL_MS = 5 * 60 * 1000;
const swCache = new Map(); // key = appId|"all", value = { ts, data }

async function swQuery(sql) {
  const r = await fetch(SW_API, {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + process.env.SUPERWALL_API_KEY,
      "Content-Type": "text/plain",
    },
    body: sql,
  });
  const text = await r.text();
  if (!r.ok) throw new Error("ClickHouse " + r.status + ": " + text.slice(0, 300));
  return text.trim().split("\n").filter(Boolean).map(JSON.parse);
}

async function fetchSwStats(appId) {
  const appFilter = appId ? "AND applicationId = " + Number(appId) : "";
  // Daily counters from open_revenue: last 2 rolling 24h windows
  const eventsSql = `
SELECT
  if(ts >= now() - INTERVAL 24 HOUR, 'today', 'yest') AS bucket,
  countIf(name='initial_purchase' AND lower(periodType)='trial') AS trial_starts,
  countIf(name='initial_purchase' AND lower(periodType)!='trial') AS direct_purchases,
  countIf(name='renewal' AND isTrialConversion=1) AS trial_conversions,
  countIf(name='cancellation') AS cancellations,
  countIf(name='billing_issue') AS billing_issues,
  countIf(name='renewal' AND isTrialConversion=0) AS renewals
FROM open_revenue.attributed_events_by_ts_rep
WHERE isSandbox = 0
  ${appFilter}
  AND ts >= now() - INTERVAL 48 HOUR
  AND ts < now()
GROUP BY bucket
FORMAT JSONEachRow`.trim();

  const opensSql = `
SELECT
  if(ts >= now() - INTERVAL 24 HOUR, 'today', 'yest') AS bucket,
  count() AS opens
FROM sw.events_rep
WHERE isSandbox = 0
  ${appFilter}
  AND name = 'paywall_open'
  AND ts >= now() - INTERVAL 48 HOUR
  AND ts < now()
GROUP BY bucket
FORMAT JSONEachRow`.trim();

  const activeSql = `
SELECT count() AS active
FROM sw.subscription_status_rep FINAL
WHERE isSandbox = 0
  AND isDeleted = 0
  ${appFilter}
  AND JSONExtractString(props, '$status') = 'ACTIVE'
FORMAT JSONEachRow`.trim();

  const [events, opens, active] = await Promise.all([
    swQuery(eventsSql),
    swQuery(opensSql),
    swQuery(activeSql),
  ]);

  const evByBucket = { today: {}, yest: {} };
  for (const row of events) evByBucket[row.bucket] = row;
  const opByBucket = { today: 0, yest: 0 };
  for (const row of opens) opByBucket[row.bucket] = Number(row.opens || 0);
  const activeCount = Number((active[0] && active[0].active) || 0);

  function num(b, k) { return Number((evByBucket[b] || {})[k] || 0); }
  function pct(n, d) { return d > 0 ? (n / d) * 100 : null; }

  function bucketMetrics(b) {
    const trialStarts = num(b, 'trial_starts');
    const directPurchases = num(b, 'direct_purchases');
    const trialConversions = num(b, 'trial_conversions');
    const cancellations = num(b, 'cancellations');
    const billingIssues = num(b, 'billing_issues');
    const renewals = num(b, 'renewals');
    const opensTotal = opByBucket[b];
    return {
      initialConversion: pct(trialStarts + directPurchases, opensTotal),
      trialConversion: pct(trialConversions, trialStarts),
      cancellationRate: activeCount > 0 ? (cancellations / activeCount) * 100 : null,
      billingIssueRate: pct(billingIssues, renewals + billingIssues),
      counts: { trialStarts, directPurchases, trialConversions, cancellations, billingIssues, renewals, opens: opensTotal },
    };
  }

  return {
    activeSubs: activeCount,
    today: bucketMetrics('today'),
    yest: bucketMetrics('yest'),
  };
}

functions.http("superwallStats", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "http://localhost:5173",
    "http://localhost:8000",
  ];
  if (allowedOrigins.includes(origin)) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
  }
  res.set("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");

  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "GET") return res.status(405).send("Not allowed");

  try {
    // Verify Firebase ID token w/ dashboard claim
    const authHeader = req.headers.authorization || "";
    if (!authHeader.startsWith("Bearer ")) return res.status(401).json({ error: "Missing token" });
    const idToken = authHeader.slice(7);
    let decoded;
    try {
      decoded = await admin.auth().verifyIdToken(idToken);
    } catch (e) {
      return res.status(401).json({ error: "Invalid token" });
    }
    if (decoded.dashboard !== true) return res.status(403).json({ error: "Forbidden" });

    const appIdRaw = req.query.appId;
    const appId = appIdRaw && /^\d+$/.test(String(appIdRaw)) ? Number(appIdRaw) : null;
    const cacheKey = appId ? String(appId) : "all";
    const now = Date.now();
    const hit = swCache.get(cacheKey);
    if (hit && (now - hit.ts) < CACHE_TTL_MS) {
      res.set("X-Cache", "HIT");
      return res.status(200).json(hit.data);
    }

    const data = await fetchSwStats(appId);
    swCache.set(cacheKey, { ts: now, data });
    res.set("X-Cache", "MISS");
    return res.status(200).json(data);
  } catch (err) {
    console.error("superwallStats error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});
