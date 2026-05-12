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

// Trial length lag — Superwall trials typically 3 days. After D, conversions/cancellations
// land 3 days later. So "today's" TTP/cancel/billing for cohort started 3 days ago.
const TRIAL_LAG_DAYS = 3;

async function fetchSwStats(appId) {
  const appFilter = appId ? "AND applicationId = " + Number(appId) : "";

  // Cohort-by-trial-start: matches Superwall Charts methodology.
  // - Trial starts: paywall-originated trials only (paywallId > 0); Charts notes
  //   "only includes trials started on a Superwall paywall".
  // - Outcomes joined via originalTransactionId so they attribute to trial-start day.
  // - Each trial gets EXACTLY ONE terminal status by priority:
  //   1. renewal (= conversion to paid; periodType is NORMAL but isTrialConversion=1)
  //   2. cancellation (TRIAL periodType)
  //   3. billing_issue (TRIAL periodType)
  //   4. expiration (TRIAL periodType)
  //   5. pending (no terminal event yet)
  //   Cancellation supersedes billing_issue supersedes expiration since users often
  //   accumulate multiple events as a trial winds down.
  // - Lookup window extends 7d past today() to capture late events of trials started
  //   in the 14-day cohort window.
  const eventsSql = `
WITH trials AS (
  SELECT
    originalTransactionId AS otid,
    toDate(ts, 'UTC') AS trial_day
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND name = 'initial_purchase'
    AND lower(periodType) = 'trial'
    AND paywallId > 0
    AND ts >= today() - 13
    AND ts < today() + 1
),
outcomes AS (
  SELECT
    originalTransactionId AS otid,
    name AS oname
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND (
      (name = 'renewal' AND isTrialConversion = 1)
      OR (name IN ('cancellation','billing_issue','expiration') AND lower(periodType) = 'trial')
    )
    AND ts >= today() - 13
    AND ts < today() + 8
),
per_trial AS (
  SELECT
    trials.otid AS otid,
    trials.trial_day AS day,
    argMin(outcomes.oname, multiIf(
      outcomes.oname = 'renewal', 1,
      outcomes.oname = 'cancellation', 2,
      outcomes.oname = 'billing_issue', 3,
      outcomes.oname = 'expiration', 4,
      99
    )) AS status
  FROM trials
  LEFT JOIN outcomes ON trials.otid = outcomes.otid
  GROUP BY trials.otid, trials.trial_day
)
SELECT
  day,
  count() AS trial_starts,
  countIf(status = 'renewal') AS trial_conversions,
  countIf(status = 'cancellation') AS trial_cancellations,
  countIf(status = 'billing_issue') AS in_billing_retry,
  countIf(status = 'expiration') AS trial_expirations,
  countIf(status = '' OR status IS NULL) AS pending_trials
FROM per_trial
GROUP BY day
ORDER BY day ASC
FORMAT JSONEachRow`.trim();

  // Funnel (ITT + Paywall Rate): cohort-by-install-day.
  // - New Users = uniq users with app_install on day d (matches Charts "New Users")
  // - Paywalled Users = of those new users, the subset that paywall_opened on day d
  // - Converted Users = of those new users, the subset that started a trial on day d
  // ITT = converted_users / new_users; Paywall Rate = paywalled_users / new_users.
  // sw.events_rep has 7-day cap; this query stays within it.
  const funnelSql = `
WITH installs AS (
  SELECT toDate(ts, 'UTC') AS day, JSONExtractString(meta, 'appUserId') AS uid
  FROM sw.events_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND name = 'app_install'
    AND ts >= today() - 6
    AND ts < today() + 1
),
paywall_opens AS (
  SELECT toDate(ts, 'UTC') AS day, JSONExtractString(meta, 'appUserId') AS uid
  FROM sw.events_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND name = 'paywall_open'
    AND ts >= today() - 6
    AND ts < today() + 1
),
trial_starts AS (
  SELECT toDate(ts, 'UTC') AS day, appUserId AS uid
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND name = 'initial_purchase'
    AND lower(periodType) = 'trial'
    AND paywallId > 0
    AND ts >= today() - 6
    AND ts < today() + 1
)
SELECT
  i.day AS day,
  uniq(i.uid) AS new_users,
  uniqIf(i.uid, p.uid IS NOT NULL AND p.uid != '') AS paywalled_users,
  uniqIf(i.uid, t.uid IS NOT NULL AND t.uid != '') AS converted_users
FROM installs i
LEFT JOIN paywall_opens p ON i.uid = p.uid AND i.day = p.day
LEFT JOIN trial_starts t ON i.uid = t.uid AND i.day = t.day
GROUP BY day
ORDER BY day ASC
FORMAT JSONEachRow`.trim();

  const [events, funnel] = await Promise.all([
    swQuery(eventsSql),
    swQuery(funnelSql),
  ]);

  // Build map by day string YYYY-MM-DD
  const evMap = new Map();
  for (const r of events) evMap.set(r.day, r);
  const fnMap = new Map();
  for (const r of funnel) fnMap.set(r.day, r);

  function dStr(d) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  function pct(n, d) { return d > 0 ? (n / d) * 100 : null; }

  // Build series for last 7 displayable days (each ending TRIAL_LAG_DAYS ago for trial metrics)
  // Most recent meaningful "today" = today - TRIAL_LAG_DAYS for trial-cohort metrics
  // But ITT + Paywall Rate are not lagged — they reflect that day's funnel.
  // We'll return:
  //   - trial metrics: series of 7 days ending today - TRIAL_LAG_DAYS
  //   - funnel metrics: series of 7 days ending today
  const todayUTC = new Date();
  todayUTC.setUTCHours(0, 0, 0, 0);

  function buildSeries(endOffset) {
    // endOffset = how many days back from today the most recent day is
    const out = [];
    for (let i = 0; i < 7; i++) {
      const d = new Date(todayUTC);
      d.setUTCDate(d.getUTCDate() - endOffset - i);
      out.push(dStr(d));
    }
    return out.reverse(); // oldest → newest
  }

  const trialDays = buildSeries(TRIAL_LAG_DAYS); // for TTP, cancel%, billing%
  const funnelDays = buildSeries(0);             // for ITT, paywall rate

  // For each trial day d:
  //   trial_starts on d → cohort. After lag, observe their conversions/cancellations/billing-issues.
  //   The brief uses: trial_conversions[d_observe] / trial_starts[d_observe - lag], where d_observe = today.
  //   Equivalent if we treat "today" as the observation day. But brief shows daily series of OBSERVATION days,
  //   each with their own cohort.
  //   Simpler approach matching brief: for each observation day d (= today - lag - i), show
  //     conversions[d] / trial_starts[d - lag]
  //   Or, cohort-anchored: for each cohort day c, show conversions[c + lag] / trial_starts[c].
  //   Brief format: trial_conversions on day d divided by trial_starts on day d (no shift), then drop the
  //   most recent lag days because they're incomplete. → "12.5% (5/40)" where 5=conversions on d, 40=trial_starts on d.
  //   Re-reading: "(In Billing Retry / Trial Starts)" daily — so numerator and denominator are SAME-DAY.
  //   So the lag is just: skip the most recent N days because trial_starts on those days haven't had time
  //   to convert/cancel/billing yet. NOT a shift.
  //   Bottom line: numerator and denominator from SAME day, but only show days that are old enough
  //   (today - lag onward).

  const trialSeries = trialDays.map(day => {
    const e = evMap.get(day) || {};
    const ts = Number(e.trial_starts || 0);
    const tc = Number(e.trial_conversions || 0);
    const tcn = Number(e.trial_cancellations || 0);
    const ibr = Number(e.in_billing_retry || 0);
    const exp = Number(e.trial_expirations || 0);
    const pend = Number(e.pending_trials || 0);
    return {
      day,
      trial_starts: ts,
      trial_conversions: tc,
      trial_cancellations: tcn,
      in_billing_retry: ibr,
      trial_expirations: exp,
      pending_trials: pend,
      ttp: pct(tc, ts),
      cancellation_rate: pct(tcn, ts),
      billing_issue_rate: pct(ibr, ts),
    };
  });

  const funnelSeries = funnelDays.map(day => {
    const f = fnMap.get(day) || {};
    const newUsers = Number(f.new_users || 0);
    const paywalled = Number(f.paywalled_users || 0);
    const converted = Number(f.converted_users || 0);
    return {
      day,
      new_users: newUsers,
      converted_users: converted,
      paywalled_users: paywalled,
      itt: pct(converted, newUsers),
      paywall_rate: pct(paywalled, newUsers),
    };
  });

  // "Today" and "yesterday" headline values = the most recent day in each series
  const trialToday = trialSeries[trialSeries.length - 1];
  const trialYest = trialSeries[trialSeries.length - 2];
  const funnelToday = funnelSeries[funnelSeries.length - 1];
  const funnelYest = funnelSeries[funnelSeries.length - 2];

  return {
    today: {
      ittRate: funnelToday.itt,
      ttpRate: trialToday.ttp,
      cancellationRate: trialToday.cancellation_rate,
      billingIssueRate: trialToday.billing_issue_rate,
      paywallRate: funnelToday.paywall_rate,
      counts: {
        trial_starts: trialToday.trial_starts,
        trial_conversions: trialToday.trial_conversions,
        trial_cancellations: trialToday.trial_cancellations,
        in_billing_retry: trialToday.in_billing_retry,
        new_users: funnelToday.new_users,
        converted_users: funnelToday.converted_users,
        paywalled_users: funnelToday.paywalled_users,
      },
    },
    yest: {
      ittRate: funnelYest.itt,
      ttpRate: trialYest.ttp,
      cancellationRate: trialYest.cancellation_rate,
      billingIssueRate: trialYest.billing_issue_rate,
      paywallRate: funnelYest.paywall_rate,
      counts: {
        trial_starts: trialYest.trial_starts,
        trial_conversions: trialYest.trial_conversions,
        trial_cancellations: trialYest.trial_cancellations,
        in_billing_retry: trialYest.in_billing_retry,
        new_users: funnelYest.new_users,
        converted_users: funnelYest.converted_users,
        paywalled_users: funnelYest.paywalled_users,
      },
    },
    series: { trial: trialSeries, funnel: funnelSeries },
    trialLagDays: TRIAL_LAG_DAYS,
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
