const functions = require("@google-cloud/functions-framework");
const admin = require("firebase-admin");
const { Webhook } = require("svix");

admin.initializeApp();
const db = admin.firestore();

// The dashboard scheduled job (Claude Code routine on the owner's machine) calls
// churnCohorts/refundCohorts with a gcloud OAuth access token instead of a Firebase
// ID token. Accept it if it resolves to the trusted owner via Google userinfo.
const TRUSTED_SERVICE_EMAILS = ["corentin@tangent-app.com"];
async function isTrustedOwnerToken(bearer) {
  try {
    const r = await fetch("https://www.googleapis.com/oauth2/v3/userinfo", {
      headers: { Authorization: "Bearer " + bearer },
    });
    if (!r.ok) return false;
    const info = await r.json();
    return info.email_verified === true && TRUSTED_SERVICE_EMAILS.includes((info.email || "").toLowerCase());
  } catch (_) {
    return false;
  }
}
// Returns true if the request is authorized: either a Firebase ID token carrying
// the dashboard claim, or a trusted-owner OAuth access token (service path).
async function isAuthorized(authHeader) {
  if (!authHeader.startsWith("Bearer ")) return false;
  const token = authHeader.slice(7);
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    if (decoded.dashboard === true) return true;
  } catch (_) { /* not a Firebase ID token — try owner path */ }
  return isTrustedOwnerToken(token);
}

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
  if (id.includes("quitalcohol") || id.includes("quit_alcohol") || id.includes("quitalc")) return "Quit Alcohol";
  if (id.includes("dora") || id.includes("speaklearnspanish") || id.includes("speak_learn_spanish")) return "Speak & Learn Spanish: Dora";
  if (id.includes("yarn")) return "Yarn Ai";
  if (id.includes("babyfood") || id.includes("baby_food")) return "Baby Food Scan";
  if (id.includes("betterbreath") || id.includes("better_breath")) return "Better Breath";
  if (id.includes("betterwalk") || id.includes("better_walk")) return "Better Walk";
  if (id.includes("girlies") || id.includes("therapy")) return "GirlTalk";
  if (id.includes("prayer")) return "Prayer";
  if (id.includes("mew")) return "Mew";
  if (id.includes("crime") || id.includes("noir")) return "Crime Novels";
  if (id.includes("murder") || id.includes("mystery")) return "Murder Mystery";
  if (id.includes("reel") || id.includes("short_stories")) return "Reel Short Stories";
  if (id.includes("daily") && id.includes("prayer")) return "Daily Prayers";
  // Stripe price IDs (web revenue, unmapped) — bucket together so dashboard isn't littered.
  if (productId && (productId.startsWith("live:price_") || productId.startsWith("price_"))) {
    return "Stripe Web";
  }
  if (productId) {
    const s = productId.split(".");
    return s.length > 2 ? s[2] : productId;
  }
  return appId || "Unknown";
}

functions.http("swWebhook", async (req, res) => {
  if (req.method !== "POST") return res.status(405).send("Not allowed");
  try {
    // Superwall sends shared secret via configured custom header `x-webhook-secret`.
    // Each app's webhook in Superwall is configured with the same secret on the
    // Tangent side (one secret in Secret Manager). Reject if missing/mismatch.
    const expected = process.env.SUPERWALL_WEBHOOK_SECRET;
    if (!expected) return res.status(500).send("Missing webhook secret");
    const submitted = req.headers["x-webhook-secret"];
    if (!submitted || submitted !== expected) {
      console.warn("Webhook auth failed: header mismatch");
      return res.status(401).send("Invalid signature");
    }

    const payload = req.body || {};
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
    } else if (type === "billing_issue") {
      tt = "billing_issue";
    } else if (type === "cancellation") {
      tt = "cancellation";
    } else if (type === "expiration") {
      tt = "expiration";
    } else {
      return res.status(200).send("Ignored");
    }

    // Dedupe: Superwall retries failed webhooks, and we backfilled some events
    // from ClickHouse on 2026-05-13. Skip if same (originalTransactionId,
    // purchased_at_ms, transaction_type) already in Firestore.
    const purchasedAtMs = data.purchasedAt ? new Date(data.purchasedAt).getTime() : null;
    if (data.originalTransactionId && purchasedAtMs) {
      try {
        const dupSnap = await db.collection("transactions")
          .where("original_transaction_id", "==", data.originalTransactionId)
          .where("transaction_type", "==", tt)
          .where("purchased_at_ms", "==", purchasedAtMs)
          .limit(1)
          .get();
        if (!dupSnap.empty) {
          return res.status(200).send("Duplicate (already stored)");
        }
      } catch (e) {
        console.warn("dedupe lookup failed:", e.message);
      }
    }

    // Recovery detection: paid renewal where a billing_issue occurred between
    // the most-recent prior paid event (renewal | new_subscription | trial_to_paid)
    // and now. Captures grace-period recoveries where expiration fires between
    // billing_issue and the eventual successful renewal.
    let recoveredFromBilling = false;
    if (tt === "renewal" && data.originalTransactionId) {
      try {
        const prevSnap = await db.collection("transactions")
          .where("original_transaction_id", "==", data.originalTransactionId)
          .where("is_sandbox", "==", data.environment === "SANDBOX")
          .orderBy("received_at", "desc")
          .limit(50) // recent history; cheap, all small docs
          .get();

        // Walk forward in time (oldest first) and find:
        //  - most recent prior "paid anchor" (renewal | new_subscription | trial_to_paid)
        //  - any billing_issue after that anchor
        let anchorTs = 0;
        let hasBillingIssueAfterAnchor = false;
        const docs = prevSnap.docs.slice().reverse(); // ascending received_at
        for (const d of docs) {
          const dd = d.data();
          const ts = dd.received_at && dd.received_at.toMillis ? dd.received_at.toMillis() : 0;
          if (["renewal", "new_subscription", "trial_to_paid"].includes(dd.transaction_type)) {
            anchorTs = ts;
            hasBillingIssueAfterAnchor = false; // reset on new anchor
          } else if (dd.transaction_type === "billing_issue" && ts > anchorTs) {
            hasBillingIssueAfterAnchor = true;
          }
        }
        recoveredFromBilling = hasBillingIssueAfterAnchor;
      } catch (e) {
        console.warn("recovery lookup failed:", e.message);
      }
    }

    // Lifetime spend for this subscription (keyed by originalTransactionId).
    // Sum all prior paid Firestore docs for this otid + the current event's price.
    // Stored on the doc so the dashboard reads it directly (no extra fetch).
    let lifetimeSpend = null;
    let lifetimePayments = null;
    const PAID_TT = ["renewal", "new_subscription", "trial_to_paid", "one_time_purchase"];
    if (PAID_TT.includes(tt) && data.originalTransactionId) {
      try {
        const snap = await db.collection("transactions")
          .where("original_transaction_id", "==", data.originalTransactionId)
          .where("is_sandbox", "==", data.environment === "SANDBOX")
          .limit(500)
          .get();
        let sum = 0, count = 0;
        snap.forEach(d => {
          const dd = d.data();
          if (PAID_TT.includes(dd.transaction_type)) { sum += Number(dd.price || 0); count++; }
          else if (dd.transaction_type === "refund") { sum -= Math.abs(Number(dd.price || 0)); }
        });
        // Add current event (not yet written)
        sum += Number(data.price || 0);
        count += 1;
        lifetimeSpend = Math.round(sum * 100) / 100;
        lifetimePayments = count;
      } catch (e) {
        console.warn("lifetime calc failed:", e.message);
      }
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
      recovered_from_billing: recoveredFromBilling,
      lifetime_spend: lifetimeSpend,
      lifetime_payments: lifetimePayments,
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
    "https://tangent-transactions-dashboard.web.app",
    "https://tangent-transactions-dashboard.firebaseapp.com",
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

// ITP lag — install → paywall → trial → 3d trial → paid.
// events_rep.app_install capped at 7d so cohort window is constrained.
// Use 4-day lag: catches most trial-to-paid conversions while still giving 3 mature days.
const ITP_LAG_DAYS = 4;

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

  // Install to Paid (ITP): cohort by install_day.
  // - Denominator: distinct users with app_install event on day d (events_rep). Matches
  //   ITT denom — true install count.
  // - Numerator: of those users, the subset with a paid outcome anywhere:
  //     - initial_purchase non-trial (direct paid)
  //     - renewal with isTrialConversion=1 (trial → paid)
  // - Cohort window: today-6 to today-(ITP_LAG_DAYS) so each cohort day has had
  //   ITP_LAG_DAYS of maturation time. Bounded by events_rep 7d cap.
  const itpSql = `
WITH installs AS (
  SELECT DISTINCT toDate(ts, 'UTC') AS day, JSONExtractString(meta, 'appUserId') AS uid
  FROM sw.events_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND name = 'app_install'
    AND ts >= today() - 6
    AND ts < today() - ${ITP_LAG_DAYS - 1}
),
paid AS (
  SELECT DISTINCT appUserId AS uid
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE isSandbox = 0
    ${appFilter}
    AND appUserId IS NOT NULL
    AND (
      (name = 'initial_purchase' AND lower(periodType) != 'trial')
      OR (name = 'renewal' AND isTrialConversion = 1)
    )
    AND ts >= today() - 7
    AND ts < today() + 1
)
SELECT
  i.day AS day,
  uniq(i.uid) AS new_users,
  uniqIf(i.uid, p.uid IS NOT NULL AND p.uid != '') AS paid_users
FROM installs i
LEFT JOIN paid p ON i.uid = p.uid
GROUP BY day
ORDER BY day ASC
FORMAT JSONEachRow`.trim();

  const [events, funnel, itp] = await Promise.all([
    swQuery(eventsSql),
    swQuery(funnelSql),
    swQuery(itpSql),
  ]);

  // Build map by day string YYYY-MM-DD
  const evMap = new Map();
  for (const r of events) evMap.set(r.day, r);
  const fnMap = new Map();
  for (const r of funnel) fnMap.set(r.day, r);
  const itpMap = new Map();
  for (const r of itp) itpMap.set(r.day, r);

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
  const itpDays = buildSeries(ITP_LAG_DAYS);     // for ITP

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

  const itpSeries = itpDays.map(day => {
    const r = itpMap.get(day) || {};
    const nu = Number(r.new_users || 0);
    const pu = Number(r.paid_users || 0);
    return {
      day,
      new_users: nu,
      paid_users: pu,
      itp: pct(pu, nu),
    };
  });

  // "Today" and "yesterday" headline values = the most recent day in each series
  const trialToday = trialSeries[trialSeries.length - 1];
  const trialYest = trialSeries[trialSeries.length - 2];
  const funnelToday = funnelSeries[funnelSeries.length - 1];
  const funnelYest = funnelSeries[funnelSeries.length - 2];
  const itpToday = itpSeries[itpSeries.length - 1];
  const itpYest = itpSeries[itpSeries.length - 2];

  return {
    today: {
      ittRate: funnelToday.itt,
      ttpRate: trialToday.ttp,
      cancellationRate: trialToday.cancellation_rate,
      billingIssueRate: trialToday.billing_issue_rate,
      paywallRate: funnelToday.paywall_rate,
      itpRate: itpToday.itp,
      counts: {
        trial_starts: trialToday.trial_starts,
        trial_conversions: trialToday.trial_conversions,
        trial_cancellations: trialToday.trial_cancellations,
        in_billing_retry: trialToday.in_billing_retry,
        new_users: funnelToday.new_users,
        converted_users: funnelToday.converted_users,
        paywalled_users: funnelToday.paywalled_users,
        itp_new_users: itpToday.new_users,
        itp_paid_users: itpToday.paid_users,
      },
    },
    yest: {
      ittRate: funnelYest.itt,
      ttpRate: trialYest.ttp,
      cancellationRate: trialYest.cancellation_rate,
      billingIssueRate: trialYest.billing_issue_rate,
      paywallRate: funnelYest.paywall_rate,
      itpRate: itpYest.itp,
      counts: {
        trial_starts: trialYest.trial_starts,
        trial_conversions: trialYest.trial_conversions,
        trial_cancellations: trialYest.trial_cancellations,
        in_billing_retry: trialYest.in_billing_retry,
        new_users: funnelYest.new_users,
        converted_users: funnelYest.converted_users,
        paywalled_users: funnelYest.paywalled_users,
        itp_new_users: itpYest.new_users,
        itp_paid_users: itpYest.paid_users,
      },
    },
    series: { trial: trialSeries, funnel: funnelSeries, itp: itpSeries },
    trialLagDays: TRIAL_LAG_DAYS,
    itpLagDays: ITP_LAG_DAYS,
  };
}

functions.http("superwallStats", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "https://tangent-transactions-dashboard.web.app",
    "https://tangent-transactions-dashboard.firebaseapp.com",
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

// Lifetime spend for a single user (on-demand, keyed by original_app_user_id).
// Sums paid events from sw.events_rep; refunds subtract. events_rep retains ~11 months.
const ltvCache = new Map(); // key = uid, value = { ts, data }
const LTV_CACHE_TTL_MS = 30 * 60 * 1000;

async function fetchUserLifetime(uid) {
  // ClickHouse string literal: escape backslash + single quote.
  const safe = uid.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const sql = `
SELECT
  sumIf(JSONExtractFloat(props, 'price'), name IN ('initial_purchase','renewal','non_renewing_purchase')) AS gross,
  sumIf(JSONExtractFloat(props, 'price'), name = 'refund') AS refunded,
  countIf(name IN ('initial_purchase','renewal','non_renewing_purchase') AND JSONExtractFloat(props,'price') > 0) AS payment_count,
  min(ts) AS first_seen,
  max(ts) AS last_seen
FROM sw.events_rep
WHERE isSandbox = 0
  AND JSONExtractString(props, 'original_app_user_id') = '${safe}'
  AND name IN ('initial_purchase','renewal','non_renewing_purchase','refund')
FORMAT JSONEachRow`.trim();

  const rows = await swQuery(sql);
  const r = rows[0] || {};
  const gross = Number(r.gross || 0);
  const refunded = Number(r.refunded || 0);
  return {
    uid,
    total: gross - refunded,
    gross,
    refunded,
    paymentCount: Number(r.payment_count || 0),
    firstSeen: r.first_seen || null,
    lastSeen: r.last_seen || null,
  };
}

functions.http("userLifetime", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "https://tangent-transactions-dashboard.web.app",
    "https://tangent-transactions-dashboard.firebaseapp.com",
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

    const uid = String(req.query.uid || "").trim();
    if (!uid) return res.status(400).json({ error: "Missing uid" });
    if (uid.length > 200) return res.status(400).json({ error: "uid too long" });

    const now = Date.now();
    const hit = ltvCache.get(uid);
    if (hit && (now - hit.ts) < LTV_CACHE_TTL_MS) {
      res.set("X-Cache", "HIT");
      return res.status(200).json(hit.data);
    }

    const data = await fetchUserLifetime(uid);
    ltvCache.set(uid, { ts: now, data });
    res.set("X-Cache", "MISS");
    return res.status(200).json(data);
  } catch (err) {
    console.error("userLifetime error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ---- Cancellation cohorts ----
// Weekly cohorts by subscription start (initial_purchase.purchasedAt), per app.
// Cancels (gross 'cancellation' events, first per otid) bucketed by age from start:
// D0 / D1-7 / D8-30 / D30+. Each bucket = % of the cohort. Denominator = subs
// started that week. Immature buckets are nulled per cohort (see maturity gates)
// so young cohorts don't read artificially low.
const churnCache = new Map(); // key = appId, value = { ts, data }
const CHURN_CACHE_TTL_MS = 30 * 60 * 1000;
const CHURN_WEEKS = 14; // how many weekly cohorts to return (client windows further)

// Bucket needs this many days elapsed since cohort start to be "complete".
// D30+ keeps accruing forever; treat 45d as settled enough to show.
const BUCKET_MATURITY_DAYS = { d0: 1, d1_7: 8, d8_30: 31, d30plus: 45 };

async function fetchChurnCohorts(appId, plan) {
  const appFilter = "applicationId = " + Number(appId);
  // Only weekly or annual are sold: annual/year keyword → annual, everything else → weekly.
  // plan filter applied via HAVING on the classified plan.
  const planHaving = plan === "annual" ? "HAVING sub_plan = 'annual'"
    : plan === "weekly" ? "HAVING sub_plan = 'weekly'"
    : "";
  const sql = `
WITH subs AS (
  SELECT originalTransactionId AS otid, min(toDate(purchasedAt)) AS start_day,
    if(argMin(productId, purchasedAt) ILIKE '%annual%' OR argMin(productId, purchasedAt) ILIKE '%year%', 'annual', 'weekly') AS sub_plan
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE ${appFilter} AND isSandbox=0 AND name='initial_purchase' AND originalTransactionId!=''
  GROUP BY otid
  ${planHaving}
),
cancels AS (
  SELECT originalTransactionId AS otid, min(toDate(ts)) AS cancel_day
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE ${appFilter} AND isSandbox=0 AND name='cancellation' AND originalTransactionId!=''
  GROUP BY otid
)
SELECT
  toString(toStartOfWeek(s.start_day, 1)) AS cohort_week,
  count() AS cohort_size,
  countIf(dateDiff('day',s.start_day,c.cancel_day)=0) AS d0,
  countIf(dateDiff('day',s.start_day,c.cancel_day) BETWEEN 1 AND 7) AS d1_7,
  countIf(dateDiff('day',s.start_day,c.cancel_day) BETWEEN 8 AND 30) AS d8_30,
  countIf(dateDiff('day',s.start_day,c.cancel_day) > 30) AS d30plus
FROM subs s
LEFT JOIN cancels c ON s.otid = c.otid
WHERE s.start_day >= today() - ${CHURN_WEEKS * 7}
GROUP BY cohort_week ORDER BY cohort_week ASC
FORMAT JSONEachRow`.trim();

  const rows = await swQuery(sql);
  const todayMs = Date.now();
  const cohorts = rows.map((r) => {
    const size = Number(r.cohort_size) || 0;
    const startMs = Date.parse(r.cohort_week + "T00:00:00Z");
    const ageDays = Math.floor((todayMs - startMs) / 86400000);
    const pct = (n) => (size > 0 ? Math.round((Number(n) / size) * 1000) / 10 : 0);
    // null a bucket if the cohort hasn't aged past the bucket's window end
    const gated = (val, key) => (ageDays >= BUCKET_MATURITY_DAYS[key] ? pct(val) : null);
    return {
      week: r.cohort_week,
      size,
      ageDays,
      d0: gated(r.d0, "d0"),
      d1_7: gated(r.d1_7, "d1_7"),
      d8_30: gated(r.d8_30, "d8_30"),
      d30plus: gated(r.d30plus, "d30plus"),
    };
  });
  return { appId: Number(appId), plan: plan || "all", cohorts, generatedAt: new Date().toISOString() };
}

functions.http("churnCohorts", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "https://tangent-transactions-dashboard.web.app",
    "https://tangent-transactions-dashboard.firebaseapp.com",
    "http://localhost:5173",
    "http://localhost:8000",
    "http://localhost:8899",
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
    const authHeader = req.headers.authorization || "";
    if (!(await isAuthorized(authHeader))) return res.status(401).json({ error: "Unauthorized" });

    const appIdRaw = req.query.appId;
    if (!appIdRaw || !/^\d+$/.test(String(appIdRaw))) {
      return res.status(400).json({ error: "appId required" });
    }
    const appId = Number(appIdRaw);
    const planRaw = String(req.query.plan || "all").toLowerCase();
    const plan = ["all", "weekly", "annual"].includes(planRaw) ? planRaw : "all";
    const cacheKey = appId + "|" + plan;
    const now = Date.now();
    const hit = churnCache.get(cacheKey);
    if (hit && (now - hit.ts) < CHURN_CACHE_TTL_MS) {
      res.set("X-Cache", "HIT");
      return res.status(200).json(hit.data);
    }
    const data = await fetchChurnCohorts(appId, plan === "all" ? null : plan);
    churnCache.set(cacheKey, { ts: now, data });
    res.set("X-Cache", "MISS");
    return res.status(200).json(data);
  } catch (err) {
    console.error("churnCohorts error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ---- Refund cohorts ----
// Same cohort model as churn: weekly cohorts by subscription start
// (initial_purchase.purchasedAt), per app, plan-split by product keyword.
// Numerator = refunds (isRefund=1, first per otid) bucketed by age from start:
// D0 / D1-7 / D8-30 / D30+. Two rate modes:
//   - txn:     refunded subs / subs started       (count based)
//   - revenue: refunded $     / purchased $        (value based, price negated)
// Both numerator+denominator counts AND dollars are returned per bucket so the
// client can flip modes without a refetch. Immature buckets nulled (maturity gates).
const refundCache = new Map(); // key = appId|plan, value = { ts, data }
const REFUND_CACHE_TTL_MS = 30 * 60 * 1000;
const REFUND_WEEKS = 14;

async function fetchRefundCohorts(appId, plan) {
  const appFilter = "applicationId = " + Number(appId);
  const planHaving = plan === "annual" ? "HAVING sub_plan = 'annual'"
    : plan === "weekly" ? "HAVING sub_plan = 'weekly'"
    : "";
  const sql = `
WITH subs AS (
  SELECT originalTransactionId AS otid, min(toDate(purchasedAt)) AS start_day,
    if(argMin(productId, purchasedAt) ILIKE '%annual%' OR argMin(productId, purchasedAt) ILIKE '%year%', 'annual', 'weekly') AS sub_plan
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE ${appFilter} AND isSandbox=0 AND name='initial_purchase' AND originalTransactionId!=''
  GROUP BY otid
  ${planHaving}
),
rev AS (
  -- Gross revenue actually collected per subscription. NOT initial_purchase.price:
  -- for trial subs that fires at $0 (trial start). Real money lands at renewal.
  -- Sum all positive-price, non-refund charge events so the $-rate denominator is
  -- true revenue taken in (else refund $ / $0 → nonsensical >100% rates).
  SELECT originalTransactionId AS otid, sum(toFloat64(price)) AS gross
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE ${appFilter} AND isSandbox=0 AND isRefund=0 AND price>0
    AND name IN ('initial_purchase','renewal','non_renewing_purchase','product_change') AND originalTransactionId!=''
  GROUP BY otid
),
refunds AS (
  SELECT originalTransactionId AS otid, min(toDate(ts)) AS refund_day,
    -sum(toFloat64(price)) AS refund_amt
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE ${appFilter} AND isSandbox=0 AND isRefund=1 AND originalTransactionId!=''
  GROUP BY otid
)
SELECT
  toString(toStartOfWeek(s.start_day, 1)) AS cohort_week,
  count() AS cohort_size,
  round(sum(rev.gross), 2) AS cohort_usd,
  countIf(dateDiff('day',s.start_day,r.refund_day)=0) AS d0_n,
  countIf(dateDiff('day',s.start_day,r.refund_day) BETWEEN 1 AND 7) AS d1_7_n,
  countIf(dateDiff('day',s.start_day,r.refund_day) BETWEEN 8 AND 30) AS d8_30_n,
  countIf(dateDiff('day',s.start_day,r.refund_day) > 30) AS d30plus_n,
  round(sumIf(r.refund_amt, dateDiff('day',s.start_day,r.refund_day)=0), 2) AS d0_usd,
  round(sumIf(r.refund_amt, dateDiff('day',s.start_day,r.refund_day) BETWEEN 1 AND 7), 2) AS d1_7_usd,
  round(sumIf(r.refund_amt, dateDiff('day',s.start_day,r.refund_day) BETWEEN 8 AND 30), 2) AS d8_30_usd,
  round(sumIf(r.refund_amt, dateDiff('day',s.start_day,r.refund_day) > 30), 2) AS d30plus_usd
FROM subs s
LEFT JOIN rev ON s.otid = rev.otid
LEFT JOIN refunds r ON s.otid = r.otid
WHERE s.start_day >= today() - ${REFUND_WEEKS * 7}
GROUP BY cohort_week ORDER BY cohort_week ASC
FORMAT JSONEachRow`.trim();

  const rows = await swQuery(sql);
  const todayMs = Date.now();
  const cohorts = rows.map((r) => {
    const size = Number(r.cohort_size) || 0;
    const usd = Number(r.cohort_usd) || 0;
    const startMs = Date.parse(r.cohort_week + "T00:00:00Z");
    const ageDays = Math.floor((todayMs - startMs) / 86400000);
    const pctN = (n) => (size > 0 ? Math.round((Number(n) / size) * 1000) / 10 : 0);
    const pctU = (d) => (usd > 0 ? Math.round((Number(d) / usd) * 1000) / 10 : 0);
    // null a bucket if the cohort hasn't aged past the bucket's window end
    const gate = (key, val) => (ageDays >= BUCKET_MATURITY_DAYS[key] ? val : null);
    return {
      week: r.cohort_week,
      size,
      usd,
      ageDays,
      // txn-rate mode (count based)
      txn: {
        d0: gate("d0", pctN(r.d0_n)),
        d1_7: gate("d1_7", pctN(r.d1_7_n)),
        d8_30: gate("d8_30", pctN(r.d8_30_n)),
        d30plus: gate("d30plus", pctN(r.d30plus_n)),
      },
      // revenue-rate mode ($ based)
      revenue: {
        d0: gate("d0", pctU(r.d0_usd)),
        d1_7: gate("d1_7", pctU(r.d1_7_usd)),
        d8_30: gate("d8_30", pctU(r.d8_30_usd)),
        d30plus: gate("d30plus", pctU(r.d30plus_usd)),
      },
    };
  });
  return { appId: Number(appId), plan: plan || "all", cohorts, generatedAt: new Date().toISOString() };
}

functions.http("refundCohorts", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "https://tangent-transactions-dashboard.web.app",
    "https://tangent-transactions-dashboard.firebaseapp.com",
    "http://localhost:5173",
    "http://localhost:8000",
    "http://localhost:8899",
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
    const authHeader = req.headers.authorization || "";
    if (!(await isAuthorized(authHeader))) return res.status(401).json({ error: "Unauthorized" });

    const appIdRaw = req.query.appId;
    if (!appIdRaw || !/^\d+$/.test(String(appIdRaw))) {
      return res.status(400).json({ error: "appId required" });
    }
    const appId = Number(appIdRaw);
    const planRaw = String(req.query.plan || "all").toLowerCase();
    const plan = ["all", "weekly", "annual"].includes(planRaw) ? planRaw : "all";
    const cacheKey = appId + "|" + plan;
    const now = Date.now();
    const hit = refundCache.get(cacheKey);
    if (hit && (now - hit.ts) < REFUND_CACHE_TTL_MS) {
      res.set("X-Cache", "HIT");
      return res.status(200).json(hit.data);
    }
    const data = await fetchRefundCohorts(appId, plan === "all" ? null : plan);
    refundCache.set(cacheKey, { ts: now, data });
    res.set("X-Cache", "MISS");
    return res.status(200).json(data);
  } catch (err) {
    console.error("refundCohorts error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ---- Daily cohort sync (Cloud Scheduler → this function) ----
// Precomputes churn + refund cohorts for every monitored app × plan and writes
// them to Firestore (churn_cohorts/{appId}__{plan}, refund_cohorts/{appId}__{plan})
// so the dashboard reads Firestore directly — no ClickHouse/function on page load.
//
// Runs server-side as the function's own service account: writes via the Admin SDK
// (no OAuth token, no key file). Invocation is locked down at the IAM layer — only
// the scheduler service account has run.invoker, so this is NOT allow-unauthenticated.
// The payload is stored as a JSON string field `payload` (client JSON.parse()s it),
// matching what scripts/sync-cohorts.sh wrote.
const SYNC_APPS = [32830, 22372, 35269]; // GirlWalk, GirlTalk, Poly AI — must match dashboard CHURN_APPS/REFUND_APPS
const SYNC_PLANS = ["all", "weekly", "annual"];

async function runCohortSync() {
  const nowIso = new Date().toISOString();
  const results = { churn: 0, refund: 0, spend: null, roas: 0, failed: [] };
  for (const appId of SYNC_APPS) {
    for (const plan of SYNC_PLANS) {
      const planArg = plan === "all" ? null : plan;
      // churn
      try {
        const data = await fetchChurnCohorts(appId, planArg);
        await db.collection("churn_cohorts").doc(`${appId}__${plan}`).set({
          payload: JSON.stringify(data),
          generatedAt: nowIso,
        });
        results.churn++;
      } catch (e) {
        results.failed.push(`churn ${appId}/${plan}: ${e.message}`);
      }
      // refund
      try {
        const data = await fetchRefundCohorts(appId, planArg);
        await db.collection("refund_cohorts").doc(`${appId}__${plan}`).set({
          payload: JSON.stringify(data),
          generatedAt: nowIso,
        });
        results.refund++;
      } catch (e) {
        results.failed.push(`refund ${appId}/${plan}: ${e.message}`);
      }
    }
  }

  // ROAS: spend FIRST (Adjust), then the cohort rollup that divides by it
  // (rule: a cron that depends on another's output runs after it, not before).
  try {
    results.spend = await runAdjustSpendSync();
  } catch (e) {
    results.failed.push(`spend sync: ${e.message}`);
  }
  for (const appId of ROAS_APPS) {
    for (const geo of ROAS_GEOS) {
      try {
        const data = await fetchRoasCohorts(appId, geo);
        await db.collection("roas_cohorts").doc(`${appId}__${geo}`).set({
          payload: JSON.stringify(data),
          generatedAt: nowIso,
        });
        results.roas++;
      } catch (e) {
        results.failed.push(`roas ${appId}/${geo}: ${e.message}`);
      }
    }
  }
  return results;
}

functions.http("dailyCohortSync", async (req, res) => {
  // Invocation is IAM-gated (scheduler SA has run.invoker); no app-level auth needed.
  try {
    const r = await runCohortSync();
    console.log("cohort sync:", JSON.stringify(r));
    const status = r.failed.length ? 207 : 200;
    return res.status(status).json({ ok: r.failed.length === 0, ...r, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error("dailyCohortSync error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ===========================================================================
// ROAS cohort tracker — spend (Adjust) ÷ cohort proceeds (Superwall ClickHouse)
//
// "Cohort-vaulted" ROAS: each spend-day's ROAS is spend that day ÷ ALL future
// proceeds from the users first acquired that day. A renewal weeks later folds
// back onto its cohort's spend-day, so old spend-days keep maturing over time.
//
// Two collections, both written only by Cloud Functions (rules: read for
// dashboard claim, write false):
//   ad_spend/{appId}__{YYYY-MM-DD}   — daily ad spend per app (from Adjust)
//   roas_cohorts/{appId}             — precomputed maturity table (JSON payload)
//
// Everything keys on Superwall applicationId (number) so spend joins revenue.
// Day boundaries use the account reporting tz (America/New_York) on BOTH sides
// so spend-days and cohort-days line up. Numerator = proceeds (net of store cut).
// ===========================================================================

const ADJUST_REPORT_URL = "https://automate.adjust.com/reports-service/report";
const ROAS_TZ = "America/New_York"; // account reporting tz — used for all day bucketing

// Maps an Adjust "app" dimension value onto the Superwall applicationId used
// everywhere else. Adjust labels don't match Superwall ids, so normalize here.
// Unmapped apps return null and are skipped (logged) rather than mis-attributed.
// NOTE: match on Adjust's actual app-name strings — verify against a live pull
// (adjustSpendSync logs unmapped names) and adjust the keywords if they differ.
function resolveAdjustAppId(adjustApp) {
  const id = String(adjustApp || "").toLowerCase();
  if (id.includes("girlwalk") || id.includes("girl walk") || id.includes("girl_walk")) return 32830; // GirlWalk
  // GirlTalk ships under the Adjust app label "Solo Girlies" (matches resolveAppName's "girlies").
  if (id.includes("girltalk") || id.includes("girl talk") || id.includes("girl_talk") || id.includes("girlies")) return 22372; // GirlTalk
  if (id.includes("poly")) return 35269; // Poly AI
  return null;
}

// Superwall applicationId → canonical app_name (mirror of the dashboard map).
// Only the apps we run paid UA for need a spend mapping; extend as campaigns grow.
const ROAS_APP_NAMES = {
  32830: "GirlWalk",
  22372: "GirlTalk",
  35269: "Poly AI",
};

// Apps to compute ROAS for. Must have both ad spend (Adjust) and revenue.
const ROAS_APPS = [32830, 22372, 35269]; // GirlWalk, GirlTalk, Poly AI

// Geo buckets: "all" (every country), plus per-ISO breakouts for the markets we
// run paid UA in. US ~89% + UK ~11% of spend; the rest is negligible and only
// shows under "all". Extend as new markets grow.
const ROAS_GEOS = ["all", "US", "GB"];

// Adjust reports country as a full name; revenue (ClickHouse countryCode) is ISO.
// Map Adjust name → ISO for the breakout geos; null = not a tracked geo (folds
// into "all" only).
function adjustCountryToIso(name) {
  const n = String(name || "").toLowerCase();
  if (n === "united states") return "US";
  if (n === "united kingdom") return "GB";
  return null;
}

// Maturity gates: a spend-day's DN column is only meaningful once N full days
// have elapsed since that day. Below the gate we return null (shown as "—").
const ROAS_MILESTONES = [0, 7, 30, 90];
const ROAS_WINDOW_DAYS = 180; // how many recent spend-days to compute (dashboard filters to 30/60/90/180)

// ---- Adjust spend ingest ----
// Pull per-app per-day per-country cost from Adjust. datePeriod is an Adjust
// date_period string, e.g. "-14d:-0d". Returns [{ appId, date, iso, spend_usd }]
// where iso is a tracked breakout geo ("US"/"GB") or null (folds into "all" only).
async function fetchAdjustSpend(datePeriod) {
  const token = process.env.ADJUST_API_TOKEN;
  if (!token) throw new Error("Missing ADJUST_API_TOKEN");

  // network_cost = actual ad-network spend (what the "Monthly Spend" report shows).
  // NOTE: the `cost` metric is attributed cost and returns almost nothing — do not use it.
  const params = new URLSearchParams({
    dimensions: "app,day,country",
    metrics: "network_cost",
    date_period: datePeriod,
  });
  const r = await fetch(`${ADJUST_REPORT_URL}?${params.toString()}`, {
    headers: { Authorization: "Bearer " + token },
  });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    throw new Error(`Adjust API ${r.status}: ${body.slice(0, 200)}`);
  }
  const json = await r.json();
  const rows = Array.isArray(json.rows) ? json.rows : [];
  const out = [];
  const unmapped = new Set();
  for (const row of rows) {
    const appId = resolveAdjustAppId(row.app);
    const date = String(row.day || row.date || "").slice(0, 10);
    if (!appId) { unmapped.add(String(row.app || "")); continue; }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;
    out.push({
      appId, date,
      iso: adjustCountryToIso(row.country),
      spend_usd: Math.round(Number(row.network_cost || 0) * 100) / 100,
    });
  }
  if (unmapped.size) console.warn("Adjust apps with no appId mapping (skipped):", [...unmapped].join(", "));
  return out;
}

// Pull spend and upsert ad_spend/{appId}__{geo}__{date} for geo in ROAS_GEOS.
// "all" sums every country; "US"/"GB" are the country breakouts. 14-day re-pull
// by default (Adjust cost restates for a few days).
async function runAdjustSpendSync(datePeriod) {
  // Adjust relative format: "-14d:-0d" = 14 days ago through today ("today"/"0d"
  // are rejected by the API; the end must be expressed as "-0d").
  const period = datePeriod || "-14d:-0d";
  const rows = await fetchAdjustSpend(period);

  // `${appId}__${geo}__${date}` -> spend_usd. Every row contributes to "all";
  // rows whose country maps to a tracked ISO also contribute to that geo.
  const agg = new Map();
  const add = (appId, geo, date, spend) => {
    const key = `${appId}__${geo}__${date}`;
    agg.set(key, Math.round(((agg.get(key) || 0) + spend) * 100) / 100);
  };
  for (const row of rows) {
    add(row.appId, "all", row.date, row.spend_usd);
    if (row.iso) add(row.appId, row.iso, row.date, row.spend_usd);
  }

  let written = 0;
  let batch = db.batch();
  let inBatch = 0;
  for (const [key, spend] of agg.entries()) {
    const [appIdStr, geo, date] = key.split("__");
    batch.set(db.collection("ad_spend").doc(key), {
      applicationId: Number(appIdStr),
      app_name: ROAS_APP_NAMES[Number(appIdStr)] || String(appIdStr),
      geo,
      date,
      spend_usd: spend,
      source: "adjust",
      updated_at: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    written++;
    if (++inBatch >= 400) { await batch.commit(); batch = db.batch(); inBatch = 0; }
  }
  if (inBatch > 0) await batch.commit();
  return { period, rowsFetched: rows.length, docsWritten: written };
}

functions.http("adjustSpendSync", async (req, res) => {
  // Owner-token gated: desktop scheduled task calls with a gcloud access token
  // resolving to the trusted owner. Mirrors churn/refund sync auth.
  try {
    const authHeader = req.headers.authorization || "";
    if (!(await isAuthorized(authHeader))) return res.status(401).json({ error: "Unauthorized" });
    const periodRaw = req.query.period;
    const period = typeof periodRaw === "string" && /^[-\d\sdtoay:]+$/i.test(periodRaw) ? periodRaw : undefined;
    const r = await runAdjustSpendSync(period);
    console.log("adjust spend sync:", JSON.stringify(r));
    return res.status(200).json({ ok: true, ...r, generatedAt: new Date().toISOString() });
  } catch (err) {
    console.error("adjustSpendSync error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

// ---- ROAS cohort rollup ----
// For one app: cohort_day(otid) = day of min(purchasedAt) in ROAS_TZ. Proceeds
// (refunds negated) are bucketed by age = ts_day − cohort_day into cumulative
// D0/D7/D30/D90 sums, plus lifetime-to-date. Joined against ad_spend to compute
// ROAS per spend-day. Only paid charge events count (trial starts fire at $0 and
// carry no proceeds, so they're naturally ~0 at D0 for trial-gated apps).
async function fetchRoasCohorts(appId, geo) {
  // geo: "all" | "US" | "GB". For a breakout geo, restrict to cohorts whose buyer
  // countryCode matches (anchored on the subscription's own country). Revenue AND
  // spend are both filtered to the same geo so the ROAS ratio is apples-to-apples.
  const g = ROAS_GEOS.includes(geo) ? geo : "all";
  const geoRevFilter = g === "all" ? "" : `AND countryCode = '${g}'`;

  const sql = `
WITH anchors AS (
  SELECT originalTransactionId AS otid,
    toDate(toTimeZone(min(purchasedAt), '${ROAS_TZ}')) AS cohort_day
  FROM open_revenue.attributed_events_by_ts_rep
  WHERE applicationId = ${Number(appId)} AND isSandbox = 0 AND originalTransactionId != ''
    AND name IN ('initial_purchase','renewal','non_renewing_purchase','product_change')
    ${geoRevFilter}
  GROUP BY otid
),
ev AS (
  -- Paid charge events (positive proceeds) PLUS refunds. Refunds arrive under
  -- name='cancellation' with isRefund=1 and already-negative proceeds, so they
  -- fall outside the paid-name list — capture them via the isRefund=1 branch or
  -- they'd be silently dropped and proceeds would be gross, not net. The many
  -- isRefund=0 'cancellation' rows (auto-renew-off, proceeds=0) are excluded.
  SELECT a.cohort_day AS cohort_day, e.originalTransactionId AS otid,
    dateDiff('day', a.cohort_day, toDate(toTimeZone(e.ts, '${ROAS_TZ}'))) AS age,
    e.isRefund AS isRef,
    if(e.isRefund = 1, -abs(toFloat64(e.proceeds)), toFloat64(e.proceeds)) AS net
  FROM open_revenue.attributed_events_by_ts_rep e
  INNER JOIN anchors a ON e.originalTransactionId = a.otid
  WHERE e.applicationId = ${Number(appId)} AND e.isSandbox = 0
    AND (e.name IN ('initial_purchase','renewal','non_renewing_purchase','product_change') OR e.isRefund = 1)
)
SELECT toString(cohort_day) AS day,
  uniqExact(otid) AS buyers,
  round(sumIf(net, age <= 0), 2) AS d0,
  round(sumIf(net, age <= 7), 2) AS d7,
  round(sumIf(net, age <= 30), 2) AS d30,
  round(sumIf(net, age <= 90), 2) AS d90,
  round(sum(net), 2) AS lifetime,
  -- total refunds ($, positive) charged back against this cohort, any age
  round(sumIf(abs(net), isRef = 1), 2) AS refunds
FROM ev
WHERE cohort_day >= today() - ${ROAS_WINDOW_DAYS}
GROUP BY cohort_day ORDER BY cohort_day DESC
FORMAT JSONEachRow`.trim();

  const revRows = await swQuery(sql);

  // Ad spend for this app + geo, keyed by date.
  const spendSnap = await db.collection("ad_spend")
    .where("applicationId", "==", Number(appId))
    .where("geo", "==", g)
    .get();
  const spendByDate = new Map();
  spendSnap.forEach((d) => {
    const v = d.data();
    if (v && v.date) spendByDate.set(v.date, Number(v.spend_usd || 0));
  });

  // Today in ROAS_TZ as a YYYY-MM-DD string (for age gates). en-CA gives ISO.
  const todayKey = new Intl.DateTimeFormat("en-CA", { timeZone: ROAS_TZ }).format(new Date());
  const todayMs = Date.parse(todayKey + "T00:00:00Z");

  // Emit RAW DOLLARS per day (proceeds cumulative at each age milestone + spend +
  // age). The dashboard divides and aggregates into day/week/month views itself —
  // ROAS ratios must be computed from summed dollars, not by averaging per-day
  // ratios, so the backend stays ratio-free. spend is null if that day has no
  // ad_spend doc yet (e.g. today, before Adjust reports).
  const rows = revRows.map((r) => {
    const day = r.day;
    const ageDays = Math.floor((todayMs - Date.parse(day + "T00:00:00Z")) / 86400000);
    const spend = spendByDate.get(day);
    const hasSpend = typeof spend === "number" && spend > 0;
    return {
      day,
      ageDays,
      buyers: Number(r.buyers) || 0,
      spend: hasSpend ? Math.round(spend * 100) / 100 : null,
      // cumulative proceeds ($) collected by each age; d0=at purchase, lifetime=to date
      p0: Number(r.d0) || 0,
      p7: Number(r.d7) || 0,
      p30: Number(r.d30) || 0,
      p90: Number(r.d90) || 0,
      proceeds: Number(r.lifetime) || 0,
      refunds: Number(r.refunds) || 0, // total refunds ($, positive) against this cohort
    };
  });

  return {
    appId: Number(appId),
    appName: ROAS_APP_NAMES[Number(appId)] || String(appId),
    geo: g,
    tz: ROAS_TZ,
    windowDays: ROAS_WINDOW_DAYS,
    rows,
    generatedAt: new Date().toISOString(),
  };
}

functions.http("roasCohorts", async (req, res) => {
  const origin = req.headers.origin || "";
  const allowedOrigins = [
    "https://tangent-apps.github.io",
    "https://tangent-transactions-dashboard.web.app",
    "https://tangent-transactions-dashboard.firebaseapp.com",
    "http://localhost:5173",
    "http://localhost:8000",
    "http://localhost:8899",
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
    const authHeader = req.headers.authorization || "";
    if (!(await isAuthorized(authHeader))) return res.status(401).json({ error: "Unauthorized" });
    const appIdRaw = req.query.appId;
    if (!appIdRaw || !/^\d+$/.test(String(appIdRaw))) return res.status(400).json({ error: "appId required" });
    const geo = ROAS_GEOS.includes(String(req.query.geo)) ? String(req.query.geo) : "all";
    const data = await fetchRoasCohorts(Number(appIdRaw), geo);
    return res.status(200).json(data);
  } catch (err) {
    console.error("roasCohorts error:", err.message);
    return res.status(500).json({ error: err.message });
  }
});
