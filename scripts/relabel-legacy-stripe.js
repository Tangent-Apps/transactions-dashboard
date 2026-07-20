#!/usr/bin/env node
/**
 * One-off cleanup after switching Stripe to the sole-source model.
 *
 * Before the fix, swWebhook wrote Stripe sales into `transactions` with app_name =
 * raw price string ("live:price_…:3days-free") or the lumped "Stripe Web". This
 * relabels those legacy docs to their real app (GirlWalk / Poly AI) by the price id
 * embedded in product_id, so they merge into the right app bucket in the dashboard.
 *
 * It also DELETES the 20 rows written by scripts/backfill-stripe.js (marked
 * `backfilled: true`) — the backfill turned out to duplicate these legacy docs, so
 * relabelling the originals is enough and the backfill copies must go.
 *
 * Price → app map is hardcoded (verified against Stripe product names; the current
 * restricted key lacks Prices Read so we can't re-resolve live — these 4 prices are
 * the only ones present on legacy docs):
 *   price_1TThE4… = "GW Weekly"   → GirlWalk
 *   price_1TThDS… = "GW Annual"   → GirlWalk
 *   price_1TXRB3… = "Annual Open" → Poly AI
 *   price_1TVwlf… = "Test" product → LEFT UNTOUCHED (ambiguous; only 4 docs)
 *
 * Run:
 *   GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/tangent-dashboard-sa.json \
 *   node scripts/relabel-legacy-stripe.js [--dry-run]
 */
const admin = require(require("path").join(__dirname, "..", "functions", "node_modules", "firebase-admin"));
const DRY = process.argv.includes("--dry-run");

const PRICE_TO_APP = {
  price_1TThE4IipeactWh97wZ7N7Dw: "GirlWalk",
  price_1TThDSIipeactWh9KgZVBiEK: "GirlWalk",
  price_1TXRB3IipeactWh9f3da6rd0: "Poly AI",
  // price_1TVwlfIipeactWh9GG2KZOhq ("Test") intentionally omitted — leave as-is.
};

admin.initializeApp({ projectId: "tangent-transactions-dashboard" });
const db = admin.firestore();
const priceOf = (pid) => { const m = (pid || "").match(/price_[A-Za-z0-9]+/); return m ? m[0] : ""; };

(async () => {
  const snap = await db.collection("transactions").where("store", "==", "STRIPE").get();
  let relabel = 0, delBackfill = 0, skipped = 0, alreadyClean = 0;
  const relabelBatch = [];
  const deleteBatch = [];

  snap.forEach((doc) => {
    const x = doc.data();
    if (x.backfilled === true) { deleteBatch.push(doc.ref); delBackfill++; return; }
    const isLegacyLabel = /^(live:|test:)?price_/.test(x.app_name || "") || x.app_name === "Stripe Web";
    if (!isLegacyLabel) { alreadyClean++; return; }
    const app = PRICE_TO_APP[priceOf(x.product_id)];
    if (!app) { skipped++; return; }        // e.g. Test price — leave untouched
    relabelBatch.push({ ref: doc.ref, app });
    relabel++;
  });

  console.log(`STRIPE docs: ${snap.size}`);
  console.log(`→ relabel: ${relabel} | delete backfill dupes: ${delBackfill} | skip(ambiguous): ${skipped} | already clean: ${alreadyClean}` + (DRY ? "  [DRY RUN]" : ""));

  if (DRY) { process.exit(0); }

  // Firestore batches cap at 500 ops.
  const commit = async (ops) => { const b = db.batch(); ops.forEach(fn => fn(b)); await b.commit(); };
  const chunks = [];
  const all = [
    ...relabelBatch.map(r => (b) => b.update(r.ref, { app_name: r.app, relabeled_from_legacy: true })),
    ...deleteBatch.map(ref => (b) => b.delete(ref)),
  ];
  for (let i = 0; i < all.length; i += 400) chunks.push(all.slice(i, i + 400));
  for (const c of chunks) await commit(c);

  console.log(`Done. relabeled=${relabel} deletedBackfill=${delBackfill}`);
  process.exit(0);
})().catch(e => { console.error("Failed:", e.message); process.exit(1); });
