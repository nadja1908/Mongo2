/**
 * src/etl/build-rank-cache.js
 *
 * Placeholder ETL: compute `rank_cache` which stores quality/popularity ranks and pct>=8.
 * This helps Q5 optimized by reading precomputed ranks instead of global sorts.
 */
import { getDb, closeDb } from '../lib/db.js';

async function build() {
  const db = await getDb();
  const games = db.collection('games');
  const out = db.collection('rank_cache');

  console.log('Building rank_cache (placeholder)...');
  // Implement rank cache using aggregation with $setWindowFields (MongoDB 5.0+). This avoids
  // loading all docs into Node memory and computes rankQuality and rankPopularity server-side.
  // We also compute pctGE8 from ratingsDistribution embedded doc.

  // compute rankQuality (dense rank by bayesAvg desc)
  const qualityPipeline = [
    // include _id so we can upsert correctly into rank_cache
    { $project: { _id: 1, name: 1, bayesAvg: { $ifNull: ['$bayesAvg', 0] }, ratingsDistribution: 1 } },
    { $setWindowFields: { sortBy: { bayesAvg: -1 }, output: { rankQuality: { $denseRank: {} } } } },
    { $project: { _id: 1, name: 1, rankQuality: 1, ratingsDistribution: 1 } }
  ];
  const qualityCursor = games.aggregate(qualityPipeline, { allowDiskUse: true });
  const bulk = [];
  while (await qualityCursor.hasNext()) {
    const r = await qualityCursor.next();
    const pctGE8 = (() => {
      const dist = r.ratingsDistribution || {};
      let total = 0, keep = 0;
      for (const k of Object.keys(dist)) {
        const n = Number(dist[k]);
        if (!Number.isFinite(n)) continue;
        total += n;
        if (Number(k) >= 8) keep += n;
      }
      return total ? (keep / total) * 100 : null;
    })();
    const doc = { _id: r._id, name: r.name, rankQuality: r.rankQuality, pctGE8 };
    bulk.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    if (bulk.length >= 500) { await out.bulkWrite(bulk, { ordered: false }); bulk.length = 0; }
  }
  if (bulk.length) await out.bulkWrite(bulk, { ordered: false });

  // compute rankPopularity and merge into rank_cache
  const popPipeline = [
    // include _id so updates target the correct document in rank_cache
    { $project: { _id: 1, name: 1, numOwned: { $ifNull: ['$popularity.numOwned', 0] } } },
    { $setWindowFields: { sortBy: { numOwned: -1 }, output: { rankPopularity: { $denseRank: {} } } } },
    { $project: { _id: 1, name: 1, rankPopularity: 1 } }
  ];
  const popCursor = games.aggregate(popPipeline, { allowDiskUse: true });
  const updates = [];
  while (await popCursor.hasNext()) {
    const r = await popCursor.next();
    updates.push({ updateOne: { filter: { _id: r._id }, update: { $set: { rankPopularity: r.rankPopularity } }, upsert: false } });
    if (updates.length >= 500) { await out.bulkWrite(updates, { ordered: false }); updates.length = 0; }
  }
  if (updates.length) await out.bulkWrite(updates, { ordered: false });
  console.log('rank_cache built (placeholder).');
}

build()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('rank_cache error:', err);
    await closeDb();
    process.exit(1);
  });
