/**
 * Q5.baseline
 * Baseline: Kvalitet (bayesAvg) vs Popularnost (popularity.numOwned), uz % ocena >= 8.
 * Strategija: globalno sortiranje po bayesAvg i po numOwned, zatim spajanje rangova u memoriji; iz ratingsDistribution izračunavam pct>=8.
 * Ovo je skupo jer zahteva dve globalne sortiranja.
 * pipelineHash: sha256:Q5-baseline-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q5-baseline-v1';

function pctGE8FromDist(dist) {
  if (!dist || typeof dist !== 'object') return null;
  let total = 0, keep = 0;
  for (const k of Object.keys(dist)) {
    const n = Number(dist[k]);
    if (!Number.isFinite(n)) continue;
    total += n;
    if (Number(k) >= 8) keep += n;
  }
  return total ? (keep / total) * 100 : null;
}

export async function run(db) {
  const games = db.collection('games');
  // Get top N by bayesAvg and top N by numOwned
  const topByQuality = await games.find({}, { projection: { name: 1, bayesAvg: 1, ratingsDistribution: 1 } }).sort({ bayesAvg: -1 }).limit(100).toArray();
  const topByPopularity = await games.find({}, { projection: { name: 1, 'popularity.numOwned': 1, ratingsDistribution: 1 } }).sort({ 'popularity.numOwned': -1 }).limit(100).toArray();

  // Build maps for ranks
  const qualityRanks = new Map();
  topByQuality.forEach((g, i) => qualityRanks.set(g._id, i + 1));
  const popRanks = new Map();
  topByPopularity.forEach((g, i) => popRanks.set(g._id, i + 1));

  // Join top ids
  const ids = new Set([...topByQuality.map(x => String(x._id)), ...topByPopularity.map(x => String(x._id))]);
  const merged = [];
  for (const id of ids) {
    const q = topByQuality.find(x => String(x._id) === id) || null;
    const p = topByPopularity.find(x => String(x._id) === id) || null;
    const doc = {
      _id: q?._id || p?._id || id,
      name: q?.name || p?.name || null,
      rankQuality: q ? qualityRanks.get(q._id) : null,
      rankPopularity: p ? popRanks.get(p._id) : null,
      pctGE8: pctGE8FromDist((q && q.ratingsDistribution) || (p && p.ratingsDistribution))
    };
    merged.push(doc);
  }

  // For baseline sample, return top 5 merged
  merged.sort((a,b) => (a.rankQuality || 1e9) - (b.rankQuality || 1e9));
  const sample = merged.slice(0,5);
  return { sample, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q5-quality-popularity',
        variant: 'baseline',
        indexSet: 'none',
        pipelineHash,
        run: async () => {
          // perform the same work: two global sorts
          await db.collection('games').find({}, { projection: { bayesAvg: 1 } }).sort({ bayesAvg: -1 }).limit(100).toArray();
          await db.collection('games').find({}, { projection: { 'popularity.numOwned': 1 } }).sort({ 'popularity.numOwned': -1 }).limit(100).toArray();
        },
        resultSample: sample
      });
  console.log('Q5.baseline ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
