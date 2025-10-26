/**
 * Q5.optimized
 *
 * Optimized: koristim `rank_cache` kolekciju (ETL) koja sadrži {_id, rankQuality, rankPopularity, pctGE8}.
 * Keširanje rangova izbegava skupo globalno sortiranje i omogućava O(1) pristup top listama.
 * Indeksi: { bayesAvg:-1 } i { "popularity.numOwned": -1 } su korisni kada gradimo rank_cache iz izvornog skupa.
 * pipelineHash: sha256:Q5-optimized-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q5-optimized-v1';

export async function run(db) {
  const cache = db.collection('rank_cache');
  const exists = await cache.countDocuments({}, { limit: 1 }) > 0;
  if (exists) {
    const sample = await cache.find().sort({ rankQuality: 1 }).limit(5).toArray();
    return { sample, pipelineHash };
  }
  // Fallback: compute small snapshot using indexes
  const topQ = await db.collection('games').find({}, { projection: { name: 1, bayesAvg: 1, ratingsDistribution: 1 } }).sort({ bayesAvg: -1 }).limit(100).toArray();
  const topP = await db.collection('games').find({}, { projection: { name: 1, 'popularity.numOwned': 1, ratingsDistribution: 1 } }).sort({ 'popularity.numOwned': -1 }).limit(100).toArray();
  const sample = topQ.slice(0,5).map(g => ({ _id: g._id, name: g.name, bayesAvg: g.bayesAvg }));
  return { sample, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q5-quality-popularity',
        variant: 'optimized',
        indexSet: 'idx-q5-bayes/idx-q5-popularity',
        pipelineHash,
        run: async () => {
          const cache = db.collection('rank_cache');
          const exists = await cache.countDocuments({}, { limit: 1 }) > 0;
          if (exists) return await cache.find().sort({ rankQuality: 1 }).toArray();
          await db.collection('games').find({}, { projection: { bayesAvg: 1 } }).sort({ bayesAvg: -1 }).limit(100).toArray();
          await db.collection('games').find({}, { projection: { 'popularity.numOwned': 1 } }).sort({ 'popularity.numOwned': -1 }).limit(100).toArray();
        },
        resultSample: sample
      });
  console.log('Q5.optimized ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
