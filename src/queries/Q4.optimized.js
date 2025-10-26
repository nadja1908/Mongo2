/**
 * Q4.optimized
 *
 * Optimized: koristim indeks { year:1, avgRating:-1, "popularity.numOwned": -1 } (idx-q4-year) ili ETL `yearly_stats`.
 * Prefix order pomaže pri sortiranju najboljih igara po godini jer omogućava range scan po year i potom sekvencijalni sort po avgRating.
 * pipelineHash: sha256:Q4-optimized-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q4-optimized-v1';

export async function run(db) {
  const yearly = db.collection('yearly_stats');
  const exists = await yearly.countDocuments({}, { limit: 1 }) > 0;
  if (exists) {
    const sample = await yearly.find().sort({ _id: 1 }).limit(5).toArray();
    return { sample, pipelineHash };
  }

  // Fallback: use index to efficiently get best per year
  const games = db.collection('games');
  const bestPerYearPipeline = [
    { $match: { year: { $ne: null } } },
    { $sort: { year: 1, avgRating: -1, 'popularity.numOwned': -1 } },
    { $group: { _id: '$year', bestGame: { $first: { _id: '$_id', name: '$name', avgRating: '$avgRating' } } } },
    { $sort: { _id: 1 } }
  ];
  const sample = await games.aggregate(bestPerYearPipeline, { allowDiskUse: true }).limit(5).toArray();
  return { sample, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q4-year-averages',
        variant: 'optimized',
        indexSet: 'idx-q4-year',
        pipelineHash,
        run: async () => {
          const yearly = db.collection('yearly_stats');
          const exists = await yearly.countDocuments({}, { limit: 1 }) > 0;
          if (exists) return await yearly.find().limit(100).toArray();
          return await db.collection('games').aggregate([
            { $match: { year: { $ne: null } } },
            { $sort: { year: 1, avgRating: -1 } },
            { $group: { _id: '$year', bestGame: { $first: { _id: '$_id', name: '$name', avgRating: '$avgRating' } }, avgRating: { $avg: '$avgRating' } } },
            { $sort: { _id: 1 } }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q4.optimized ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
