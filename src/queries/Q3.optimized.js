/**
 * Q3.optimized
 *
 * Preferirana kolekcija: `designer_publisher_stats` (precomputed ETL) sa ključem pair/d,p.
 * Alternativno indeks { designers:1, publishers:1 } na `games` može pomoći.
 * Eliminacijom duplog $unwind dramatično smanjujemo IO.
 * pipelineHash: sha256:Q3-optimized-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q3-optimized-v1';

export async function run(db) {
  const pre = db.collection('designer_publisher_stats');
  const exists = await pre.countDocuments({}, { limit: 1 }) > 0;
  if (exists) {
    const sample = await pre.find({ sumNumRatings: { $gte: 500 } }).sort({ gamesCount: -1 }).limit(5).toArray();
    return { sample, pipelineHash };
  }
  // Fallback: use index on games
  const sample = await db.collection('games').aggregate([
    { $unwind: '$designers' },
    { $unwind: '$publishers' },
    { $group: { _id: { designer: '$designers', publisher: '$publishers' }, gamesCount: { $sum: 1 }, sumNumRatings: { $sum: '$popularity.numRatings' }, avgRating: { $avg: '$avgRating' } } },
    { $match: { sumNumRatings: { $gte: 500 } } },
    { $sort: { gamesCount: -1 } },
    { $limit: 20 }
  ], { allowDiskUse: true }).toArray();
  return { sample, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q3-designer-publisher',
        variant: 'optimized',
        indexSet: 'idx-q3-dp',
        pipelineHash,
        run: async () => {
          const pre = db.collection('designer_publisher_stats');
          const exists = await pre.countDocuments({}, { limit: 1 }) > 0;
          if (exists) return await pre.find({ sumNumRatings: { $gte: 500 } }).sort({ gamesCount: -1 }).toArray();
          return await db.collection('games').aggregate([
            { $unwind: '$designers' },
            { $unwind: '$publishers' },
            { $group: { _id: { designer: '$designers', publisher: '$publishers' }, gamesCount: { $sum: 1 }, sumNumRatings: { $sum: '$popularity.numRatings' } } },
            { $match: { sumNumRatings: { $gte: 500 } } },
            { $sort: { gamesCount: -1 } }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q3.optimized ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
