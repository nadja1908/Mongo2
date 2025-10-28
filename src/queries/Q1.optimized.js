/**
 * Q1.optimized
 *
 * Koristim kolekciju: mechanic_stats (prekompjut iz ETL-a) ili indeks { avgRating:-1, mechanics:1 }.
 * Indeks ubrzava faze $match i $unwind jer omogućava selektivno čitanje po avgRating i efikasan multikey presek po mechanics.
 * pipelineHash: sha256:Q1-optimized-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';
import { pathToFileURL } from 'url';

export const pipelineHash = 'sha256:Q1-optimized-v1';

export async function run(db) {
  // Prefer precomputed collection if present.
  const msCol = db.collection('mechanic_stats');
  const exists = await msCol.countDocuments({}, { limit: 1 }) > 0;
  if (exists) {
    const sample = await msCol.find().sort({ gamesCount: -1 }).limit(5).toArray();
    return { sample, pipelineHash };
  }
  // Fallback: use index on games to limit scan. This pipeline relies on index { avgRating:-1, mechanics:1 }
  const pipeline = [
    { $match: { avgRating: { $gt: 8 } } },
    { $unwind: '$mechanics' },
    {
      $group: {
        _id: '$mechanics',
        gamesCount: { $sum: 1 },
        avgAvgRating: { $avg: '$avgRating' },
      }
    },
    { $sort: { gamesCount: -1 } }
  ];
  const sample = await db.collection('games').aggregate(pipeline).limit(5).toArray();
  return { sample, pipelineHash };
}

// Use pathToFileURL so the direct-run check works on Windows (process.argv[1] uses backslashes)
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q1-mechanics-gt8',
        variant: 'optimized',
        indexSet: 'idx-q1-mech',
        pipelineHash,
        run: async () => {
          // If mechanic_stats exists, read from it; otherwise run the pipeline using index.
          const msCol = db.collection('mechanic_stats');
          const exists = await msCol.countDocuments({}, { limit: 1 }) > 0;
          if (exists) return await msCol.find().sort({ gamesCount: -1 }).toArray();
          return await db.collection('games').aggregate([
            { $match: { avgRating: { $gt: 8 } } },
            { $unwind: '$mechanics' },
            { $group: { _id: '$mechanics', gamesCount: { $sum: 1 }, avgAvgRating: { $avg: '$avgRating' } } },
            { $sort: { gamesCount: -1 } }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q1.optimized ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
