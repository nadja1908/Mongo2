/**
 * Q1.baseline
 * Baseline: Najpopularnije mehanike za igre sa avgRating > 8.
 * Bez indeksa/bez pomoćnih kolekcija; puni skupi pipeline sa $match + $unwind + $group.
 * pipelineHash: sha256:Q1-baseline-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q1-baseline-v1';

export async function run(db) {
  const pipeline = [
    { $match: { avgRating: { $gt: 8 } } },
    { $unwind: '$mechanics' },
    {
      $group: {
        _id: '$mechanics',
        gamesCount: { $sum: 1 },
        avgAvgRating: { $avg: '$avgRating' },
        avgBayes: { $avg: '$bayesAvg' },
        sumNumRatings: { $sum: '$popularity.numRatings' },
        sumNumOwned: { $sum: '$popularity.numOwned' },
        avgStdDev: { $avg: '$stdDev' }
      }
    },
    { $sort: { gamesCount: -1 } }
  ];
  const sample = await db.collection('games').aggregate(pipeline).limit(5).toArray();
  return { sample, pipelineHash };
}

// Runnable as a script
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q1-mechanics-gt8',
        variant: 'baseline',
        indexSet: 'none',
        pipelineHash,
        run: async () => { await db.collection('games').aggregate([
          { $match: { avgRating: { $gt: 8 } } },
          { $unwind: '$mechanics' },
          { $group: { _id: '$mechanics', gamesCount: { $sum: 1 }, avgAvgRating: { $avg: '$avgRating' } } },
          { $sort: { gamesCount: -1 } }
        ], { allowDiskUse: true }).toArray(); },
        resultSample: sample
      });
  console.log('Q1.baseline ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
