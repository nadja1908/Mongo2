/**
 * Q3.baseline
 * Baseline: Prosek po paru dizajner–izdavač (samo parovi sa ≥ 500 glasova).
 * Pipeline: $unwind designers + $unwind publishers + $group + $match sumNumRatings >= 500 + sort.
 * Ovo je skupo jer koristi dupli $unwind koji multiplicira dokaze i IO.
 * pipelineHash: sha256:Q3-baseline-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q3-baseline-v1';

export async function run(db) {
  const pipeline = [
    { $unwind: '$designers' },
    { $unwind: '$publishers' },
    { $group: { _id: { designer: '$designers', publisher: '$publishers' }, gamesCount: { $sum: 1 }, sumNumRatings: { $sum: '$popularity.numRatings' }, avgRating: { $avg: '$avgRating' } } },
    { $match: { sumNumRatings: { $gte: 500 } } },
    { $sort: { gamesCount: -1 } },
    { $limit: 20 }
  ];
  const sample = await db.collection('games').aggregate(pipeline, { allowDiskUse: true }).toArray();
  return { sample, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q3-designer-publisher',
        variant: 'baseline',
        indexSet: 'none',
        pipelineHash,
        run: async () => {
          await db.collection('games').aggregate([
            { $unwind: '$designers' },
            { $unwind: '$publishers' },
            { $group: { _id: { designer: '$designers', publisher: '$publishers' }, gamesCount: { $sum: 1 }, sumNumRatings: { $sum: '$popularity.numRatings' } } },
            { $match: { sumNumRatings: { $gte: 500 } } },
            { $sort: { gamesCount: -1 } },
            { $limit: 50 }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q3.baseline ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
