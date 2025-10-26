/**
 * Q4.baseline
 * Baseline: Prosečne ocene po godini.
 * Pipeline: $group by year -> avg, count, sumOwned; plus best igra po godini (max avgRating) i najpopularnija po godini.
 * Bez indeksa; grupisanje nad svim dokumentima je skupo.
 * pipelineHash: sha256:Q4-baseline-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q4-baseline-v1';

export async function run(db) {
  const games = db.collection('games');
  const statsPipeline = [
    { $match: { year: { $ne: null } } },
    { $group: { _id: '$year', avgRating: { $avg: '$avgRating' }, count: { $sum: 1 }, sumOwned: { $sum: '$popularity.numOwned' } } },
    { $sort: { _id: 1 } }
  ];

  // Best per year (by avgRating) and most popular per year (by numOwned)
  const bestPipeline = [
    { $match: { year: { $ne: null } } },
    { $sort: { year: 1, avgRating: -1, 'popularity.numOwned': -1 } },
    { $group: { _id: '$year', bestGame: { $first: { _id: '$_id', name: '$name', avgRating: '$avgRating' } }, mostOwned: { $first: { _id: '$_id', name: '$name', numOwned: '$popularity.numOwned' } } } },
    { $sort: { _id: 1 } }
  ];

  const [stats, bests] = await Promise.all([
    games.aggregate(statsPipeline, { allowDiskUse: true }).toArray(),
    games.aggregate(bestPipeline, { allowDiskUse: true }).toArray()
  ]);

  return { sample: { stats: stats.slice(0,5), bests: bests.slice(0,5) }, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q4-year-averages',
        variant: 'baseline',
        indexSet: 'none',
        pipelineHash,
        run: async () => {
          await db.collection('games').aggregate([
            { $match: { year: { $ne: null } } },
            { $group: { _id: '$year', avgRating: { $avg: '$avgRating' }, count: { $sum: 1 } } },
            { $sort: { _id: 1 } }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q4.baseline ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
