/**
 * Q2.optimized
 *
 * Preferirana kolekcija: `theme_count_rank` (precomputed ETL) koja sadrži {_id, name, themesCount, avgRating, numOwned}.
 * Indeks (ako ostajemo na `games`): { themes:1, avgRating:-1 } daje prednost pri sortiranju po themesCount/avgRating.
 * pipelineHash: sha256:Q2-optimized-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q2-optimized-v1';

export async function run(db) {
  const pre = db.collection('theme_count_rank');
  const exists = await pre.countDocuments({}, { limit: 1 }) > 0;
  if (exists) {
    const sample = await pre.find().sort({ themesCount: -1, avgRating: -1 }).limit(5).toArray();
    return { sample, pipelineHash };
  }
  // Fallback on games with index idx-q2-themes
  const sample = await db.collection('games').aggregate([
    { $project: { name: 1, themesCount: { $size: { $ifNull: ['$themes', []] } }, avgRating: 1, 'popularity.numOwned': 1 } },
    { $sort: { themesCount: -1, avgRating: -1 } },
    { $limit: 5 }
  ], { allowDiskUse: true }).toArray();
  return { sample, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q2-most-themes',
        variant: 'optimized',
        indexSet: 'idx-q2-themes',
        pipelineHash,
        run: async () => {
          const pre = db.collection('theme_count_rank');
          const exists = await pre.countDocuments({}, { limit: 1 }) > 0;
          if (exists) return await pre.find().sort({ themesCount: -1, avgRating: -1 }).toArray();
          return await db.collection('games').aggregate([
            { $project: { name: 1, themesCount: { $size: { $ifNull: ['$themes', []] } }, avgRating: 1 } },
            { $sort: { themesCount: -1, avgRating: -1 } },
            { $limit: 100 }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q2.optimized ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
