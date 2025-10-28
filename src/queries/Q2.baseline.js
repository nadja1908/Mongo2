/**
 * Q2.baseline
 * Baseline: Igre sa najviše tema.
 * Pipeline: $project themesCount using $size -> sort desc -> limit. Also compute average/median and buckets (0-5,6-10,>10).
 * Bez indeksa/bez prekompjutovanja; koristi $project/$size nad celom kolekcijom što je skupo za IO.
 * pipelineHash: sha256:Q2-baseline-v1
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

export const pipelineHash = 'sha256:Q2-baseline-v1';

export async function run(db) {
  const coll = db.collection('games');
  const pipeline = [
    { $project: { name: 1, themesCount: { $size: { $ifNull: ['$themes', []] } }, avgRating: 1, 'popularity.numOwned': 1 } },
    { $sort: { themesCount: -1, avgRating: -1 } },
    { $limit: 100 },
    {
      $group: {
        _id: null,
        top: { $push: { _id: '$_id', name: '$name', themesCount: '$themesCount', avgRating: '$avgRating', numOwned: '$popularity.numOwned' } },
        avgThemes: { $avg: '$themesCount' }
      }
    }
  ];

  
  const bucketPipeline = [
    { $project: { themesCount: { $size: { $ifNull: ['$themes', []] } } } },
    { $bucket: { groupBy: '$themesCount', boundaries: [0,6,11,1000], default: 'other', output: { count: { $sum: 1 } } } }
  ];

  const top = await coll.aggregate(pipeline, { allowDiskUse: true }).toArray();
  const buckets = await coll.aggregate(bucketPipeline, { allowDiskUse: true }).toArray();

  return { sample: { top: top[0]?.top?.slice(0,3) || [], buckets }, pipelineHash };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const db = await getDb();
    try {
      const { sample } = await run(db);
      const ms = await measure({
        queryId: 'Q2-most-themes',
        variant: 'baseline',
        indexSet: 'none',
        pipelineHash,
        run: async () => {
          await db.collection('games').aggregate([
            { $project: { name: 1, themesCount: { $size: { $ifNull: ['$themes', []] } }, avgRating: 1 } },
            { $sort: { themesCount: -1, avgRating: -1 } },
            { $limit: 100 }
          ], { allowDiskUse: true }).toArray();
        },
        resultSample: sample
      });
  console.log('Q2.baseline ms (max):', ms);
    } finally {
      await closeDb();
    }
  })();
}
