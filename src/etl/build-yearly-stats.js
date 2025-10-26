/**
 * src/etl/build-yearly-stats.js
 *
 * Precompute stats per year: count, avgRating, top game by avgRating and by numOwned.
 * This enables Q4.optimized to read a small collection instead of grouping the whole dataset.
 */
import { getDb, closeDb } from '../lib/db.js';

async function build() {
  const db = await getDb();
  const games = db.collection('games');
  const out = db.collection('yearly_stats');

  console.log('Building yearly_stats...');
  const pipeline = [
    { $match: { year: { $ne: null } } },
    {
      $group: {
        _id: '$year',
        avgRating: { $avg: { $ifNull: ['$avgRating', '$bayesAvg'] } },
        count: { $sum: 1 },
        sumOwned: { $sum: { $ifNull: ['$popularity.numOwned', 0] } }
      }
    },
    { $sort: { _id: 1 } }
  ];

  const stats = await games.aggregate(pipeline, { allowDiskUse: true }).toArray();
  const bulk = [];
  for (const s of stats) {
+    bulk.push({ replaceOne: { filter: { _id: s._id }, replacement: s, upsert: true } });
  }
  if (bulk.length) await out.bulkWrite(bulk, { ordered: false });
  console.log('yearly_stats built:', stats.length);
}

build()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('yearly_stats error:', err);
    await closeDb();
    process.exit(1);
  });

