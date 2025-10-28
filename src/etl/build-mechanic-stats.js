/**
 * src/etl/build-mechanic-stats.js
 *
 * Aggregates `games` collection to precompute mechanic-level statistics for games with avgRating > 8.
 * Output collection: `mechanic_stats` with documents like:
 * {
 *   _id: "Mechanic Name",
 *   gamesCount: Number,
 *   avgAvgRating: Number,
 *   avgBayes: Number,
 *   sumNumRatings: Number,
 *   sumNumOwned: Number,
 *   avgStdDev: Number
 * }
 *
 * Comment: For optimized Q1 we can either use this precomputed collection or rely on an index { avgRating:-1, mechanics:1 }.
 */
import { getDb, closeDb } from '../lib/db.js';

async function build() {
  const db = await getDb();
  const games = db.collection('games');
  const out = db.collection('mechanic_stats');
  // Ensure we don't leave stale documents from a previous (less-restrictive) run.
  // If we previously built mechanic_stats without the avgRating filter then
  // some mechanics may remain in the output even though the new pipeline
  // intentionally excludes them. Clear the collection before writing.
  console.log('Clearing existing mechanic_stats collection (to avoid stale docs)...');
  await out.deleteMany({});

  console.log('Building mechanic_stats (only games with avgRating > 8)...');
  // Be permissive: datasets may not contain avgRating/bayesAvg for all games (different dumps).
  // Instead, aggregate stats for any game that has a non-empty mechanics array. Use $ifNull
  // to provide safe numeric fallbacks so the aggregation always produces numbers.
  const pipeline = [
    // Only include games that have mechanics and where avgRating is strictly > 8
    { $match: { mechanics: { $exists: true, $ne: [], $ne: null } } },
    { $match: { avgRating: { $gt: 8 } } },
    { $unwind: '$mechanics' },
    {
      $group: {
        _id: '$mechanics',
        gamesCount: { $sum: 1 },
        avgAvgRating: { $avg: { $ifNull: ['$avgRating', '$bayesAvg'] } },
        avgBayes: { $avg: { $ifNull: ['$bayesAvg', '$avgRating'] } },
        sumNumRatings: { $sum: { $ifNull: ['$popularity.numRatings', 0] } },
        sumNumOwned: { $sum: { $ifNull: ['$popularity.numOwned', 0] } },
        avgStdDev: { $avg: { $ifNull: ['$stdDev', 0] } }
      }
    },
    { $sort: { gamesCount: -1 } }
  ];

  const cursor = games.aggregate(pipeline, { allowDiskUse: true });
  const bulk = [];
  let count = 0;
  while (await cursor.hasNext()) {
    const doc = await cursor.next();
    bulk.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    count++;
    if (bulk.length >= 500) {
      await out.bulkWrite(bulk, { ordered: false });
      bulk.length = 0;
    }
  }
  if (bulk.length) await out.bulkWrite(bulk, { ordered: false });
  console.log(`mechanic_stats built: ${count} mechanics aggregated.`);
}

build()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('mechanic_stats error:', err);
    await closeDb();
    process.exit(1);
  });
