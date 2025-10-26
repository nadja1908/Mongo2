/**
 * src/etl/build-theme-count-rank.js
 *
 * Placeholder ETL: computes `theme_count_rank` collection that stores {_id: gameId, name, themesCount, avgRating, numOwned}
 * This helps Q2 optimized where we avoid $project + $size over full collection.
 */
import { getDb, closeDb } from '../lib/db.js';

async function build() {
  const db = await getDb();
  const games = db.collection('games');
  const out = db.collection('theme_count_rank');

  console.log('Building theme_count_rank (placeholder)...');
  const cursor = games.find({}, { projection: { name: 1, mechanics: 1, themes: 1, avgRating: 1, 'popularity.numOwned': 1 } });
  const bulk = [];
  while (await cursor.hasNext()) {
    const g = await cursor.next();
    const doc = {
      _id: g._id,
      name: g.name,
      themesCount: Array.isArray(g.themes) ? g.themes.length : 0,
      avgRating: g.avgRating,
      numOwned: g.popularity?.numOwned || 0,
    };
    bulk.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    if (bulk.length >= 500) {
      await out.bulkWrite(bulk, { ordered: false });
      bulk.length = 0;
    }
  }
  if (bulk.length) await out.bulkWrite(bulk, { ordered: false });
  console.log('theme_count_rank built (placeholder).');
}

build()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('theme_count_rank error:', err);
    await closeDb();
    process.exit(1);
  });
