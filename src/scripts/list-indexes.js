import { getDb, closeDb } from '../lib/db.js';

async function listIndexes() {
  const db = await getDb();
  try {
    const names = ['games','mechanic_stats','theme_count_rank','designer_publisher_stats','yearly_stats','rank_cache'];
    for (const n of names) {
      try {
        const idx = await db.collection(n).indexes();
        console.log(`Indexes for ${n}:`);
        console.log(idx.map(i => ({ name: i.name, key: i.key }))); 
      } catch (e) {
        console.log(`  collection ${n} not present`);
      }
    }
  } finally {
    await closeDb();
  }
}

listIndexes().catch(err => { console.error(err); process.exit(1); });
