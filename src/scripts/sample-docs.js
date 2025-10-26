import { getDb, closeDb } from '../lib/db.js';

async function sample() {
  const db = await getDb();
  try {
    console.log('One games_raw doc:');
    const raw = await db.collection('games_raw').findOne();
    console.log(JSON.stringify(raw, null, 2).slice(0, 1000));

    console.log('\nOne games doc:');
    const g = await db.collection('games').findOne();
    console.log(JSON.stringify(g, null, 2).slice(0, 1000));
  } finally {
    await closeDb();
  }
}

sample().catch(err => { console.error(err); process.exit(1); });
