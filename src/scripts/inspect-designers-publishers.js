import { getDb, closeDb } from '../lib/db.js';

async function inspect() {
  const db = await getDb();
  try {
    const d = await db.collection('designers_raw').findOne();
    console.log('One designers_raw doc:');
    console.log(JSON.stringify(d, null, 2).slice(0, 1000));

    const p = await db.collection('publishers_raw').findOne();
    console.log('\nOne publishers_raw doc:');
    console.log(JSON.stringify(p, null, 2).slice(0, 1000));

    const gWithBGG = await db.collection('games').findOne({ $or: [{ BGGId: { $exists: true, $ne: null } }, { bggId: { $exists: true, $ne: null } }, { gameId: { $exists: true, $ne: null } }] });
    console.log('\nOne game with BGGId/gameId if any:');
    console.log(JSON.stringify(gWithBGG, null, 2).slice(0, 1000));

    const gWithDesigners = await db.collection('games').findOne({ designers: { $exists: true, $ne: [] } });
    console.log('\nOne game with non-empty designers if any:');
    console.log(JSON.stringify(gWithDesigners, null, 2).slice(0, 1000));
  } finally {
    await closeDb();
  }
}

inspect().catch(err => { console.error(err); process.exit(1); });
