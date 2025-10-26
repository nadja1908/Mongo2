import { getDb, closeDb } from '../lib/db.js';

async function counts() {
  const db = await getDb();
  try {
    const cols = await db.listCollections().toArray();
    for (const c of cols) {
      const name = c.name;
      const cnt = await db.collection(name).countDocuments();
      console.log(`${name}: ${cnt}`);
    }
  } finally {
    await closeDb();
  }
}

counts().catch(err => { console.error(err); process.exit(1); });
