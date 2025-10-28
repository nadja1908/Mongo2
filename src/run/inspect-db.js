import { getDb, closeDb } from '../lib/db.js';

// Simple inspector: lists collections, counts and one sample document each.
// Usage: node src/run/inspect-db.js

async function main() {
  const db = await getDb();
  try {
    const cols = await db.listCollections().toArray();
    const out = [];
    for (const c of cols) {
      const name = c.name;
      const coll = db.collection(name);
      const count = await coll.countDocuments();
      const sample = await coll.findOne({}, { projection: {} });
      out.push({ collection: name, count, sample });
    }
    console.log(JSON.stringify(out, null, 2));
  } finally {
    await closeDb();
  }
}

main().catch(err => {
  console.error('Inspect failed:', err);
  process.exit(1);
});
