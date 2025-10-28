import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
dotenv.config();

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27018';

const collName = process.argv[2];
if (!collName) {
  console.error('Usage: node src/run/find-db-by-collection.js <collectionName>');
  process.exit(1);
}

async function main() {
  const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
  try {
    await client.connect();
    const admin = client.db().admin();
    const dbs = (await admin.listDatabases()).databases;

    const matches = [];
    for (const d of dbs) {
      try {
        const db = client.db(d.name);
        const cols = await db.listCollections({}, { nameOnly: true }).toArray();
        const names = cols.map(c => c.name);
        if (names.includes(collName)) {
          const count = await db.collection(collName).estimatedDocumentCount();
          matches.push({ db: d.name, collection: collName, count });
        }
      } catch (err) {
        // ignore per-db errors
      }
    }

    if (!matches.length) {
      console.log(`No database contains a collection named '${collName}'`);
      return;
    }

    console.log('Found collection in:');
    for (const m of matches) console.log(`  ${m.db} -> ${m.collection} (${m.count.toLocaleString()} documents)`);
  } catch (err) {
    console.error('Error while searching databases:', err && err.stack ? err.stack : err);
    process.exitCode = 1;
  } finally {
    await client.close();
  }
}

main();
