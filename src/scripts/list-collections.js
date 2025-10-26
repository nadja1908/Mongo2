import { MongoClient } from 'mongodb';

const uri = process.env.MONGO_URI || 'mongodb://localhost:27018';
const dbName = process.env.DB_NAME || 'mongo_database2';

async function main() {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5000 });
  try {
    await client.connect();
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();
    if (!collections.length) {
      console.log(`Connected to ${uri}/${dbName} — no collections found.`);
      return;
    }

    console.log(`Connected to ${uri}/${dbName} — found ${collections.length} collections:`);
    for (const coll of collections) {
      try {
        const count = await db.collection(coll.name).estimatedDocumentCount();
        console.log(`${coll.name}: ${count.toLocaleString()} documents`);
      } catch (err) {
        console.error(`  ${coll.name}: error counting docs:`, err && err.stack ? err.stack : err);
      }
    }
  } catch (err) {
    console.error('Error connecting or listing collections:', err && err.stack ? err.stack : err);
    process.exitCode = 1;
  } finally {
    await client.close();
  }
}

main();
