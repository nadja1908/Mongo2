import { MongoClient } from 'mongodb';

async function explainNoIndex() {
  const uri = process.env.MONGO_URI || 'mongodb://localhost:27018';
  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db('mongo_database2');
    // drop optimized index if present
    try { await db.collection('games').dropIndex('idx-q1-mech'); console.log('dropped idx-q1-mech'); } catch (e) { /* ignore */ }

    const pipeline = [
      { $match: { avgRating: { $gt: 8 } } },
      { $unwind: '$mechanics' },
      { $group: { _id: '$mechanics', gamesCount: { $sum: 1 } } },
      { $sort: { gamesCount: -1 } }
    ];
    const expl = await db.collection('games').aggregate(pipeline).explain('executionStats');
    console.log(JSON.stringify(expl, null, 2));
  } finally {
    await client.close();
  }
}

explainNoIndex().catch(err => { console.error(err); process.exit(1); });
