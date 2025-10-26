/**
 * src/etl/clean-invalid-years.js
 *
 * Deletes documents from `games` where `year` exists and is less than 1000.
 * This is useful to remove bad/placeholder year values (e.g. 0, 1) that distort
 * yearly aggregations. The script logs how many documents were removed and prints
 * a small sample of remaining documents that have a non-null year.
 */
import { getDb, closeDb } from '../lib/db.js';

async function run() {
  const db = await getDb();
  const games = db.collection('games');

  console.log('Cleaning invalid years: removing games with year < 1000...');
  const filter = { year: { $exists: true, $lt: 1000 } };
  const toRemove = await games.countDocuments(filter);
  if (toRemove === 0) {
    console.log('No documents to remove.');
    await closeDb();
    return;
  }

  const res = await games.deleteMany(filter);
  console.log(`Deleted documents: ${res.deletedCount} (requested ${toRemove})`);

  // show a small sample of remaining docs with a valid year
  const sample = await games.find({ year: { $exists: true, $gte: 1000 } }).limit(5).toArray();
  console.log('Sample of remaining games with valid year:', sample.map(d => ({ _id: d._id, name: d.name, year: d.year })));

  await closeDb();
}

run().catch(async (err) => {
  console.error('clean-invalid-years error:', err);
  await closeDb();
  process.exit(1);
});
