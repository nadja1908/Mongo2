/**
 * Drop the optimized indexes used by the experiment so we can measure baseline without them.
 * Usage: node src/run/drop-optimized-indexes.js
 */
import { getDb, closeDb } from '../lib/db.js';

async function drop() {
  const db = await getDb();
  const games = db.collection('games');
  const mech = db.collection('mechanic_stats');
  const dp = db.collection('designer_publisher_stats');

  const toDropGames = ['idx-q1-mech','idx-q2-themes','idx-q3-designers','idx-q3-publishers','idx-q4-year','idx-q5-bayes','idx-q5-popularity'];
  const toDropMech = ['idx-q1-mechstats-avg','idx-q1-mechstats-count'];
  const toDropDp = ['idx-q3-dpstats'];

  console.log('Dropping optimized indexes (if present) ...');
  for (const n of toDropGames) {
    try { await games.dropIndex(n); console.log('dropped', n); } catch (e) { /* ignore */ }
  }
  for (const n of toDropMech) {
    try { await mech.dropIndex(n); console.log('dropped', n); } catch (e) { /* ignore */ }
  }
  for (const n of toDropDp) {
    try { await dp.dropIndex(n); console.log('dropped', n); } catch (e) { /* ignore */ }
  }

  await closeDb();
}

drop().catch(err => { console.error('drop indexes failed:', err); process.exit(1); });
