/**
 * src/run/full-run.js
 *
 * Orchestrator to run the full experiment sequence:
 * 1) Drop optimized indexes (so baseline runs without them)
 * 2) Run baseline measurements
 * 3) Run ETL to (re)build helper collections
 * 4) Create optimized indexes
 * 5) Run optimized measurements
 * 6) Generate report
 *
 * Usage: node src/run/full-run.js
 */
import { execSync } from 'child_process';
import { getDb, closeDb } from '../lib/db.js';

async function dropOptimizedIndexes() {
  const db = await getDb();
  const games = db.collection('games');
  const mech = db.collection('mechanic_stats');
  const dp = db.collection('designer_publisher_stats');

  const toDropGames = ['idx-q1-mech','idx-q2-themes','idx-q3-designers','idx-q3-publishers','idx-q4-year','idx-q5-bayes','idx-q5-popularity'];
  const toDropMech = ['idx-q1-mechstats-avg','idx-q1-mechstats-count'];
  const toDropDp = ['idx-q3-dpstats'];

  console.log('Dropping optimized indexes (if present)...');
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

function runCmd(cmd, opts = {}) {
  console.log('\n>>> Running:', cmd);
  try {
    const out = execSync(cmd, { stdio: 'inherit', ...opts });
    return out;
  } catch (err) {
    console.error('Command failed:', cmd);
    throw err;
  }
}

async function main() {
  try {
    // 1) Drop indexes so baseline is measured without them
    await dropOptimizedIndexes();

    // 2) Baseline (no optimized indexes)
    runCmd('node src/run/run-baseline.js');

    // 3) ETL (rebuild games and helpers)
    runCmd('node src/etl/build-games.js');
    runCmd('node src/etl/build-mechanic-stats.js');
    runCmd('node src/etl/build-designer-publisher-stats.js');
    runCmd('node src/etl/build-yearly-stats.js');
    runCmd('node src/etl/build-rank-cache.js');

    // 4) Create optimized indexes
    runCmd('node src/indexes/create-optimized-indexes.js');

    // 5) Optimized runs
    runCmd('node src/run/run-optimized.js');

    // 6) Report
    runCmd('node src/report/generate-report.js');

    console.log('\nFull run complete. Reports: report/metrics.csv, report/summary.md');
  } catch (err) {
    console.error('Full run failed:', err.message || err);
    process.exit(1);
  }
}

main();
