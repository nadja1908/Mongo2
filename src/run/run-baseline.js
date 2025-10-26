/**
 * Runner for baseline queries Q1..Q5.
 * Imports each query module and uses measure() to record metrics into runMetrics.
 * This avoids relying on file-level main checks in each query file.
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

const QUERIES = [
  { id: 'Q1-mechanics-gt8', path: '../queries/Q1.baseline.js', indexSet: 'none' },
  { id: 'Q2-most-themes', path: '../queries/Q2.baseline.js', indexSet: 'none' },
  { id: 'Q3-designer-publisher', path: '../queries/Q3.baseline.js', indexSet: 'none' },
  { id: 'Q4-year-averages', path: '../queries/Q4.baseline.js', indexSet: 'none' },
  { id: 'Q5-quality-popularity', path: '../queries/Q5.baseline.js', indexSet: 'none' }
];

async function main() {
  const db = await getDb();
  for (const q of QUERIES) {
    console.log('Running', q.id);
    const mod = await import(q.path);
    const { pipelineHash } = mod;
    // run once to get sample
    const { sample } = await mod.run(db);
    // measure executes the pipeline multiple times and writes runMetrics
    const ms = await measure({ queryId: q.id, variant: 'baseline', indexSet: q.indexSet, pipelineHash, run: async () => { await mod.run(db); }, resultSample: sample });
  console.log(`${q.id} max ms:`, ms);
  }
  await closeDb();
}

main().catch(async (err) => {
  console.error('Baseline runner error:', err);
  await closeDb();
  process.exit(1);
});
