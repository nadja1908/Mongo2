/**
 * Runner for optimized queries Q1..Q5.
 * Imports each optimized query module and measures them, writing to runMetrics.
 */
import { getDb, closeDb } from '../lib/db.js';
import { measure } from '../lib/measure.js';

const QUERIES = [
  { id: 'Q1-mechanics-gt8', path: '../queries/Q1.optimized.js', indexSet: 'idx-q1-mech' },
  { id: 'Q2-most-themes', path: '../queries/Q2.optimized.js', indexSet: 'idx-q2-themes' },
  { id: 'Q3-designer-publisher', path: '../queries/Q3.optimized.js', indexSet: 'idx-q3-dp' },
  { id: 'Q4-year-averages', path: '../queries/Q4.optimized.js', indexSet: 'idx-q4-year' },
  { id: 'Q5-quality-popularity', path: '../queries/Q5.optimized.js', indexSet: 'idx-q5-bayes/idx-q5-popularity' }
];

async function main() {
  const db = await getDb();
  for (const q of QUERIES) {
    console.log('Running', q.id);
    const mod = await import(q.path);
    const { pipelineHash } = mod;
    const { sample } = await mod.run(db);
    const ms = await measure({ queryId: q.id, variant: 'optimized', indexSet: q.indexSet, pipelineHash, run: async () => { await mod.run(db); }, resultSample: sample });
  console.log(`${q.id} max ms:`, ms);
  }
  await closeDb();
}

main().catch(async (err) => {
  console.error('Optimized runner error:', err);
  await closeDb();
  process.exit(1);
});
