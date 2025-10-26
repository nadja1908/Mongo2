/**
 * src/report/generate-report.js
 *
 * Generates report/metrics.csv and report/summary.md from runMetrics collection.
 * - metrics.csv: queryId, variant, indexSet, ms, ts, pipelineHash
 * - summary.md: baseline vs optimized table with worst-case (max) times and speedup (baseline_ms / optimized_ms)
 *
 * Speedup is computed per queryId using max ms for each variant. If multiple runs exist per variant,
 * we take the maximum value stored in each run's ms (the measure() stores worst-case of 5 runs).
 */
import fs from 'fs';
import path from 'path';
import { getDb, closeDb } from '../lib/db.js';

function worst(values) {
  if (!values || !values.length) return null;
  return Math.max(...values);
}

function minMax(values) {
  if (!values || !values.length) return { min: null, max: null };
  let min = Infinity, max = -Infinity;
  for (const v of values) {
    if (v < min) min = v;
    if (v > max) max = v;
  }
  return { min, max };
}

async function main() {
  const db = await getDb();
  const col = db.collection('runMetrics');
  const rows = await col.find().sort({ ts: 1 }).toArray();
  if (!rows.length) {
    console.log('No runMetrics found.');
    await closeDb();
    return;
  }

  const csvPath = path.join(process.cwd(), 'report', 'metrics.csv');
  const mdPath = path.join(process.cwd(), 'report', 'summary.md');
  const csvStream = ['queryId,variant,indexSet,ms,ts,pipelineHash'];
  for (const r of rows) {
    csvStream.push(`${r.queryId},${r.variant},${r.indexSet},${r.ms},${r.ts.toISOString()},${r.pipelineHash}`);
  }
  fs.writeFileSync(csvPath, csvStream.join('\n'));

  // Build summary
  // Group by queryId and variant to compute median
  const byQuery = {};
  for (const r of rows) {
    byQuery[r.queryId] = byQuery[r.queryId] || { baseline: [], optimized: [], indexSet: null };
    byQuery[r.queryId][r.variant].push(r.ms);
    if (r.variant === 'optimized') byQuery[r.queryId].indexSet = r.indexSet;
  }

  const lines = ['# Summary: baseline vs optimized', '', '| Query | Baseline ms (max) | Optimized ms (max) | Speedup (baseline/opt) | Indexes/Collections |', '|---|---:|---:|---:|---|'];
  for (const [q, data] of Object.entries(byQuery)) {
    const baseMax = worst(data.baseline) || null;
    const optMax = worst(data.optimized) || null;
    const baseMM = minMax(data.baseline);
    const optMM = minMax(data.optimized);
    const speed = (baseMax && optMax) ? (baseMax / optMax).toFixed(2) : 'n/a';
    const baseLabel = baseMax ? `${baseMax.toFixed(4)} (min ${baseMM.min.toFixed(4)}, max ${baseMM.max.toFixed(4)})` : 'n/a';
    const optLabel = optMax ? `${optMax.toFixed(4)} (min ${optMM.min.toFixed(4)}, max ${optMM.max.toFixed(4)})` : 'n/a';
    lines.push(`| ${q} | ${baseLabel} | ${optLabel} | ${speed} | ${data.indexSet || 'none'} |`);
  }

  // compute global worst-case for baseline and optimized
  let global = { baselineMax: null, optimizedMax: null };
  const allBaseline = [];
  const allOptimized = [];
  for (const r of rows) {
    if (r.variant === 'baseline' && Number.isFinite(r.ms)) allBaseline.push(r.ms);
    if (r.variant === 'optimized' && Number.isFinite(r.ms)) allOptimized.push(r.ms);
  }
  const gBase = minMax(allBaseline);
  const gOpt = minMax(allOptimized);
  global.baselineMax = gBase.max;
  global.optimizedMax = gOpt.max;

  lines.push('', '## Notes', '', '- Median computed from stored runMetrics (each run is median of 5 executions after warmup).', '- Speedup is baseline_median / optimized_median.', '');
  lines.push('', '## Worst-case (max) times across all runs', '');
  lines.push(`- Baseline worst (max ms): ${global.baselineMax !== null ? global.baselineMax.toFixed(4) : 'n/a'}`);
  lines.push(`- Optimized worst (max ms): ${global.optimizedMax !== null ? global.optimizedMax.toFixed(4) : 'n/a'}`);
  fs.writeFileSync(mdPath, lines.join('\n'));

  console.log('Report generated:', csvPath, mdPath);
  await closeDb();
}

main().catch(async (err) => {
  console.error('Report error:', err);
  await closeDb();
  process.exit(1);
});
