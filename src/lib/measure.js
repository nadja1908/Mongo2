/**
 * src/lib/measure.js
 * Funkcija za merenje vremena izvršavanja upita.
 * Beleži krajnje (worst-case / max) vreme od 5 pokretanja (sa jednim warmup), koristi process.hrtime.bigint().
 * Upisuje metrikе u kolekciju runMetrics.
 *
 * Format metrika:
 * {
 *   ts: ISODate,
 *   datasetVersion: "v1",
 *   queryId: "Q1-mechanics-gt8",
 *   variant: "baseline"|"optimized",
 *   indexSet: "none"|"idx-mech-avg-1"|...,
 *   pipelineHash: "sha256:<neki-id>",
 *   ms: Number,
 *   resultSample: <opciono: prvih par rezultata>
 * }
 */
import { getDb } from './db.js';

export async function measure({ queryId, variant, indexSet, pipelineHash, run, resultSample }) {
  // Warmup
  await run();
  const times = [];
  for (let i = 0; i < 5; i++) {
    const start = process.hrtime.bigint();
    await run();
    const end = process.hrtime.bigint();
    times.push(Number(end - start) / 1e6); // ms
  }
  // record worst-case (maximum) of measured runs
  const ms = Math.max(...times);
  const db = await getDb();
  await db.collection('runMetrics').insertOne({
    ts: new Date(),
    datasetVersion: 'v1',
    queryId,
    variant,
    indexSet,
    pipelineHash,
    ms,
    resultSample,
  });
  return ms;
}
