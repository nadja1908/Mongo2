import fs from 'fs';
import { parse } from 'csv-parse';
import { getDb, closeDb } from '../lib/db.js';

async function importSimple(filePath, colName) {
  const db = await getDb();
  const col = db.collection(colName);
  console.log(`Importing ${filePath} -> ${colName}`);
  // drop existing
  await col.drop().catch(() => {});
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    const parser = parse({ columns: false, skip_empty_lines: true, relax_quotes: true });
    let first = true;
    let inserted = 0;
    let batch = [];
    const BATCH = parseInt(process.env.IMPORT_BATCH || '1000', 10);

    async function flushBatch() {
      if (!batch.length) return;
      const toInsert = batch;
      batch = [];
      try {
        const res = await col.insertMany(toInsert, { ordered: false });
        inserted += toInsert.length;
        console.log(`Inserted ${inserted} into ${colName}`);
      } catch (err) {
        console.error('Error on insertMany:', err && err.stack ? err.stack : err);
        // rethrow to stop the import
        throw err;
      }
    }

    parser.on('data', (rec) => {
      try {
        if (first) { first = false; return; }
        const raw = Array.isArray(rec) ? rec.join(',') : String(rec);
        const names = raw.split(',').map(s => s.trim()).filter(Boolean);
        const doc = { raw: raw, names, _importedAt: new Date() };
        batch.push(doc);
        if (batch.length >= BATCH) {
          parser.pause && parser.pause();
          flushBatch().then(() => parser.resume && parser.resume()).catch(err => reject(err));
        }
      } catch (err) {
        reject(err);
      }
    });

    parser.on('error', err => reject(err));

    parser.on('end', () => {
      // final flush
      flushBatch().then(() => resolve()).catch(err => reject(err));
    });

    // ensure stream errors are propagated
    stream.on('error', err => reject(err));

    stream.pipe(parser);
  }).finally(async () => await closeDb());
}

async function main() {
  try {
    await importSimple('c:\\\\Users\\\\djord\\\\Downloads\\\\archive\\\\designers_reduced.csv','designers_raw');
    await importSimple('c:\\\\Users\\\\djord\\\\Downloads\\\\archive\\\\publishers_reduced.csv','publishers_raw');
    console.log('Designers/Publishers import finished');
  } catch (e) {
    console.error('Import error:', e.message || e);
    process.exit(1);
  }
}

main();
