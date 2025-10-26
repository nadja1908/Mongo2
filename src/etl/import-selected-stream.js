import fs from 'fs';
import { parse } from 'csv-parse';
import { getDb, closeDb } from '../lib/db.js';

const FILES = [
  { path: 'c:\\Users\\djord\\Downloads\\archive\\themes.csv', col: 'themes_raw' },
  { path: 'c:\\Users\\djord\\Downloads\\archive\\subcategories.csv', col: 'subcategories_raw' },
  { path: 'c:\\Users\\djord\\Downloads\\archive\\ratings_distribution.csv', col: 'ratings_distribution_raw' },
  { path: 'c:\\Users\\djord\\Downloads\\archive\\user_ratings.csv', col: 'user_ratings_raw' },
  { path: 'c:\\Users\\djord\\Downloads\\archive\\artists_reduced.csv', col: 'artists_raw' }
];

const BATCH = parseInt(process.env.IMPORT_BATCH || '5000', 10);

async function importFile(filePath, collectionName) {
  const db = await getDb();
  const col = db.collection(collectionName);
  if (!fs.existsSync(filePath)) {
    console.log(`File not found: ${filePath} — skipping.`);
    return;
  }
  console.log(`Importing ${filePath} -> ${collectionName}`);
  await col.drop().catch(() => {});

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    const parser = parse({ columns: false, skip_empty_lines: true, relax_quotes: true });
    let first = true;
    let batch = [];
    let inserted = 0;

    async function flush() {
      if (!batch.length) return;
      const toInsert = batch;
      batch = [];
      try {
        await col.insertMany(toInsert, { ordered: false });
        inserted += toInsert.length;
        console.log(`Inserted ${inserted} into ${collectionName}`);
      } catch (err) {
        console.error('Insert error:', err && err.stack ? err.stack : err);
        throw err;
      }
    }

    parser.on('data', (rec) => {
      try {
        if (first) { first = false; return; }
        const raw = Array.isArray(rec) ? rec.join(',') : String(rec);
        const names = raw.split(',').map(s => s.trim()).filter(Boolean);
        const doc = { raw, names, _importedAt: new Date() };
        batch.push(doc);
        if (batch.length >= BATCH) {
          parser.pause && parser.pause();
          flush().then(() => parser.resume && parser.resume()).catch(err => reject(err));
        }
      } catch (err) { reject(err); }
    });

    parser.on('error', err => reject(err));

    parser.on('end', () => {
      flush().then(() => resolve()).catch(err => reject(err));
    });

    stream.on('error', err => reject(err));
    stream.pipe(parser);
  });
}

async function main() {
  for (const f of FILES) {
    try {
      await importFile(f.path, f.col);
    } catch (e) {
      console.error(`Failed to import ${f.path} -> ${f.col}:`, e && e.stack ? e.stack : e);
    }
  }
  await closeDb();
  console.log('Selected imports finished');
}

main().catch(async (e) => {
  console.error('Import-selected error:', e && e.stack ? e.stack : e);
  await closeDb();
  process.exit(1);
});
