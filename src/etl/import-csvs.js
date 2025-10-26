/**
 * src/etl/import-csvs.js
 *
 * Simple CSV import helper that reads CSV files and writes them into *_raw collections.
 * Usage: set the `CSV_FILES` mapping below to point to your local CSV paths and run:
 *
 *   node src/etl/import-csvs.js
 *
 * Note: this is intentionally minimal and robust for medium-sized files. For very large CSVs
 * consider using mongoimport or a streaming approach.
 */
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import { getDb, closeDb } from '../lib/db.js';

// Map of local CSV paths -> target collection names (modify to your environment)
const CSV_FILES = {
  // Example paths — update these to your actual files if you want to import them.
  'c:\\Users\\djord\\Downloads\\archive\\games.csv': 'games_raw',
  'c:\\Users\\djord\\Downloads\\archive\\mechanics.csv': 'mechanics_raw',
  'c:\\Users\\djord\\Downloads\\archive\\themes.csv': 'themes_raw',
  'c:\\Users\\djord\\Downloads\\archive\\subcategories.csv': 'subcategories_raw',
  'c:\\Users\\djord\\Downloads\\archive\\user_ratings.csv': 'user_ratings_raw',
  'c:\\Users\\djord\\Downloads\\archive\\ratings_distribution.csv': 'ratings_distribution_raw',
  'c:\\Users\\djord\\Downloads\\archive\\designers_reduced.csv': 'designers_raw',
  'c:\\Users\\djord\\Downloads\\archive\\publishers_reduced.csv': 'publishers_raw',
  // artists file had a typo in your message; adjust path if present:
  'c:\\Users\\djord\\Downloads\\archive\\artists_reduced.csv': 'artists_raw'
};

async function importFile(filePath, collectionName) {
  const db = await getDb();
  const col = db.collection(collectionName);
  console.log(`Importing ${filePath} -> ${collectionName}`);
  const content = fs.readFileSync(filePath, 'utf8');
  const records = parse(content, {
    columns: true,
    skip_empty_lines: true,
  });
  if (!records.length) {
    console.log(`No records in ${filePath}`);
    return;
  }
  // Insert in batches to avoid memory/packet limits.
  
  const batchSize = 1000;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize).map(r => {
      // Convert empty strings to nulls and numbers where obvious
      Object.keys(r).forEach(k => {
        if (r[k] === '') r[k] = null;
        else if (!isNaN(r[k]) && r[k] !== null) {
          const n = Number(r[k]);
          r[k] = Number.isFinite(n) ? n : r[k];
        }
      });
      return r;
    });
    await col.insertMany(batch, { ordered: false });
    console.log(`Inserted ${Math.min(i + batchSize, records.length)} / ${records.length} into ${collectionName}`);
  }
}

async function main() {
  for (const [p, col] of Object.entries(CSV_FILES)) {
    try {
      if (!fs.existsSync(p)) {
        console.log(`File not found: ${p} — skipping.`);
        continue;
      }
      await importFile(p, col);
    } catch (e) {
      console.error(`Error importing ${p}:`, e.message);
    }
  }
  await closeDb();
  console.log('CSV import complete.');
}

main().catch(async (err) => {
  console.error('Import error:', err);
  await closeDb();
  process.exit(1);
});
