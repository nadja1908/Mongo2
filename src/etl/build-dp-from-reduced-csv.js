/**
 * Build designer_publisher_stats from pivoted `designers_reduced.csv` and `publishers_reduced.csv`.
 *
 * Assumes each reduced CSV has header: id,<name1>,<name2>,..., and rows with 0/1 flags.
 * The script streams both files, builds maps gameId -> [names], then computes pair counts.
 * Set ARCHIVE_DIR env var if your CSVs live elsewhere. Defaults to C:\\Users\\djord\\Downloads\\archive
 */
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse';
import { getDb, closeDb } from '../lib/db.js';

const ARCHIVE_DIR = process.env.ARCHIVE_DIR || 'C:\\Users\\djord\\Downloads\\archive';
const DESIGNERS_FILE = process.env.DESIGNERS_FILE || 'designers_reduced.csv';
const PUBLISHERS_FILE = process.env.PUBLISHERS_FILE || 'publishers_reduced.csv';

function streamParse(filePath) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    const parser = parse({ relax_column_count: true });
    const rows = [];
    parser.on('readable', () => {
      let r;
      while ((r = parser.read())) rows.push(r);
    });
    parser.on('error', reject);
    parser.on('end', () => resolve(rows));
    stream.pipe(parser);
  });
}

async function build() {
  const designersPath = path.join(ARCHIVE_DIR, DESIGNERS_FILE);
  const publishersPath = path.join(ARCHIVE_DIR, PUBLISHERS_FILE);
  if (!fs.existsSync(designersPath) || !fs.existsSync(publishersPath)) {
    console.error('Reduced CSVs not found at', designersPath, 'or', publishersPath);
    process.exit(1);
  }
  console.log('Parsing designers reduced CSV (this may take a minute)...');
  const dRows = await streamParse(designersPath);
  console.log('Parsing publishers reduced CSV...');
  const pRows = await streamParse(publishersPath);

  if (dRows.length < 2 || pRows.length < 2) {
    console.error('CSV parsing failed or empty files');
    process.exit(1);
  }

  const dHeader = dRows[0].map(s => (s || '').toString().trim());
  const pHeader = pRows[0].map(s => (s || '').toString().trim());

  const designersMap = new Map();
  for (let r = 1; r < dRows.length; r++) {
    const row = dRows[r];
    const id = (row[0] || '').toString().trim();
    if (!id) continue;
    const names = [];
    for (let i = 1; i < dHeader.length && i < row.length; i++) {
      if ((row[i] || '').toString().trim() === '1') names.push(dHeader[i]);
    }
    if (names.length) designersMap.set(id, names);
  }

  const publishersMap = new Map();
  for (let r = 1; r < pRows.length; r++) {
    const row = pRows[r];
    const id = (row[0] || '').toString().trim();
    if (!id) continue;
    const names = [];
    for (let i = 1; i < pHeader.length && i < row.length; i++) {
      if ((row[i] || '').toString().trim() === '1') names.push(pHeader[i]);
    }
    if (names.length) publishersMap.set(id, names);
  }

  console.log(`Found designers for ${designersMap.size} ids, publishers for ${publishersMap.size} ids`);

  // Build pair stats across intersection of ids
  const pairStats = new Map();
  let seen = 0;
  for (const [id, dList] of designersMap.entries()) {
    const pList = publishersMap.get(id);
    if (!pList) continue;
    for (const d of dList) {
      for (const p of pList) {
        const key = `${d}||${p}`;
        const s = pairStats.get(key) || { _id: { designer: d, publisher: p }, gamesCount: 0 };
        s.gamesCount += 1;
        pairStats.set(key, s);
      }
    }
    seen += 1;
    if (seen % 5000 === 0) console.log(`  processed ${seen} ids`);
  }

  console.log(`Computed ${pairStats.size} pairs; writing to DB...`);
  const db = await getDb();
  try {
    const out = db.collection('designer_publisher_stats');
    const bulk = [];
    for (const [pair, s] of pairStats.entries()) {
      const doc = { _id: s._id, pair, gamesCount: s.gamesCount };
      bulk.push({ replaceOne: { filter: { pair }, replacement: doc, upsert: true } });
      if (bulk.length >= 500) { await out.bulkWrite(bulk, { ordered: false }); bulk.length = 0; }
    }
    if (bulk.length) await out.bulkWrite(bulk, { ordered: false });
    console.log('designer_publisher_stats written; total:', await out.countDocuments());
  } finally {
    await closeDb();
  }
}

build().catch(err => { console.error(err); process.exit(1); });
