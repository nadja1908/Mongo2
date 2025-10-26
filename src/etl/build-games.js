/**
 * src/etl/build-games.js
 *
 * ETL script that reads from `games_raw` (and optionally other *_raw collections)
 * and writes a denormalized `games` collection. The output document is tuned for
 * analytics/reading: arrays for mechanics, themes, subcategories, designers, artists, publishers,
 * plus popularity, ranks and ratingsDistribution.
 *
 * Notes / assumptions:
 * - The repository includes raw collections (imported from BSON/CSV):
 *   `games_raw`, `mechanics_raw`, `themes_raw`, `subcategories_raw`, `designers_raw` (or reduced),
 *   `publishers_raw`, `ratings_distribution_raw`, `user_ratings_raw`.
 * - Raw schemas vary across dumps. This script attempts to map common fields and
 *   falls back to copying fields where detection is ambiguous. If your `games_raw`
 *   contains different field names, update the field-mapping section below.
 *
 * Behavior:
 *  - For each document in `games_raw`, construct a compact `games` document and upsert
 *    into `games` with `_id` set to the BGG id (if present) or original `_id`.
 *  - If a `ratings_distribution_raw` collection exists, attach ratingsDistribution and total.
 *  - Log counts written/updated.
 *
 * Comments explaining choices:
 *  - Denormalization: storing arrays of mechanics/themes/designers inside `games` avoids runtime joins
 *    and $lookup/$unwind costs in many analytical queries.
 *  - We keep `ratingsDistribution` as an embedded doc to allow per-game percentile computations.
 */
import { getDb, closeDb } from '../lib/db.js';

function mapNumber(v, fallback = null) {
  if (v === undefined || v === null) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

async function build() {
  const db = await getDb();
  const src = db.collection('games_raw');
  const rd = db.collection('ratings_distribution_raw');
  const designersCol = db.collection('designers_raw');
  const publishersCol = db.collection('publishers_raw');
  const out = db.collection('games');

  // Preload helper collections into memory maps to avoid per-document queries
  console.log('Preloading helper collections (ratings/designers/publishers) ...');
  const rdMap = new Map();
  try {
    const rdDocs = await rd.find().toArray();
    for (const r of rdDocs) {
      const keys = [r.gameId, r.BGGId, r.id, r._id].filter(Boolean).map(String);
      for (const k of keys) rdMap.set(k, r);
    }
    console.log(`  ratings_distribution entries loaded: ${rdMap.size}`);
  } catch (e) {
    console.log('  ratings_distribution_raw not present or failed to load — continuing');
  }

  const designersMap = new Map();
  try {
    await designersCol.find().forEach(d => {
      const name = d.name || d.Name || d.Designer || d.DesignerName || d.designer || '';
      const keys = [d.BGGId, d.gameId, d.id, d._id].filter(Boolean).map(String);
      for (const k of keys) {
        const arr = designersMap.get(k) || [];
        if (name) arr.push(String(name));
        designersMap.set(k, arr);
      }
    });
    console.log(`  designers entries grouped: ${designersMap.size}`);
  } catch (e) {
    console.log('  designers_raw not present or failed to load — continuing');
  }

  const publishersMap = new Map();
  try {
    await publishersCol.find().forEach(p => {
      const name = p.name || p.Name || p.Publisher || p.PublisherName || p.publisher || '';
      const keys = [p.BGGId, p.gameId, p.id, p._id].filter(Boolean).map(String);
      for (const k of keys) {
        const arr = publishersMap.get(k) || [];
        if (name) arr.push(String(name));
        publishersMap.set(k, arr);
      }
    });
    console.log(`  publishers entries grouped: ${publishersMap.size}`);
  } catch (e) {
    console.log('  publishers_raw not present or failed to load — continuing');
  }

  console.log('Starting build-games ETL...');
  const cursor = src.find().batchSize(1000);
  let written = 0;
  let bulkOps = [];
  let processed = 0;
  while (await cursor.hasNext()) {
    const doc = await cursor.next();
    // Try common field names across different dumps.
  // Support common alternate capitalizations from various CSV/BSON dumps
  const bggId = doc.bggId || doc.gameId || doc.id || doc.BGGId || doc.BGGID || doc._id;
  const name = doc.name || doc.title || doc.primaryName || doc.Name || doc.Title || '';
  const year = mapNumber(doc.yearPublished || doc.YearPublished || doc.year || doc.yearPublishedInt || doc.Year);
  const avgRating = mapNumber(doc.average || doc.avgRating || doc.AvgRating || doc.rating || doc.avg || doc.Avg);
  const bayesAvg = mapNumber(doc.bayesAverage || doc.bayesAvg || doc.bayesianAverage || doc.BayesAvgRating || doc.BayesAvg || doc.BayesAvgRating);
  const stdDev = mapNumber(doc.stdDev || doc.stdDeviation || doc.sd || doc.StdDev);

    // Popularity - multiple possible shapes in raw data.
    
    const popularity = {
      numOwned: mapNumber(doc.numOwned || doc.NumOwned || doc.popularity?.numOwned || doc.owned || doc.ownedCount, 0),
      numWants: mapNumber(doc.want || doc.wants || doc.wishlist || doc.numWant || doc.NumWant || doc.NumWish, 0),
      numRatings: mapNumber(doc.numRatings || doc.NumUserRatings || doc.NumRatings || doc.usersRated || doc.votes || doc.numVotes, 0),
    };

    // Ranks - keep as-is if present, else try to extract common fields.
    const ranks = doc.ranks || doc.rank || {};

    // Common array fields - prefer arrays if present; otherwise try to detect binary column encodings.
    
  const mechanics = Array.isArray(doc.mechanics) ? doc.mechanics : (Array.isArray(doc.Mechanics) ? doc.Mechanics : (doc.mechanicList || doc.mechanicsList || []));
  const themes = Array.isArray(doc.themes) ? doc.themes : (Array.isArray(doc.Themes) ? doc.Themes : (doc.themeList || doc.themesList || []));
  const subcategories = Array.isArray(doc.subcategories) ? doc.subcategories : (Array.isArray(doc.Subcategories) ? doc.Subcategories : (doc.subcategoryList || []));
  const designers = Array.isArray(doc.designers) ? doc.designers : (Array.isArray(doc.Designers) ? doc.Designers : (doc.designerList || []));
  const artists = Array.isArray(doc.artists) ? doc.artists : (Array.isArray(doc.Artists) ? doc.Artists : (doc.artistList || []));
  const publishers = Array.isArray(doc.publishers) ? doc.publishers : (Array.isArray(doc.Publishers) ? doc.Publishers : (doc.publisherList || []));

    // Fallback: detect binary flags columns (very dataset-specific). We'll check for keys with 0/1 values
    // and, if any are found and none of the arrays above are present, build arrays from those keys.
    function detectBinaryFieldsAsArray(sourceObj) {
      const candidateKeys = Object.keys(sourceObj || {});
      const values = candidateKeys.map(k => sourceObj[k]);
      const binaryCount = values.filter(v => v === 0 || v === 1).length;
      if (binaryCount > Math.max(5, candidateKeys.length * 0.1)) {
        // Heuristic: many binary columns => treat keys with value 1 as items.
        return candidateKeys.filter(k => sourceObj[k] === 1);
      }
      return null;
    }

    // If arrays empty, try to detect in the doc itself.
    
    const maybeMechanics = (mechanics && mechanics.length) ? mechanics : detectBinaryFieldsAsArray(doc);
    const finalMechanics = Array.isArray(maybeMechanics) ? maybeMechanics : [];

    // ratingsDistribution - lookup from preloaded map to avoid per-doc queries
    let ratingsDistribution = null;
    let ratingsTotal = null;
    try {
      const rdDoc = rdMap.get(String(bggId));
      if (rdDoc) {
        ratingsDistribution = rdDoc.distribution || rdDoc.ratingsDistribution || rdDoc.dist || rdDoc;
        if (ratingsDistribution && typeof ratingsDistribution === 'object') {
          const vals = Object.values(ratingsDistribution).map(v => Number(v)).filter(n => Number.isFinite(n));
          if (vals.length > 0) ratingsTotal = vals.reduce((a, b) => a + b, 0);
        }
      }
    } catch (e) {
      // ignore
    }

    const newDoc = {
      _id: bggId,
      name,
      year,
      avgRating,
      bayesAvg,
      stdDev,
      popularity,
      ranks,
      mechanics: finalMechanics,
      themes,
      subcategories,
      // enrich designers/publishers from preloaded maps if arrays missing
      designers: (designers && designers.length) ? designers : (designersMap.get(String(bggId)) || []),
      artists,
      publishers: (publishers && publishers.length) ? publishers : (publishersMap.get(String(bggId)) || []),
      ratingsDistribution,
      ratingsTotal,
      // keep raw for debugging reference (optional, can be removed to save space)
      _raw: { sourceId: doc._id }
    };

    processed += 1;
    bulkOps.push({ replaceOne: { filter: { _id: newDoc._id }, replacement: newDoc, upsert: true } });
    if (bulkOps.length >= 1000) {
      const res = await out.bulkWrite(bulkOps, { ordered: false });
      written += (res.upsertedCount || 0) + (res.modifiedCount || 0);
      bulkOps = [];
      if (processed % 5000 === 0) console.log(`  processed ${processed} rows, written ${written}`);
    }
  }
  if (bulkOps.length) {
    const res = await out.bulkWrite(bulkOps, { ordered: false });
    written += (res.upsertedCount || 0) + (res.modifiedCount || 0);
  }

  console.log(`ETL complete. Documents upserted/modified: ${written}`);
}

build()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('ETL error:', err);
    await closeDb();
    process.exit(1);
  });
