/**
 * src/etl/build-designer-publisher-stats.js
 *
 * Placeholder ETL: aggregate designer-publisher pairs into `designer_publisher_stats`.
 * This script is intentionally minimal; extend the grouping keys/filters for your dataset.
 */
import { getDb, closeDb } from '../lib/db.js';

async function build() {
  const db = await getDb();
  const games = db.collection('games');
  const out = db.collection('designer_publisher_stats');

  console.log('Building designer_publisher_stats (placeholder)...');
  // Improved ETL: preload designers_raw and publishers_raw into maps keyed by game id(s)
  // then iterate `games` and build pair stats in-memory, finally bulk write to `designer_publisher_stats`.
  console.log('  Preloading designers_raw and publishers_raw into maps...');
  const designersCol = db.collection('designers_raw');
  const publishersCol = db.collection('publishers_raw');
  const designersMap = new Map();
  const publishersMap = new Map();

  try {
    await designersCol.find().forEach(d => {
      const keys = [d.gameId, d.BGGId, d.id, d._id].filter(Boolean).map(String);
      const name = d.name || d.Name || d.Designer || d.designer || '';
      for (const k of keys) {
        const arr = designersMap.get(k) || [];
        if (name) arr.push(String(name));
        designersMap.set(k, arr);
      }
    });
  } catch (e) {
    // ignore if not present
  }
  try {
    await publishersCol.find().forEach(p => {
      const keys = [p.gameId, p.BGGId, p.id, p._id].filter(Boolean).map(String);
      const name = p.name || p.Name || p.Publisher || p.publisher || '';
      for (const k of keys) {
        const arr = publishersMap.get(k) || [];
        if (name) arr.push(String(name));
        publishersMap.set(k, arr);
      }
    });
  } catch (e) {
    // ignore if not present
  }

  console.log('  Scanning games to compute designer-publisher pair stats...');
  const cursor = games.find().batchSize(1000);
  const pairStats = new Map();
  let seen = 0;
  while (await cursor.hasNext()) {
    const g = await cursor.next();
    // try multiple ids to find matching designers/publishers
    const gidCandidates = [g._id, g._id && String(g._id), g.BGGId, g.bggId, g.gameId].filter(Boolean).map(String);
    // pick designers from doc or map
    let designers = Array.isArray(g.designers) && g.designers.length ? g.designers : null;
    let publishers = Array.isArray(g.publishers) && g.publishers.length ? g.publishers : null;
    if (!designers) {
      for (const k of gidCandidates) { if (designersMap.get(k)) { designers = designersMap.get(k); break; } }
    }
    if (!publishers) {
      for (const k of gidCandidates) { if (publishersMap.get(k)) { publishers = publishersMap.get(k); break; } }
    }
    if (!designers || !publishers) continue; // need both to build pair

    const numRatings = (g.popularity && g.popularity.numRatings) ? Number(g.popularity.numRatings) : 0;
    const avg = (g.avgRating != null) ? Number(g.avgRating) : ((g.bayesAvg != null) ? Number(g.bayesAvg) : null);

    for (const d of designers) {
      for (const p of publishers) {
        const key = `${d}||${p}`;
        const s = pairStats.get(key) || { _id: { designer: d, publisher: p }, gamesCount: 0, sumNumRatings: 0, avgSum: 0, avgCount: 0 };
        s.gamesCount += 1;
        s.sumNumRatings += Number.isFinite(numRatings) ? numRatings : 0;
        if (avg != null && Number.isFinite(avg)) { s.avgSum += avg; s.avgCount += 1; }
        pairStats.set(key, s);
      }
    }
    seen += 1;
    if (seen % 5000 === 0) console.log(`  scanned ${seen} games, pairs so far: ${pairStats.size}`);
  }

  // Bulk write results
  console.log(`  Writing ${pairStats.size} pair stats to designer_publisher_stats...`);
  const bulk = [];
  for (const [pair, s] of pairStats.entries()) {
    const doc = { _id: s._id, pair, gamesCount: s.gamesCount, sumNumRatings: s.sumNumRatings, avgRating: (s.avgCount ? s.avgSum / s.avgCount : null) };
    bulk.push({ replaceOne: { filter: { pair }, replacement: doc, upsert: true } });
    if (bulk.length >= 500) {
      await out.bulkWrite(bulk, { ordered: false });
      bulk.length = 0;
    }
  }
  if (bulk.length) await out.bulkWrite(bulk, { ordered: false });
  const total = await out.countDocuments();
  console.log(`designer_publisher_stats built. total pairs: ${total}`);
}

build()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('designer_publisher_stats error:', err);
    await closeDb();
    process.exit(1);
  });
