/**
 * src/indexes/create-optimized-indexes.js
 *
 * Creates the optimized index set described in the specification.
 * Each index includes a comment indicating which query uses it and which pipeline phase
 * (match/sort/group) benefits from the index.
 */
import { getDb, closeDb } from '../lib/db.js';

async function create() {
  const db = await getDb();
  const games = db.collection('games');

  console.log('Creating optimized indexes...');

  // Indeks { avgRating:-1, mechanics:1 } koristi se u Q1 (baseline/optimized).
  // Upit: Q1 (Najpopularnije mehanike) — faze: $match (avgRating>8) i zatim $unwind/$group over mechanics.
  // Motivacija: omogućava selektivno čitanje dokumenata sa visokim avgRating i pristup multikey polju mechanics bez skeniranja cele kolekcije.
  await games.createIndex({ avgRating: -1, mechanics: 1 }, { name: 'idx-q1-mech' });

  // Indeks { themes:1, avgRating:-1 }
  // Upit: Q2 (Igre sa najviše tema) — faze: $project/$sort; koristi se kad ostajemo u `games` kolekciji.
  // Motivacija: prefix index po themes (multikey) sa sekundarnim sort poljem avgRating pomaže pri sortiranju i redukuje IO.
  await games.createIndex({ themes: 1, avgRating: -1 }, { name: 'idx-q2-themes' });

  // Indeks { designers:1, publishers:1 }
  // NOTE: MongoDB ne dozvoljava indeksiranje paralelnih nizova u jednom indeksu
  // (npr. { designers:1, publishers:1 } kada su oba polja arrays). Zato kreiramo
  // odvojene indekse za designers i publishers.
  // Upit: Q3 (dizajner–izdavač parovi) — faza: $match/$group; za najbolje performanse koristimo
  // prekompjutovanu kolekciju `designer_publisher_stats`. Ako ostajemo u `games`, pojedinačni
  // indeksi po dizajneru/izdavaču pomažu pri filtriranju pre grupisanja.
  await games.createIndex({ designers: 1 }, { name: 'idx-q3-designers' });
  await games.createIndex({ publishers: 1 }, { name: 'idx-q3-publishers' });

  // Indeks { year:1, avgRating:-1, "popularity.numOwned": -1 }
  // Upit: Q4 (prosečne ocene po godini) — faze: $match (year) i $sort (avgRating / popularity)
  // Motivacija: prefix na year omogućava range/group by year, a sort prefix na avgRating i popularity pomaže pri najboljim igrama po godini.
  await games.createIndex({ year: 1, avgRating: -1, 'popularity.numOwned': -1 }, { name: 'idx-q4-year' });

  // Indeks { bayesAvg: -1 }
  // Upit: Q5 (Quality vs Popularity) — faza: global sort po bayesAvg kada gradimo rangove.
  // Motivacija: omogućava brže rankiranje prema kvalitetu (bayesAvg).
  await games.createIndex({ bayesAvg: -1 }, { name: 'idx-q5-bayes' });

  // Indeks { "popularity.numOwned": -1 }
  // Upit: Q5 (Popularity rank) — faza: sort po popularity.numOwned.
  // Motivacija: omogućava brzi top-N po broju vlasnika.
  await games.createIndex({ 'popularity.numOwned': -1 }, { name: 'idx-q5-popularity' });

  // Additional indexes on helper collections to speed up optimized queries that read them
  const mechStats = db.collection('mechanic_stats');
  await mechStats.createIndex({ avgAvgRating: -1 }, { name: 'idx-q1-mechstats-avg' });
  // also index by gamesCount because Q1.optimized sorts by gamesCount
  await mechStats.createIndex({ gamesCount: -1 }, { name: 'idx-q1-mechstats-count' });

  const dpStats = db.collection('designer_publisher_stats');
  // index by gamesCount and sumNumRatings to allow fast top pairs retrieval
  await dpStats.createIndex({ gamesCount: -1, sumNumRatings: -1 }, { name: 'idx-q3-dpstats' });

  const themeRank = db.collection('theme_count_rank');
  // speed up Q2 when reading precomputed theme counts
  await themeRank.createIndex({ themesCount: -1, avgRating: -1 }, { name: 'idx-q2-theme-rank' });

  const rankCache = db.collection('rank_cache');
  // speed up Q5 when reading cached ranks
  await rankCache.createIndex({ rankQuality: 1 }, { name: 'idx-q5-rankquality' });
  await rankCache.createIndex({ rankPopularity: 1 }, { name: 'idx-q5-rankpopularity' });

  console.log('Optimized indexes created.');
}

create()
  .then(() => closeDb())
  .catch(async (err) => {
    console.error('create-optimized-indexes error:', err);
    await closeDb();
    process.exit(1);
  });
