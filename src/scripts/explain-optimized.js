import { getDb, closeDb } from '../lib/db.js';

async function explain() {
  const db = await getDb();
  try {
    console.log('Running explain for Q1 (optimized pipeline on `games`)');
    const q1 = [
      { $match: { avgRating: { $gt: 8 } } },
      { $unwind: '$mechanics' },
      { $group: { _id: '$mechanics', gamesCount: { $sum: 1 }, avgAvgRating: { $avg: '$avgRating' } } },
      { $sort: { gamesCount: -1 } }
    ];
  const q1Explain = await db.collection('games').aggregate(q1).explain('executionStats');
  console.log('Q1 explain (full):');
  console.log(JSON.stringify(q1Explain, null, 2).slice(0, 2000));

    console.log('\nRunning explain for Q2 (optimized pipeline on `games`)');
    const q2 = [
      { $project: { name: 1, themesCount: { $size: { $ifNull: ['$themes', []] } }, avgRating: 1, 'popularity.numOwned': 1 } },
      { $sort: { themesCount: -1, avgRating: -1 } },
      { $limit: 5 }
    ];
    const q2Explain = await db.collection('games').aggregate(q2, { allowDiskUse: true }).explain('executionStats');
    console.log('Q2 explain summary:');
    console.log({ executionTimeMillis: q2Explain.executionStats?.executionTimeMillis, totalDocsExamined: q2Explain.executionStats?.totalDocsExamined, totalKeysExamined: q2Explain.executionStats?.totalKeysExamined });

    console.log('\nRunning explain for Q3 (optimized pipeline on `games`)');
    const q3 = [
      { $unwind: '$designers' },
      { $unwind: '$publishers' },
      { $group: { _id: { designer: '$designers', publisher: '$publishers' }, gamesCount: { $sum: 1 }, sumNumRatings: { $sum: '$popularity.numRatings' }, avgRating: { $avg: '$avgRating' } } },
      { $match: { sumNumRatings: { $gte: 500 } } },
      { $sort: { gamesCount: -1 } },
      { $limit: 20 }
    ];
  const q3Explain = await db.collection('games').aggregate(q3, { allowDiskUse: true }).explain('executionStats');
  console.log('Q3 explain (full):');
  console.log(JSON.stringify(q3Explain, null, 2).slice(0, 2000));

    console.log('\nRunning explain for Q4 (optimized pipeline on `games`)');
    const q4 = [
      { $match: { year: { $ne: null } } },
      { $sort: { year: 1, avgRating: -1, 'popularity.numOwned': -1 } },
      { $group: { _id: '$year', bestGame: { $first: { _id: '$_id', name: '$name', avgRating: '$avgRating' } } } },
      { $sort: { _id: 1 } }
    ];
    const q4Explain = await db.collection('games').aggregate(q4, { allowDiskUse: true }).explain('executionStats');
    console.log('Q4 explain summary:');
    console.log({ executionTimeMillis: q4Explain.executionStats?.executionTimeMillis, totalDocsExamined: q4Explain.executionStats?.totalDocsExamined, totalKeysExamined: q4Explain.executionStats?.totalKeysExamined });

    console.log('\nRunning explain for Q5 (optimized: top by bayesAvg and by popularity) - find().sort() explains');
    const q5aExplain = await db.collection('games').find({}, { projection: { name: 1, bayesAvg: 1 } }).sort({ bayesAvg: -1 }).limit(100).explain('executionStats');
    console.log('Q5 (bayesAvg) explain summary:');
    console.log({ executionTimeMillis: q5aExplain.executionStats?.executionTimeMillis, totalDocsExamined: q5aExplain.executionStats?.totalDocsExamined, totalKeysExamined: q5aExplain.executionStats?.totalKeysExamined, winningPlan: q5aExplain.queryPlanner?.winningPlan?.inputStage?.stage || q5aExplain.queryPlanner?.winningPlan?.stage });
    const q5bExplain = await db.collection('games').find({}, { projection: { name: 1, 'popularity.numOwned': 1 } }).sort({ 'popularity.numOwned': -1 }).limit(100).explain('executionStats');
    console.log('Q5 (popularity) explain summary:');
    console.log({ executionTimeMillis: q5bExplain.executionStats?.executionTimeMillis, totalDocsExamined: q5bExplain.executionStats?.totalDocsExamined, totalKeysExamined: q5bExplain.executionStats?.totalKeysExamined, winningPlan: q5bExplain.queryPlanner?.winningPlan?.inputStage?.stage || q5bExplain.queryPlanner?.winningPlan?.stage });

  } finally {
    await closeDb();
  }
}

explain().catch(err => { console.error(err); process.exit(1); });
