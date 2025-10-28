import path from 'path';
import { pathToFileURL } from 'url';
import { getDb, closeDb } from '../lib/db.js';

// Usage:
//   node src/run/run-query.js src/queries/Q1.optimized.js
// or
//   node src/run/run-query.js Q1.optimized

const arg = process.argv[2];
if (!arg) {
  console.error('Usage: node src/run/run-query.js <query-module-path|query-name>');
  process.exit(1);
}

let modulePath = arg;
// If the user passed a short query name (like Q1.optimized), resolve to src/queries/<name>.js
if (!modulePath.startsWith('.') && !modulePath.startsWith('/') && !modulePath.includes('\\')) {
  modulePath = path.join('src', 'queries', modulePath.endsWith('.js') ? modulePath : `${modulePath}.js`);
}

const absPath = path.resolve(modulePath);
const fileUrl = pathToFileURL(absPath).href;

(async () => {
  try {
    const mod = await import(fileUrl);
    if (typeof mod.run !== 'function') {
      console.error(`Module ${modulePath} does not export an async run(db) function`);
      process.exit(2);
    }

    const db = await getDb();
    try {
      const result = await mod.run(db);
      // Pretty print JSON result to terminal
      console.log(JSON.stringify(result, null, 2));
    } finally {
      await closeDb();
    }
  } catch (err) {
    console.error('Error running query:', err);
    process.exit(3);
  }
})();
