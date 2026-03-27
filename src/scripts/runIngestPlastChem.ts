import { ingestPlastChem } from '../pipelines/ingestPlastChem.js';

(async () => {
  console.log('=== BioLens: Ingest PlastChem Database ===');
  console.log(`Started: ${new Date().toISOString()}`);
  const dbPath = process.argv[2] || process.env.PLASTCHEM_DB_PATH || undefined;
  if (dbPath) console.log(`XLSX path: ${dbPath}`);
  try {
    await ingestPlastChem(dbPath);
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }
  console.log(`Finished: ${new Date().toISOString()}`);
})();
