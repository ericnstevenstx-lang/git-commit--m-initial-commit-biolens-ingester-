import { ingestByCategories } from '../pipelines/ingestOpenFoodFacts';

(async () => {
  console.log('=== BioLens: Ingest Open Food Facts ===');
  console.log(`Started: ${new Date().toISOString()}`);
  try {
    await ingestByCategories();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }
  console.log(`Finished: ${new Date().toISOString()}`);
})();
