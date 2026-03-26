import { generateStandardComparisons } from '../pipelines/computeTariff';

(async () => {
  console.log('=== BioLens: Compute Tariff Comparisons ===');
  console.log(`Started: ${new Date().toISOString()}`);
  try {
    await generateStandardComparisons();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }
  console.log(`Finished: ${new Date().toISOString()}`);
})();
