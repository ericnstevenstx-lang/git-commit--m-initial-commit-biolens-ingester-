import { classifyMaterials } from '../pipelines/classifyHTS';

(async () => {
  console.log('=== BioLens: Classify HTS Codes ===');
  console.log(`Started: ${new Date().toISOString()}`);
  try {
    await classifyMaterials();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }
  console.log(`Finished: ${new Date().toISOString()}`);
})();
