import { resolveOrigins } from '../pipelines/resolveOrigin.js';

(async () => {
  console.log('=== BioLens: Resolve Origins ===');
  console.log(`Started: ${new Date().toISOString()}`);
  try {
    await resolveOrigins();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }
  console.log(`Finished: ${new Date().toISOString()}`);
})();
