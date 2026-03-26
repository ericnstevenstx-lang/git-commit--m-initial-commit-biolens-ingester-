import { enrichChemicals } from '../pipelines/enrichChemicals.js';

(async () => {
  console.log('=== BioLens: Enrich Chemicals (CPDat + PubChem) ===');
  console.log(`Started: ${new Date().toISOString()}`);
  console.log(
    `Mode: ${process.env.CPDAT_CSV_PATH ? 'Bulk CSV' : process.env.CTX_API_KEY ? 'CTX API' : 'UNCONFIGURED'}`
  );
  try {
    await enrichChemicals();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }
  console.log(`Finished: ${new Date().toISOString()}`);
})();
