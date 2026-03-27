/**
 * Script runner: UPC Enrichment
 * Phase 1: Fill missing country_of_origin via UPCitemdb
 * Phase 2: Discover new products by chemical name search
 *
 * Usage: npm run enrich:upc
 */
import { runEnrichUPC } from '../pipelines/enrichUPC';

async function main() {
  console.log('=== UPC Enrichment Pipeline ===');
  console.log(`Started at ${new Date().toISOString()}`);

  try {
    await runEnrichUPC();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }

  console.log(`Finished at ${new Date().toISOString()}`);
}

main();
