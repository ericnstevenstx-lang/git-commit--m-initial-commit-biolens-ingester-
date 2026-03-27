/**
 * Script runner: Reverse GTIN Lookup
 * Searches OFF/OPF for products containing known chemicals.
 *
 * Usage: npm run lookup:reverse-gtin
 */
import { runReverseGTINLookup } from '../pipelines/reverseGTINLookup';

async function main() {
  console.log('=== Reverse GTIN Lookup ===');
  console.log(`Started at ${new Date().toISOString()}`);

  try {
    await runReverseGTINLookup();
  } catch (err) {
    console.error('Fatal error:', (err as Error).message);
    process.exit(1);
  }

  console.log(`Finished at ${new Date().toISOString()}`);
}

main();
