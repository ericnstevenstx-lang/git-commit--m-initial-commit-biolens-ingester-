import { runIngestCosmetics } from "../pipelines/ingestCosmetics.js";

async function main() {
  console.log("Starting cosmetics ingestion (EWG Skin Deep)...");
  const { upserted, errors } = await runIngestCosmetics({
    log: (m) => console.log(m),
  });
  console.log(`Finished. Upserted: ${upserted}, errors: ${errors}.`);
  process.exit(errors > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
