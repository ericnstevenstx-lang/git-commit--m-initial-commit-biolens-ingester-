import { runExportAudit } from "../pipelines/exportAudit.js";

async function main() {
  console.log("Exporting audit CSVs to out/...");
  const { files } = await runExportAudit({
    log: (m) => console.log(m),
  });
  console.log(`Finished. Files: ${files.join(", ")}.`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
