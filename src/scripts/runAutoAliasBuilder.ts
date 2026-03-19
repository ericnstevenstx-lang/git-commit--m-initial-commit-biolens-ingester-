import { runAutoAliasBuilder } from "../pipelines/autoAliasBuilder.js";

async function main() {
  console.log("Starting auto-alias builder (expand materials + aliases)...");
  const summary = await runAutoAliasBuilder({
    log: (m) => console.log(m),
  });

  console.log("Auto-alias builder finished:");
  console.log(JSON.stringify(summary, null, 2));

  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

