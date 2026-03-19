import { runMapIngredients } from "../pipelines/mapIngredients.js";

async function main() {
  console.log("Mapping ingredients to materials via v_material_alias_lookup_safe...");
  const { mapped, updated } = await runMapIngredients({
    log: (m) => console.log(m),
  });
  console.log(`Finished. Mapped: ${mapped}, rows marked mapped: ${updated}.`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
