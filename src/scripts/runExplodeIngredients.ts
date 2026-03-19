import { runExplodeIngredients } from "../pipelines/explodeIngredients.js";

async function main() {
  console.log("Exploding ingredients from source_products_raw into source_product_ingredients_raw...");
  const { inserted, productsProcessed } = await runExplodeIngredients({
    log: (m) => console.log(m),
  });
  console.log(`Finished. Products processed: ${productsProcessed}, ingredients inserted: ${inserted}.`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
