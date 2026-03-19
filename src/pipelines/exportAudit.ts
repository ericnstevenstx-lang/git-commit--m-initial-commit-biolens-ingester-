import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { stringify } from "csv-stringify";
import { supabase } from "../supabase.js";

const OUT_DIR = "out";

export async function runExportAudit(opts: {
  outDir?: string;
  log?: (msg: string) => void;
}): Promise<{ files: string[] }> {
  const log = opts.log ?? ((m: string) => console.log(m));
  const outDir = opts.outDir ?? OUT_DIR;

  const { mkdir } = await import("node:fs/promises");
  await mkdir(outDir, { recursive: true });

  const files: string[] = [];

  const productsPath = `${outDir}/products_raw.csv`;
  const { data: products } = await supabase.from("source_products_raw").select("*");
  await pipeline(
    stringify(products ?? [], { header: true }),
    createWriteStream(productsPath)
  );
  files.push(productsPath);
  log(`Wrote ${productsPath} (${(products ?? []).length} rows).`);

  const ingredientsPath = `${outDir}/product_ingredients_raw.csv`;
  const { data: ingredients } = await supabase.from("source_product_ingredients_raw").select("*");
  await pipeline(
    stringify(ingredients ?? [], { header: true }),
    createWriteStream(ingredientsPath)
  );
  files.push(ingredientsPath);
  log(`Wrote ${ingredientsPath} (${(ingredients ?? []).length} rows).`);

  const mappedPath = `${outDir}/mapped_ingredients.csv`;
  const { data: mapRows } = await supabase.from("ingredient_material_map").select("*");
  const rawIds = [...new Set((mapRows ?? []).map((r: { raw_ingredient_id: string }) => r.raw_ingredient_id))];
  let ingredientDetails: Map<string, { ingredient_raw: string; normalized_token: string; source_product_id: string; source: string }> = new Map();
  if (rawIds.length > 0) {
    const chunk = 200;
    for (let i = 0; i < rawIds.length; i += chunk) {
      const { data: ing } = await supabase
        .from("source_product_ingredients_raw")
        .select("id, ingredient_raw, normalized_token, source_product_id, source")
        .in("id", rawIds.slice(i, i + chunk));
      for (const row of ing ?? []) {
        ingredientDetails.set(row.id, {
          ingredient_raw: row.ingredient_raw ?? "",
          normalized_token: row.normalized_token ?? "",
          source_product_id: row.source_product_id ?? "",
          source: row.source ?? "",
        });
      }
    }
  }
  const mappedRows = (mapRows ?? []).map((r: Record<string, unknown>) => {
    const detail = ingredientDetails.get(r.raw_ingredient_id as string);
    return {
      id: r.id,
      raw_ingredient_id: r.raw_ingredient_id,
      material_id: r.material_id,
      mapping_method: r.mapping_method,
      mapping_confidence: r.mapping_confidence,
      ingredient_raw: detail?.ingredient_raw ?? "",
      normalized_token: detail?.normalized_token ?? "",
      source_product_id: detail?.source_product_id ?? "",
      source: detail?.source ?? "",
    };
  });
  await pipeline(
    stringify(mappedRows, { header: true }),
    createWriteStream(mappedPath)
  );
  files.push(mappedPath);
  log(`Wrote ${mappedPath} (${mappedRows.length} rows).`);

  const unmappedPath = `${outDir}/unmapped_ingredients.csv`;
  const { data: unmapped } = await supabase
    .from("source_product_ingredients_raw")
    .select("id, source_product_id, source, ingredient_position, ingredient_raw, normalized_token, parse_status")
    .eq("parse_status", "parsed");
  await pipeline(
    stringify(unmapped ?? [], { header: true }),
    createWriteStream(unmappedPath)
  );
  files.push(unmappedPath);
  log(`Wrote ${unmappedPath} (${(unmapped ?? []).length} rows).`);

  log(`Export done. Files: ${files.join(", ")}.`);
  return { files };
}
