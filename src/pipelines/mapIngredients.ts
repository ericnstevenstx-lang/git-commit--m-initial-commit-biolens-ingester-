import { supabase } from "../supabase.js";

const BATCH_SIZE = 500;
const MAPPING_METHOD = "exact_alias";

/**
 * Map ingredients to materials via v_material_alias_lookup_safe (normalized_token = normalized_alias).
 * Upsert ingredient_material_map; mark source_product_ingredients_raw as parse_status = 'mapped'.
 */
export async function runMapIngredients(opts: {
  source?: string;
  log?: (msg: string) => void;
}): Promise<{ mapped: number; updated: number }> {
  const log = opts.log ?? ((m: string) => console.log(m));
  const source = opts.source ?? "ewg";

  let offset = 0;
  let mapped = 0;
  let updated = 0;

  while (true) {
    const { data: ingredients, error: listErr } = await supabase
      .from("source_product_ingredients_raw")
      .select("id, normalized_token")
      .eq("source", source)
      .eq("parse_status", "parsed")
      .not("normalized_token", "is", null)
      .range(offset, offset + BATCH_SIZE - 1);

    if (listErr) {
      log(`Error listing ingredients: ${listErr.message}`);
      throw new Error(listErr.message);
    }
    if (!ingredients?.length) break;

    const tokens = [...new Set(ingredients.map((r) => r.normalized_token as string).filter(Boolean))];
    const { data: lookup, error: lookupErr } = await supabase
      .from("v_material_alias_lookup_safe")
      .select("normalized_alias, material_id")
      .in("normalized_alias", tokens);

    if (lookupErr) {
      log(`Lookup error: ${lookupErr.message}`);
      throw new Error(lookupErr.message);
    }

    const aliasToMaterial = new Map<string, string>();
    for (const row of lookup ?? []) aliasToMaterial.set(row.normalized_alias, row.material_id);

    const mapRows: Array<{ raw_ingredient_id: string; material_id: string; mapping_method: string; mapping_confidence: number }> = [];
    const idsToMark: string[] = [];

    for (const ing of ingredients) {
      const materialId = ing.normalized_token ? aliasToMaterial.get(ing.normalized_token) : null;
      if (materialId) {
        mapRows.push({
          raw_ingredient_id: ing.id,
          material_id: materialId,
          mapping_method: MAPPING_METHOD,
          mapping_confidence: 1,
        });
        idsToMark.push(ing.id);
      }
    }

    if (mapRows.length > 0) {
      const { error: upsertErr } = await supabase.from("ingredient_material_map").upsert(mapRows, {
        onConflict: "raw_ingredient_id,material_id",
      });
      if (upsertErr) {
        log(`Upsert map error: ${upsertErr.message}`);
        throw new Error(upsertErr.message);
      }
      mapped += mapRows.length;

      const { error: updateErr } = await supabase
        .from("source_product_ingredients_raw")
        .update({ parse_status: "mapped" })
        .in("id", idsToMark);
      if (updateErr) log(`Update status warning: ${updateErr.message}`);
      else updated += idsToMark.length;
    }

    log(`Batch: ${mapRows.length} mapped, ${idsToMark.length} marked; total mapped ${mapped}.`);
    offset += BATCH_SIZE;
  }

  log(`Done. Mapped: ${mapped}, rows marked mapped: ${updated}.`);
  return { mapped, updated };
}
