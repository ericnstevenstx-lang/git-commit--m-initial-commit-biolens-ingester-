import { supabase } from "../supabase.js";

const BATCH_SIZE = 100;
const MAPPING_METHOD = "exact_alias";
const MAX_LOOKUP_RETRIES = 3;
const RETRY_BASE_MS = 1500;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchLookupWithRetry(
  tokens: string[],
  log: (msg: string) => void
): Promise<Array<{ normalized_alias: string; material_id: string }>> {
  let lastErr: unknown;

  for (let attempt = 1; attempt <= MAX_LOOKUP_RETRIES; attempt++) {
    try {
      const { data, error } = await supabase
        .from("v_material_alias_lookup_safe")
        .select("normalized_alias, material_id")
        .in("normalized_alias", tokens);

      if (error) throw new Error(error.message);
      return data ?? [];
    } catch (err) {
      lastErr = err;
      const msg = err instanceof Error ? err.message : String(err);

      if (attempt < MAX_LOOKUP_RETRIES) {
        const waitMs = RETRY_BASE_MS * attempt;
        log(`Lookup retry ${attempt}/${MAX_LOOKUP_RETRIES - 1} after error: ${msg}. Waiting ${waitMs}ms...`);
        await sleep(waitMs);
      }
    }
  }

  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

/**
 * Map ingredients to materials via v_material_alias_lookup_safe
 * (normalized_token = normalized_alias).
 *
 * Upsert ingredient_material_map; mark source_product_ingredients_raw
 * as parse_status = 'mapped'.
 */
export async function runMapIngredients(opts: {
  source?: string;
  log?: (msg: string) => void;
}): Promise<{ mapped: number; updated: number }> {
  const log = opts.log ?? ((m: string) => console.log(m));
  const source = opts.source ?? "ewg";

  log("NEW mapIngredients.ts LOADED");

  let mapped = 0;
  let updated = 0;
  let batchNumber = 0;
  let lastSeenId: string | null = null;

  while (true) {
    batchNumber += 1;

    let query = supabase
      .from("source_product_ingredients_raw")
      .select("id, normalized_token")
      .eq("source", source)
      .eq("parse_status", "parsed")
      .not("normalized_token", "is", null)
      .order("id", { ascending: true })
      .limit(BATCH_SIZE);

    if (lastSeenId) {
      query = query.gt("id", lastSeenId);
    }

    const { data: ingredients, error: listErr } = await query;

    if (listErr) {
      log(`Error listing ingredients: ${listErr.message}`);
      throw new Error(listErr.message);
    }

    if (!ingredients?.length) {
      break;
    }

    lastSeenId = ingredients[ingredients.length - 1].id;

    const tokens = [
      ...new Set(
        ingredients
          .map((r) => r.normalized_token as string)
          .filter(Boolean)
      ),
    ];

    let lookup: Array<{ normalized_alias: string; material_id: string }> = [];
    try {
      lookup = await fetchLookupWithRetry(tokens, log);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`Lookup error: ${msg}`);
      throw new Error(msg);
    }

    const aliasToMaterial = new Map<string, string>();
    for (const row of lookup) {
      aliasToMaterial.set(row.normalized_alias, row.material_id);
    }

    const mapRows: Array<{
      raw_ingredient_id: string;
      material_id: string;
      mapping_method: string;
      mapping_confidence: number;
    }> = [];

    const idsToMark: string[] = [];

    for (const ing of ingredients) {
      const materialId = ing.normalized_token
        ? aliasToMaterial.get(ing.normalized_token)
        : null;

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
      const { error: upsertErr } = await supabase
        .from("ingredient_material_map")
        .upsert(mapRows, {
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

      if (updateErr) {
        log(`Update status warning: ${updateErr.message}`);
      } else {
        updated += idsToMark.length;
      }
    }

    log(`Batch ${batchNumber}: ${mapRows.length} mapped, ${idsToMark.length} marked; total mapped ${mapped}.`);
  }

  log(`Done. Mapped: ${mapped}, rows marked mapped: ${updated}.`);
  return { mapped, updated };
}
