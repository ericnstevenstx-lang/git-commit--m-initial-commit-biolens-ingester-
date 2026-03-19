import { supabase } from "../supabase.js";

const PAGE_SIZE_TOKENS = 50;
const MATERIAL_ENTITY_TYPE = "substance";
const ALIAS_TYPE = "common";
const MAPPING_METHOD = "auto_alias_builder";
const ALIAS_CONFIDENCE = 1;

function isBadToken(token: string): boolean {
  const t = token.trim().toLowerCase();
  if (!t) return true;
  const bad = new Set(["unknown", "n/a", "and", "or"]);
  return bad.has(t);
}

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

/** Human-friendly display form: convert underscores/hyphens to spaces and title-case. */
function titleCaseMaterialName(token: string): string {
  const spaced = token.replace(/[_-]+/g, " ");
  const words = spaced.split(/\s+/g).filter(Boolean);
  return words
    .map((w) => {
      const lower = w.toLowerCase();
      return lower.length ? lower[0].toUpperCase() + lower.slice(1) : lower;
    })
    .join(" ");
}

function aliasDisplayFormFromToken(token: string): string {
  return titleCaseMaterialName(token);
}

async function getMaterialIdByNormalizedName(normalizedToken: string): Promise<string | null> {
  const { data, error } = await supabase
    .from("materials")
    .select("id")
    .eq("normalized_name", normalizedToken)
    .maybeSingle();
  if (error) throw new Error(error.message);
  return data?.id ?? null;
}

async function getMaterialIdFromSafeAlias(normalizedToken: string): Promise<string | null> {
  const { data, error } = await supabase
    .from("v_material_alias_lookup_safe")
    .select("material_id")
    .eq("normalized_alias", normalizedToken)
    .maybeSingle();
  if (error) throw new Error(error.message);
  return data?.material_id ?? null;
}

async function ensureMaterialForToken(normalizedToken: string): Promise<{ materialId: string; created: boolean }> {
  const existingId = await getMaterialIdByNormalizedName(normalizedToken);
  if (existingId) return { materialId: existingId, created: false };

  // Defensive: if the token already maps uniquely via aliases, don't create a duplicate material.
  const safeMaterialId = await getMaterialIdFromSafeAlias(normalizedToken);
  if (safeMaterialId) return { materialId: safeMaterialId, created: false };

  const materialName = titleCaseMaterialName(normalizedToken);

  const { data, error } = await supabase
    .from("materials")
    .upsert(
      {
        material_name: materialName,
        normalized_name: normalizedToken,
        entity_type: MATERIAL_ENTITY_TYPE,
        review_status: "published",
      },
      { onConflict: "normalized_name" }
    )
    .select("id")
    .maybeSingle();

  if (error) throw new Error(error.message);
  if (!data?.id) throw new Error(`Failed to create/get material for token=${normalizedToken}`);

  return { materialId: data.id, created: true };
}

async function ensureAliasForMaterial(params: {
  materialId: string;
  normalizedToken: string;
}): Promise<{ aliasCreated: boolean }> {
  const { materialId, normalizedToken } = params;
  const { data: existing, error: exErr } = await supabase
    .from("material_aliases")
    .select("id")
    .eq("material_id", materialId)
    .eq("normalized_alias", normalizedToken)
    .limit(1);

  if (exErr) throw new Error(exErr.message);
  if ((existing ?? []).length > 0) return { aliasCreated: false };

  const alias = aliasDisplayFormFromToken(normalizedToken);

  const { error: insErr } = await supabase.from("material_aliases").upsert(
    {
      material_id: materialId,
      alias,
      normalized_alias: normalizedToken,
      alias_type: ALIAS_TYPE,
      confidence_score: ALIAS_CONFIDENCE,
    },
    {
      onConflict: "material_id,normalized_alias",
    }
  );
  if (insErr) throw new Error(insErr.message);

  return { aliasCreated: true };
}

async function remapParsedIngredientsForTokens(tokens: string[], log: (msg: string) => void): Promise<number> {
  // Use the safe alias view for unambiguous normalized_alias -> material_id mapping.
  const { data: safeRows, error: safeErr } = await supabase
    .from("v_material_alias_lookup_safe")
    .select("normalized_alias, material_id")
    .in("normalized_alias", tokens);
  if (safeErr) throw new Error(safeErr.message);

  const aliasToMaterial = new Map<string, string>();
  for (const r of safeRows ?? []) aliasToMaterial.set(r.normalized_alias, r.material_id);

  const { data: ingredients, error: ingErr } = await supabase
    .from("source_product_ingredients_raw")
    .select("id, normalized_token")
    .eq("parse_status", "parsed")
    .in("normalized_token", tokens);
  if (ingErr) throw new Error(ingErr.message);
  if (!ingredients?.length) return 0;

  const idsToMark: string[] = [];
  const mapRows: Array<{
    raw_ingredient_id: string;
    material_id: string;
    mapping_confidence: number;
    mapping_method: string;
  }> = [];

  for (const ing of ingredients) {
    const token = ing.normalized_token;
    if (!token) continue;
    const materialId = aliasToMaterial.get(token);
    if (!materialId) continue;
    idsToMark.push(ing.id);
    mapRows.push({
      raw_ingredient_id: ing.id,
      material_id: materialId,
      mapping_confidence: 1,
      mapping_method: MAPPING_METHOD,
    });
  }

  if (mapRows.length === 0) {
    log(`Remap: no ingredients matched safe alias mappings in batch.`);
    return 0;
  }

  // Upsert ingredient_material_map; conflict key is (raw_ingredient_id, material_id).
  const { error: mapErr } = await supabase.from("ingredient_material_map").upsert(mapRows, {
    onConflict: "raw_ingredient_id,material_id",
  });
  if (mapErr) throw new Error(mapErr.message);

  // Mark the ingredients as mapped.
  const { error: updErr } = await supabase
    .from("source_product_ingredients_raw")
    .update({ parse_status: "mapped" })
    .in("id", idsToMark);
  if (updErr) throw new Error(updErr.message);

  return idsToMark.length;
}

export async function runAutoAliasBuilder(opts?: {
  log?: (msg: string) => void;
}): Promise<{
  tokens_processed: number;
  aliases_created: number;
  materials_created: number;
  ingredient_rows_mapped: number;
}> {
  const log = opts?.log ?? ((m: string) => console.log(m));

  let tokensProcessed = 0;
  let aliasesCreated = 0;
  let materialsCreated = 0;
  let ingredientRowsMapped = 0;
  const attemptedTokens = new Set<string>();

  log(`Auto-alias builder: starting...`);

  while (true) {
    const { data: queueRows, error: qErr } = await supabase
      .from("v_unmapped_ingredient_queue")
      .select("normalized_token, occurrences")
      .order("occurrences", { ascending: false })
      .limit(PAGE_SIZE_TOKENS);

    if (qErr) throw new Error(qErr.message);
    const rawTokens = (queueRows ?? []).map((r) => (r.normalized_token ? collapseSpaces(String(r.normalized_token)) : ""));
    const tokens = rawTokens
      .filter((t) => !isBadToken(t))
      .filter((t) => t !== "");

    if (tokens.length === 0) {
      log(`Auto-alias builder: no unmapped tokens remain (or only bad tokens).`);
      break;
    }

    const newTokens = [...new Set(tokens)].filter((t) => !attemptedTokens.has(t));
    if (newTokens.length === 0) {
      log(`Auto-alias builder: top tokens were already attempted in this run; stopping to avoid infinite loop.`);
      break;
    }

    // Mark as attempted up-front to avoid reprocessing a large batch if mapping creates no progress.
    newTokens.forEach((t) => attemptedTokens.add(t));
    log(`Processing token batch (${newTokens.length} new tokens): ${newTokens.join(", ")}`);

    // 1-2: ensure materials + aliases exist.
    for (const normalizedToken of newTokens) {
      tokensProcessed++;

      // Ensure material exists (or reuse safe alias mapping).
      const { materialId, created } = await ensureMaterialForToken(normalizedToken);
      if (created) materialsCreated++;

      // Ensure alias exists for material+normalized_alias.
      const { aliasCreated } = await ensureAliasForMaterial({ materialId, normalizedToken });
      if (aliasCreated) aliasesCreated++;

      log(
        `Token "${normalizedToken}": material ${created ? "created" : "reused"} (${materialId}); alias ${
          aliasCreated ? "created" : "already exists"
        }.`
      );
    }

    // 3-5: remap ingredients using the safe alias view.
    const mappedInBatch = await remapParsedIngredientsForTokens(newTokens, log);
    ingredientRowsMapped += mappedInBatch;
    log(`Batch done: mapped ${mappedInBatch} ingredient rows.`);
  }

  const summary = {
    tokens_processed: tokensProcessed,
    aliases_created: aliasesCreated,
    materials_created: materialsCreated,
    ingredient_rows_mapped: ingredientRowsMapped,
  };
  log(`Auto-alias builder: finished. Summary: ${JSON.stringify(summary)}`);
  return summary;
}

