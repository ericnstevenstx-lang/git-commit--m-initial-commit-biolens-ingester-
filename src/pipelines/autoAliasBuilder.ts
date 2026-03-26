import { supabase } from "../supabase.js";
import { normalizeInci } from "../normalize/inci.js";

const PAGE_SIZE_TOKENS = 50;
const ALIAS_TYPE = "common";
const MAPPING_METHOD = "auto_alias_builder";
const ALIAS_CONFIDENCE = 1;

const BAD_TOKENS = new Set([
  "and",
  "or",
  "with",
  "contains",
  "may contain",
  "unknown",
  "null",
  "none",
  "ingredients",
  "active ingredients",
  "inactive ingredients",
  "other ingredients",
  "please see package",
]);

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function shouldSkipToken(token: string): boolean {
  if (!token) return true;
  if (token.length < 2) return true;
  if (BAD_TOKENS.has(token)) return true;
  return false;
}

function classifyEntityType(token: string): "class" | "material" | "substance" {
  // F:
  // if token contains "fragrance" or equals "parfum" -> class
  if (token === "parfum" || token.includes("fragrance")) return "class";

  // if token contains "extract", "oil", "butter", "wax", or "juice" -> material
  const materialKeywords = ["extract", "oil", "butter", "wax", "juice"];
  if (materialKeywords.some((k) => token.includes(k))) return "material";

  // otherwise default substance
  return "substance";
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

async function ensureMaterialForToken(
  normalizedToken: string,
  entityType: "class" | "material" | "substance"
): Promise<{ materialId: string; created: boolean }> {
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
        entity_type: entityType,
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
  skipped_tokens_count: number;
  skipped_examples: string[];
}> {
  const log = opts?.log ?? ((m: string) => console.log(m));

  let tokensProcessed = 0;
  let aliasesCreated = 0;
  let materialsCreated = 0;
  let ingredientRowsMapped = 0;
  let skippedTokensCount = 0;
  const skippedExamples: string[] = [];

  log(`Auto-alias builder: starting...`);

  let lastToken = "";
  while (true) {
    const { data: queueRows, error: qErr } = await supabase
      .from("v_unmapped_ingredient_queue")
      .select("normalized_token, occurrences")
      .order("normalized_token", { ascending: true })
      .gt("normalized_token", lastToken)
      .limit(PAGE_SIZE_TOKENS);

    if (qErr) throw new Error(qErr.message);
    if (!queueRows || queueRows.length === 0) {
      log(`Auto-alias builder: queue exhausted.`);
      break;
    }

    const tokensToProcessSet = new Set<string>();
    const tokensToProcess: string[] = [];

    for (const row of queueRows) {
      if (!row.normalized_token) continue;
      const rawToken = String(row.normalized_token);

      // Re-normalize to tighten (handles older bad data) while preserving idempotency.
      const cleaned = normalizeInci(rawToken);

      lastToken = rawToken;

      if (shouldSkipToken(cleaned)) {
        skippedTokensCount++;
        if (cleaned && skippedExamples.length < 10 && !skippedExamples.includes(cleaned)) skippedExamples.push(cleaned);
        continue;
      }

      if (!tokensToProcessSet.has(cleaned)) {
        tokensToProcessSet.add(cleaned);
        tokensToProcess.push(cleaned);
      }
    }

    if (tokensToProcess.length === 0) {
      log(`Batch: all tokens were skipped (${queueRows.length} queue rows).`);
      continue;
    }

    log(`Processing token batch (${tokensToProcess.length} tokens): ${tokensToProcess.join(", ")}`);

    // 1-2: ensure materials + aliases exist.
    for (const normalizedToken of tokensToProcess) {
      tokensProcessed++;

      const entityType = classifyEntityType(normalizedToken);

      const { materialId, created } = await ensureMaterialForToken(normalizedToken, entityType);
      if (created) materialsCreated++;

      const { aliasCreated } = await ensureAliasForMaterial({ materialId, normalizedToken });
      if (aliasCreated) aliasesCreated++;

      log(
        `Token "${normalizedToken}": material ${created ? "created" : "reused"} (${materialId}); entity_type=${entityType}; alias ${
          aliasCreated ? "created" : "already exists"
        }.`
      );
    }

    // 3-5: remap ingredients using the safe alias view.
    const mappedInBatch = await remapParsedIngredientsForTokens(tokensToProcess, log);
    ingredientRowsMapped += mappedInBatch;
    log(`Batch done: mapped ${mappedInBatch} ingredient rows.`);
  }

  const summary = {
    tokens_processed: tokensProcessed,
    aliases_created: aliasesCreated,
    materials_created: materialsCreated,
    ingredient_rows_mapped: ingredientRowsMapped,
    skipped_tokens_count: skippedTokensCount,
    skipped_examples: skippedExamples,
  };
  log(`Auto-alias builder: finished. Summary: ${JSON.stringify(summary)}`);
  return summary;
}

