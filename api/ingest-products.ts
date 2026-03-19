import type { VercelRequest, VercelResponse } from "@vercel/node";
import { createClient } from "@supabase/supabase-js";

const MAPPING_METHOD = "api_exact_alias_match";

function normalizeIngredient(raw: string): string {
  let s = raw.trim();
  const paren = s.indexOf(" (");
  if (paren !== -1) s = s.slice(0, paren).trim();
  return s.toLowerCase().replace(/\s+/g, " ").trim();
}

function splitIngredientList(text: string | null | undefined): string[] {
  if (!text || !String(text).trim()) return [];
  return String(text)
    .split(/[,;]/)
    .map((s) => s.trim())
    .filter(Boolean);
}

interface ProductInput {
  external_product_id: string;
  product_name?: string | null;
  brand?: string | null;
  category?: string | null;
  subcategory?: string | null;
  ingredient_list_text?: string | null;
  inci_text?: string | null;
  source_url?: string | null;
  raw_payload?: Record<string, unknown> | null;
}

interface IngestBody {
  source: string;
  products: ProductInput[];
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Method not allowed" });
    return;
  }

  const ingestKey = req.headers["x-ingest-key"];
  if (ingestKey !== process.env.INGEST_API_KEY) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    res.status(500).json({ error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" });
    return;
  }

  let body: IngestBody;
  try {
    body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
  } catch {
    res.status(400).json({ error: "Invalid JSON body" });
    return;
  }

  if (!body?.source || !Array.isArray(body.products)) {
    res.status(400).json({ error: "Body must include source and products array" });
    return;
  }

  const source = String(body.source).trim();
  const products: ProductInput[] = body.products.filter(
    (p) => p && p.external_product_id != null && String(p.external_product_id).trim() !== ""
  );

  const supabase = createClient(url, key, { auth: { persistSession: false } });

  const { data: reg, error: regErr } = await supabase
    .from("registry_sources")
    .select("id")
    .eq("code", source)
    .single();

  if (regErr || !reg) {
    res.status(400).json({
      error: "Source not found in registry_sources",
      detail: "Add a row with code matching the request 'source' (e.g. genspark)",
    });
    return;
  }
  const registrySourceId = reg.id;

  const productRows = products.map((p) => ({
    registry_source_id: registrySourceId,
    source,
    external_product_id: String(p.external_product_id).trim(),
    product_name: p.product_name ?? null,
    brand: p.brand ?? null,
    category: p.category ?? null,
    subcategory: p.subcategory ?? null,
    ingredient_list_text: p.ingredient_list_text ?? null,
    inci_text: p.inci_text ?? null,
    source_url: p.source_url ?? null,
    raw_payload: p.raw_payload ?? {},
  }));

  const { data: upsertedProducts, error: upsertErr } = await supabase
    .from("source_products_raw")
    .upsert(productRows, { onConflict: "source,external_product_id" })
    .select("id, external_product_id");

  if (upsertErr) {
    res.status(500).json({ error: "Upsert products failed", detail: upsertErr.message });
    return;
  }

  const productIdByExternal = new Map<string, string>();
  for (const row of upsertedProducts ?? []) {
    productIdByExternal.set(row.external_product_id, row.id);
  }

  const ingredientRows: Array<{
    source_product_id: string;
    source: string;
    ingredient_position: number;
    ingredient_raw: string;
    normalized_token: string;
    parse_status: string;
  }> = [];

  for (const p of products) {
    const sourceProductId = productIdByExternal.get(String(p.external_product_id).trim());
    if (!sourceProductId) continue;
    const text = p.inci_text ?? p.ingredient_list_text;
    const tokens = splitIngredientList(text);
    for (let i = 0; i < tokens.length; i++) {
      const raw = tokens[i];
      const normalized = normalizeIngredient(raw);
      if (!normalized) continue;
      ingredientRows.push({
        source_product_id: sourceProductId,
        source,
        ingredient_position: i + 1,
        ingredient_raw: raw,
        normalized_token: normalized,
        parse_status: "parsed",
      });
    }
  }

  if (ingredientRows.length === 0) {
    res.status(200).json({
      products_received: products.length,
      products_upserted: upsertedProducts?.length ?? 0,
      ingredient_rows_created: 0,
      ingredient_rows_mapped: 0,
    });
    return;
  }

  const { data: insertedIngredients, error: insertIngErr } = await supabase
    .from("source_product_ingredients_raw")
    .insert(ingredientRows)
    .select("id, normalized_token");

  if (insertIngErr) {
    res.status(500).json({ error: "Insert ingredients failed", detail: insertIngErr.message });
    return;
  }

  const tokens = [...new Set((insertedIngredients ?? []).map((r) => r.normalized_token).filter(Boolean))];
  const { data: lookup, error: lookupErr } = await supabase
    .from("v_material_alias_lookup_safe")
    .select("normalized_alias, material_id")
    .in("normalized_alias", tokens);

  if (lookupErr) {
    res.status(500).json({ error: "Material lookup failed", detail: lookupErr.message });
    return;
  }

  const aliasToMaterial = new Map<string, string>();
  for (const row of lookup ?? []) aliasToMaterial.set(row.normalized_alias, row.material_id);

  const mapRows: Array<{
    raw_ingredient_id: string;
    material_id: string;
    mapping_confidence: number;
    mapping_method: string;
  }> = [];
  const idsToMark: string[] = [];

  for (const ing of insertedIngredients ?? []) {
    const materialId = ing.normalized_token ? aliasToMaterial.get(ing.normalized_token) : null;
    if (materialId) {
      mapRows.push({
        raw_ingredient_id: ing.id,
        material_id: materialId,
        mapping_confidence: 1,
        mapping_method: MAPPING_METHOD,
      });
      idsToMark.push(ing.id);
    }
  }

  if (mapRows.length > 0) {
    await supabase.from("ingredient_material_map").upsert(mapRows, {
      onConflict: "raw_ingredient_id,material_id",
    });
    if (idsToMark.length > 0) {
      await supabase
        .from("source_product_ingredients_raw")
        .update({ parse_status: "mapped" })
        .in("id", idsToMark);
    }
  }

  res.status(200).json({
    products_received: products.length,
    products_upserted: upsertedProducts?.length ?? 0,
    ingredient_rows_created: ingredientRows.length,
    ingredient_rows_mapped: idsToMark.length,
  });
}
