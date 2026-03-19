import { supabase } from "../supabase.js";
import { normalizeInci } from "../normalize/inci.js";

const EWG_SOURCE = "ewg";
const BATCH_SIZE = 500;

/**
 * Split ingredient list text by comma (and common separators) into ordered tokens.
 */
function splitIngredientList(text: string | null): string[] {
  if (!text || !text.trim()) return [];
  return text
    .split(/[,;]/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export async function runExplodeIngredients(opts: {
  source?: string;
  log?: (msg: string) => void;
}): Promise<{ inserted: number; productsProcessed: number }> {
  const log = opts.log ?? ((m: string) => console.log(m));
  const source = opts.source ?? EWG_SOURCE;

  let inserted = 0;
  let productsProcessed = 0;
  let offset = 0;
  const pageSize = 100;

  while (true) {
    const { data: allProducts, error: listErr } = await supabase
      .from("source_products_raw")
      .select("id, ingredient_list_text, inci_text")
      .eq("source", source)
      .range(offset, offset + pageSize - 1);

    if (listErr) {
      log(`Error listing products: ${listErr.message}`);
      throw new Error(listErr.message);
    }
    if (!allProducts?.length) break;

    const productIds = allProducts.map((p) => p.id);
    const { data: existing } = await supabase
      .from("source_product_ingredients_raw")
      .select("source_product_id")
      .in("source_product_id", productIds);
    const idsWithIngredients = new Set((existing ?? []).map((r) => r.source_product_id));
    const products = allProducts.filter((p) => !idsWithIngredients.has(p.id));

    const rows: Array<{
      source_product_id: string;
      source: string;
      ingredient_position: number;
      ingredient_raw: string;
      normalized_token: string;
      parse_status: string;
    }> = [];

    for (const p of products) {
      const text = p.inci_text ?? p.ingredient_list_text;
      const tokens = splitIngredientList(text);
      for (let i = 0; i < tokens.length; i++) {
        const raw = tokens[i];
        const normalized = normalizeInci(raw);
        if (!normalized) continue;
        rows.push({
          source_product_id: p.id,
          source,
          ingredient_position: i + 1,
          ingredient_raw: raw,
          normalized_token: normalized,
          parse_status: "parsed",
        });
      }
      productsProcessed++;
    }

    if (rows.length > 0) {
      const { error: insErr } = await supabase.from("source_product_ingredients_raw").insert(rows);
      if (insErr) {
        log(`Insert error: ${insErr.message}`);
        throw new Error(insErr.message);
      }
      inserted += rows.length;
      log(`Exploded ${rows.length} ingredients from ${products.length} products (total inserted ${inserted}).`);
    }

    offset += pageSize;
    if (products.length < pageSize) break;
  }

  log(`Done. Products processed: ${productsProcessed}, ingredients inserted: ${inserted}.`);
  return { inserted, productsProcessed };
}
