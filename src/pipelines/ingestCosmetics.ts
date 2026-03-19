import { supabase } from "../supabase.js";
import { streamEwgProducts } from "../connectors/ewg.js";

const EWG_SOURCE = "ewg";
const BATCH_SIZE = 50;

export async function runIngestCosmetics(opts: {
  category?: string | null;
  rateLimitMs?: number;
  log?: (msg: string) => void;
}): Promise<{ upserted: number; errors: number }> {
  const log = opts.log ?? ((m: string) => console.log(m));

  const { data: reg, error: regErr } = await supabase
    .from("registry_sources")
    .select("id")
    .eq("code", EWG_SOURCE)
    .single();

  if (regErr || !reg) {
    log("ERROR: No registry_sources row with code 'ewg'. Add one or check DB.");
    throw new Error(regErr?.message ?? "registry_sources ewg not found");
  }
  const registrySourceId = reg.id;

  let upserted = 0;
  let errors = 0;
  let batch: Array<Record<string, unknown>> = [];

  for await (const p of streamEwgProducts({
    category: opts.category,
    rateLimitMs: opts.rateLimitMs,
    log,
  })) {
    const row = {
      registry_source_id: registrySourceId,
      source: p.source,
      external_product_id: p.external_product_id,
      product_name: p.product_name,
      brand: p.brand,
      category: p.category,
      ingredient_list_text: p.ingredient_list_text,
      inci_text: p.ingredient_list_text,
      source_url: p.source_url,
      disclosures: {},
      raw_payload: p.raw_payload,
    };

    batch.push(row);
    if (batch.length >= BATCH_SIZE) {
      const { error } = await supabase.from("source_products_raw").upsert(batch, {
        onConflict: "source,external_product_id",
      });
      if (error) {
        log(`Upsert error: ${error.message}`);
        errors += batch.length;
      } else {
        upserted += batch.length;
        log(`Upserted batch: ${batch.length} (total ${upserted}).`);
      }
      batch = [];
    }
  }

  if (batch.length > 0) {
    const { error } = await supabase.from("source_products_raw").upsert(batch, {
      onConflict: "source,external_product_id",
    });
    if (error) {
      log(`Upsert error: ${error.message}`);
      errors += batch.length;
    } else {
      upserted += batch.length;
    }
  }

  return { upserted, errors };
}
