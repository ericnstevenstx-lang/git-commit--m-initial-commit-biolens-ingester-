/**
 * Pipeline: Ingest Open Beauty Facts (local CSV)
 *
 * Reads the bulk CSV from en.openbeautyfacts.org.products.csv
 * and upserts products into source_products_raw with source='obf'.
 *
 * Stream-based: processes in batches of 100, doesn't load full file.
 * Idempotent: upserts by (source, external_product_id).
 */
import { supabase } from '../supabase.js';
import { parseOBFCsv, OBFProduct } from '../connectors/openBeautyFacts.js';
import path from 'node:path';

const DEFAULT_CSV_PATH = path.resolve(
  process.cwd(),
  'en.openbeautyfacts.org.products.csv'
);

async function getRegistrySourceId(): Promise<string> {
  // Try 'obf' first, then 'open_beauty_facts'
  const { data: obf } = await supabase
    .from('registry_sources')
    .select('id')
    .eq('code', 'open_beauty_facts')
    .maybeSingle();

  if (obf) return obf.id;

  const { data: obf2 } = await supabase
    .from('registry_sources')
    .select('id')
    .eq('code', 'obf')
    .maybeSingle();

  if (obf2) return obf2.id;

  throw new Error(
    'registry_sources row with code="open_beauty_facts" or "obf" not found.'
  );
}

function toRawRow(product: OBFProduct, registrySourceId: string) {
  // Extract first category as primary, second as sub
  const categories = (product.categories_en || '').split(',').map((c) => c.trim());

  return {
    registry_source_id: registrySourceId,
    source: 'obf',
    external_product_id: `obf-${product.code}`,
    barcode: product.code,
    gtin: product.code,
    product_name: product.product_name,
    brand: product.brands,
    category: categories[0] || null,
    subcategory: categories[1] || null,
    quantity: product.quantity,
    ingredient_list_text: product.ingredients_text,
    inci_text: product.ingredients_text,
    country_of_origin: product.origins_en || null,
    countries_sold: product.countries_en || null,
    manufacturing_places: product.manufacturing_places,
    labels_claims: product.labels_en,
    packaging_text: product.packaging_en,
    stores: product.stores,
    source_url: `https://world.openbeautyfacts.org/product/${product.code}`,
    raw_payload: {
      image_url: product.image_url,
      categories_en: product.categories_en,
      source_file: 'en.openbeautyfacts.org.products.csv',
    },
  };
}

async function upsertBatch(rows: ReturnType<typeof toRawRow>[]): Promise<number> {
  if (rows.length === 0) return 0;

  const { data, error } = await supabase
    .from('source_products_raw')
    .upsert(rows, {
      onConflict: 'source,external_product_id',
      ignoreDuplicates: false,
    })
    .select('id');

  if (error) {
    console.error('[ingestOBF] Upsert error:', error.message);
    return 0;
  }

  return data?.length || 0;
}

export async function ingestOpenBeautyFacts(
  csvPath?: string
): Promise<void> {
  const filePath = csvPath || process.env.OBF_CSV_PATH || DEFAULT_CSV_PATH;
  console.log(`[ingestOBF] Reading: ${filePath}`);

  const registrySourceId = await getRegistrySourceId();
  let totalIngested = 0;
  let batchNum = 0;

  await parseOBFCsv(filePath, async (products) => {
    batchNum++;
    const rows = products.map((p) => toRawRow(p, registrySourceId));
    const count = await upsertBatch(rows);
    totalIngested += count;

    if (batchNum % 10 === 0) {
      console.log(
        `[ingestOBF] Batch ${batchNum}: ${count} upserted (total: ${totalIngested})`
      );
    }
  });

  console.log(`[ingestOBF] Complete. Total ingested: ${totalIngested}`);
}
