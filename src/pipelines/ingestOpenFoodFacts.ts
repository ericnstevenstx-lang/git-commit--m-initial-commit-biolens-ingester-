/**
 * Pipeline: Ingest Open Food Facts
 *
 * Fetches products from Open Food Facts by category or barcode list,
 * upserts into source_products_raw with source='off'.
 * Populates gtin, country_of_origin, manufacturing_places, ingredients.
 *
 * Idempotent: upserts by (source, external_product_id).
 *
 * Verified columns against source_products_raw schema:
 *   registry_source_id, source, external_product_id, barcode, gtin,
 *   product_name, brand, category, subcategory, quantity, size_text,
 *   ingredient_list_text, inci_text, country_of_origin, countries_sold,
 *   manufacturing_places, labels_claims, packaging_text, stores,
 *   source_url, raw_payload
 */
import { supabase } from '../supabase.js';
import {
  fetchByCategory,
  fetchByBarcode,
  normalizeCountryTag,
  OFFProduct,
} from '../connectors/openFoodFacts.js';

const TARGET_CATEGORIES = [
  'Cleaning products',
  'Personal care',
  'Body care',
  'Hair care',
  'Skin care',
  'Cosmetics',
  'Baby products',
  'Laundry detergents',
  'Dishwashing products',
  'Household products',
];

async function getRegistrySourceId(): Promise<string> {
  const { data, error } = await supabase
    .from('registry_sources')
    .select('id')
    .eq('code', 'off')
    .single();

  if (error || !data) {
    throw new Error(
      `registry_sources row with code='off' not found. Run seed SQL first.`
    );
  }
  return data.id;
}

function toRawRow(product: OFFProduct, registrySourceId: string) {
  let countryOfOrigin: string | null = null;
  if (product.origins) {
    countryOfOrigin = product.origins;
  }

  const countriesSold = product.countries_tags
    .map(normalizeCountryTag)
    .filter(Boolean)
    .join(', ');

  return {
    registry_source_id: registrySourceId,
    source: 'off',
    external_product_id: `off-${product.code}`,
    barcode: product.code,
    gtin: product.code,
    product_name: product.product_name,
    brand: product.brands,
    category: product.categories_tags_en?.[0] || null,
    subcategory: product.categories_tags_en?.[1] || null,
    quantity: product.quantity,
    ingredient_list_text: product.ingredients_text || product.ingredients_text_en,
    inci_text: product.ingredients_text_en || product.ingredients_text,
    country_of_origin: countryOfOrigin,
    countries_sold: countriesSold || null,
    manufacturing_places: product.manufacturing_places,
    labels_claims: product.labels,
    packaging_text: product.packaging,
    stores: product.stores,
    source_url: `https://world.openfoodfacts.org/product/${product.code}`,
    raw_payload: {
      nutriscore_grade: product.nutriscore_grade,
      ecoscore_grade: product.ecoscore_grade,
      nova_group: product.nova_group,
      image_url: product.image_url,
      categories_tags_en: product.categories_tags_en,
      countries_tags: product.countries_tags,
    },
  };
}

async function upsertBatch(
  rows: ReturnType<typeof toRawRow>[]
): Promise<number> {
  if (rows.length === 0) return 0;

  const { data, error } = await supabase
    .from('source_products_raw')
    .upsert(rows, {
      onConflict: 'source,external_product_id',
      ignoreDuplicates: false,
    })
    .select('id');

  if (error) {
    console.error('[ingestOFF] Upsert error:', error.message);
    return 0;
  }

  return data?.length || 0;
}

export async function ingestByCategories(
  categories: string[] = TARGET_CATEGORIES,
  maxPagesPerCategory = 5,
  pageSize = 50
): Promise<void> {
  const registrySourceId = await getRegistrySourceId();
  let totalIngested = 0;

  for (const category of categories) {
    console.log(`[ingestOFF] Fetching category: ${category}`);

    // Try Open Products Facts first for non-food categories
    let products = await fetchByCategory(
      category,
      maxPagesPerCategory,
      pageSize,
      true
    );

    if (products.length === 0) {
      // Fallback to Open Food Facts
      products = await fetchByCategory(
        category,
        maxPagesPerCategory,
        pageSize,
        false
      );
    }

    console.log(`[ingestOFF] Got ${products.length} products for ${category}`);

    const withIngredients = products.filter(
      (p) => p.ingredients_text || p.ingredients_text_en
    );
    console.log(`[ingestOFF] ${withIngredients.length} have ingredient data`);

    const rows = withIngredients.map((p) => toRawRow(p, registrySourceId));

    const BATCH = 100;
    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);
      const count = await upsertBatch(chunk);
      totalIngested += count;
      console.log(
        `[ingestOFF] Upserted ${count} (batch ${Math.floor(i / BATCH) + 1})`
      );
    }
  }

  console.log(`[ingestOFF] Total ingested: ${totalIngested}`);
}

export async function ingestByBarcodes(barcodes: string[]): Promise<void> {
  const registrySourceId = await getRegistrySourceId();
  let ingested = 0;

  for (const barcode of barcodes) {
    let product = await fetchByBarcode(barcode, true);
    if (!product) {
      product = await fetchByBarcode(barcode, false);
    }
    if (!product) {
      console.log(`[ingestOFF] No data for barcode ${barcode}`);
      continue;
    }

    const row = toRawRow(product, registrySourceId);
    const count = await upsertBatch([row]);
    ingested += count;
    console.log(`[ingestOFF] ${barcode}: ${product.product_name || 'unnamed'}`);
  }

  console.log(`[ingestOFF] Barcode ingest complete: ${ingested} products`);
}
