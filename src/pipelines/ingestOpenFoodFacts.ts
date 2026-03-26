/**
 * Pipeline: Ingest Open Food Facts
 *
 * Fetches products from Open Food Facts by category or barcode list,
 * upserts into source_products_raw with source='off'.
 * Populates gtin, country_of_origin, manufacturing_places, ingredients.
 *
 * Idempotent: upserts by (source, external_product_id).
 */
import { supabase } from '../supabase';
import {
  fetchByCategory,
  fetchByBarcode,
  searchProducts,
  normalizeCountryTag,
  OFFProduct,
} from '../connectors/openFoodFacts';

// Categories relevant to BioLens material intelligence
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

/**
 * Resolve the registry_source_id for 'off' from registry_sources.
 */
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

/**
 * Convert an OFFProduct to a source_products_raw row shape.
 */
function toRawRow(product: OFFProduct, registrySourceId: string) {
  // Resolve first country tag to ISO code for country_of_origin
  let countryOfOrigin: string | null = null;
  if (product.origins) {
    // origins field is free text, often a country name
    countryOfOrigin = product.origins;
  }

  // countries_sold from countries_tags
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

/**
 * Upsert a batch of products into source_products_raw.
 */
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

/**
 * Main: ingest by category browse.
 */
export async function ingestByCategories(
  categories: string[] = TARGET_CATEGORIES,
  maxPagesPerCategory = 5,
  pageSize = 50
): Promise<void> {
  const registrySourceId = await getRegistrySourceId();
  let totalIngested = 0;

  for (const category of categories) {
    console.log(`[ingestOFF] Fetching category: ${category}`);
    const products = await fetchByCategory(
      category,
      maxPagesPerCategory,
      pageSize,
      true // use Open Products Facts for non-food
    );

    if (products.length === 0) {
      // Fallback to Open Food Facts
      const foodProducts = await fetchByCategory(
        category,
        maxPagesPerCategory,
        pageSize,
        false
      );
      products.push(...foodProducts);
    }

    console.log(`[ingestOFF] Got ${products.length} products for ${category}`);

    // Filter out products with no ingredients
    const withIngredients = products.filter(
      (p) => p.ingredients_text || p.ingredients_text_en
    );
    console.log(
      `[ingestOFF] ${withIngredients.length} have ingredient data`
    );

    const rows = withIngredients.map((p) => toRawRow(p, registrySourceId));

    // Batch upsert in chunks of 100
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

/**
 * Ingest by a list of barcodes (e.g. from FiberFoundry product GTINs).
 */
export async function ingestByBarcodes(
  barcodes: string[]
): Promise<void> {
  const registrySourceId = await getRegistrySourceId();
  let ingested = 0;

  for (const barcode of barcodes) {
    // Try Open Products Facts first, then Open Food Facts
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
