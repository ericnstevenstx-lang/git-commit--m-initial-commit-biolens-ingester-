/**
 * Pipeline: Reverse GTIN Lookup
 *
 * Searches Open Food Facts / Open Products Facts for products
 * that contain known chemicals from our materials table.
 *
 * Flow:
 *   1. Pull material canonical_names + aliases from Supabase
 *   2. For each chemical name, search OFF/OPF
 *   3. Filter results to products that have barcodes (GTINs)
 *   4. Upsert into source_products_raw with source='off'
 *
 * This bridges the gap between our 16K+ chemicals and real
 * GTIN-bearing commercial products.
 */
import { supabase } from '../supabase';
import {
  searchProducts,
  OFFProduct,
  normalizeCountryTag,
} from '../connectors/openFoodFacts';

const MAX_MATERIALS = Number(process.env.REVERSE_GTIN_MAX_MATERIALS) || 500;
const MAX_PAGES_PER_SEARCH = Number(process.env.REVERSE_GTIN_MAX_PAGES) || 3;
const PAGE_SIZE = 50;

/**
 * Resolve the registry_source_id for 'off'.
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
 * Fetch material names to search for.
 * Prioritizes materials that have known identifiers (CAS, PubChem)
 * since those are more likely to appear in ingredient lists.
 */
async function getMaterialSearchTerms(): Promise<string[]> {
  // Get materials with identifiers (higher quality chemicals)
  const { data: identified, error: idErr } = await supabase
    .from('material_identifiers')
    .select('material_id, materials!inner(canonical_name, is_active)')
    .in('id_type', ['cas_rn', 'pubchem_cid'])
    .limit(MAX_MATERIALS);

  if (idErr) {
    console.error('[reverseGTIN] Error fetching identified materials:', idErr.message);
  }

  const names = new Set<string>();

  if (identified) {
    for (const row of identified) {
      const mat = (row as any).materials;
      if (mat?.canonical_name && mat?.is_active !== false) {
        names.add(mat.canonical_name);
      }
    }
  }

  // If we don't have enough identified materials, also pull top aliases
  if (names.size < MAX_MATERIALS) {
    const remaining = MAX_MATERIALS - names.size;
    const { data: aliases, error: aliasErr } = await supabase
      .from('material_aliases')
      .select('alias, alias_type')
      .in('alias_type', ['inci', 'common_name', 'cas_name'])
      .order('confidence', { ascending: false })
      .limit(remaining);

    if (aliasErr) {
      console.error('[reverseGTIN] Error fetching aliases:', aliasErr.message);
    }

    if (aliases) {
      for (const row of aliases) {
        // Skip very short aliases (too generic) and CAS numbers (not searchable on OFF)
        if (row.alias.length > 3 && !/^\d{2,7}-\d{2}-\d$/.test(row.alias)) {
          names.add(row.alias);
        }
      }
    }
  }

  console.log(`[reverseGTIN] Collected ${names.size} search terms`);
  return Array.from(names);
}

/**
 * Convert an OFFProduct to a source_products_raw row.
 */
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
      discovered_via: 'reverse_gtin_lookup',
    },
  };
}

/**
 * Upsert a batch into source_products_raw.
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
    console.error('[reverseGTIN] Upsert error:', error.message);
    return 0;
  }

  return data?.length || 0;
}

/**
 * Main: search OFF/OPF for products containing known chemicals.
 */
export async function runReverseGTINLookup(): Promise<void> {
  const registrySourceId = await getRegistrySourceId();
  const searchTerms = await getMaterialSearchTerms();

  let totalIngested = 0;
  let totalSearched = 0;

  for (const term of searchTerms) {
    totalSearched++;
    if (totalSearched % 50 === 0) {
      console.log(
        `[reverseGTIN] Progress: ${totalSearched}/${searchTerms.length} terms, ${totalIngested} products ingested`
      );
    }

    // Search both OFF and OPF
    const allProducts: OFFProduct[] = [];

    for (let page = 1; page <= MAX_PAGES_PER_SEARCH; page++) {
      // Try OFF first (larger database)
      const offResults = await searchProducts(term, page, PAGE_SIZE, false);
      if (offResults.length === 0) break;
      allProducts.push(...offResults);
    }

    // Also try OPF for non-food products
    const opfResults = await searchProducts(term, 1, PAGE_SIZE, true);
    allProducts.push(...opfResults);

    // Filter: must have a barcode and ingredient data
    const valid = allProducts.filter(
      (p) =>
        p.code &&
        p.code.length >= 8 &&
        (p.ingredients_text || p.ingredients_text_en)
    );

    if (valid.length === 0) continue;

    // Deduplicate by barcode
    const seen = new Set<string>();
    const unique = valid.filter((p) => {
      if (seen.has(p.code)) return false;
      seen.add(p.code);
      return true;
    });

    const rows = unique.map((p) => toRawRow(p, registrySourceId));

    // Batch upsert
    const BATCH = 100;
    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);
      const count = await upsertBatch(chunk);
      totalIngested += count;
    }

    if (unique.length > 0) {
      console.log(
        `[reverseGTIN] "${term}" → ${unique.length} products with GTINs`
      );
    }
  }

  console.log(
    `[reverseGTIN] Complete: searched ${totalSearched} terms, ingested ${totalIngested} products`
  );
}
