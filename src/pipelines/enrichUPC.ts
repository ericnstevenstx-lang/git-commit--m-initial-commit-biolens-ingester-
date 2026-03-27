/**
 * Pipeline: Enrich UPC / GTIN products
 *
 * Two phases:
 *   Phase 1 — Fill missing country_of_origin on existing products
 *             by looking up their barcode via UPCitemdb.
 *
 *   Phase 2 — Discover new products by searching UPCitemdb
 *             for known chemical/material names and upserting
 *             any GTIN-bearing results into source_products_raw.
 *
 * Respects UPCitemdb free tier (100 req/day) with configurable rate limiting.
 */
import { supabase } from '../supabase';
import { lookupUPC, searchUPC } from '../connectors/upcItemDb';

const ENRICH_BATCH_SIZE = Number(process.env.ENRICH_UPC_BATCH_SIZE) || 50;

/**
 * Resolve the registry_source_id for 'upcitemdb'.
 * Creates it if it doesn't exist.
 */
async function getOrCreateSourceId(): Promise<string> {
  // Try to find existing
  const { data: existing } = await supabase
    .from('registry_sources')
    .select('id')
    .eq('code', 'upcitemdb')
    .maybeSingle();

  if (existing) return existing.id;

  // Create the source
  const { data: created, error } = await supabase
    .from('registry_sources')
    .insert({
      code: 'upcitemdb',
      name: 'UPCitemdb',
      url: 'https://www.upcitemdb.com',
      source_type: 'api',
    })
    .select('id')
    .single();

  if (error || !created) {
    throw new Error(`Failed to create registry_sources row for upcitemdb: ${error?.message}`);
  }

  return created.id;
}

/**
 * Phase 1: Enrich existing products that have a barcode but no country_of_origin.
 */
export async function enrichMissingOrigins(): Promise<void> {
  console.log('[enrichUPC] Phase 1: Filling missing country_of_origin...');

  const { data: products, error } = await supabase
    .from('source_products_raw')
    .select('id, barcode, gtin, product_name')
    .not('barcode', 'is', null)
    .is('country_of_origin', null)
    .limit(ENRICH_BATCH_SIZE);

  if (error) {
    console.error('[enrichUPC] Query error:', error.message);
    return;
  }

  if (!products || products.length === 0) {
    console.log('[enrichUPC] No products need origin enrichment');
    return;
  }

  console.log(`[enrichUPC] Found ${products.length} products missing country_of_origin`);

  let enriched = 0;

  for (const product of products) {
    const barcode = product.barcode || product.gtin;
    if (!barcode) continue;

    const result = await lookupUPC(barcode);
    if (!result || !result.country) continue;

    const { error: updateErr } = await supabase
      .from('source_products_raw')
      .update({
        country_of_origin: result.country,
        raw_payload: supabase.rpc ? undefined : undefined, // preserve existing
      })
      .eq('id', product.id);

    if (updateErr) {
      console.error(`[enrichUPC] Update error for ${barcode}:`, updateErr.message);
      continue;
    }

    enriched++;
    console.log(
      `[enrichUPC] ${barcode} (${product.product_name}) → origin: ${result.country}`
    );
  }

  console.log(`[enrichUPC] Phase 1 complete: enriched ${enriched}/${products.length}`);
}

/**
 * Phase 2: Discover new products by searching UPCitemdb for chemical names.
 */
export async function discoverProductsByChemical(): Promise<void> {
  console.log('[enrichUPC] Phase 2: Discovering products via chemical name search...');

  const sourceId = await getOrCreateSourceId();

  // Get chemical names that are common in consumer products
  const { data: materials, error: matErr } = await supabase
    .from('materials')
    .select('canonical_name')
    .eq('is_active', true)
    .in('material_type', ['chemical', 'additive', 'extract'])
    .not('canonical_name', 'is', null)
    .limit(ENRICH_BATCH_SIZE);

  if (matErr) {
    console.error('[enrichUPC] Material query error:', matErr.message);
    return;
  }

  if (!materials || materials.length === 0) {
    console.log('[enrichUPC] No materials to search');
    return;
  }

  let totalDiscovered = 0;

  for (const mat of materials) {
    const name = mat.canonical_name;
    if (!name || name.length < 4) continue;

    const results = await searchUPC(name);
    if (results.length === 0) continue;

    // Build rows for products with valid barcodes
    const rows = results
      .filter((r) => r.ean && r.ean.length >= 8)
      .map((r) => ({
        registry_source_id: sourceId,
        source: 'upcitemdb',
        external_product_id: `upcitemdb-${r.ean}`,
        barcode: r.ean,
        gtin: r.ean,
        product_name: r.title || null,
        brand: r.brand || null,
        category: r.category || null,
        country_of_origin: r.country || null,
        source_url: `https://www.upcitemdb.com/upc/${r.ean}`,
        raw_payload: {
          description: r.description,
          images: r.images,
          offers_count: r.offers.length,
          discovered_via_chemical: name,
        },
      }));

    if (rows.length === 0) continue;

    const { data, error: upsertErr } = await supabase
      .from('source_products_raw')
      .upsert(rows, {
        onConflict: 'source,external_product_id',
        ignoreDuplicates: false,
      })
      .select('id');

    if (upsertErr) {
      console.error(`[enrichUPC] Upsert error for "${name}":`, upsertErr.message);
      continue;
    }

    const count = data?.length || 0;
    totalDiscovered += count;

    if (count > 0) {
      console.log(`[enrichUPC] "${name}" → ${count} new products`);
    }
  }

  console.log(`[enrichUPC] Phase 2 complete: discovered ${totalDiscovered} products`);
}

/**
 * Main: run both phases.
 */
export async function runEnrichUPC(): Promise<void> {
  await enrichMissingOrigins();
  await discoverProductsByChemical();
}
