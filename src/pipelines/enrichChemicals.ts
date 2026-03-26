/**
 * Pipeline: Enrich Chemicals
 *
 * Takes mapped ingredients from ingredient_material_map and enriches
 * the material_chemical_constituents table with CAS numbers, PubChem CIDs,
 * functional use categories, and weight fractions from CPDat.
 *
 * Two modes:
 *   1. API mode (CTX_API_KEY set): looks up each ingredient via CTX Exposure API
 *   2. Bulk mode (CPDAT_CSV_PATH set): matches against pre-downloaded CSV
 *
 * Idempotent: upserts by (material_id, cas_number, source_name).
 */
import { supabase } from '../supabase';
import {
  resolveChemical,
  fetchChemicalRecord,
  parseBulkCSV,
  buildNameIndex,
  BulkChemicalRow,
} from '../connectors/cpdat';

const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 20;
const CPDAT_CSV_PATH = process.env.CPDAT_CSV_PATH || '';
const USE_API = !!process.env.CTX_API_KEY;

interface MappedIngredient {
  material_id: string;
  material_name: string;
  ingredient_raw: string;
  normalized_token: string;
}

/**
 * Fetch all mapped ingredients that don't yet have chemical constituent data.
 */
async function getUnprocessedIngredients(): Promise<MappedIngredient[]> {
  // Get mapped ingredients with their material info
  const { data: mappings, error } = await supabase
    .from('ingredient_material_map')
    .select(`
      material_id,
      source_product_ingredients_raw!inner(
        ingredient_raw,
        normalized_token
      )
    `)
    .limit(5000);

  if (error || !mappings) {
    console.error('[enrichChemicals] Failed to fetch mappings:', error?.message);
    return [];
  }

  // Get material names
  const materialIds = [...new Set(mappings.map((m: any) => m.material_id))];
  const { data: materials } = await supabase
    .from('materials')
    .select('id, name')
    .in('id', materialIds);

  const nameMap = new Map(
    (materials || []).map((m: any) => [m.id, m.name])
  );

  // Get materials that already have chemical constituents
  const { data: existing } = await supabase
    .from('material_chemical_constituents')
    .select('material_id')
    .in('material_id', materialIds);

  const alreadyEnriched = new Set(
    (existing || []).map((e: any) => e.material_id)
  );

  // Filter to materials not yet enriched
  const results: MappedIngredient[] = [];
  const seen = new Set<string>();

  for (const mapping of mappings) {
    const mid = (mapping as any).material_id;
    if (alreadyEnriched.has(mid)) continue;
    if (seen.has(mid)) continue;
    seen.add(mid);

    const ing = (mapping as any).source_product_ingredients_raw;
    results.push({
      material_id: mid,
      material_name: nameMap.get(mid) || '',
      ingredient_raw: ing?.ingredient_raw || '',
      normalized_token: ing?.normalized_token || '',
    });
  }

  return results;
}

/**
 * Enrich via CTX API: resolve chemical identity, fetch CPDat data,
 * upsert into material_chemical_constituents.
 */
async function enrichViaAPI(ingredients: MappedIngredient[]): Promise<number> {
  let enriched = 0;

  for (const ing of ingredients) {
    const searchTerm = ing.normalized_token || ing.ingredient_raw;
    if (!searchTerm) continue;

    console.log(`[enrichChemicals] API lookup: ${searchTerm}`);

    // Resolve to DTXSID
    const identity = await resolveChemical(searchTerm);
    if (!identity || !identity.dtxsid) {
      console.log(`[enrichChemicals] No DTXSID for: ${searchTerm}`);
      continue;
    }

    // Fetch CPDat record
    const record = await fetchChemicalRecord(identity.dtxsid);
    if (!record) {
      // Still upsert basic identity even without CPDat data
      const { error } = await supabase
        .from('material_chemical_constituents')
        .upsert(
          {
            material_id: ing.material_id,
            chemical_name: identity.preferred_name,
            cas_number: identity.casrn,
            pubchem_cid: null,
            functional_use: null,
            weight_fraction: null,
            source_name: 'ctx_api',
            confidence: 0.6,
          },
          { onConflict: 'material_id,cas_number,source_name' }
        );

      if (!error) enriched++;
      continue;
    }

    // Determine primary functional use
    const primaryFunction =
      record.functional_uses.length > 0
        ? record.functional_uses[0].functional_category
        : null;

    const { error } = await supabase
      .from('material_chemical_constituents')
      .upsert(
        {
          material_id: ing.material_id,
          chemical_name: record.chemical_name,
          cas_number: record.casrn,
          pubchem_cid: null, // PubChem CID enrichment is separate
          functional_use: primaryFunction,
          weight_fraction: null,
          source_name: 'cpdat',
          confidence: 0.75,
        },
        { onConflict: 'material_id,cas_number,source_name' }
      );

    if (error) {
      console.error(
        `[enrichChemicals] Upsert error for ${searchTerm}:`,
        error.message
      );
    } else {
      enriched++;
      console.log(
        `[enrichChemicals] ${searchTerm} -> ${record.casrn || 'no CAS'} ` +
          `(${primaryFunction || 'no function'}) ` +
          `[${record.product_uses.length} product uses, ${record.functional_uses.length} functions]`
      );
    }
  }

  return enriched;
}

/**
 * Enrich via bulk CSV: match ingredient names against pre-parsed CPDat data.
 */
async function enrichViaBulk(
  ingredients: MappedIngredient[],
  csvPath: string
): Promise<number> {
  console.log(`[enrichChemicals] Loading bulk CSV: ${csvPath}`);
  const rows = parseBulkCSV(csvPath);
  if (rows.length === 0) {
    console.error('[enrichChemicals] No rows parsed from CSV');
    return 0;
  }

  const nameIndex = buildNameIndex(rows);
  console.log(
    `[enrichChemicals] Built name index: ${nameIndex.size} unique chemical names`
  );

  let enriched = 0;

  for (const ing of ingredients) {
    const searchKey = (ing.normalized_token || ing.ingredient_raw)
      .toLowerCase()
      .trim();

    if (!searchKey) continue;

    const matches = nameIndex.get(searchKey);
    if (!matches || matches.length === 0) continue;

    // Take the first match (highest confidence from CPDat)
    const match = matches[0];

    const { error } = await supabase
      .from('material_chemical_constituents')
      .upsert(
        {
          material_id: ing.material_id,
          chemical_name: match.preferred_name,
          cas_number: match.casrn || null,
          pubchem_cid: null,
          functional_use: match.functional_use || null,
          weight_fraction: match.weight_fraction_predicted,
          source_name: 'cpdat_bulk',
          confidence: 0.70,
        },
        { onConflict: 'material_id,cas_number,source_name' }
      );

    if (error) {
      console.error(
        `[enrichChemicals] Bulk upsert error for ${searchKey}:`,
        error.message
      );
    } else {
      enriched++;
      if (enriched % 50 === 0) {
        console.log(`[enrichChemicals] Bulk enriched: ${enriched}`);
      }
    }
  }

  return enriched;
}

/**
 * PubChem CID enrichment pass.
 * For material_chemical_constituents rows that have a CAS number
 * but no pubchem_cid, look up the CID via PubChem REST API.
 */
async function enrichPubChemCIDs(): Promise<number> {
  const { data: rows, error } = await supabase
    .from('material_chemical_constituents')
    .select('id, cas_number, chemical_name')
    .not('cas_number', 'is', null)
    .is('pubchem_cid', null)
    .limit(500);

  if (error || !rows || rows.length === 0) {
    console.log('[enrichChemicals] No rows need PubChem CID enrichment');
    return 0;
  }

  console.log(`[enrichChemicals] PubChem CID lookup for ${rows.length} chemicals`);
  let updated = 0;

  for (const row of rows) {
    const cas = (row as any).cas_number;
    if (!cas) continue;

    try {
      const url = `https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/${encodeURIComponent(cas)}/cids/JSON`;
      const res = await fetch(url);

      if (!res.ok) continue;

      const json = (await res.json()) as Record<string, unknown>;
      const cids = (json as any)?.IdentifierList?.CID;
      if (!cids || cids.length === 0) continue;

      const cid = cids[0];

      const { error: updateError } = await supabase
        .from('material_chemical_constituents')
        .update({ pubchem_cid: cid })
        .eq('id', (row as any).id);

      if (!updateError) {
        updated++;
      }

      // PubChem rate limit: 5 requests/second
      await new Promise((r) => setTimeout(r, 250));
    } catch {
      // Skip on error, non-critical
      continue;
    }
  }

  return updated;
}

// ============================================================
// Main
// ============================================================

export async function enrichChemicals(): Promise<void> {
  console.log('[enrichChemicals] Fetching unprocessed ingredients...');
  const ingredients = await getUnprocessedIngredients();
  console.log(
    `[enrichChemicals] ${ingredients.length} materials to enrich`
  );

  if (ingredients.length === 0) {
    console.log('[enrichChemicals] Nothing to process');
    return;
  }

  let enriched = 0;

  if (CPDAT_CSV_PATH) {
    // Bulk CSV mode
    enriched = await enrichViaBulk(ingredients, CPDAT_CSV_PATH);
    console.log(
      `[enrichChemicals] Bulk CSV enrichment: ${enriched} chemicals`
    );
  } else if (USE_API) {
    // API mode (batched)
    for (let i = 0; i < ingredients.length; i += BATCH_SIZE) {
      const batch = ingredients.slice(i, i + BATCH_SIZE);
      const count = await enrichViaAPI(batch);
      enriched += count;
      console.log(
        `[enrichChemicals] API batch ${Math.floor(i / BATCH_SIZE) + 1}: ${count} enriched`
      );
    }
  } else {
    console.error(
      '[enrichChemicals] Neither CTX_API_KEY nor CPDAT_CSV_PATH is set. ' +
        'Set CTX_API_KEY for API mode or CPDAT_CSV_PATH for bulk CSV mode.'
    );
    return;
  }

  // PubChem CID enrichment pass
  console.log('[enrichChemicals] Running PubChem CID enrichment...');
  const cidCount = await enrichPubChemCIDs();
  console.log(`[enrichChemicals] PubChem CIDs added: ${cidCount}`);

  console.log(
    `[enrichChemicals] Complete. Total enriched: ${enriched}, PubChem CIDs: ${cidCount}`
  );
}
