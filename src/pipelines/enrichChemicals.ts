/**
 * Pipeline: Enrich Chemicals
 *
 * Takes mapped ingredients from ingredient_material_map and enriches
 * material_chemical_constituents with CAS numbers, PubChem CIDs,
 * functional use categories from CPDat.
 *
 * Column mapping:
 *   materials.material_name (not .name)
 *   ingredient_material_map.match_confidence (not .confidence)
 *   material_chemical_constituents.confidence (our table, correct as-is)
 */
import { supabase } from '../supabase.js';
import {
  resolveChemical,
  fetchChemicalRecord,
  parseBulkCSV,
  buildNameIndex,
} from '../connectors/cpdat.js';

const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 20;
const CPDAT_CSV_PATH = process.env.CPDAT_CSV_PATH || '';
const USE_API = !!process.env.CTX_API_KEY;

interface MappedIngredient {
  material_id: string;
  material_name: string;
  ingredient_raw: string;
  normalized_token: string;
}

async function getUnprocessedIngredients(): Promise<MappedIngredient[]> {
  // Get all material_ids that have been mapped
  const { data: mappings, error } = await supabase
    .from('ingredient_material_map')
    .select('material_id')
    .limit(5000);

  if (error || !mappings) {
    console.error('[enrichChemicals] Failed to fetch mappings:', error?.message);
    return [];
  }

  // Unique material IDs
  const materialIds = [...new Set(mappings.map((m) => m.material_id))];

  // Get material names
  const { data: materials } = await supabase
    .from('materials')
    .select('id, material_name, normalized_name')
    .in('id', materialIds);

  const nameMap = new Map(
    (materials || []).map((m) => [m.id, m.material_name || m.normalized_name || ''])
  );

  // Get materials that already have chemical constituents
  const { data: existing } = await supabase
    .from('material_chemical_constituents')
    .select('material_id')
    .in('material_id', materialIds);

  const alreadyEnriched = new Set(
    (existing || []).map((e) => e.material_id)
  );

  // Filter to materials not yet enriched
  const results: MappedIngredient[] = [];
  const seen = new Set<string>();

  for (const mapping of mappings) {
    const mid = mapping.material_id;
    if (alreadyEnriched.has(mid)) continue;
    if (seen.has(mid)) continue;
    seen.add(mid);

    results.push({
      material_id: mid,
      material_name: nameMap.get(mid) || '',
      ingredient_raw: nameMap.get(mid) || '',
      normalized_token: (nameMap.get(mid) || '').toLowerCase(),
    });
  }

  return results;
}

async function enrichViaAPI(ingredients: MappedIngredient[]): Promise<number> {
  let enriched = 0;

  for (const ing of ingredients) {
    const searchTerm = ing.normalized_token || ing.ingredient_raw;
    if (!searchTerm) continue;

    console.log(`[enrichChemicals] API lookup: ${searchTerm}`);

    const identity = await resolveChemical(searchTerm);
    if (!identity || !identity.dtxsid) {
      console.log(`[enrichChemicals] No DTXSID for: ${searchTerm}`);
      continue;
    }

    const record = await fetchChemicalRecord(identity.dtxsid);
    if (!record) {
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
          pubchem_cid: null,
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
    const cas = row.cas_number;
    if (!cas) continue;

    try {
      const url = `https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/${encodeURIComponent(cas)}/cids/JSON`;
      const res = await fetch(url);

      if (!res.ok) continue;

      const json = (await res.json()) as Record<string, unknown>;
      const idList = json.IdentifierList as Record<string, unknown> | undefined;
      const cids = idList?.CID as number[] | undefined;
      if (!cids || cids.length === 0) continue;

      const cid = cids[0];

      const { error: updateError } = await supabase
        .from('material_chemical_constituents')
        .update({ pubchem_cid: cid })
        .eq('id', row.id);

      if (!updateError) updated++;

      // PubChem rate limit: 5 requests/second
      await new Promise((r) => setTimeout(r, 250));
    } catch {
      continue;
    }
  }

  return updated;
}

export async function enrichChemicals(): Promise<void> {
  console.log('[enrichChemicals] Fetching unprocessed ingredients...');
  const ingredients = await getUnprocessedIngredients();
  console.log(`[enrichChemicals] ${ingredients.length} materials to enrich`);

  if (ingredients.length === 0) {
    console.log('[enrichChemicals] Nothing to process');
    return;
  }

  let enriched = 0;

  if (CPDAT_CSV_PATH) {
    enriched = await enrichViaBulk(ingredients, CPDAT_CSV_PATH);
    console.log(`[enrichChemicals] Bulk CSV enrichment: ${enriched} chemicals`);
  } else if (USE_API) {
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

  console.log('[enrichChemicals] Running PubChem CID enrichment...');
  const cidCount = await enrichPubChemCIDs();
  console.log(`[enrichChemicals] PubChem CIDs added: ${cidCount}`);

  console.log(
    `[enrichChemicals] Complete. Total enriched: ${enriched}, PubChem CIDs: ${cidCount}`
  );
}
