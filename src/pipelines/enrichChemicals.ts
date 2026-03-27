/**
 * Pipeline: Enrich Chemicals
 *
 * Takes mapped ingredients from ingredient_material_map and enriches
 * material_chemical_constituents with CAS numbers, PubChem CIDs,
 * functional use categories from CPDat.
 *
 * Column mapping:
 *   materials.material_name
 *   ingredient_material_map.match_confidence
 *   material_chemical_constituents.confidence
 */

import { supabase } from '../supabase.js';
import {
  resolveChemical,
  fetchChemicalRecord,
  parseBulkCSV,
  buildNameIndex,
} from '../connectors/cpdat.js';

const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 50;
const CPDAT_CSV_PATH = process.env.CPDAT_CSV_PATH || '';
const USE_API = !!process.env.CTX_API_KEY;
const MATERIAL_SCAN_LIMIT = Number(process.env.ENRICH_MATERIAL_LIMIT) || 20000;
const TARGET_ENRICH_LIMIT = Number(process.env.ENRICH_TARGET_LIMIT) || 200;

interface MappedIngredient {
  material_id: string;
  material_name: string;
  ingredient_raw: string;
  normalized_token: string;
  usage_count?: number;
}

async function getUnprocessedIngredients(): Promise<MappedIngredient[]> {
  // Pull a larger pool of mapped materials so enrichment is not starved.
  const { data: mappings, error } = await supabase
    .from('ingredient_material_map')
    .select('material_id')
    .limit(MATERIAL_SCAN_LIMIT);

  if (error || !mappings) {
    console.error('[enrichChemicals] Failed to fetch mappings:', error?.message);
    return [];
  }

  const usageMap = new Map<string, number>();
  for (const row of mappings) {
    const current = usageMap.get(row.material_id) || 0;
    usageMap.set(row.material_id, current + 1);
  }

  const materialIds = [...usageMap.keys()];
  if (materialIds.length === 0) return [];

  const { data: materials, error: materialsError } = await supabase
    .from('materials')
    .select('id, material_name, normalized_name')
    .in('id', materialIds);

  if (materialsError) {
    console.error('[enrichChemicals] Failed to fetch materials:', materialsError.message);
    return [];
  }

  const nameMap = new Map<string, string>();
  for (const m of materials || []) {
    nameMap.set(m.id, m.material_name || m.normalized_name || '');
  }

  const { data: existing, error: existingError } = await supabase
    .from('material_chemical_constituents')
    .select('material_id')
    .in('material_id', materialIds);

  if (existingError) {
    console.error('[enrichChemicals] Failed to fetch existing constituents:', existingError.message);
    return [];
  }

  const alreadyEnriched = new Set((existing || []).map((e) => e.material_id));

  const results: MappedIngredient[] = [];

  for (const [materialId, count] of usageMap.entries()) {
    if (alreadyEnriched.has(materialId)) continue;

    const materialName = nameMap.get(materialId) || '';
    if (!materialName.trim()) continue;

    results.push({
      material_id: materialId,
      material_name: materialName,
      ingredient_raw: materialName,
      normalized_token: materialName.toLowerCase().trim(),
      usage_count: count,
    });
  }

  results.sort((a, b) => (b.usage_count || 0) - (a.usage_count || 0));

  return results.slice(0, TARGET_ENRICH_LIMIT);
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

      if (error) {
        console.error(`[enrichChemicals] API upsert error for ${searchTerm}:`, error.message);
      } else {
        enriched++;
      }
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
      console.error(`[enrichChemicals] Upsert error for ${searchTerm}:`, error.message);
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
          confidence: 0.7,
        },
        { onConflict: 'material_id,cas_number,source_name' }
      );

    if (error) {
      console.error(`[enrichChemicals] Bulk upsert error for ${searchKey}:`, error.message);
    } else {
      enriched++;
      if (enriched % 25 === 0) {
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
      const url = `https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/${encodeURIComponent(
        cas
      )}/cids/JSON`;

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
