/**
 * Pipeline: Ingest PlastChem
 *
 * Reads the PlastChem XLSX (16K+ chemicals in plastics) and:
 * 1. Matches chemicals to existing BioLens materials by CAS number or name
 * 2. Upserts into material_chemical_constituents with hazard/function data
 * 3. For polymer-associated chemicals, links to the corresponding polymer material
 * 4. Populates material_identifiers with CAS RN, SMILES, InChI
 *    (this unblocks the reverse GTIN lookup pipeline)
 *
 * This enriches the Toxicity/Exposure Intelligence panel with research-grade
 * hazard data specifically for petrochemical materials.
 */
import { supabase } from '../supabase.js';
import { parsePlastChemDB, PlastChemEntry } from '../connectors/plastchem.js';
import path from 'node:path';

const DEFAULT_DB_PATH = path.resolve(process.cwd(), 'plastchem_db_v1.01.xlsx');

// Map PlastChem polymer keys to BioLens material names
const POLYMER_MATERIAL_MAP: Record<string, string[]> = {
  PE: ['polyethylene', 'hdpe', 'ldpe', 'expanded polyethylene', 'pe lined paper'],
  PP: ['polypropylene', 'nonwoven polypropylene', 'recycled pp', 'pp trigger sprayer', 'pp woven fabric', 'meltblown polypropylene', 'spunbond polypropylene'],
  PVC: ['pvc', 'pvc leather', 'vinyl', 'vinyl coating', 'vinyl chloride', 'vinyl glove material'],
  PET: ['pet plastic', 'polyester', 'recycled polyester', 'pet film', 'petg', 'mylar', 'dimethyl terephthalate', 'terephthalic acid'],
  PS: ['polystyrene', 'extruded polystyrene', 'expanded polystyrene', 'styrene'],
  PU: ['polyurethane', 'pu foam'],
  ABS: ['abs'],
  PA: ['nylon', 'polyamide'],
  PC: ['polycarbonate'],
  PMMA: ['acrylic', 'acrylic resin', 'methyl methacrylate'],
  PLA: ['pla', 'polylactic acid'],
  rubber: ['rubber', 'natural rubber', 'synthetic rubber'],
  silicone: ['silicone', 'dimethicone'],
};

interface MaterialMatch {
  material_id: string;
  material_name: string;
}

/**
 * Build a lookup cache of CAS -> material_id and normalized_name -> material_id.
 */
async function buildMaterialLookup(): Promise<{
  byName: Map<string, MaterialMatch>;
  byCAS: Map<string, MaterialMatch>;
}> {
  // Fetch materials with the actual column names
  const { data: materials, error } = await supabase
    .from('materials')
    .select('id, material_name, normalized_name, cas_rn');

  if (error || !materials) {
    console.error('[plastchem] Failed to fetch materials:', error?.message);
    return { byName: new Map(), byCAS: new Map() };
  }

  const byName = new Map<string, MaterialMatch>();
  const byCAS = new Map<string, MaterialMatch>();

  for (const m of materials) {
    const name = m.material_name;
    if (!name) continue;
    const match: MaterialMatch = { material_id: m.id, material_name: name };

    byName.set(name.toLowerCase(), match);
    if (m.normalized_name) {
      byName.set(m.normalized_name.toLowerCase(), match);
    }
    if (m.cas_rn) {
      byCAS.set(m.cas_rn.trim(), match);
    }
  }

  // Also check material_aliases for broader matching
  const { data: aliases } = await supabase
    .from('material_aliases')
    .select('material_id, alias, normalized_alias');

  if (aliases) {
    for (const a of aliases) {
      const mat = materials.find((m: any) => m.id === a.material_id);
      if (!mat) continue;
      const match: MaterialMatch = { material_id: mat.id, material_name: mat.material_name };
      if (a.alias) byName.set(a.alias.toLowerCase(), match);
      if (a.normalized_alias) byName.set(a.normalized_alias.toLowerCase(), match);
    }
  }

  return { byName, byCAS };
}

/**
 * Find all BioLens materials that correspond to a PlastChem polymer type.
 */
function findPolymerMaterials(
  polymerKey: string,
  byName: Map<string, MaterialMatch>
): MaterialMatch[] {
  const names = POLYMER_MATERIAL_MAP[polymerKey] || [];
  const matches: MaterialMatch[] = [];

  for (const name of names) {
    const match = byName.get(name.toLowerCase());
    if (match) matches.push(match);
  }

  return matches;
}

/**
 * Build the functional_use string from PlastChem data.
 */
function buildFunctionalUse(entry: PlastChemEntry): string {
  const parts: string[] = [];

  if (entry.function_category) parts.push(entry.function_category);
  if (entry.specific_function) parts.push(entry.specific_function);

  const hazards: string[] = [];
  if (entry.hazard.persistent) hazards.push('persistent');
  if (entry.hazard.bioaccumulative) hazards.push('bioaccumulative');
  if (entry.hazard.mobile) hazards.push('mobile');
  if (entry.hazard.toxic) hazards.push('toxic');
  if (entry.hazard.cmr) hazards.push('CMR');
  if (entry.hazard.endocrine_disruptor) hazards.push('endocrine_disruptor');

  if (hazards.length > 0) {
    parts.push(`[hazard: ${hazards.join(', ')}]`);
  }

  return parts.join(' | ') || 'plastic chemical';
}

/**
 * Populate material_identifiers with CAS RN, SMILES, InChI from PlastChem.
 * This unblocks the reverse GTIN lookup pipeline which queries this table.
 */
async function populateMaterialIdentifiers(
  entries: PlastChemEntry[],
  byName: Map<string, MaterialMatch>,
  byCAS: Map<string, MaterialMatch>
): Promise<number> {
  console.log('[plastchem] Populating material_identifiers...');
  let inserted = 0;

  // Batch to reduce API calls
  const identifierRows: Array<{
    material_id: string;
    id_type: string;
    id_value: string;
    is_primary: boolean;
    source_code: string;
    confidence: number;
  }> = [];

  for (const entry of entries) {
    // Find the matching material
    const match = entry.cas_rn
      ? byCAS.get(entry.cas_rn) || byName.get(entry.chemical_name.toLowerCase())
      : byName.get(entry.chemical_name.toLowerCase());

    if (!match) continue;

    // Add CAS RN
    if (entry.cas_rn) {
      identifierRows.push({
        material_id: match.material_id,
        id_type: 'cas_rn',
        id_value: entry.cas_rn,
        is_primary: true,
        source_code: 'plastchem',
        confidence: 0.95,
      });
    }

    // Add SMILES
    if (entry.smiles) {
      identifierRows.push({
        material_id: match.material_id,
        id_type: 'smiles',
        id_value: entry.smiles,
        is_primary: false,
        source_code: 'plastchem',
        confidence: 0.90,
      });
    }

    // Add InChI
    if (entry.inchi) {
      identifierRows.push({
        material_id: match.material_id,
        id_type: entry.inchi.startsWith('InChIKey=') ? 'inchikey' : 'inchi',
        id_value: entry.inchi,
        is_primary: false,
        source_code: 'plastchem',
        confidence: 0.90,
      });
    }
  }

  // Batch upsert in chunks of 200
  const BATCH = 200;
  for (let i = 0; i < identifierRows.length; i += BATCH) {
    const chunk = identifierRows.slice(i, i + BATCH);
    const { error } = await supabase
      .from('material_identifiers')
      .upsert(chunk, {
        onConflict: 'id_type,id_value',
        ignoreDuplicates: true,
      });

    if (error) {
      console.error(`[plastchem] Identifier upsert error (batch ${Math.floor(i / BATCH) + 1}):`, error.message);
    } else {
      inserted += chunk.length;
    }
  }

  console.log(`[plastchem] Inserted ${inserted} material identifiers`);
  return inserted;
}

export async function ingestPlastChem(dbPath?: string): Promise<void> {
  const filePath = dbPath || process.env.PLASTCHEM_DB_PATH || DEFAULT_DB_PATH;

  // Parse the XLSX
  const entries = parsePlastChemDB(filePath);
  if (entries.length === 0) {
    console.error('[plastchem] No entries parsed');
    return;
  }

  // Build material lookup
  const { byName, byCAS } = await buildMaterialLookup();
  console.log(
    `[plastchem] Material lookup: ${byName.size} by name, ${byCAS.size} by CAS`
  );

  // === Phase 1: Populate material_identifiers (CAS, SMILES, InChI) ===
  await populateMaterialIdentifiers(entries, byName, byCAS);

  // === Phase 1b: Backfill cas_rn on materials table where we matched ===
  console.log('[plastchem] Backfilling cas_rn on materials table...');
  let casBackfilled = 0;
  for (const entry of entries) {
    if (!entry.cas_rn) continue;
    const match = byName.get(entry.chemical_name.toLowerCase());
    if (!match) continue;
    // Only update if material doesn't already have a CAS
    const { error: updateErr } = await supabase
      .from('materials')
      .update({ cas_rn: entry.cas_rn })
      .eq('id', match.material_id)
      .is('cas_rn', null);
    if (!updateErr) casBackfilled++;
  }
  console.log(`[plastchem] Backfilled ${casBackfilled} CAS numbers on materials`);

  // === Phase 2: Upsert into material_chemical_constituents ===
  let upserted = 0;
  let skipped = 0;
  let matched = 0;

  for (const entry of entries) {
    // Strategy 1: Match chemical CAS to existing material CAS
    const directMatch = entry.cas_rn ? byCAS.get(entry.cas_rn) : null;

    // Strategy 2: Match chemical name to existing material name
    const nameMatch = !directMatch
      ? byName.get(entry.chemical_name.toLowerCase())
      : null;

    // Strategy 3: Find polymer materials this chemical is associated with
    const polymerMatches: MaterialMatch[] = [];
    for (const [polymerKey, isPresent] of Object.entries(entry.polymers)) {
      if (isPresent) {
        polymerMatches.push(...findPolymerMaterials(polymerKey, byName));
      }
    }

    // Deduplicate polymer matches
    const seenIds = new Set<string>();
    const uniquePolymerMatches = polymerMatches.filter((m) => {
      if (seenIds.has(m.material_id)) return false;
      seenIds.add(m.material_id);
      return true;
    });

    // Collect all target material_ids
    const targets: MaterialMatch[] = [];
    if (directMatch) targets.push(directMatch);
    if (nameMatch && !targets.find((t) => t.material_id === nameMatch.material_id)) {
      targets.push(nameMatch);
    }
    for (const pm of uniquePolymerMatches) {
      if (!targets.find((t) => t.material_id === pm.material_id)) {
        targets.push(pm);
      }
    }

    if (targets.length === 0) {
      skipped++;
      continue;
    }

    matched++;
    const functionalUse = buildFunctionalUse(entry);

    // Upsert for each target material
    for (const target of targets) {
      const { error } = await supabase
        .from('material_chemical_constituents')
        .upsert(
          {
            material_id: target.material_id,
            chemical_name: entry.chemical_name,
            cas_number: entry.cas_rn || null,
            pubchem_cid: null,
            functional_use: functionalUse,
            weight_fraction: null,
            source_name: 'plastchem',
            confidence: entry.hazard.of_concern ? 0.85 : 0.70,
          },
          { onConflict: 'material_id,cas_number,source_name' }
        );

      if (error) {
        if (!entry.cas_rn) {
          const { error: err2 } = await supabase
            .from('material_chemical_constituents')
            .insert({
              material_id: target.material_id,
              chemical_name: entry.chemical_name,
              cas_number: null,
              pubchem_cid: null,
              functional_use: functionalUse,
              weight_fraction: null,
              source_name: 'plastchem',
              confidence: entry.hazard.of_concern ? 0.85 : 0.70,
            });
          if (!err2) upserted++;
        }
      } else {
        upserted++;
      }
    }

    if (matched % 500 === 0) {
      console.log(
        `[plastchem] Progress: ${matched} matched, ${upserted} upserted, ${skipped} skipped`
      );
    }
  }

  // Summary
  const concernCount = entries.filter((e) => e.hazard.of_concern).length;
  console.log(`[plastchem] Complete.`);
  console.log(`  Entries parsed: ${entries.length}`);
  console.log(`  Matched to materials: ${matched}`);
  console.log(`  Upserted constituents: ${upserted}`);
  console.log(`  Skipped (no match): ${skipped}`);
  console.log(`  Chemicals of concern in source: ${concernCount}`);
}
