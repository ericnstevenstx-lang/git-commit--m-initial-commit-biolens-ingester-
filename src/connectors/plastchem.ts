/**
 * PlastChem Connector
 *
 * Parses the PlastChem database XLSX (v1.01) from Zenodo.
 * Uses the "Overview database" sheet which contains 17,933 chemicals with:
 *   - CAS RN, PubChem name, IUPAC name
 *   - Risk classification (Red/Orange/Watch/White/Grey list)
 *   - Hazard, Persistence, Bioaccumulation, Mobility, Toxicity scores
 *   - Harmonized functions, production volume
 */
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const XLSX = require('xlsx');
import fs from 'node:fs';

export interface PlastChemEntry {
  plastchem_id: number;
  cas_rn: string;
  chemical_name: string;          // pubchem_name
  iupac_name: string | null;
  list_classification: string;    // Red_list, Orange_list, etc.
  hazard_score: number | null;
  persistence_score: number | null;
  bioaccumulation_score: number | null;
  mobility_score: number | null;
  toxicity_score: number | null;
  harmonized_functions: string | null;
  production_volume_tons: number | null;
  mea_names: string | null;       // multilateral environmental agreement
  precedent_names: string | null;  // e.g. California_P65
  // Derived flags
  hazard: {
    persistent: boolean;
    bioaccumulative: boolean;
    mobile: boolean;
    toxic: boolean;
    cmr: boolean;
    endocrine_disruptor: boolean;
    of_concern: boolean;
  };
  function_category: string | null;
  specific_function: string | null;
  // Polymer associations are not in Overview sheet
  polymers: Record<string, boolean>;
  smiles: string | null;
  inchi: string | null;
  source_row: number;
}

/**
 * Parse the "Overview database" sheet from PlastChem XLSX.
 */
export function parsePlastChemDB(filePath: string): PlastChemEntry[] {
  if (!fs.existsSync(filePath)) {
    console.error(`[plastchem] File not found: ${filePath}`);
    return [];
  }

  console.log(`[plastchem] Reading: ${filePath}`);
  const workbook = XLSX.readFile(filePath, { type: 'file' });

  // Use "Overview database" sheet (the main data sheet)
  const targetSheet = 'Overview database';
  if (!workbook.SheetNames.includes(targetSheet)) {
    console.error(`[plastchem] Sheet "${targetSheet}" not found. Available: ${workbook.SheetNames.join(', ')}`);
    return [];
  }

  const sheet = workbook.Sheets[targetSheet];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' }) as Record<string, unknown>[];

  if (rows.length === 0) {
    console.error('[plastchem] No data rows found');
    return [];
  }

  console.log(`[plastchem] ${rows.length} rows in "${targetSheet}"`);

  const entries: PlastChemEntry[] = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    const cas = String(row['cas'] || '').trim();
    const name = String(row['pubchem_name'] || '').trim();

    // Skip rows without CAS or name
    if (!cas && !name) continue;
    // Skip invalid CAS (must contain hyphen)
    if (cas && !cas.includes('-')) continue;

    const listClass = String(row['PlastChem_lists'] || '').trim();
    const hazardScore = row['Hazard_score'] !== '' ? Number(row['Hazard_score']) : null;
    const persistScore = row['Persistence_score'] !== '' ? Number(row['Persistence_score']) : null;
    const bioaccumScore = row['Bioaccumulation_score'] !== '' ? Number(row['Bioaccumulation_score']) : null;
    const mobilityScore = row['Mobility_score'] !== '' ? Number(row['Mobility_score']) : null;
    const toxScore = row['Toxicity_score'] !== '' ? Number(row['Toxicity_score']) : null;
    const harmonized = String(row['Harmonized_functions'] || '').trim() || null;
    const prodVolume = row['total_production_volume_tons'] !== '' ? Number(row['total_production_volume_tons']) : null;

    // Derive hazard flags from scores and list classification
    const isRedOrOrange = listClass === 'Red_list' || listClass === 'Orange_list';
    const isMEA = listClass === 'MEA_list';

    entries.push({
      plastchem_id: Number(row['plastchem_ID'] || 0),
      cas_rn: cas,
      chemical_name: name,
      iupac_name: String(row['iupac_name'] || '').trim() || null,
      list_classification: listClass,
      hazard_score: hazardScore,
      persistence_score: persistScore,
      bioaccumulation_score: bioaccumScore,
      mobility_score: mobilityScore,
      toxicity_score: toxScore,
      harmonized_functions: harmonized,
      production_volume_tons: prodVolume,
      mea_names: String(row['MEA_names'] || '').trim() || null,
      precedent_names: String(row['Precedent_names'] || '').trim() || null,
      hazard: {
        persistent: (persistScore !== null && persistScore >= 1),
        bioaccumulative: (bioaccumScore !== null && bioaccumScore >= 1),
        mobile: (mobilityScore !== null && mobilityScore >= 1),
        toxic: (toxScore !== null && toxScore >= 1),
        cmr: isRedOrOrange && (hazardScore !== null && hazardScore >= 2),
        endocrine_disruptor: false, // Not directly in this sheet
        of_concern: isRedOrOrange || isMEA,
      },
      function_category: harmonized ? harmonized.split(';')[0]?.trim() || null : null,
      specific_function: harmonized && harmonized.includes(';') ? harmonized.split(';').slice(1).join(';').trim() : null,
      polymers: {},  // Not in Overview sheet
      smiles: null,  // Not in Overview sheet
      inchi: null,   // Not in Overview sheet
      source_row: i + 2,
    });
  }

  console.log(`[plastchem] Parsed ${entries.length} valid chemical entries`);

  // Stats
  const redCount = entries.filter(e => e.list_classification === 'Red_list').length;
  const orangeCount = entries.filter(e => e.list_classification === 'Orange_list').length;
  const meaCount = entries.filter(e => e.list_classification === 'MEA_list').length;
  console.log(`[plastchem] Red: ${redCount}, Orange: ${orangeCount}, MEA: ${meaCount}`);

  return entries;
}

/**
 * Parse the hazard information XLSX (placeholder — not needed for Overview sheet).
 */
export interface PlastChemHazardEntry {
  cas_rn: string;
  chemical_name: string;
  hazard_source: string;
  hazard_category: string;
  hazard_detail: string | null;
}

export function parsePlastChemHazards(filePath: string): PlastChemHazardEntry[] {
  if (!fs.existsSync(filePath)) {
    console.error(`[plastchem] Hazard file not found: ${filePath}`);
    return [];
  }

  const workbook = XLSX.readFile(filePath, { type: 'file' });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' }) as Record<string, unknown>[];

  console.log(`[plastchem] Hazard file: ${rows.length} rows, sheet: ${sheetName}`);

  const headers = Object.keys(rows[0] || {});
  const colCAS = headers.find(h => h.toLowerCase().includes('cas')) || null;

  if (!colCAS) {
    console.error('[plastchem] Cannot find CAS column in hazard file');
    return [];
  }

  return rows
    .filter(r => {
      const cas = String(r[colCAS!] || '').trim();
      return cas && cas.includes('-');
    })
    .map(r => ({
      cas_rn: String(r[colCAS!]).trim(),
      chemical_name: String(r[headers.find(h => h.toLowerCase().includes('name')) || ''] || '').trim(),
      hazard_source: 'plastchem',
      hazard_category: String(r[headers.find(h => h.toLowerCase().includes('hazard')) || ''] || '').trim(),
      hazard_detail: null,
    }));
}
