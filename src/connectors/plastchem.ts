/**
 * PlastChem Connector
 *
 * Parses the PlastChem database XLSX (v1.01) from Zenodo:
 * https://zenodo.org/records/15397723
 *
 * The main file (plastchem_db_v1.01.xlsx) contains:
 *   - Chemical identifiers (CAS RN, name, SMILES, InChI)
 *   - Polymer associations (PE, PP, PVC, PET, PS, etc.)
 *   - Function categories (additive, processing aid, monomer, NIAS)
 *   - Hazard flags (persistent, bioaccumulative, mobile, toxic)
 *
 * The hazard file (Hazard_information_PlastChem_v1.01.xlsx) contains:
 *   - Detailed hazard classifications per chemical
 *
 * Uses SheetJS (xlsx) to parse .xlsx files.
 */
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const XLSX = require('xlsx');
import fs from 'node:fs';

export interface PlastChemEntry {
  cas_rn: string;
  chemical_name: string;
  smiles: string | null;
  inchi: string | null;
  function_category: string | null;  // additive, processing_aid, monomer, nias
  specific_function: string | null;  // plasticizer, flame_retardant, stabilizer, etc.
  // Polymer associations (true = found in this polymer type)
  polymers: {
    PE: boolean;
    PP: boolean;
    PVC: boolean;
    PET: boolean;
    PS: boolean;
    PU: boolean;
    ABS: boolean;
    PA: boolean;
    PC: boolean;
    PMMA: boolean;
    PLA: boolean;
    rubber: boolean;
    silicone: boolean;
    other: boolean;
  };
  // Hazard flags
  hazard: {
    persistent: boolean;
    bioaccumulative: boolean;
    mobile: boolean;
    toxic: boolean;
    cmr: boolean;  // carcinogenic, mutagenic, reproductive toxicant
    endocrine_disruptor: boolean;
    of_concern: boolean;  // overall "chemical of concern" flag
  };
  source_row: number;
}

export interface PlastChemHazardEntry {
  cas_rn: string;
  chemical_name: string;
  hazard_source: string;
  hazard_category: string;
  hazard_detail: string | null;
}

/**
 * Helper to check if a cell value represents "true" / "yes" / "1" / "x"
 */
function isTruthy(val: unknown): boolean {
  if (val === null || val === undefined || val === '') return false;
  const s = String(val).toLowerCase().trim();
  return s === '1' || s === 'yes' || s === 'true' || s === 'x' || s === 'y';
}

/**
 * Find a column by checking multiple possible header names.
 * Returns the first matching header found.
 */
function findColumn(
  headers: string[],
  candidates: string[]
): string | null {
  const lowerHeaders = headers.map((h) => h.toLowerCase().trim());
  for (const candidate of candidates) {
    const idx = lowerHeaders.indexOf(candidate.toLowerCase());
    if (idx >= 0) return headers[idx];
  }
  // Partial match
  for (const candidate of candidates) {
    const found = headers.find((h) =>
      h.toLowerCase().includes(candidate.toLowerCase())
    );
    if (found) return found;
  }
  return null;
}

/**
 * Parse the main PlastChem database XLSX.
 */
export function parsePlastChemDB(filePath: string): PlastChemEntry[] {
  if (!fs.existsSync(filePath)) {
    console.error(`[plastchem] File not found: ${filePath}`);
    return [];
  }

  console.log(`[plastchem] Reading: ${filePath}`);
  const workbook = XLSX.readFile(filePath, { type: 'file' });

  // Use first sheet
  const sheetName = workbook.SheetNames[0];
  console.log(`[plastchem] Sheet: ${sheetName}`);

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' }) as Record<
    string,
    unknown
  >[];

  if (rows.length === 0) {
    console.error('[plastchem] No data rows found');
    return [];
  }

  // Detect column names from first row keys
  const headers = Object.keys(rows[0]);
  console.log(`[plastchem] ${rows.length} rows, ${headers.length} columns`);
  console.log(`[plastchem] Sample headers: ${headers.slice(0, 15).join(', ')}`);

  // Map columns flexibly
  const colCAS = findColumn(headers, ['CAS RN', 'CAS', 'cas_rn', 'CASRN', 'CAS_RN']);
  const colName = findColumn(headers, [
    'Chemical name', 'Name', 'chemical_name', 'Substance name', 'preferred_name',
  ]);
  const colSMILES = findColumn(headers, ['SMILES', 'smiles', 'Canonical SMILES']);
  const colInChI = findColumn(headers, ['InChI', 'inchi', 'InChIKey']);
  const colFunction = findColumn(headers, [
    'Function', 'function', 'Function category', 'function_category',
    'Functional use', 'Top-level function',
  ]);
  const colSpecificFunc = findColumn(headers, [
    'Specific function', 'specific_function', 'Sub-function',
    'Detailed function', 'Function detail',
  ]);

  if (!colCAS && !colName) {
    console.error('[plastchem] Cannot find CAS or Name columns');
    console.error('[plastchem] Available headers:', headers.join(', '));
    return [];
  }

  console.log(`[plastchem] CAS column: ${colCAS}`);
  console.log(`[plastchem] Name column: ${colName}`);
  console.log(`[plastchem] Function column: ${colFunction}`);

  // Polymer columns - search by common abbreviations
  const polymerCols: Record<string, string | null> = {
    PE: findColumn(headers, ['PE', 'Polyethylene']),
    PP: findColumn(headers, ['PP', 'Polypropylene']),
    PVC: findColumn(headers, ['PVC', 'Polyvinyl chloride']),
    PET: findColumn(headers, ['PET', 'Polyethylene terephthalate']),
    PS: findColumn(headers, ['PS', 'Polystyrene']),
    PU: findColumn(headers, ['PU', 'PUR', 'Polyurethane']),
    ABS: findColumn(headers, ['ABS']),
    PA: findColumn(headers, ['PA', 'Polyamide', 'Nylon']),
    PC: findColumn(headers, ['PC', 'Polycarbonate']),
    PMMA: findColumn(headers, ['PMMA', 'Polymethyl methacrylate']),
    PLA: findColumn(headers, ['PLA', 'Polylactic acid', 'Bioplastics']),
    rubber: findColumn(headers, ['Rubber', 'rubber']),
    silicone: findColumn(headers, ['Silicone', 'silicone']),
    other: findColumn(headers, ['Other polymers', 'Other', 'other']),
  };

  // Hazard columns
  const colPersistent = findColumn(headers, ['P', 'Persistent', 'persistent', 'Persistence']);
  const colBioaccum = findColumn(headers, ['B', 'Bioaccumulative', 'bioaccumulative', 'Bioaccumulation']);
  const colMobile = findColumn(headers, ['M', 'Mobile', 'mobile', 'Mobility']);
  const colToxic = findColumn(headers, ['T', 'Toxic', 'toxic', 'Toxicity']);
  const colCMR = findColumn(headers, ['CMR', 'cmr', 'Carcinogenic']);
  const colED = findColumn(headers, ['ED', 'Endocrine', 'endocrine_disruptor']);
  const colConcern = findColumn(headers, [
    'Chemical of concern', 'CoC', 'of_concern', 'Concern', 'Priority',
  ]);

  const entries: PlastChemEntry[] = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const cas = colCAS ? String(row[colCAS] || '').trim() : '';
    const name = colName ? String(row[colName] || '').trim() : '';

    // Skip rows without both CAS and name
    if (!cas && !name) continue;
    // Skip invalid CAS patterns (must have at least one hyphen)
    if (cas && !cas.includes('-') && cas.length > 0) continue;

    entries.push({
      cas_rn: cas,
      chemical_name: name,
      smiles: colSMILES ? String(row[colSMILES] || '') || null : null,
      inchi: colInChI ? String(row[colInChI] || '') || null : null,
      function_category: colFunction ? String(row[colFunction] || '') || null : null,
      specific_function: colSpecificFunc ? String(row[colSpecificFunc] || '') || null : null,
      polymers: {
        PE: polymerCols.PE ? isTruthy(row[polymerCols.PE]) : false,
        PP: polymerCols.PP ? isTruthy(row[polymerCols.PP]) : false,
        PVC: polymerCols.PVC ? isTruthy(row[polymerCols.PVC]) : false,
        PET: polymerCols.PET ? isTruthy(row[polymerCols.PET]) : false,
        PS: polymerCols.PS ? isTruthy(row[polymerCols.PS]) : false,
        PU: polymerCols.PU ? isTruthy(row[polymerCols.PU]) : false,
        ABS: polymerCols.ABS ? isTruthy(row[polymerCols.ABS]) : false,
        PA: polymerCols.PA ? isTruthy(row[polymerCols.PA]) : false,
        PC: polymerCols.PC ? isTruthy(row[polymerCols.PC]) : false,
        PMMA: polymerCols.PMMA ? isTruthy(row[polymerCols.PMMA]) : false,
        PLA: polymerCols.PLA ? isTruthy(row[polymerCols.PLA]) : false,
        rubber: polymerCols.rubber ? isTruthy(row[polymerCols.rubber]) : false,
        silicone: polymerCols.silicone ? isTruthy(row[polymerCols.silicone]) : false,
        other: polymerCols.other ? isTruthy(row[polymerCols.other]) : false,
      },
      hazard: {
        persistent: colPersistent ? isTruthy(row[colPersistent]) : false,
        bioaccumulative: colBioaccum ? isTruthy(row[colBioaccum]) : false,
        mobile: colMobile ? isTruthy(row[colMobile]) : false,
        toxic: colToxic ? isTruthy(row[colToxic]) : false,
        cmr: colCMR ? isTruthy(row[colCMR]) : false,
        endocrine_disruptor: colED ? isTruthy(row[colED]) : false,
        of_concern: colConcern ? isTruthy(row[colConcern]) : false,
      },
      source_row: i + 2, // +2 for header row + 0-index
    });
  }

  console.log(`[plastchem] Parsed ${entries.length} valid chemical entries`);
  return entries;
}

/**
 * Parse the hazard information XLSX.
 */
export function parsePlastChemHazards(filePath: string): PlastChemHazardEntry[] {
  if (!fs.existsSync(filePath)) {
    console.error(`[plastchem] Hazard file not found: ${filePath}`);
    return [];
  }

  const workbook = XLSX.readFile(filePath, { type: 'file' });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' }) as Record<
    string,
    unknown
  >[];

  const headers = Object.keys(rows[0] || {});
  const colCAS = findColumn(headers, ['CAS RN', 'CAS', 'cas_rn', 'CASRN']);
  const colName = findColumn(headers, ['Chemical name', 'Name', 'chemical_name']);

  if (!colCAS) {
    console.error('[plastchem] Cannot find CAS column in hazard file');
    return [];
  }

  console.log(`[plastchem] Hazard file: ${rows.length} rows`);

  return rows
    .filter((r) => {
      const cas = String(r[colCAS!] || '').trim();
      return cas && cas.includes('-');
    })
    .map((r) => ({
      cas_rn: String(r[colCAS!]).trim(),
      chemical_name: colName ? String(r[colName] || '').trim() : '',
      hazard_source: String(r[findColumn(headers, ['Source', 'source']) || ''] || 'plastchem').trim(),
      hazard_category: String(
        r[findColumn(headers, ['Hazard', 'hazard_category', 'Category']) || ''] || ''
      ).trim(),
      hazard_detail: String(
        r[findColumn(headers, ['Detail', 'hazard_detail', 'Description']) || ''] || ''
      ).trim() || null,
    }));
}
