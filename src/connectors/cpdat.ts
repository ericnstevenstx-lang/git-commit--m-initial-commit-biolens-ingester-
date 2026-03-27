/**
 * CPDat Connector
 *
 * Two access modes:
 * 1. CTX Exposure API (requires free API key from ccte_api@epa.gov)
 * 2. Bulk CSV parsing from Figshare download (CPDat v4.0)
 *
 * API docs: https://api-ccte.epa.gov/docs/exposure.html
 */
import fs from 'node:fs';
import path from 'node:path';
import { parse } from 'csv-parse/sync';

const CTX_API_BASE = 'https://api-ccte.epa.gov/exposure';
const CTX_CHEM_BASE = 'https://api-ccte.epa.gov/chemical';
const CTX_API_KEY = process.env.CTX_API_KEY || '';
const RATE_LIMIT_MS = Number(process.env.CTX_RATE_LIMIT_MS) || 500;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface CPDatFunctionalUse {
  dtxsid: string;
  casrn: string | null;
  chemical_name: string;
  functional_category: string;
  reported_function: string;
  source: string;
}

export interface CPDatProductUse {
  dtxsid: string;
  casrn: string | null;
  chemical_name: string;
  product_category: string;
  product_family: string;
  product_type: string;
  puc_kind: string;
}

export interface CPDatChemicalRecord {
  dtxsid: string;
  casrn: string | null;
  chemical_name: string;
  functional_uses: CPDatFunctionalUse[];
  product_uses: CPDatProductUse[];
}

export interface ChemicalIdentity {
  dtxsid: string;
  casrn: string | null;
  preferred_name: string;
  molecular_formula: string | null;
}

function ctxHeaders(): Record<string, string> {
  if (!CTX_API_KEY) {
    throw new Error(
      '[cpdat] CTX_API_KEY not set. Request a free key from ccte_api@epa.gov'
    );
  }
  return {
    Accept: 'application/json',
    'x-api-key': CTX_API_KEY,
  };
}

export async function resolveChemical(
  identifier: string
): Promise<ChemicalIdentity | null> {
  const url = `${CTX_CHEM_BASE}/search/by-word/${encodeURIComponent(identifier)}`;

  try {
    const res = await fetch(url, { headers: ctxHeaders() });
    if (!res.ok) {
      console.log(`[cpdat] Chemical search ${res.status}: ${identifier}`);
      return null;
    }
    const results = (await res.json()) as Record<string, unknown>[];
    if (!results || results.length === 0) return null;

    const top = results[0];
    await sleep(RATE_LIMIT_MS);
    return {
      dtxsid: String(top.dtxsid || ''),
      casrn: (top.casrn as string) || null,
      preferred_name: String(top.preferredName || top.preferred_name || identifier),
      molecular_formula: (top.molecularFormula as string) || null,
    };
  } catch (err) {
    console.error(`[cpdat] Resolve error for ${identifier}:`, (err as Error).message);
    return null;
  }
}

export async function fetchFunctionalUses(
  dtxsid: string
): Promise<CPDatFunctionalUse[]> {
  const url = `${CTX_API_BASE}/cpdat/fc/${dtxsid}`;

  try {
    const res = await fetch(url, { headers: ctxHeaders() });
    if (!res.ok) {
      console.log(`[cpdat] Functional use ${res.status}: ${dtxsid}`);
      return [];
    }
    const data = (await res.json()) as Record<string, unknown>[];
    await sleep(RATE_LIMIT_MS);

    return (data || []).map((d) => ({
      dtxsid,
      casrn: (d.casrn as string) || null,
      chemical_name: String(d.chemicalName || d.chemical_name || ''),
      functional_category: String(d.harmonizedFunctionalUse || d.functionCategory || ''),
      reported_function: String(d.reportedFunction || d.reported_function || ''),
      source: String(d.dataSource || d.source || 'cpdat'),
    }));
  } catch (err) {
    console.error(`[cpdat] FC fetch error ${dtxsid}:`, (err as Error).message);
    return [];
  }
}

export async function fetchProductUses(
  dtxsid: string
): Promise<CPDatProductUse[]> {
  const url = `${CTX_API_BASE}/cpdat/puc/${dtxsid}`;

  try {
    const res = await fetch(url, { headers: ctxHeaders() });
    if (!res.ok) {
      console.log(`[cpdat] Product use ${res.status}: ${dtxsid}`);
      return [];
    }
    const data = (await res.json()) as Record<string, unknown>[];
    await sleep(RATE_LIMIT_MS);

    return (data || []).map((d) => ({
      dtxsid,
      casrn: (d.casrn as string) || null,
      chemical_name: String(d.chemicalName || d.chemical_name || ''),
      product_category: String(d.productCategory || d.pucName || ''),
      product_family: String(d.productFamily || d.pucFamily || ''),
      product_type: String(d.productType || d.pucType || ''),
      puc_kind: String(d.pucKind || d.kind || 'unknown'),
    }));
  } catch (err) {
    console.error(`[cpdat] PUC fetch error ${dtxsid}:`, (err as Error).message);
    return [];
  }
}

export async function fetchChemicalRecord(
  dtxsid: string
): Promise<CPDatChemicalRecord | null> {
  const [functionalUses, productUses] = await Promise.all([
    fetchFunctionalUses(dtxsid),
    fetchProductUses(dtxsid),
  ]);

  if (functionalUses.length === 0 && productUses.length === 0) return null;

  return {
    dtxsid,
    casrn: functionalUses[0]?.casrn || productUses[0]?.casrn || null,
    chemical_name: functionalUses[0]?.chemical_name || productUses[0]?.chemical_name || dtxsid,
    functional_uses: functionalUses,
    product_uses: productUses,
  };
}

export interface BulkChemicalRow {
  dtxsid: string;
  casrn: string;
  preferred_name: string;
  functional_use: string;
  product_use_category: string;
  weight_fraction_lower: number | null;
  weight_fraction_upper: number | null;
  weight_fraction_predicted: number | null;
  source_document: string;
}

export function parseBulkCSV(filePath: string): BulkChemicalRow[] {
  if (!fs.existsSync(filePath)) {
    console.error(`[cpdat] File not found: ${filePath}`);
    return [];
  }

  const content = fs.readFileSync(filePath, 'utf-8');
  const records = parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  }) as Record<string, string>[];

  console.log(`[cpdat] Parsed ${records.length} rows from ${path.basename(filePath)}`);

  if (records.length > 0) {
    console.log('[cpdat] Sample headers:', Object.keys(records[0]).join(', '));
    console.log('[cpdat] Sample row:', JSON.stringify(records[0], null, 2));
  }

  const rows = records.map((r) => {
    const dtxsid =
      r.DTXSID ||
      r.dtxsid ||
      r.dsstox_substance_id ||
      r['DTXSID'] ||
      '';

    const casrn =
      r.CASRN ||
      r.casrn ||
      r.cas_number ||
      r['CASRN'] ||
      '';

    const name =
      r.preferredName ||
      r.preferred_name ||
      r.chemical_name ||
      r.chemicalname ||
      r.CHEMICAL_NAME ||
      r.preferredname ||
      r['Chemical Name'] ||
      r['chemical name'] ||
      '';

    const funcUse =
      r.harmonized_functional_use ||
      r.functional_use ||
      r.reported_functional_use ||
      r.functional_use_category ||
      r.functional_use_name ||
      r.FUNCTIONAL_USE ||
      r['Functional Use'] ||
      '';

    const puc =
      r.PUC ||
      r.puc_name ||
      r.product_use_category ||
      r.gen_cat ||
      r.product_category ||
      r.PRODUCT_CATEGORY ||
      r['Product Category'] ||
      '';

    const wfLower = parseFloat(r.weight_fraction_lower || r.lower_weight_fraction || '');
    const wfUpper = parseFloat(r.weight_fraction_upper || r.upper_weight_fraction || '');
    const wfPred = parseFloat(r.weight_fraction_predicted || r.predicted_weight_fraction || '');
    const source = r.source || r.data_source || r.document_title || 'cpdat_bulk';

    return {
      dtxsid,
      casrn,
      preferred_name: String(name).trim(),
      functional_use: String(funcUse).trim(),
      product_use_category: String(puc).trim(),
      weight_fraction_lower: isNaN(wfLower) ? null : wfLower,
      weight_fraction_upper: isNaN(wfUpper) ? null : wfUpper,
      weight_fraction_predicted: isNaN(wfPred) ? null : wfPred,
      source_document: source,
    };
  });

  const nonEmptyNames = rows.filter((r) => r.preferred_name).length;
  console.log(`[cpdat] Rows with non-empty preferred_name: ${nonEmptyNames}`);

  return rows;
}

export function buildNameIndex(rows: BulkChemicalRow[]): Map<string, BulkChemicalRow[]> {
  const index = new Map<string, BulkChemicalRow[]>();
  for (const row of rows) {
    const key = row.preferred_name.toLowerCase().trim();
    if (!key) continue;
    const existing = index.get(key) || [];
    existing.push(row);
    index.set(key, existing);
  }
  return index;
}

export function buildCASIndex(rows: BulkChemicalRow[]): Map<string, BulkChemicalRow[]> {
  const index = new Map<string, BulkChemicalRow[]>();
  for (const row of rows) {
    if (!row.casrn) continue;
    const key = row.casrn.trim();
    const existing = index.get(key) || [];
    existing.push(row);
    index.set(key, existing);
  }
  return index;
}
