/**
 * GS1 Prefix Connector
 * Resolves a GTIN/barcode string to its issuing country
 * using the gs1_country_prefixes table in Supabase.
 */
import { supabase } from '../supabase';

export interface GS1Resolution {
  prefix: string;
  country_code: string;
  country_name: string;
  gs1_organization: string | null;
  feoc_flagged: boolean;
  uflpa_risk: boolean;
}

/**
 * Extract the 3-digit GS1 prefix from a GTIN/barcode string.
 * Works with GTIN-8, GTIN-12 (UPC-A), GTIN-13 (EAN), GTIN-14.
 */
export function extractPrefix(gtin: string): string | null {
  const cleaned = gtin.replace(/\D/g, '');
  if (cleaned.length < 8) return null;

  // GTIN-14: first digit is packaging indicator, prefix is digits 2-4
  if (cleaned.length === 14) return cleaned.substring(1, 4);
  // GTIN-13 (EAN): prefix is first 3 digits
  if (cleaned.length === 13) return cleaned.substring(0, 3);
  // GTIN-12 (UPC-A): pad to 13, prefix is '0' + first 2 digits
  if (cleaned.length === 12) return '0' + cleaned.substring(0, 2);
  // GTIN-8: prefix is first 3 digits (limited range)
  if (cleaned.length === 8) return cleaned.substring(0, 3);

  return null;
}

/**
 * Resolve a GTIN to country info via the gs1_country_prefixes table.
 * Returns null if prefix not found or GTIN is invalid.
 */
export async function resolveGTIN(gtin: string): Promise<GS1Resolution | null> {
  const prefix = extractPrefix(gtin);
  if (!prefix) {
    console.log(`[gs1] Invalid GTIN format: ${gtin}`);
    return null;
  }

  const { data, error } = await supabase
    .from('gs1_country_prefixes')
    .select('*')
    .eq('prefix', prefix)
    .maybeSingle();

  if (error) {
    console.error(`[gs1] Lookup error for prefix ${prefix}:`, error.message);
    return null;
  }

  if (!data) {
    console.log(`[gs1] No match for prefix ${prefix} (GTIN: ${gtin})`);
    return null;
  }

  return {
    prefix: data.prefix,
    country_code: data.country_code,
    country_name: data.country_name,
    gs1_organization: data.gs1_organization,
    feoc_flagged: data.feoc_flagged,
    uflpa_risk: data.uflpa_risk,
  };
}

/**
 * Batch resolve multiple GTINs. Returns a Map of gtin -> resolution.
 */
export async function batchResolveGTINs(
  gtins: string[]
): Promise<Map<string, GS1Resolution>> {
  const results = new Map<string, GS1Resolution>();

  // Extract unique prefixes
  const prefixMap = new Map<string, string[]>(); // prefix -> gtins
  for (const gtin of gtins) {
    const prefix = extractPrefix(gtin);
    if (!prefix) continue;
    const existing = prefixMap.get(prefix) || [];
    existing.push(gtin);
    prefixMap.set(prefix, existing);
  }

  if (prefixMap.size === 0) return results;

  const prefixes = Array.from(prefixMap.keys());
  const { data, error } = await supabase
    .from('gs1_country_prefixes')
    .select('*')
    .in('prefix', prefixes);

  if (error) {
    console.error('[gs1] Batch lookup error:', error.message);
    return results;
  }

  if (!data) return results;

  for (const row of data) {
    const gtinsForPrefix = prefixMap.get(row.prefix) || [];
    const resolution: GS1Resolution = {
      prefix: row.prefix,
      country_code: row.country_code,
      country_name: row.country_name,
      gs1_organization: row.gs1_organization,
      feoc_flagged: row.feoc_flagged,
      uflpa_risk: row.uflpa_risk,
    };
    for (const gtin of gtinsForPrefix) {
      results.set(gtin, resolution);
    }
  }

  return results;
}
