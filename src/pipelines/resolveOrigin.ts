/**
 * Pipeline: Resolve Origin
 *
 * Iterates source_products_raw rows that have a gtin/barcode/upc
 * but no country_of_origin. Resolves via GS1 prefix table.
 *
 * Idempotent: only processes rows with null country_of_origin.
 */

import { supabase } from '../supabase.js';
import { batchResolveGTINs } from '../connectors/gs1Prefix.js';

const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 100;

function toTextArray(value: string | null | undefined): string[] | null {
  if (!value) return null;
  const trimmed = value.trim();
  return trimmed ? [trimmed] : null;
}

export async function resolveOrigins(): Promise<void> {
  let totalResolved = 0;
  let offset = 0;

  while (true) {
    const { data: rows, error } = await supabase
      .from('source_products_raw')
      .select('id, gtin, barcode, upc, raw_payload')
      .or('gtin.not.is.null,barcode.not.is.null,upc.not.is.null')
      .is('country_of_origin', null)
      .range(offset, offset + BATCH_SIZE - 1);

    if (error) {
      console.error('[resolveOrigin] Query error:', error.message);
      break;
    }

    if (!rows || rows.length === 0) {
      console.log('[resolveOrigin] No more rows to process');
      break;
    }

    console.log(
      `[resolveOrigin] Processing batch of ${rows.length} at offset ${offset}`
    );

    const gtinEntries: { id: string; gtin: string }[] = [];

    for (const row of rows) {
      const gtin = row.gtin || row.barcode || row.upc;
      if (gtin && gtin.length >= 8) {
        gtinEntries.push({ id: row.id, gtin });
      }
    }

    if (gtinEntries.length === 0) {
      offset += BATCH_SIZE;
      continue;
    }

    const resolutions = await batchResolveGTINs(
      gtinEntries.map((e) => e.gtin)
    );

    for (const entry of gtinEntries) {
      const resolution = resolutions.get(entry.gtin);
      if (!resolution) continue;

      const originalRow = rows.find((r) => r.id === entry.id);
      const existingPayload =
        (originalRow?.raw_payload as Record<string, unknown>) || {};

      const updatedPayload = {
        ...existingPayload,
        gs1_resolution: {
          prefix: resolution.prefix,
          country_code: resolution.country_code,
          country_name: resolution.country_name,
          gs1_organization: resolution.gs1_organization,
          feoc_flagged: resolution.feoc_flagged,
          uflpa_risk: resolution.uflpa_risk,
          resolved_at: new Date().toISOString(),
          resolution_method: 'gs1_prefix',
        },
      };

      const { error: updateError } = await supabase
        .from('source_products_raw')
        .update({
          country_of_origin: toTextArray(resolution.country_code),
          raw_payload: updatedPayload,
        })
        .eq('id', entry.id);

      if (updateError) {
        console.error(
          `[resolveOrigin] Update error for ${entry.id}:`,
          updateError.message
        );
      } else {
        totalResolved++;

        if (resolution.feoc_flagged) {
          console.log(
            `[resolveOrigin] FEOC FLAG: ${entry.gtin} -> ${resolution.country_name} (${resolution.country_code})`
          );
        }

        if (resolution.uflpa_risk) {
          console.log(
            `[resolveOrigin] UFLPA RISK: ${entry.gtin} -> ${resolution.country_name} (${resolution.country_code})`
          );
        }
      }
    }

    offset += BATCH_SIZE;
  }

  console.log(`[resolveOrigin] Complete. Resolved ${totalResolved} origins.`);
}
