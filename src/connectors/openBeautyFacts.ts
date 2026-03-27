/**
 * Open Beauty Facts Local CSV Connector
 *
 * Parses the bulk CSV download from:
 * https://static.openbeautyfacts.org/data/en.openbeautyfacts.org.products.csv.gz
 *
 * The CSV is tab-delimited, UTF-8 encoded.
 * Key fields: code, product_name, brands, categories_en, ingredients_text,
 *   countries_en, origins_en, manufacturing_places, labels_en, packaging_en,
 *   stores, quantity
 */
import fs from 'node:fs';
import { createReadStream } from 'node:fs';
import { parse } from 'csv-parse';

export interface OBFProduct {
  code: string;
  product_name: string | null;
  brands: string | null;
  categories_en: string | null;
  ingredients_text: string | null;
  countries_en: string | null;
  origins_en: string | null;
  manufacturing_places: string | null;
  labels_en: string | null;
  packaging_en: string | null;
  stores: string | null;
  quantity: string | null;
  image_url: string | null;
}

/**
 * Stream-parse the OBF CSV file in chunks to avoid loading 163MB into memory.
 * Calls onBatch for every `batchSize` products that have ingredients.
 */
export async function parseOBFCsv(
  filePath: string,
  onBatch: (products: OBFProduct[]) => Promise<void>,
  batchSize = 100
): Promise<number> {
  if (!fs.existsSync(filePath)) {
    throw new Error(`[obf] File not found: ${filePath}`);
  }

  return new Promise((resolve, reject) => {
    let total = 0;
    let withIngredients = 0;
    let batch: OBFProduct[] = [];

    const parser = createReadStream(filePath).pipe(
      parse({
        delimiter: '\t',
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        trim: true,
        quote: '"',
        escape: '"',
        on_record: (record) => {
          // Skip records without a barcode
          if (!record.code || record.code.length < 8) return null;
          return record;
        },
      })
    );

    parser.on('data', async (row: Record<string, string>) => {
      total++;

      const ingredientsText =
        row.ingredients_text ||
        row.ingredients_text_en ||
        row.ingredients_text_fr ||
        '';

      // Only keep products with ingredient data
      if (!ingredientsText.trim()) return;

      withIngredients++;

      const product: OBFProduct = {
        code: row.code,
        product_name:
          row.product_name || row.product_name_en || row.product_name_fr || null,
        brands: row.brands || null,
        categories_en: row.categories_en || row.main_category_en || null,
        ingredients_text: ingredientsText,
        countries_en: row.countries_en || null,
        origins_en: row.origins_en || row.origins || null,
        manufacturing_places: row.manufacturing_places || null,
        labels_en: row.labels_en || null,
        packaging_en: row.packaging_en || row.packaging || null,
        stores: row.stores || null,
        quantity: row.quantity || null,
        image_url: row.image_url || row.image_front_url || null,
      };

      batch.push(product);

      if (batch.length >= batchSize) {
        parser.pause();
        const currentBatch = [...batch];
        batch = [];
        try {
          await onBatch(currentBatch);
        } catch (err) {
          console.error('[obf] Batch processing error:', (err as Error).message);
        }
        parser.resume();
      }
    });

    parser.on('end', async () => {
      // Process remaining batch
      if (batch.length > 0) {
        try {
          await onBatch(batch);
        } catch (err) {
          console.error('[obf] Final batch error:', (err as Error).message);
        }
      }
      console.log(
        `[obf] CSV complete. Total rows: ${total}, With ingredients: ${withIngredients}`
      );
      resolve(withIngredients);
    });

    parser.on('error', (err) => {
      console.error('[obf] CSV parse error:', err.message);
      reject(err);
    });
  });
}
