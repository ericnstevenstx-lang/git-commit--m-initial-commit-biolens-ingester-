/**
 * UPCitemdb.com Connector
 *
 * Looks up UPC/EAN/GTIN barcodes to get product metadata:
 *   - product title, brand, description
 *   - country of origin (when available)
 *   - category, images, offers
 *
 * Free tier: 100 req/day. Paid plans available.
 * API docs: https://www.upcitemdb.com/wp/docs/main/development/
 */

const UPCITEMDB_API_BASE = 'https://api.upcitemdb.com/prod/trial';
const RATE_LIMIT_MS = Number(process.env.UPCITEMDB_RATE_LIMIT_MS) || 1500;

export interface UPCItemResult {
  ean: string;
  title: string;
  description: string;
  brand: string;
  category: string;
  country: string | null;       // country of origin when available
  images: string[];
  offers: UPCOffer[];
  raw: Record<string, unknown>;
}

export interface UPCOffer {
  merchant: string;
  domain: string;
  title: string;
  price: string;
  currency: string;
  link: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Look up a single UPC/EAN barcode via UPCitemdb.
 * Returns null if not found or rate limited.
 */
export async function lookupUPC(
  barcode: string
): Promise<UPCItemResult | null> {
  const url = `${UPCITEMDB_API_BASE}/lookup?upc=${encodeURIComponent(barcode)}`;

  try {
    const res = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'BioLens-Ingester/1.0',
      },
    });

    if (res.status === 429) {
      console.log('[upcitemdb] Rate limited, waiting 60s...');
      await sleep(60_000);
      return null;
    }

    if (!res.ok) {
      console.log(`[upcitemdb] ${res.status} for barcode ${barcode}`);
      return null;
    }

    const json = (await res.json()) as Record<string, unknown>;

    if (json.code !== 'OK' || !json.items) {
      return null;
    }

    const items = json.items as Record<string, unknown>[];
    if (items.length === 0) return null;

    const item = items[0];
    const offers = ((item.offers as Record<string, unknown>[]) || []).map(
      (o) => ({
        merchant: String(o.merchant || ''),
        domain: String(o.domain || ''),
        title: String(o.title || ''),
        price: String(o.price || ''),
        currency: String(o.currency || ''),
        link: String(o.link || ''),
      })
    );

    await sleep(RATE_LIMIT_MS);

    return {
      ean: String(item.ean || barcode),
      title: String(item.title || ''),
      description: String(item.description || ''),
      brand: String(item.brand || ''),
      category: String(item.category || ''),
      country: (item.country as string) || null,
      images: (item.images as string[]) || [],
      offers,
      raw: item,
    };
  } catch (err) {
    console.error(
      `[upcitemdb] Fetch error for ${barcode}:`,
      (err as Error).message
    );
    return null;
  }
}

/**
 * Search UPCitemdb by product name / keyword.
 * Returns up to 100 results.
 */
export async function searchUPC(
  query: string
): Promise<UPCItemResult[]> {
  const url = `${UPCITEMDB_API_BASE}/search?s=${encodeURIComponent(query)}&match=contain&type=product`;

  try {
    const res = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'BioLens-Ingester/1.0',
      },
    });

    if (res.status === 429) {
      console.log('[upcitemdb] Rate limited on search');
      await sleep(60_000);
      return [];
    }

    if (!res.ok) {
      console.log(`[upcitemdb] Search error ${res.status}: ${query}`);
      return [];
    }

    const json = (await res.json()) as Record<string, unknown>;

    if (json.code !== 'OK' || !json.items) return [];

    const items = json.items as Record<string, unknown>[];

    await sleep(RATE_LIMIT_MS);

    return items.map((item) => ({
      ean: String(item.ean || ''),
      title: String(item.title || ''),
      description: String(item.description || ''),
      brand: String(item.brand || ''),
      category: String(item.category || ''),
      country: (item.country as string) || null,
      images: (item.images as string[]) || [],
      offers: ((item.offers as Record<string, unknown>[]) || []).map((o) => ({
        merchant: String(o.merchant || ''),
        domain: String(o.domain || ''),
        title: String(o.title || ''),
        price: String(o.price || ''),
        currency: String(o.currency || ''),
        link: String(o.link || ''),
      })),
      raw: item,
    }));
  } catch (err) {
    console.error(`[upcitemdb] Search error:`, (err as Error).message);
    return [];
  }
}
