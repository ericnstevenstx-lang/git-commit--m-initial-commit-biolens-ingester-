/**
 * Open Food Facts Connector
 * Fetches product data from Open Food Facts and Open Products Facts APIs.
 * Rate limited to 100 req/min for reads per OFF guidelines.
 *
 * API docs: https://openfoodfacts.github.io/openfoodfacts-server/api/
 */

const OFF_API_BASE = 'https://world.openfoodfacts.org/api/v2';
const OPF_API_BASE = 'https://world.openproductsfacts.org/api/v2';

const RATE_LIMIT_MS = Number(process.env.OFF_RATE_LIMIT_MS) || 600;

export interface OFFProduct {
  code: string;
  product_name: string | null;
  brands: string | null;
  categories_tags_en: string[];
  ingredients_text: string | null;
  ingredients_text_en: string | null;
  countries_tags: string[];
  origins: string | null;
  manufacturing_places: string | null;
  stores: string | null;
  labels: string | null;
  packaging: string | null;
  quantity: string | null;
  image_url: string | null;
  nutriscore_grade: string | null;
  ecoscore_grade: string | null;
  nova_group: number | null;
  raw: Record<string, unknown>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const COUNTRY_TAG_MAP: Record<string, string> = {
  'en:united-states': 'US',
  'en:france': 'FR',
  'en:germany': 'DE',
  'en:united-kingdom': 'GB',
  'en:canada': 'CA',
  'en:australia': 'AU',
  'en:italy': 'IT',
  'en:spain': 'ES',
  'en:japan': 'JP',
  'en:china': 'CN',
  'en:india': 'IN',
  'en:brazil': 'BR',
  'en:mexico': 'MX',
  'en:south-korea': 'KR',
  'en:netherlands': 'NL',
  'en:belgium': 'BE',
  'en:switzerland': 'CH',
  'en:sweden': 'SE',
  'en:norway': 'NO',
  'en:denmark': 'DK',
  'en:finland': 'FI',
  'en:portugal': 'PT',
  'en:austria': 'AT',
  'en:poland': 'PL',
  'en:czech-republic': 'CZ',
  'en:hungary': 'HU',
  'en:romania': 'RO',
  'en:russia': 'RU',
  'en:turkey': 'TR',
  'en:thailand': 'TH',
  'en:vietnam': 'VN',
  'en:indonesia': 'ID',
  'en:malaysia': 'MY',
  'en:philippines': 'PH',
  'en:singapore': 'SG',
  'en:new-zealand': 'NZ',
  'en:south-africa': 'ZA',
  'en:argentina': 'AR',
  'en:colombia': 'CO',
  'en:chile': 'CL',
  'en:peru': 'PE',
  'en:ireland': 'IE',
  'en:greece': 'GR',
  'en:israel': 'IL',
  'en:egypt': 'EG',
  'en:saudi-arabia': 'SA',
  'en:united-arab-emirates': 'AE',
};

export function normalizeCountryTag(tag: string): string | null {
  return COUNTRY_TAG_MAP[tag] || null;
}

function parseProduct(raw: Record<string, unknown>): OFFProduct {
  const p = (raw.product || raw) as Record<string, unknown>;
  return {
    code: String(p.code || ''),
    product_name: (p.product_name as string) || (p.product_name_en as string) || null,
    brands: (p.brands as string) || null,
    categories_tags_en: (p.categories_tags_en as string[]) || [],
    ingredients_text: (p.ingredients_text as string) || null,
    ingredients_text_en: (p.ingredients_text_en as string) || null,
    countries_tags: (p.countries_tags as string[]) || [],
    origins: (p.origins as string) || null,
    manufacturing_places: (p.manufacturing_places as string) || null,
    stores: (p.stores as string) || null,
    labels: (p.labels as string) || null,
    packaging: (p.packaging as string) || null,
    quantity: (p.quantity as string) || null,
    image_url: (p.image_url as string) || null,
    nutriscore_grade: (p.nutriscore_grade as string) || null,
    ecoscore_grade: (p.ecoscore_grade as string) || null,
    nova_group: (p.nova_group as number) || null,
    raw: p,
  };
}

export async function fetchByBarcode(
  barcode: string,
  useProductsFacts = false
): Promise<OFFProduct | null> {
  const base = useProductsFacts ? OPF_API_BASE : OFF_API_BASE;
  const url = `${base}/product/${barcode}`;

  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'BioLens-Ingester/1.0 (contact@thebioeconomyfoundation.org)',
      },
    });

    if (!res.ok) {
      console.log(`[off] ${res.status} for barcode ${barcode}`);
      return null;
    }

    const json = (await res.json()) as Record<string, unknown>;
    if (json.status === 0 || !json.product) {
      console.log(`[off] Product not found: ${barcode}`);
      return null;
    }

    await sleep(RATE_LIMIT_MS);
    return parseProduct(json);
  } catch (err) {
    console.error(`[off] Fetch error for ${barcode}:`, (err as Error).message);
    return null;
  }
}

export async function searchProducts(
  query: string,
  page = 1,
  pageSize = 50,
  useProductsFacts = false
): Promise<OFFProduct[]> {
  const base = useProductsFacts ? OPF_API_BASE : OFF_API_BASE;
  const params = new URLSearchParams({
    search_terms: query,
    page: String(page),
    page_size: String(pageSize),
    json: '1',
  });
  const url = `${base}/search?${params}`;

  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'BioLens-Ingester/1.0 (contact@thebioeconomyfoundation.org)',
      },
    });

    if (!res.ok) {
      console.error(`[off] Search error ${res.status}: ${query}`);
      return [];
    }

    const json = (await res.json()) as Record<string, unknown>;
    const products = (json.products as Record<string, unknown>[]) || [];

    await sleep(RATE_LIMIT_MS);
    return products.map(parseProduct);
  } catch (err) {
    console.error(`[off] Search fetch error:`, (err as Error).message);
    return [];
  }
}

export async function fetchByCategory(
  categoryTag: string,
  maxPages = 5,
  pageSize = 50,
  useProductsFacts = false
): Promise<OFFProduct[]> {
  const base = useProductsFacts ? OPF_API_BASE : OFF_API_BASE;
  const all: OFFProduct[] = [];

  for (let page = 1; page <= maxPages; page++) {
    const url = `${base}/search?categories_tags_en=${encodeURIComponent(categoryTag)}&page=${page}&page_size=${pageSize}&json=1`;

    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'BioLens-Ingester/1.0 (contact@thebioeconomyfoundation.org)',
        },
      });

      if (!res.ok) break;

      const json = (await res.json()) as Record<string, unknown>;
      const products = (json.products as Record<string, unknown>[]) || [];

      if (products.length === 0) break;
      all.push(...products.map(parseProduct));

      console.log(`[off] Category ${categoryTag} page ${page}: ${products.length} products`);
      await sleep(RATE_LIMIT_MS);
    } catch (err) {
      console.error(`[off] Category fetch error page ${page}:`, (err as Error).message);
      break;
    }
  }

  return all;
}
