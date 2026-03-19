import * as cheerio from "cheerio";
import { config } from "../config.js";

const EWG_SOURCE = "ewg";
const PRODUCT_LINK_REGEX = /\/skindeep\/products\/(\d+)-[^/"'\s]+/g;
/** Match canonical category href: /skindeep/browse/category/<slug>/ (slug exactly as-is, no normalisation). */
const CATEGORY_HREF_PATTERN = /\/skindeep\/browse\/category\/([^/?#]+)\/?/;

export interface EwgProduct {
  source: string;
  external_product_id: string;
  product_name: string | null;
  brand: string | null;
  category: string | null;
  ingredient_list_text: string | null;
  source_url: string;
  raw_payload: Record<string, unknown>;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const FETCH_OPTS = {
  headers: { "User-Agent": "BioLens-Ingester/1.0 (cosmetics research)" },
  redirect: "follow" as const,
};

async function fetchHtml(url: string): Promise<string> {
  const full = url.startsWith("http") ? url : `${config.ewg.baseUrl}${url}`;
  const res = await fetch(full, FETCH_OPTS);
  if (!res.ok) throw new Error(`EWG fetch ${res.status}: ${full}`);
  return res.text();
}

/** Fetch HTML without throwing on 4xx/5xx; caller can handle 404. */
async function fetchHtmlSafe(
  url: string
): Promise<{ ok: true; html: string } | { ok: false; status: number }> {
  const full = url.startsWith("http") ? url : `${config.ewg.baseUrl}${url}`;
  const res = await fetch(full, FETCH_OPTS);
  if (!res.ok) return { ok: false, status: res.status };
  const html = await res.text();
  return { ok: true, html };
}

/**
 * Extract unique product IDs from search/category listing HTML.
 */
export function extractProductIdsFromHtml(html: string): string[] {
  const ids = new Set<string>();
  let m: RegExpExecArray | null;
  PRODUCT_LINK_REGEX.lastIndex = 0;
  while ((m = PRODUCT_LINK_REGEX.exec(html)) !== null) ids.add(m[1]);
  return [...ids];
}

/**
 * Fetch one search listing page and return product IDs.
 */
export async function fetchSearchPage(category: string | null, page: number): Promise<string[]> {
  const params = new URLSearchParams();
  if (category) params.set("category", category);
  params.set("page", String(page));
  const url = `${config.ewg.baseUrl}/skindeep/search/?${params.toString()}`;
  const html = await fetchHtml(url);
  return extractProductIdsFromHtml(html);
}

/**
 * Fetch a category browse page (e.g. /skindeep/browse/category/body_wash__cleanser/) and return product IDs.
 * Supports ?page=N for pagination. On 404, logs and returns [] so the run can continue.
 */
export async function fetchCategoryBrowsePage(
  categorySlug: string,
  page: number,
  log?: (msg: string) => void
): Promise<string[]> {
  const path =
    page > 1
      ? `/skindeep/browse/category/${categorySlug}/?page=${page}`
      : `/skindeep/browse/category/${categorySlug}/`;
  const url = `${config.ewg.baseUrl}${path}`;
  const result = await fetchHtmlSafe(url);
  if (!result.ok) {
    if (result.status === 404) {
      (log ?? (() => {}))(`Category page 404, skipping: ${path}`);
      return [];
    }
    throw new Error(`EWG fetch ${result.status}: ${url}`);
  }
  return extractProductIdsFromHtml(result.html);
}

/**
 * Extract category slugs from actual anchor hrefs only.
 * Matches /skindeep/browse/category/<slug>/ and returns <slug> exactly as in the href (no lowercasing).
 * Deduplicates; does not derive slugs from label text.
 */
export function extractCategorySlugsFromHtml(html: string, baseUrl: string): string[] {
  const $ = cheerio.load(html);
  const slugs = new Set<string>();
  const baseOrigin = new URL(baseUrl).origin;

  $("a[href]").each((_, el) => {
    let href = $(el).attr("href");
    if (!href || typeof href !== "string") return;
    href = href.trim();
    if (!href) return;
    if (href.startsWith("http")) {
      if (!href.startsWith(baseOrigin)) return;
      href = href.slice(baseOrigin.length) || "/";
    }
    if (!href.startsWith("/")) return;
    const m = href.match(CATEGORY_HREF_PATTERN);
    if (m) slugs.add(m[1]);
  });

  return [...slugs];
}

/**
 * Fetch main search page and return category slugs from real anchor hrefs (canonical, as-is).
 */
export async function fetchCategorySlugs(): Promise<string[]> {
  const url = `${config.ewg.baseUrl}/skindeep/search/`;
  const html = await fetchHtml(url);
  return extractCategorySlugsFromHtml(html, config.ewg.baseUrl);
}

/**
 * Parse EWG product page HTML into EwgProduct.
 */
export function parseProductPage(html: string, productId: string, sourceUrl: string): EwgProduct {
  const $ = cheerio.load(html);
  const raw: Record<string, unknown> = { product_id: productId };

  const titleEl = $("h1").first();
  const productName = titleEl.length ? titleEl.text().trim() || null : null;
  raw.product_name = productName;

  let brand: string | null = null;
  $("p, div").each((_, el) => {
    const text = $(el).text().trim();
    if (text === "BRAND") {
      const next = $(el).next();
      const link = next.find('a[href*="/skindeep/browse/brands/"]').first();
      if (link.length) brand = link.text().trim() || null;
      return false;
    }
  });
  raw.brand = brand;

  let category: string | null = null;
  $("p, div").each((_, el) => {
    const text = $(el).text().trim();
    if (text === "CATEGORY") {
      const next = $(el).next();
      const link = next.find('a[href*="/skindeep/browse/category/"]').first();
      if (link.length) category = link.text().trim() || null;
      return false;
    }
  });
  raw.category = category;

  let ingredientListText: string | null = null;
  const bodyText = $("body").text();
  const packagingMatch = bodyText.match(/Ingredients from packaging:\s*([\s\S]*?)(?=\n##|Product's animal|Understanding scores|$)/i);
  if (packagingMatch) {
    ingredientListText = packagingMatch[1]
      .replace(/\s+/g, " ")
      .trim()
      .replace(/^[:.\s]+|[:.\s]+$/g, "") || null;
  }
  raw.ingredient_list_text = ingredientListText;

  return {
    source: EWG_SOURCE,
    external_product_id: productId,
    product_name: productName,
    brand,
    category,
    ingredient_list_text: ingredientListText,
    source_url: sourceUrl,
    raw_payload: raw,
  };
}

/**
 * Fetch a single product by ID and return EwgProduct.
 */
export async function fetchProduct(productId: string): Promise<EwgProduct> {
  const path = `/skindeep/products/${productId}/`;
  const sourceUrl = `${config.ewg.baseUrl}${path}`;
  const html = await fetchHtml(sourceUrl);
  return parseProductPage(html, productId, sourceUrl);
}

/**
 * Collect product IDs by iterating category browse pages.
 * Fetches category list first, then for each category pages until no product links.
 */
export async function* enumerateProductIds(opts: {
  category?: string | null;
  rateLimitMs?: number;
  log?: (msg: string) => void;
}): AsyncGenerator<string> {
  const rateLimitMs = opts.rateLimitMs ?? config.ewg.rateLimitMs;
  const log = opts.log ?? (() => {});
  const seen = new Set<string>();

  const categoryFilter = opts.category ?? null;
  let categorySlugs: string[];

  if (categoryFilter) {
    categorySlugs = [categoryFilter];
    log(`Using single category filter: ${categoryFilter}`);
  } else {
    log("Fetching category list from EWG (parsing anchor hrefs only)...");
    categorySlugs = await fetchCategorySlugs();
    await sleep(rateLimitMs);
    log(`Found ${categorySlugs.length} category slugs from hrefs.`);
    for (const slug of categorySlugs) log(`Discovered slug: ${slug}`);
  }

  for (const slug of categorySlugs) {
    log(`Crawling category: ${slug}`);
    let page = 1;
    while (true) {
      const ids = await fetchCategoryBrowsePage(slug, page, log);
      await sleep(rateLimitMs);
      if (ids.length === 0) break;
      let newCount = 0;
      for (const id of ids) {
        if (!seen.has(id)) {
          seen.add(id);
          newCount++;
          yield id;
        }
      }
      log(`Category ${slug} page ${page}: ${ids.length} links, ${newCount} new; total ${seen.size}.`);
      page++;
    }
  }
}

/**
 * Stream EWG products: enumerate IDs then fetch each product page.
 * No artificial limit; rate-limited only.
 */
export async function* streamEwgProducts(opts: {
  category?: string | null;
  rateLimitMs?: number;
  log?: (msg: string) => void;
}): AsyncGenerator<EwgProduct> {
  const rateLimitMs = opts.rateLimitMs ?? config.ewg.rateLimitMs;
  const log = opts.log ?? (() => {});

  let index = 0;
  for await (const productId of enumerateProductIds(opts)) {
    try {
      const product = await fetchProduct(productId);
      index++;
      if (index % 50 === 0) log(`Fetched ${index} products...`);
      yield product;
    } catch (err) {
      log(`Error fetching product ${productId}: ${err}`);
    }
    await sleep(rateLimitMs);
  }
}
