// apps/ingester-node/src/audits/unresolvedExport.ts
// If you keep the current flat repo structure, use: src/audits/unresolvedExport.ts

import { createClient } from "@supabase/supabase-js";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { z } from "zod";

const EnvSchema = z.object({
  SUPABASE_URL: z.string().url(),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
  AUDIT_OUTPUT_DIR: z.string().default("out"),
  UNRESOLVED_EXPORT_LIMIT: z.coerce.number().default(10000),
});

const env = EnvSchema.parse(process.env);

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

type RawIngredientRow = {
  id: string;
  source_product_id: string;
  ingredient_raw: string;
  normalized_token: string | null;
  token_type: string | null;
  parent_group: string | null;
  parser_confidence: number | null;
  resolution_state: string | null;
  final_confidence_score: number | null;
  created_at?: string | null;
};

type ProductRow = {
  id: string;
  source: string;
  external_product_id: string | null;
};

type MappingRow = {
  raw_ingredient_id: string;
  material_id: string;
  match_method: string | null;
  match_confidence: number | null;
  review_status: string | null;
};

function csvEscape(value: unknown): string {
  if (value == null) return "";
  const s = String(value);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows: Record<string, unknown>[]): string {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((h) => csvEscape(row[h])).join(",")),
  ];
  return lines.join("\n");
}

function timestampForFile() {
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}_${hh}${mi}${ss}`;
}

async function fetchRawIngredients(limit: number): Promise<RawIngredientRow[]> {
  const states = ["parsed", "ambiguous", "human_review_required", "mapped_low_confidence"];

  const { data, error } = await supabase
    .from("source_product_ingredients_raw")
    .select(`
      id,
      source_product_id,
      ingredient_raw,
      normalized_token,
      token_type,
      parent_group,
      parser_confidence,
      resolution_state,
      final_confidence_score,
      created_at
    `)
    .in("resolution_state", states)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  return (data ?? []) as RawIngredientRow[];
}

async function fetchProductsByIds(productIds: string[]): Promise<Map<string, ProductRow>> {
  if (!productIds.length) return new Map();

  const { data, error } = await supabase
    .from("source_products_raw")
    .select("id, source, external_product_id")
    .in("id", productIds);

  if (error) throw error;

  const map = new Map<string, ProductRow>();
  for (const row of (data ?? []) as ProductRow[]) map.set(row.id, row);
  return map;
}

async function fetchMappingsByRawIngredientIds(rawIds: string[]): Promise<Map<string, MappingRow[]>> {
  if (!rawIds.length) return new Map();

  const { data, error } = await supabase
    .from("ingredient_material_map")
    .select("raw_ingredient_id, material_id, match_method, match_confidence, review_status")
    .in("raw_ingredient_id", rawIds);

  if (error) throw error;

  const map = new Map<string, MappingRow[]>();
  for (const row of (data ?? []) as MappingRow[]) {
    const arr = map.get(row.raw_ingredient_id) ?? [];
    arr.push(row);
    map.set(row.raw_ingredient_id, arr);
  }
  return map;
}

async function fetchOpenReviewQueueByRawIds(rawIds: string[]): Promise<Set<string>> {
  if (!rawIds.length) return new Set();

  const { data, error } = await supabase
    .from("material_review_queue")
    .select("raw_ingredient_id, status")
    .in("raw_ingredient_id", rawIds)
    .eq("status", "open");

  if (error) throw error;

  return new Set((data ?? []).map((r: any) => r.raw_ingredient_id));
}

async function main() {
  const rawRows = await fetchRawIngredients(env.UNRESOLVED_EXPORT_LIMIT);
  const rawIds = rawRows.map((r) => r.id);
  const productIds = [...new Set(rawRows.map((r) => r.source_product_id).filter(Boolean))];

  const [productMap, mappingMap, openReviewSet] = await Promise.all([
    fetchProductsByIds(productIds),
    fetchMappingsByRawIngredientIds(rawIds),
    fetchOpenReviewQueueByRawIds(rawIds),
  ]);

  const unresolvedRows = rawRows.map((row) => {
    const product = productMap.get(row.source_product_id);
    const mappings = mappingMap.get(row.id) ?? [];
    const bestMapping = [...mappings].sort(
      (a, b) => (b.match_confidence ?? 0) - (a.match_confidence ?? 0)
    )[0];

    return {
      raw_ingredient_id: row.id,
      source_product_id: row.source_product_id,
      source_code: product?.source ?? "",
      external_product_id: product?.external_product_id ?? "",
      ingredient_raw: row.ingredient_raw,
      normalized_token: row.normalized_token ?? "",
      token_type: row.token_type ?? "",
      parent_group: row.parent_group ?? "",
      parser_confidence: row.parser_confidence ?? "",
      resolution_state: row.resolution_state ?? "",
      final_confidence_score: row.final_confidence_score ?? "",
      has_mapping: mappings.length > 0 ? "true" : "false",
      best_material_id: bestMapping?.material_id ?? "",
      best_match_method: bestMapping?.match_method ?? "",
      best_match_confidence: bestMapping?.match_confidence ?? "",
      best_review_status: bestMapping?.review_status ?? "",
      has_open_review_queue: openReviewSet.has(row.id) ? "true" : "false",
      created_at: row.created_at ?? "",
    };
  });

  await mkdir(env.AUDIT_OUTPUT_DIR, { recursive: true });
  const filePath = path.join(
    env.AUDIT_OUTPUT_DIR,
    `unresolved_materials_${timestampForFile()}.csv`
  );

  await writeFile(filePath, toCsv(unresolvedRows), "utf8");

  console.log(
    JSON.stringify({
      ok: true,
      file_path: filePath,
      row_count: unresolvedRows.length,
    }, null, 2)
  );
}

main().catch((err) => {
  console.error("[unresolvedExport] fatal", err);
  process.exit(1);
});
