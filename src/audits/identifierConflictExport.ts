// apps/ingester-node/src/audits/identifierConflictExport.ts
// If you keep the current flat repo structure, use: src/audits/identifierConflictExport.ts
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { z } from "zod";

const EnvSchema = z.object({
  SUPABASE_URL: z.string().url(),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
  AUDIT_OUTPUT_DIR: z.string().default("out"),
});

const env = EnvSchema.parse(process.env);

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

type IdentifierRow = {
  material_id: string;
  id_type: string;
  id_value: string;
  source_code: string | null;
  confidence: number | null;
};

type MaterialRow = {
  id: string;
  canonical_name: string | null;
  display_name: string | null;
  material_type: string | null;
  confidence_score: number | null;
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

async function fetchIdentifiers(): Promise<IdentifierRow[]> {
  const { data, error } = await supabase
    .from("material_identifiers")
    .select("material_id, id_type, id_value, source_code, confidence")
    .in("id_type", ["pubchem_cid", "inchikey", "cas_rn"]);

  if (error) throw error;
  return (data ?? []) as IdentifierRow[];
}

async function fetchMaterialsByIds(materialIds: string[]): Promise<Map<string, MaterialRow>> {
  if (!materialIds.length) return new Map();

  const { data, error } = await supabase
    .from("materials")
    .select("id, canonical_name, display_name, material_type, confidence_score")
    .in("id", materialIds);

  if (error) throw error;

  const map = new Map<string, MaterialRow>();
  for (const row of (data ?? []) as MaterialRow[]) map.set(row.id, row);
  return map;
}

async function main() {
  const ids = await fetchIdentifiers();

  const byMaterialAndType = new Map<string, Set<string>>();
  const byIdentifierAndType = new Map<string, Set<string>>();

  for (const row of ids) {
    const materialTypeKey = `${row.material_id}::${row.id_type}`;
    if (!byMaterialAndType.has(materialTypeKey)) byMaterialAndType.set(materialTypeKey, new Set());
    byMaterialAndType.get(materialTypeKey)!.add(row.id_value);

    const identifierTypeKey = `${row.id_type}::${row.id_value}`;
    if (!byIdentifierAndType.has(identifierTypeKey)) byIdentifierAndType.set(identifierTypeKey, new Set());
    byIdentifierAndType.get(identifierTypeKey)!.add(row.material_id);
  }

  const conflictMaterialIds = new Set<string>();
  const conflictRows: Array<Record<string, unknown>> = [];

  // Conflict type 1: one material has multiple strong values of the same identifier type
  for (const [key, values] of byMaterialAndType.entries()) {
    if (values.size <= 1) continue;

    const [materialId, idType] = key.split("::");
    conflictMaterialIds.add(materialId);

    conflictRows.push({
      conflict_type: "multiple_identifier_values_for_same_material",
      material_id: materialId,
      identifier_type: idType,
      identifier_values: Array.from(values).join(" | "),
      related_material_ids: "",
    });
  }

  // Conflict type 2: one strong identifier points to multiple materials
  for (const [key, materialIds] of byIdentifierAndType.entries()) {
    if (materialIds.size <= 1) continue;

    const [idType, idValue] = key.split("::");
    for (const materialId of materialIds) conflictMaterialIds.add(materialId);

    conflictRows.push({
      conflict_type: "same_identifier_attached_to_multiple_materials",
      material_id: "",
      identifier_type: idType,
      identifier_values: idValue,
      related_material_ids: Array.from(materialIds).join(" | "),
    });
  }

  const materialMap = await fetchMaterialsByIds([...conflictMaterialIds]);

  const enriched = conflictRows.map((row) => {
    const material = row.material_id ? materialMap.get(String(row.material_id)) : null;
    return {
      conflict_type: row.conflict_type,
      material_id: row.material_id,
      canonical_name: material?.canonical_name ?? "",
      display_name: material?.display_name ?? "",
      material_type: material?.material_type ?? "",
      material_confidence_score: material?.confidence_score ?? "",
      identifier_type: row.identifier_type,
      identifier_values: row.identifier_values,
      related_material_ids: row.related_material_ids,
    };
  });

  await mkdir(env.AUDIT_OUTPUT_DIR, { recursive: true });
  const filePath = path.join(
    env.AUDIT_OUTPUT_DIR,
    `identifier_conflicts_${timestampForFile()}.csv`
  );

  await writeFile(filePath, toCsv(enriched), "utf8");

  console.log(
    JSON.stringify({
      ok: true,
      file_path: filePath,
      row_count: enriched.length,
    }, null, 2)
  );
}

main().catch((err) => {
  console.error("[identifierConflictExport] fatal", err);
  process.exit(1);
});
