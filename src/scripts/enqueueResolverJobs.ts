// apps/ingester-node/src/scripts/enqueueResolverJobs.ts
// If you keep the current flat repo structure, use: src/scripts/enqueueResolverJobs.ts
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { z } from "zod";

const EnvSchema = z.object({
  SUPABASE_URL: z.string().url(),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
  ENQUEUE_LIMIT: z.coerce.number().default(5000),
  ENQUEUE_BATCH_SIZE: z.coerce.number().default(500),
});

const env = EnvSchema.parse(process.env);

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

type CandidateRow = {
  id: string;
  resolution_state: string | null;
  final_confidence_score: number | null;
};

async function fetchCandidateRawIngredients(limit: number): Promise<CandidateRow[]> {
  const targetStates = ["parsed", "ambiguous", "mapped_low_confidence"];

  const { data, error } = await supabase
    .from("source_product_ingredients_raw")
    .select("id, resolution_state, final_confidence_score")
    .in("resolution_state", targetStates)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  return (data ?? []) as CandidateRow[];
}

async function fetchExistingJobs(rawIds: string[]): Promise<Set<string>> {
  if (!rawIds.length) return new Set();

  const { data, error } = await supabase
    .from("resolver_jobs")
    .select("raw_ingredient_id, status")
    .in("raw_ingredient_id", rawIds)
    .in("status", ["queued", "running", "succeeded"]);

  if (error) throw error;

  return new Set((data ?? []).map((r: any) => r.raw_ingredient_id).filter(Boolean));
}

async function fetchResolvedMappings(rawIds: string[]): Promise<Set<string>> {
  if (!rawIds.length) return new Set();

  const { data, error } = await supabase
    .from("ingredient_material_map")
    .select("raw_ingredient_id, review_status, match_confidence")
    .in("raw_ingredient_id", rawIds);

  if (error) throw error;

  const keep = new Set<string>();
  for (const row of data ?? []) {
    const reviewStatus = row.review_status;
    const confidence = row.match_confidence ?? 0;

    // Skip enqueue if already strongly resolved or manually approved
    if (reviewStatus === "manually_approved" || confidence >= 0.85) {
      keep.add(row.raw_ingredient_id);
    }
  }
  return keep;
}

async function insertJobs(rawIds: string[]) {
  if (!rawIds.length) return 0;

  const rows = rawIds.map((id) => ({
    job_type: "resolve_ingredient",
    raw_ingredient_id: id,
    priority: 100,
    status: "queued",
    payload: {},
  }));

  const { error } = await supabase.from("resolver_jobs").insert(rows);
  if (error) throw error;
  return rows.length;
}

function chunk<T>(arr: T[], size: number): T[][];
function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function main() {
  const candidates = await fetchCandidateRawIngredients(env.ENQUEUE_LIMIT);
  const rawIds = candidates.map((r) => r.id);

  const [existingJobIds, resolvedMappingIds] = await Promise.all([
    fetchExistingJobs(rawIds),
    fetchResolvedMappings(rawIds),
  ]);

  const toEnqueue = candidates
    .filter((r) => !existingJobIds.has(r.id))
    .filter((r) => !resolvedMappingIds.has(r.id))
    .map((r) => r.id);

  let inserted = 0;
  for (const group of chunk(toEnqueue, env.ENQUEUE_BATCH_SIZE)) {
    inserted += await insertJobs(group);
  }

  console.log(
    JSON.stringify({
      ok: true,
      candidates_seen: candidates.length,
      already_had_job: existingJobIds.size,
      already_resolved: resolvedMappingIds.size,
      newly_enqueued: inserted,
    }, null, 2)
  );
}

main().catch((err) => {
  console.error("[enqueueResolverJobs] fatal", err);
  process.exit(1);
});
