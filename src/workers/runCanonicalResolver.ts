import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { z } from "zod";
import { CanonicalResolver } from "../resolver/canonicalResolver";

const EnvSchema = z.object({
  SUPABASE_URL: z.string().url(),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
  INTERNAL_FUNCTION_BEARER: z.string().min(1),
  LOOKUP_API_URL: z.string().url(),
  RESOLVER_VERSION: z.string().default("resolver-v1"),
  WORKER_ID: z.string().default(`worker-${process.pid}`),
  BATCH_SIZE: z.coerce.number().default(20),
  POLL_MS: z.coerce.number().default(2000),
});

const env = EnvSchema.parse(process.env);

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const resolver = new CanonicalResolver(env);

async function claimJobs() {
  const { data, error } = await supabase.rpc("claim_resolver_jobs", {
    p_worker_id: env.WORKER_ID,
    p_job_type: "resolve_ingredient",
    p_batch_size: env.BATCH_SIZE,
  });

  if (error) throw error;
  return data ?? [];
}

async function markSuccess(jobId: string, result: any) {
  const { error } = await supabase
    .from("resolver_jobs")
    .update({
      status: "succeeded",
      result_json: result,
      updated_at: new Date().toISOString(),
      locked_by: null,
      locked_at: null,
    })
    .eq("id", jobId);

  if (error) throw error;
}

async function markFailure(job: any, err: Error) {
  const nextStatus = job.attempt_count >= job.max_attempts ? "dead_letter" : "queued";
  const retryDelayMs = Math.min(60 * 60 * 1000, Math.pow(2, job.attempt_count) * 60_000);

  const { error } = await supabase
    .from("resolver_jobs")
    .update({
      status: nextStatus,
      last_error: err.message,
      locked_by: null,
      locked_at: null,
      updated_at: new Date().toISOString(),
      scheduled_at:
        nextStatus === "queued"
          ? new Date(Date.now() + retryDelayMs).toISOString()
          : job.scheduled_at,
    })
    .eq("id", job.id);

  if (error) throw error;
}

async function main() {
  console.log(`[resolver-worker] started as ${env.WORKER_ID}`);

  for (;;) {
    const jobs = await claimJobs();

    if (!jobs.length) {
      await new Promise((r) => setTimeout(r, env.POLL_MS));
      continue;
    }

    for (const job of jobs) {
      try {
        const result = await resolver.resolveRawIngredient(job.raw_ingredient_id);
        await markSuccess(job.id, result);
      } catch (err: any) {
        console.error(`[resolver-worker] job failed ${job.id}`, err);
        await markFailure(job, err);
      }
    }
  }
}

main().catch((err) => {
  console.error("[resolver-worker] fatal", err);
  process.exit(1);
});
