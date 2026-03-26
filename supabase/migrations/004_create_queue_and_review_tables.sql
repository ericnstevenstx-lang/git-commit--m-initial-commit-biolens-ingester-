-- 004_create_queue_and_review_tables.sql

-- =========================
-- resolver_jobs
-- =========================
create table if not exists public.resolver_jobs (
  id uuid primary key default gen_random_uuid(),
  job_type text not null
    check (job_type in ('resolve_ingredient', 'recompute_confidence', 'pubchem_enrich')),
  raw_ingredient_id uuid,
  material_id uuid,
  priority integer not null default 100,
  status text not null default 'queued'
    check (status in ('queued', 'running', 'succeeded', 'failed', 'dead_letter')),
  attempt_count integer not null default 0,
  max_attempts integer not null default 5,
  scheduled_at timestamptz not null default now(),
  locked_by text,
  locked_at timestamptz,
  payload jsonb not null default '{}'::jsonb,
  result_json jsonb,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists ix_resolver_jobs_claim
  on public.resolver_jobs (status, scheduled_at, priority desc, created_at)
  where status = 'queued';

create index if not exists ix_resolver_jobs_raw_ingredient_id
  on public.resolver_jobs (raw_ingredient_id);

create index if not exists ix_resolver_jobs_material_id
  on public.resolver_jobs (material_id);

-- =========================
-- material_review_queue
-- =========================
create table if not exists public.material_review_queue (
  id uuid primary key default gen_random_uuid(),
  raw_ingredient_id uuid not null,
  proposed_material_id uuid,
  candidate_json jsonb not null default '[]'::jsonb,
  reason_code text not null
    check (reason_code in (
      'ambiguous_candidates',
      'low_confidence',
      'identifier_conflict',
      'new_material_required',
      'parser_uncertain',
      'needs_review',
      'human_review_required'
    )),
  status text not null default 'open'
    check (status in ('open', 'approved', 'rejected', 'deferred')),
  reviewer_notes text,
  resolved_by text,
  resolved_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists ix_material_review_queue_status_created
  on public.material_review_queue (status, created_at);

create index if not exists ix_material_review_queue_raw_ingredient_id
  on public.material_review_queue (raw_ingredient_id);

create index if not exists ix_material_review_queue_proposed_material_id
  on public.material_review_queue (proposed_material_id);

-- =========================
-- updated_at triggers
-- =========================
do $$
begin
  if not exists (
    select 1 from pg_trigger where tgname = 'trg_resolver_jobs_set_updated_at'
  ) then
    create trigger trg_resolver_jobs_set_updated_at
    before update on public.resolver_jobs
    for each row execute function public.set_updated_at();
  end if;

  if not exists (
    select 1 from pg_trigger where tgname = 'trg_material_review_queue_set_updated_at'
  ) then
    create trigger trg_material_review_queue_set_updated_at
    before update on public.material_review_queue
    for each row execute function public.set_updated_at();
  end if;
end $$;
