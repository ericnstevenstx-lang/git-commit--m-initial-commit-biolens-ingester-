-- 005_rpc_claim_resolver_jobs.sql

create or replace function public.claim_resolver_jobs(
  p_worker_id text,
  p_job_type text,
  p_batch_size integer default 25
)
returns setof public.resolver_jobs
language plpgsql
as $$
begin
  return query
  with claimable as (
    select j.id
    from public.resolver_jobs j
    where j.status = 'queued'
      and j.job_type = p_job_type
      and j.scheduled_at <= now()
    order by j.priority desc, j.created_at asc
    limit p_batch_size
    for update skip locked
  )
  update public.resolver_jobs j
     set status = 'running',
         locked_by = p_worker_id,
         locked_at = now(),
         attempt_count = j.attempt_count + 1,
         updated_at = now()
    from claimable c
   where j.id = c.id
   returning j.*;
end;

$$;

