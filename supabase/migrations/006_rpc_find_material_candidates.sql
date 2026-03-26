-- 006_rpc_find_material_candidates.sql

create or replace function public.find_material_candidates(
  p_normalized_text text,
  p_limit integer default 10
)
returns table (
  material_id uuid,
  matched_alias text,
  normalized_alias text,
  match_method text,
  similarity_score numeric,
  alias_type text
)
language sql
stable
as $$
  with q as (
    select lower(unaccent(trim(p_normalized_text))) as needle
  ),
  exact_matches as (
    select
      ma.material_id,
      ma.alias as matched_alias,
      ma.normalized_alias,
      'alias_exact'::text as match_method,
      1.0::numeric as similarity_score,
      ma.alias_type
    from public.material_aliases ma
    cross join q
    where lower(unaccent(ma.normalized_alias)) = q.needle
  ),
  fuzzy_matches as (
    select
      ma.material_id,
      ma.alias as matched_alias,
      ma.normalized_alias,
      'alias_fuzzy'::text as match_method,
      similarity(lower(unaccent(ma.normalized_alias)), q.needle)::numeric as similarity_score,
      ma.alias_type
    from public.material_aliases ma
    cross join q
    where lower(unaccent(ma.normalized_alias)) % q.needle
      and similarity(lower(unaccent(ma.normalized_alias)), q.needle) >= 0.65
  ),
  unioned as (
    select * from exact_matches
    union all
    select * from fuzzy_matches
  )
  select
    u.material_id,
    u.matched_alias,
    u.normalized_alias,
    u.match_method,
    u.similarity_score,
    u.alias_type
  from unioned u
  order by
    case when u.match_method = 'alias_exact' then 0 else 1 end,
    u.similarity_score desc,
    u.matched_alias asc
  limit p_limit;

$$;
