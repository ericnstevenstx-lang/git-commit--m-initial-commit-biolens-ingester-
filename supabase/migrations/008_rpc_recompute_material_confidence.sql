-- 008_rpc_recompute_material_confidence.sql

create or replace function public.recompute_material_confidence(
  p_material_id uuid
)
returns numeric
language plpgsql
as $$
declare
  v_avg_mapping numeric := 0;
  v_identifier_completeness numeric := 0;
  v_cross_source_diversity numeric := 0;
  v_alias_coherence numeric := 0;
  v_manual_review_boost numeric := 0;
  v_score numeric := 0;
  v_identifier_count integer := 0;
  v_source_count integer := 0;
  v_alias_count integer := 0;
  v_preferred_alias_count integer := 0;
  v_manual_count integer := 0;
begin
  -- Average confirmed mapping confidence
  select coalesce(avg(imm.match_confidence), 0)
    into v_avg_mapping
  from public.ingredient_material_map imm
  where imm.material_id = p_material_id
    and coalesce(imm.review_status, '') <> 'rejected';

  -- Identifier completeness
  select count(*) into v_identifier_count
  from public.material_identifiers mi
  where mi.material_id = p_material_id;

  v_identifier_completeness :=
    case
      when v_identifier_count >= 3 then 1.0
      when v_identifier_count = 2 then 0.85
      when v_identifier_count = 1 then 0.65
      else 0.30
    end;

  -- Cross-source diversity: how many different product sources mention this material
  select count(distinct sp.source)
    into v_source_count
  from public.ingredient_material_map imm
  join public.source_product_ingredients_raw spi on spi.id = imm.raw_ingredient_id
  join public.source_products_raw sp on sp.id = spi.source_product_id
  where imm.material_id = p_material_id;

  v_cross_source_diversity :=
    case
      when v_source_count >= 4 then 1.0
      when v_source_count = 3 then 0.92
      when v_source_count = 2 then 0.84
      when v_source_count = 1 then 0.72
      else 0.40
    end;

  -- Alias coherence
  select
    count(*) as alias_count,
    count(*) filter (where coalesce(is_preferred, false) = true) as preferred_alias_count
  into v_alias_count, v_preferred_alias_count
  from public.material_aliases ma
  where ma.material_id = p_material_id;

  v_alias_coherence :=
    case
      when v_alias_count = 0 then 0.40
      when v_alias_count between 1 and 3 then 0.82
      when v_alias_count between 4 and 10 then 0.92
      else 0.88
    end;

  if v_preferred_alias_count > 0 then
    v_alias_coherence := least(1.0, v_alias_coherence + 0.05);
  end if;

  -- Manual review boost
  select count(*) into v_manual_count
  from public.ingredient_material_map imm
  where imm.material_id = p_material_id
    and imm.review_status = 'manually_approved';

  v_manual_review_boost :=
    case
      when v_manual_count > 0 then 1.0
      else 0.0
    end;

  v_score :=
      (0.45 * coalesce(v_avg_mapping, 0))
    + (0.20 * v_identifier_completeness)
    + (0.15 * v_cross_source_diversity)
    + (0.10 * v_alias_coherence)
    + (0.10 * v_manual_review_boost);

  v_score := greatest(0, least(1, round(v_score::numeric, 4)));

  update public.materials
     set confidence_score = v_score,
         normalization_status = case
           when v_score >= 0.85 then 'canonical'
           when v_score >= 0.65 then 'provisional'
           when coalesce(normalization_status, '') = 'deprecated' then 'deprecated'
           else 'ambiguous'
         end,
         resolution_state = case
           when coalesce(resolution_state, '') = 'locked_canonical' then 'locked_canonical'
           when coalesce(resolution_state, '') = 'deprecated' then 'deprecated'
           when v_score >= 0.90 then 'canonical'
           when v_score >= 0.75 then 'canonical_candidate'
           else 'provisional'
         end,
         updated_at = now()
   where id = p_material_id;

  return v_score;
end;

$$;
