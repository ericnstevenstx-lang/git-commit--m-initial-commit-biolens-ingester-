-- 007_rpc_persist_resolution_result.sql

create or replace function public.persist_resolution_result(
  p_payload jsonb
)
returns jsonb
language plpgsql
as $$
declare
  v_raw_ingredient_id uuid := (p_payload->>'raw_ingredient_id')::uuid;
  v_material_id uuid := nullif(p_payload->>'material_id', '')::uuid;
  v_mapping_id uuid;
  v_match_method text := coalesce(p_payload->>'match_method', 'new_material');
  v_final_confidence numeric := coalesce((p_payload->>'final_confidence_score')::numeric, 0);
  v_resolution_state text := coalesce(p_payload->>'resolution_state', 'human_review_required');
  v_review_status text := coalesce(p_payload->>'review_status', 'needs_review');
  v_new_material jsonb := p_payload->'new_material';
begin
  if v_raw_ingredient_id is null then
    raise exception 'persist_resolution_result: raw_ingredient_id is required';
  end if;

  -- ==========================================
  -- Create new material if needed
  -- ==========================================
  if v_material_id is null and v_new_material is not null then
    insert into public.materials (
      canonical_name,
      display_name,
      material_type,
      normalization_status,
      confidence_score,
      resolution_state,
      is_active,
      created_at,
      updated_at
    )
    values (
      coalesce(v_new_material->>'canonical_name', 'Unnamed Material'),
      coalesce(v_new_material->>'display_name', v_new_material->>'canonical_name', 'Unnamed Material'),
      coalesce(v_new_material->>'material_type', 'unknown'),
      case
        when v_review_status = 'manually_approved' then 'canonical'
        when v_final_confidence >= 0.85 then 'canonical'
        else 'provisional'
      end,
      v_final_confidence,
      case
        when v_review_status = 'manually_approved' then 'locked_canonical'
        when v_final_confidence >= 0.85 then 'canonical_candidate'
        else 'provisional'
      end,
      true,
      now(),
      now()
    )
    returning id into v_material_id;
  end if;

  -- ==========================================
  -- Upsert aliases
  -- ==========================================
  insert into public.material_aliases (
    material_id,
    alias,
    normalized_alias,
    alias_type,
    source_code,
    confidence,
    is_preferred,
    created_at
  )
  select
    v_material_id,
    x.alias,
    x.normalized_alias,
    coalesce(x.alias_type, 'label'),
    x.source_code,
    x.confidence,
    coalesce(x.is_preferred, false),
    now()
  from jsonb_to_recordset(coalesce(p_payload->'aliases_to_add', '[]'::jsonb)) as x(
    alias text,
    normalized_alias text,
    alias_type text,
    source_code text,
    confidence numeric,
    is_preferred boolean
  )
  where v_material_id is not null
  on conflict (material_id, normalized_alias, alias_type) do update
    set alias = excluded.alias,
        source_code = coalesce(excluded.source_code, public.material_aliases.source_code),
        confidence = greatest(coalesce(public.material_aliases.confidence, 0), coalesce(excluded.confidence, 0)),
        is_preferred = public.material_aliases.is_preferred or excluded.is_preferred;

  -- ==========================================
  -- Upsert identifiers
  -- ==========================================
  insert into public.material_identifiers (
    material_id,
    id_type,
    id_value,
    is_primary,
    source_code,
    confidence,
    metadata,
    created_at,
    updated_at
  )
  select
    v_material_id,
    x.id_type,
    x.id_value,
    coalesce(x.is_primary, false),
    x.source_code,
    x.confidence,
    coalesce(x.metadata, '{}'::jsonb),
    now(),
    now()
  from jsonb_to_recordset(coalesce(p_payload->'identifiers_to_add', '[]'::jsonb)) as x(
    id_type text,
    id_value text,
    is_primary boolean,
    source_code text,
    confidence numeric,
    metadata jsonb
  )
  where v_material_id is not null
  on conflict (id_type, id_value) do update
    set material_id = public.material_identifiers.material_id,
        is_primary = public.material_identifiers.is_primary or excluded.is_primary,
        confidence = greatest(coalesce(public.material_identifiers.confidence, 0), coalesce(excluded.confidence, 0)),
        source_code = coalesce(public.material_identifiers.source_code, excluded.source_code),
        metadata = coalesce(public.material_identifiers.metadata, '{}'::jsonb) || coalesce(excluded.metadata, '{}'::jsonb),
        updated_at = now();

  -- ==========================================
  -- Upsert mapping row
  -- ==========================================
  if v_material_id is not null then
    insert into public.ingredient_material_map (
      raw_ingredient_id,
      material_id,
      match_method,
      match_confidence,
      review_status,
      resolver_version,
      evidence,
      created_at,
      updated_at
    )
    values (
      v_raw_ingredient_id,
      v_material_id,
      v_match_method,
      v_final_confidence,
      v_review_status,
      p_payload->>'resolver_version',
      coalesce(p_payload->'evidence', '{}'::jsonb),
      now(),
      now()
    )
    on conflict (raw_ingredient_id, material_id) do update
      set match_method = excluded.match_method,
          match_confidence = excluded.match_confidence,
          review_status = excluded.review_status,
          resolver_version = excluded.resolver_version,
          evidence = coalesce(public.ingredient_material_map.evidence, '{}'::jsonb) || coalesce(excluded.evidence, '{}'::jsonb),
          updated_at = now()
    returning id into v_mapping_id;
  else
    v_mapping_id := null;
  end if;

  -- ==========================================
  -- Update raw ingredient state
  -- ==========================================
  update public.source_product_ingredients_raw
     set resolution_state = v_resolution_state,
         final_confidence_score = v_final_confidence,
         updated_at = now()
   where id = v_raw_ingredient_id;

  -- ==========================================
  -- If manually approved, lock material
  -- ==========================================
  if v_material_id is not null and v_review_status = 'manually_approved' then
    update public.materials
       set confidence_score = greatest(coalesce(confidence_score, 0), v_final_confidence),
           normalization_status = 'canonical',
           resolution_state = 'locked_canonical',
           updated_at = now()
     where id = v_material_id;
  elsif v_material_id is not null then
    update public.materials
       set confidence_score = greatest(coalesce(confidence_score, 0), v_final_confidence),
           normalization_status = case
             when v_final_confidence >= 0.85 then 'canonical'
             else coalesce(normalization_status, 'provisional')
           end,
           resolution_state = case
             when v_final_confidence >= 0.85 and coalesce(resolution_state, '') not in ('locked_canonical', 'deprecated') then 'canonical_candidate'
             else coalesce(resolution_state, 'provisional')
           end,
           updated_at = now()
     where id = v_material_id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'material_id', v_material_id,
    'mapping_id', v_mapping_id
  );
end;

$$;
