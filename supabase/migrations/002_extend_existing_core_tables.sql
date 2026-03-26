-- 002_extend_existing_core_tables.sql

-- =========================
-- materials
-- =========================
alter table if exists public.materials
  add column if not exists canonical_name text,
  add column if not exists display_name text,
  add column if not exists material_type text
    check (material_type in (
      'chemical', 'mixture', 'extract', 'polymer', 'mineral',
      'material', 'additive', 'label_term', 'unknown'
    )),
  add column if not exists normalization_status text default 'provisional'
    check (normalization_status in ('canonical', 'provisional', 'ambiguous', 'deprecated')),
  add column if not exists confidence_score numeric(5,4),
  add column if not exists resolution_state text default 'provisional'
    check (resolution_state in (
      'provisional',
      'canonical_candidate',
      'canonical',
      'locked_canonical',
      'ambiguous',
      'deprecated'
    )),
  add column if not exists is_active boolean default true,
  add column if not exists created_at timestamptz default now(),
  add column if not exists updated_at timestamptz default now();

-- If an old "name" column exists, backfill canonical_name/display_name from it.
do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'materials'
      and column_name = 'name'
  ) then
    execute '
      update public.materials
      set canonical_name = coalesce(canonical_name, name),
          display_name   = coalesce(display_name, name)
      where canonical_name is null or display_name is null
    ';
  end if;
end $$;

create index if not exists ix_materials_canonical_name_trgm
  on public.materials using gin (canonical_name gin_trgm_ops);

create index if not exists ix_materials_display_name_trgm
  on public.materials using gin (display_name gin_trgm_ops);

-- =========================
-- material_aliases
-- =========================
alter table if exists public.material_aliases
  add column if not exists material_id uuid,
  add column if not exists alias text,
  add column if not exists normalized_alias text,
  add column if not exists alias_type text default 'label'
    check (alias_type in (
      'label',
      'inci',
      'common_name',
      'iupac',
      'brand_label',
      'cas_name',
      'pubchem_synonym',
      'e_number',
      'misspelling',
      'other'
    )),
  add column if not exists source_code text,
  add column if not exists language_code text default 'en',
  add column if not exists is_preferred boolean default false,
  add column if not exists confidence numeric(5,4),
  add column if not exists created_at timestamptz default now();

create index if not exists ix_material_aliases_normalized_alias
  on public.material_aliases (normalized_alias);

create index if not exists ix_material_aliases_normalized_alias_trgm
  on public.material_aliases using gin (normalized_alias gin_trgm_ops);

create unique index if not exists ux_material_aliases_material_norm_type
  on public.material_aliases (material_id, normalized_alias, alias_type);

-- =========================
-- source_product_ingredients_raw
-- =========================
alter table if exists public.source_product_ingredients_raw
  add column if not exists source_product_id uuid,
  add column if not exists ingredient_raw text,
  add column if not exists normalized_token text,
  add column if not exists token_type text default 'ingredient'
    check (token_type in (
      'ingredient',
      'additive',
      'allergen',
      'fragrance_group',
      'color_additive',
      'contains_less_than',
      'unknown'
    )),
  add column if not exists raw_position integer,
  add column if not exists parent_group text,
  add column if not exists parser_confidence numeric(5,4),
  add column if not exists resolution_state text default 'raw'
    check (resolution_state in (
      'raw',
      'parsed',
      'mapped_low_confidence',
      'mapped_high_confidence',
      'ambiguous',
      'human_review_required',
      'locked_canonical'
    )),
  add column if not exists final_confidence_score numeric(5,4),
  add column if not exists parse_notes text,
  add column if not exists parser_version text,
  add column if not exists created_at timestamptz default now(),
  add column if not exists updated_at timestamptz default now();

create index if not exists ix_spi_raw_normalized_token
  on public.source_product_ingredients_raw (normalized_token);

create index if not exists ix_spi_raw_resolution_state
  on public.source_product_ingredients_raw (resolution_state);

create index if not exists ix_spi_raw_source_product_id
  on public.source_product_ingredients_raw (source_product_id);

-- =========================
-- ingredient_material_map
-- =========================
alter table if exists public.ingredient_material_map
  add column if not exists raw_ingredient_id uuid,
  add column if not exists material_id uuid,
  add column if not exists match_method text default 'alias_exact'
    check (match_method in (
      'alias_exact',
      'alias_fuzzy',
      'alias_normalized',
      'rule_based',
      'pubchem_synonym',
      'pubchem_identifier',
      'manual_review',
      'new_material'
    )),
  add column if not exists match_confidence numeric(5,4),
  add column if not exists review_status text default 'auto_accepted'
    check (review_status in (
      'auto_accepted',
      'needs_review',
      'manually_approved',
      'rejected'
    )),
  add column if not exists resolver_version text,
  add column if not exists evidence jsonb default '{}'::jsonb,
  add column if not exists created_at timestamptz default now(),
  add column if not exists updated_at timestamptz default now();

create index if not exists ix_imm_raw_ingredient_id
  on public.ingredient_material_map (raw_ingredient_id);

create index if not exists ix_imm_material_id
  on public.ingredient_material_map (material_id);

create unique index if not exists ux_imm_raw_material
  on public.ingredient_material_map (raw_ingredient_id, material_id);

-- =========================
-- source_products_raw helpful indexes
-- =========================
create index if not exists ix_source_products_raw_source
  on public.source_products_raw (source);

-- =========================
-- updated_at triggers
-- =========================
do $$
begin
  if not exists (
    select 1 from pg_trigger where tgname = 'trg_materials_set_updated_at'
  ) then
    create trigger trg_materials_set_updated_at
    before update on public.materials
    for each row execute function public.set_updated_at();
  end if;

  if not exists (
    select 1 from pg_trigger where tgname = 'trg_spi_raw_set_updated_at'
  ) then
    create trigger trg_spi_raw_set_updated_at
    before update on public.source_product_ingredients_raw
    for each row execute function public.set_updated_at();
  end if;

  if not exists (
    select 1 from pg_trigger where tgname = 'trg_imm_set_updated_at'
  ) then
    create trigger trg_imm_set_updated_at
    before update on public.ingredient_material_map
    for each row execute function public.set_updated_at();
  end if;
end $$;
