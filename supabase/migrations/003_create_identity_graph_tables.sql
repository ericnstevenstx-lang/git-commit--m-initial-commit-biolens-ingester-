-- 003_create_identity_graph_tables.sql

-- =========================
-- material_identifiers
-- =========================
create table if not exists public.material_identifiers (
  id uuid primary key default gen_random_uuid(),
  material_id uuid not null references public.materials(id) on delete cascade,
  id_type text not null
    check (id_type in (
      'pubchem_cid',
      'cas_rn',
      'inchikey',
      'inchi',
      'smiles',
      'e_number',
      'unii',
      'dtxsid',
      'wikidata',
      'other'
    )),
  id_value text not null,
  is_primary boolean default false,
  source_code text,
  confidence numeric(5,4),
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Important:
-- admin-approval/index.ts uses upsert on (id_type, id_value)
-- so this unique index is required.
create unique index if not exists ux_material_identifiers_type_value
  on public.material_identifiers (id_type, id_value);

create index if not exists ix_material_identifiers_material_id
  on public.material_identifiers (material_id);

create index if not exists ix_material_identifiers_type
  on public.material_identifiers (id_type);

-- =========================
-- material_relationships
-- =========================
create table if not exists public.material_relationships (
  id uuid primary key default gen_random_uuid(),
  material_id uuid not null references public.materials(id) on delete cascade,
  related_material_id uuid not null references public.materials(id) on delete cascade,
  relationship_type text not null
    check (relationship_type in (
      'synonym_of',
      'parent_of',
      'child_of',
      'component_of',
      'contains',
      'derived_from',
      'same_as',
      'broader_than',
      'narrower_than'
    )),
  source_code text,
  confidence numeric(5,4),
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  check (material_id <> related_material_id)
);

create unique index if not exists ux_material_relationships_triplet
  on public.material_relationships (material_id, related_material_id, relationship_type);

create index if not exists ix_material_relationships_material_id
  on public.material_relationships (material_id);

create index if not exists ix_material_relationships_related_material_id
  on public.material_relationships (related_material_id);

-- =========================
-- optional source weights
-- =========================
create table if not exists public.source_weights (
  source_code text primary key,
  source_weight numeric(5,4) not null,
  notes text
);

insert into public.source_weights (source_code, source_weight, notes) values
  ('pubchem', 0.95, 'canonical chemistry reference'),
  ('openfoodfacts', 0.82, 'open food product evidence'),
  ('openbeautyfacts', 0.82, 'open beauty product evidence'),
  ('ewg', 0.76, 'useful enrichment but not canonical'),
  ('manual_review', 0.99, 'reviewer-approved truth')
on conflict (source_code) do update
set source_weight = excluded.source_weight,
    notes = excluded.notes;

-- =========================
-- updated_at triggers
-- =========================
do $$
begin
  if not exists (
    select 1 from pg_trigger where tgname = 'trg_material_identifiers_set_updated_at'
  ) then
    create trigger trg_material_identifiers_set_updated_at
    before update on public.material_identifiers
    for each row execute function public.set_updated_at();
  end if;

  if not exists (
    select 1 from pg_trigger where tgname = 'trg_material_relationships_set_updated_at'
  ) then
    create trigger trg_material_relationships_set_updated_at
    before update on public.material_relationships
    for each row execute function public.set_updated_at();
  end if;
end $$;
