# BioLens Cosmetics Ingester

Node/TypeScript app that ingests cosmetics product and ingredient data from **EWG Skin Deep** into Supabase, then maps ingredients to your material taxonomy via `v_material_alias_lookup_safe`.

## Prerequisites

- Node.js 18+
- Supabase project with existing tables: `source_products_raw`, `source_product_ingredients_raw`, `ingredient_material_map`, `registry_sources`, `materials`, `material_aliases`, view `v_material_alias_lookup_safe`
- A `registry_sources` row with `code = 'ewg'` (e.g. name "EWG Skin Deep")

## Environment

Copy `.env.example` to `.env` and set:

- **SUPABASE_URL** (required) – your Supabase project URL
- **SUPABASE_SERVICE_ROLE_KEY** (required) – service role key for server-side writes
- **EWG_BASE_URL** (optional) – default `https://www.ewg.org`
- **EWG_RATE_LIMIT_MS** (optional) – delay between EWG requests (default 2000)

Missing required env vars cause an immediate exit with a clear error.

## Install

```bash
npm install
```

## Run steps (exact order)

### 1. Ingest products from EWG

Fetches EWG Skin Deep by category browse, then product pages; upserts into `source_products_raw` by `(source, external_product_id)`.

```bash
npm run ingest:cosmetics
```

- No artificial limit; pages through categories until no more product links.
- Rate-limited by `EWG_RATE_LIMIT_MS`.

### 2. Explode ingredients

Splits `ingredient_list_text` / `inci_text` from `source_products_raw` into one row per ingredient in `source_product_ingredients_raw` with INCI normalization and `parse_status = 'parsed'`. Idempotent: skips products that already have ingredient rows.

```bash
npm run explode:ingredients
```

### 3. Map ingredients to materials

Exact match of `normalized_token` to `v_material_alias_lookup_safe.normalized_alias`; upserts `ingredient_material_map` and sets `parse_status = 'mapped'` on matched rows.

```bash
npm run map:ingredients
```

### 4. Export audit CSVs

Writes to `out/`:

- `products_raw.csv` – `source_products_raw`
- `product_ingredients_raw.csv` – `source_product_ingredients_raw`
- `mapped_ingredients.csv` – mapping rows with ingredient detail
- `unmapped_ingredients.csv` – ingredients still `parse_status = 'parsed'`

```bash
npm run export:audit
```

## CLI commands summary

| Command | Description |
|--------|-------------|
| `npm run ingest:cosmetics` | Ingest products from EWG into `source_products_raw` |
| `npm run explode:ingredients` | Explode ingredient lists into `source_product_ingredients_raw` |
| `npm run map:ingredients` | Map ingredients to materials, update `ingredient_material_map` and status |
| `npm run export:audit` | Export audit CSVs to `out/` |

## Behaviour

- **Idempotent**: Product upsert by `(source, external_product_id)`; explode skips products that already have ingredients; map upsert by `(raw_ingredient_id, material_id)`.
- **No schema creation**: Uses existing tables only.
- **Logging**: All steps log to console.
- **No mock data**: All data comes from EWG (and your DB for mapping).
- This phase does **not** implement toxicity, scoring, or FDA cosmetics listing as primary source; FDA is for supplemental metadata only when you add it later.

## INCI normalization

Applied before alias lookup:

- Lowercase
- Trim and collapse spaces
- Remove parenthetical dual names (e.g. `WATER (AQUA)` → `water`)
- Original string kept as `ingredient_raw`

## Project structure

- `src/config.ts` – env validation and config
- `src/supabase.ts` – Supabase client (service role)
- `src/connectors/ewg.ts` – EWG fetch and parse
- `src/normalize/inci.ts` – INCI normalization
- `src/pipelines/ingestCosmetics.ts` – product upsert
- `src/pipelines/explodeIngredients.ts` – ingredient explosion
- `src/pipelines/mapIngredients.ts` – material mapping
- `src/pipelines/exportAudit.ts` – CSV export
- `src/scripts/run*.ts` – CLI entrypoints
