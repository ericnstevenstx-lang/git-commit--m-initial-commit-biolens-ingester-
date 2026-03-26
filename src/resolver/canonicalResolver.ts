import { createClient, SupabaseClient } from "@supabase/supabase-js";
import { z } from "zod";
import {
  RawIngredientRow,
  ResolveCandidate,
  ResolverEnv,
  IdentifierRecord,
  MaterialType,
} from "./types";
import { arbitrateResolution } from "./conflictArbiter";
import { GraphBuilder } from "./graphBuilder";

const EnvSchema = z.object({
  SUPABASE_URL: z.string().url(),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
  INTERNAL_FUNCTION_BEARER: z.string().min(1),
  LOOKUP_API_URL: z.string().url(),
  RESOLVER_VERSION: z.string().default("resolver-v1"),
  WORKER_ID: z.string().default(`worker-${process.pid}`),
});

function normalizeIngredient(input: string): string {
  return input
    .toLowerCase()
    .normalize("NFKD")
    .replace(/\s*\(([^)]+)\)\s*/g, "")
    .replace(/\borganic\b/g, "")
    .replace(/\bmay contain\b/g, "")
    .replace(/[^\p{L}\p{N}\s\-\+\/]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractIdentifiers(raw: string): IdentifierRecord[] {
  const ids: IdentifierRecord[] = [];

  const cas = raw.match(/\b\d{2,7}-\d{2}-\d\b/);
  if (cas) {
    ids.push({
      idType: "cas_rn",
      idValue: cas[0],
      confidence: 0.92,
      sourceCode: "resolver",
    });
  }

  const eNum = raw.match(/\bE\d{3,4}[A-Za-z]?\b/);
  if (eNum) {
    ids.push({
      idType: "e_number",
      idValue: eNum[0].toUpperCase(),
      confidence: 0.90,
      sourceCode: "resolver",
    });
  }

  return ids;
}

function inferMaterialType(normalized: string, tokenType?: string | null): MaterialType {
  if (tokenType === "fragrance_group") return "label_term";
  if (tokenType === "contains_less_than") return "label_term";
  if (/extract/i.test(normalized)) return "extract";
  if (/resin|polymer|polyethylene|polypropylene|polyester|nylon/i.test(normalized)) return "polymer";
  if (/flavor|fragrance|parfum/i.test(normalized)) return "label_term";
  if (/e\d{3,4}[a-z]?/i.test(normalized)) return "additive";
  return "chemical";
}

function sourceWeightFor(sourceCode: string): number {
  switch (sourceCode) {
    case "pubchem":
      return 0.95;
    case "openfoodfacts":
    case "openbeautyfacts":
      return 0.82;
    case "ewg":
      return 0.76;
    case "manual_review":
      return 0.99;
    default:
      return 0.75;
  }
}

export class CanonicalResolver {
  private env: ResolverEnv;
  private supabase: SupabaseClient;
  private graphBuilder: GraphBuilder;

  constructor(envInput: Record<string, string | undefined>) {
    this.env = EnvSchema.parse(envInput) as ResolverEnv;
    this.supabase = createClient(
      this.env.SUPABASE_URL,
      this.env.SUPABASE_SERVICE_ROLE_KEY,
      { auth: { persistSession: false } }
    );

    this.graphBuilder = new GraphBuilder({
      supabaseUrl: this.env.SUPABASE_URL,
      serviceRoleKey: this.env.SUPABASE_SERVICE_ROLE_KEY,
    });
  }

  async fetchRawIngredient(rawIngredientId: string): Promise<RawIngredientRow> {
    const { data, error } = await this.supabase
      .from("source_product_ingredients_raw")
      .select(`
        id,
        source_product_id,
        ingredient_raw,
        normalized_token,
        token_type,
        parent_group,
        raw_position,
        parser_confidence,
        resolution_state,
        final_confidence_score
      `)
      .eq("id", rawIngredientId)
      .single();

    if (error) throw error;
    return data as RawIngredientRow;
  }

  async fetchSourceProduct(sourceProductId: string) {
    const { data, error } = await this.supabase
      .from("source_products_raw")
      .select("id, source, external_product_id, created_at, updated_at")
      .eq("id", sourceProductId)
      .single();

    if (error) throw error;
    return data as {
      id: string;
      source: string;
      external_product_id: string | null;
      created_at?: string | null;
      updated_at?: string | null;
    };
  }

  async aliasCandidates(normalized: string): Promise<ResolveCandidate[]> {
    const { data, error } = await this.supabase.rpc("find_material_candidates", {
      p_normalized_text: normalized,
      p_limit: 10,
    });

    if (error) throw error;

    return (data ?? []).map((row: any) => ({
      materialId: row.material_id,
      canonicalName: row.matched_alias,
      displayName: row.matched_alias,
      materialType: "chemical" as MaterialType,
      matchMethod: row.match_method,
      similarityScore: Number(row.similarity_score),
      identifierStrength: row.match_method === "alias_exact" ? 0.92 : 0.78,
      sourceWeight: 0.84,
      parserQuality: 0.93,
      evidence: {
        alias: row.matched_alias,
        normalized_alias: row.normalized_alias,
        alias_type: row.alias_type,
      },
      aliasesToAdd: [],
      identifiers: [],
    }));
  }

  ruleCandidates(normalized: string, raw: string, tokenType?: string | null): ResolveCandidate[] {
    const out: ResolveCandidate[] = [];

    if (normalized === "water" || normalized === "aqua") {
      out.push({
        canonicalName: "Water",
        displayName: "Water",
        materialType: "chemical",
        matchMethod: "rule_based",
        similarityScore: 0.98,
        identifierStrength: 0.85,
        sourceWeight: 0.85,
        parserQuality: 0.95,
        evidence: { rule: "water_aqua_equivalence" },
        aliasesToAdd: [
          {
            alias: raw,
            normalized_alias: normalized,
            alias_type: "label",
            source_code: "resolver",
            confidence: 0.95,
            is_preferred: false,
          },
        ],
      });
    }

    if (normalized === "fragrance" || normalized === "parfum") {
      out.push({
        canonicalName: "Fragrance",
        displayName: "Fragrance",
        materialType: "label_term",
        matchMethod: "rule_based",
        similarityScore: 0.90,
        identifierStrength: 0.60,
        sourceWeight: 0.80,
        parserQuality: 0.94,
        ambiguityGroup: "group_term",
        evidence: { rule: "group_term_fragrance", tokenType },
        aliasesToAdd: [
          {
            alias: raw,
            normalized_alias: normalized,
            alias_type: "label",
            source_code: "resolver",
            confidence: 0.90,
            is_preferred: false,
          },
        ],
      });
    }

    if (/^e\d{3,4}[a-z]?$/i.test(normalized)) {
      out.push({
        canonicalName: normalized.toUpperCase(),
        displayName: normalized.toUpperCase(),
        materialType: "additive",
        matchMethod: "rule_based",
        similarityScore: 0.87,
        identifierStrength: 0.90,
        sourceWeight: 0.86,
        parserQuality: 0.92,
        evidence: { rule: "e_number_detected" },
        identifiers: [
          {
            idType: "e_number",
            idValue: normalized.toUpperCase(),
            confidence: 0.92,
            sourceCode: "resolver",
          },
        ],
        aliasesToAdd: [],
      });
    }

    return out;
  }

  async lookupApiCandidates(input: {
    queryText: string;
    identifiers: IdentifierRecord[];
    sourceCode: string;
  }): Promise<ResolveCandidate[]> {
    const res = await fetch(this.env.LOOKUP_API_URL, {
      method: "POST",
   headers: {
  "content-type": "application/json",
  "x-internal-bearer": this.env.INTERNAL_FUNCTION_BEARER,
},
      body: JSON.stringify({
        query_text: input.queryText,
        identifiers: input.identifiers,
        source_code: input.sourceCode,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`lookup-api failed: ${res.status} ${body}`);
    }

    const json = await res.json();

    return (json.candidates ?? []).map((c: any) => ({
      materialId: c.material_id,
      canonicalName: c.canonical_name,
      displayName: c.display_name ?? c.canonical_name,
      materialType: (c.material_type ?? "chemical") as MaterialType,
      matchMethod: c.match_method,
      similarityScore: Number(c.similarity_score ?? 0.75),
      identifierStrength: Number(c.identifier_strength ?? 0.90),
      sourceWeight: sourceWeightFor(c.source_code ?? "pubchem"),
      parserQuality: 0.90,
      evidence: c.evidence ?? {},
      identifiers: c.identifiers ?? [],
      aliasesToAdd: c.aliases_to_add ?? [],
    }));
  }

  private daysOld(isoDate?: string | null) {
    if (!isoDate) return 365;
    const ageMs = Date.now() - new Date(isoDate).getTime();
    return Math.max(0, Math.floor(ageMs / (1000 * 60 * 60 * 24)));
  }

  async persistDecision(params: {
    rawRow: RawIngredientRow;
    normalized: string;
    decision: ReturnType<typeof arbitrateResolution>;
  }) {
    const winner = params.decision.winningCandidate;

    const payload = {
      raw_ingredient_id: params.rawRow.id,
      material_id: winner?.materialId ?? null,
      match_method: winner?.matchMethod ?? "new_material",
      final_confidence_score: params.decision.finalConfidenceScore,
      resolution_state: params.decision.resolutionState,
      review_status: params.decision.reviewStatus,
      resolver_version: this.env.RESOLVER_VERSION,
      aliases_to_add: params.decision.aliasesToAdd,
      identifiers_to_add: params.decision.identifiersToAdd.map((id) => ({
        id_type: id.idType,
        id_value: id.idValue,
        is_primary: !!id.isPrimary,
        source_code: id.sourceCode,
        confidence: id.confidence,
        metadata: id.metadata ?? {},
      })),
      new_material: winner?.materialId
        ? null
        : {
            canonical_name: winner?.canonicalName ?? params.normalized,
            display_name: winner?.displayName ?? winner?.canonicalName ?? params.normalized,
            material_type: winner?.materialType ?? inferMaterialType(params.normalized, params.rawRow.token_type),
          },
      evidence: {
        normalized: params.normalized,
        token_type: params.rawRow.token_type,
        parent_group: params.rawRow.parent_group,
        candidates: params.decision.candidates,
      },
    };

    const { data, error } = await this.supabase.rpc("persist_resolution_result", {
      p_payload: payload,
    });

    if (error) throw error;
    return data as { ok: boolean; material_id: string; mapping_id: string };
  }

  async enqueueReview(params: {
    rawIngredientId: string;
    proposedMaterialId?: string;
    candidates: ResolveCandidate[];
    reasonCode: string;
  }) {
    const { error } = await this.supabase.from("material_review_queue").insert({
      raw_ingredient_id: params.rawIngredientId,
      proposed_material_id: params.proposedMaterialId ?? null,
      candidate_json: params.candidates,
      reason_code: params.reasonCode,
      status: "open",
    });

    if (error) throw error;
  }

  async resolveRawIngredient(rawIngredientId: string) {
    const rawRow = await this.fetchRawIngredient(rawIngredientId);
    const sourceProduct = await this.fetchSourceProduct(rawRow.source_product_id);

    const normalized =
      rawRow.normalized_token?.trim() || normalizeIngredient(rawRow.ingredient_raw);

    const parserQuality = rawRow.parser_confidence ?? 0.90;
    const localIds = extractIdentifiers(rawRow.ingredient_raw);

    const aliasCandidates = await this.aliasCandidates(normalized);
    const rules = this.ruleCandidates(normalized, rawRow.ingredient_raw, rawRow.token_type);

    const lookupCandidates =
      aliasCandidates[0]?.similarityScore >= 0.90
        ? []
        : await this.lookupApiCandidates({
            queryText: normalized,
            identifiers: localIds,
            sourceCode: sourceProduct.source,
          });

    const allCandidates = [
      ...aliasCandidates,
      ...rules,
      ...lookupCandidates,
    ];

    const decision = arbitrateResolution({
      rawIngredientId,
      normalized,
      candidates: allCandidates,
      parserQuality,
      distinctSourceCount: 1, // can later be upgraded by material evidence rollup
      daysOld: this.daysOld(sourceProduct.updated_at ?? sourceProduct.created_at),
    });

    const persisted = await this.persistDecision({
      rawRow,
      normalized,
      decision,
    });

    if (decision.needsReview) {
      await this.enqueueReview({
        rawIngredientId,
        proposedMaterialId: persisted.material_id,
        candidates: decision.candidates,
        reasonCode: decision.reasonCode ?? "needs_review",
      });
    }

    if (persisted.material_id) {
      await this.graphBuilder.buildForMaterial(persisted.material_id, rawRow.ingredient_raw);
      await this.graphBuilder.buildGroupRelationshipsForRawIngredient(rawIngredientId);
    }

    return {
      ok: true,
      raw_ingredient_id: rawIngredientId,
      material_id: persisted.material_id,
      mapping_id: persisted.mapping_id,
      resolution_state: decision.resolutionState,
      final_confidence_score: decision.finalConfidenceScore,
      needs_review: decision.needsReview,
      winning_candidate: decision.winningCandidate?.canonicalName ?? null,
    };
  }
}
