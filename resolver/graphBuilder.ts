import { createClient, SupabaseClient } from "@supabase/supabase-js";

type RelationshipType =
  | "synonym_of"
  | "parent_of"
  | "child_of"
  | "component_of"
  | "contains"
  | "derived_from"
  | "same_as"
  | "broader_than"
  | "narrower_than";

interface MaterialRow {
  id: string;
  canonical_name: string | null;
  display_name: string | null;
  material_type: string | null;
}

interface RawIngredientWithMap {
  id: string;
  ingredient_raw: string;
  normalized_token: string | null;
  token_type: string | null;
  material_id: string;
}

export class GraphBuilder {
  private supabase: SupabaseClient;

  constructor(params: { supabaseUrl: string; serviceRoleKey: string }) {
    this.supabase = createClient(params.supabaseUrl, params.serviceRoleKey, {
      auth: { persistSession: false },
    });
  }

  async upsertRelationship(input: {
    materialId: string;
    relatedMaterialId: string;
    relationshipType: RelationshipType;
    sourceCode: string;
    confidence: number;
    metadata?: Record<string, unknown>;
  }) {
    const payload = {
      material_id: input.materialId,
      related_material_id: input.relatedMaterialId,
      relationship_type: input.relationshipType,
      source_code: input.sourceCode,
      confidence: input.confidence,
      metadata: input.metadata ?? {},
    };

    const { error } = await this.supabase
      .from("material_relationships")
      .upsert(payload, {
        onConflict: "material_id,related_material_id,relationship_type",
      });

    if (error) throw error;
  }

  async getMaterialByName(name: string): Promise<MaterialRow | null> {
    const { data, error } = await this.supabase
      .from("materials")
      .select("id, canonical_name, display_name, material_type")
      .ilike("canonical_name", name)
      .limit(1)
      .maybeSingle();

    if (error) throw error;
    return data;
  }

  async buildGroupRelationshipsForRawIngredient(rawIngredientId: string) {
    const { data, error } = await this.supabase
      .from("ingredient_material_map")
      .select(`
        material_id,
        source_product_ingredients_raw!inner (
          id,
          ingredient_raw,
          normalized_token,
          token_type
        )
      `)
      .eq("raw_ingredient_id", rawIngredientId)
      .maybeSingle();

    if (error) throw error;
    if (!data) return;

    const mappedMaterialId = data.material_id as string;
    const raw = data.source_product_ingredients_raw as unknown as RawIngredientWithMap;

    // Example: fragrance/parfum maps broader_than common fragrance constituents later
    if (raw.token_type === "fragrance_group") {
      const limonene = await this.getMaterialByName("Limonene");
      if (limonene) {
        await this.upsertRelationship({
          materialId: mappedMaterialId,
          relatedMaterialId: limonene.id,
          relationshipType: "broader_than",
          sourceCode: "graph_builder",
          confidence: 0.70,
          metadata: { rule: "fragrance_group_constituent_seed" },
        });
      }
    }

    if (raw.token_type === "contains_less_than") {
      // This is a semantic containment hint, not concentration truth.
      // The actual component relationship should be reinforced only when explicit.
      return;
    }
  }

  async buildCompositeRelationships(materialId: string, rawIngredientText: string) {
    const parts = rawIngredientText
      .split(/\s(?:\+|and|\/)\s/i)
      .map((s) => s.trim())
      .filter(Boolean);

    if (parts.length < 2) return;

    for (const part of parts) {
      const { data: candidates, error } = await this.supabase.rpc("find_material_candidates", {
        p_normalized_text: part.toLowerCase(),
        p_limit: 3,
      });
      if (error) throw error;

      const first = candidates?.[0];
      if (!first?.material_id) continue;

      await this.upsertRelationship({
        materialId,
        relatedMaterialId: first.material_id,
        relationshipType: "contains",
        sourceCode: "graph_builder",
        confidence: 0.82,
        metadata: {
          rule: "composite_contains_split",
          raw_part: part,
        },
      });

      await this.upsertRelationship({
        materialId: first.material_id,
        relatedMaterialId: materialId,
        relationshipType: "component_of",
        sourceCode: "graph_builder",
        confidence: 0.82,
        metadata: {
          rule: "composite_contains_split",
          raw_part: part,
        },
      });
    }
  }

  async seedDerivedFromRelationships() {
    const derivationSeeds: Array<{ material: string; parent: string; confidence: number }> = [
      { material: "Polyester", parent: "Petroleum", confidence: 0.90 },
      { material: "PLA", parent: "Corn Starch", confidence: 0.86 },
      { material: "Melamine Resin", parent: "Melamine", confidence: 0.92 },
    ];

    for (const seed of derivationSeeds) {
      const child = await this.getMaterialByName(seed.material);
      const parent = await this.getMaterialByName(seed.parent);

      if (!child || !parent) continue;

      await this.upsertRelationship({
        materialId: child.id,
        relatedMaterialId: parent.id,
        relationshipType: "derived_from",
        sourceCode: "graph_builder",
        confidence: seed.confidence,
        metadata: { rule: "seed_derived_from" },
      });
    }
  }

  async recomputeMaterialConfidence(materialId: string) {
    const { error } = await this.supabase.rpc("recompute_material_confidence", {
      p_material_id: materialId,
    });
    if (error) throw error;
  }

  async buildForMaterial(materialId: string, rawIngredientText?: string) {
    if (rawIngredientText) {
      await this.buildCompositeRelationships(materialId, rawIngredientText);
    }
    await this.recomputeMaterialConfidence(materialId);
  }
}
