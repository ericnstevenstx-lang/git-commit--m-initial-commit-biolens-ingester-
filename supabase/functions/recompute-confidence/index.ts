// supabase/functions/recompute-confidence/index.ts

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const INTERNAL_FUNCTION_BEARER = Deno.env.get("INTERNAL_FUNCTION_BEARER") || "";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function unique<T>(arr: T[]): T[] {
  return [...new Set(arr)];
}

function deriveResolutionState(score: number): string {
  if (score >= 0.85) return "mapped_high_confidence";
  if (score >= 0.65) return "mapped_low_confidence";
  if (score > 0) return "human_review_required";
  return "parsed";
}

async function recomputeRawIngredientState(rawIngredientId: string) {
  const { data, error } = await supabase
    .from("ingredient_material_map")
    .select("match_confidence, review_status")
    .eq("raw_ingredient_id", rawIngredientId);

  if (error) throw error;

  const rows = data ?? [];
  const best = rows.reduce((max, row) => Math.max(max, row.match_confidence ?? 0), 0);

  const resolutionState = deriveResolutionState(best);

  const { error: updateErr } = await supabase
    .from("source_product_ingredients_raw")
    .update({
      final_confidence_score: best,
      resolution_state: resolutionState,
    })
    .eq("id", rawIngredientId);

  if (updateErr) throw updateErr;

  return {
    raw_ingredient_id: rawIngredientId,
    final_confidence_score: best,
    resolution_state: resolutionState,
  };
}

async function materialIdsFromRawIngredientIds(rawIngredientIds: string[]): Promise<string[]> {
  if (!rawIngredientIds.length) return [];

  const { data, error } = await supabase
    .from("ingredient_material_map")
    .select("raw_ingredient_id, material_id")
    .in("raw_ingredient_id", rawIngredientIds);

  if (error) throw error;
  return unique((data ?? []).map((r: any) => r.material_id).filter(Boolean));
}

async function recomputeMaterial(materialId: string) {
  const { data, error } = await supabase.rpc("recompute_material_confidence", {
    p_material_id: materialId,
  });

  if (error) throw error;

  return {
    material_id: materialId,
    material_confidence_score: data,
  };
}

serve(async (req) => {
  try {
    const auth = req.headers.get("authorization") || "";
    if (!INTERNAL_FUNCTION_BEARER || auth !== `Bearer ${INTERNAL_FUNCTION_BEARER}`) {
      return json({ error: "unauthorized" }, 401);
    }

    const body = await req.json().catch(() => ({}));

    const materialIds = unique(
      [
        ...(Array.isArray(body.material_ids) ? body.material_ids : []),
        ...(body.material_id ? [body.material_id] : []),
      ].filter(Boolean)
    );

    const rawIngredientIds = unique(
      [
        ...(Array.isArray(body.raw_ingredient_ids) ? body.raw_ingredient_ids : []),
        ...(body.raw_ingredient_id ? [body.raw_ingredient_id] : []),
      ].filter(Boolean)
    );

    const rawResults = [];
    for (const rawId of rawIngredientIds) {
      rawResults.push(await recomputeRawIngredientState(rawId));
    }

    const derivedMaterialIds = await materialIdsFromRawIngredientIds(rawIngredientIds);
    const allMaterialIds = unique([...materialIds, ...derivedMaterialIds]);

    const materialResults = [];
    for (const materialId of allMaterialIds) {
      materialResults.push(await recomputeMaterial(materialId));
    }

    return json({
      ok: true,
      raw_results: rawResults,
      material_results: materialResults,
      raw_count: rawResults.length,
      material_count: materialResults.length,
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
