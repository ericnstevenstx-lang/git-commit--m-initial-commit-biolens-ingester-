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

async function setReviewStatus(queueId: string, status: "approved" | "rejected" | "deferred", notes?: string) {
  const { error } = await supabase
    .from("material_review_queue")
    .update({
      status,
      reviewer_notes: notes ?? null,
      resolved_at: new Date().toISOString(),
    })
    .eq("id", queueId);

  if (error) throw error;
}

async function lockRawIngredient(rawIngredientId: string, confidence = 0.99) {
  const { error } = await supabase
    .from("source_product_ingredients_raw")
    .update({
      resolution_state: "locked_canonical",
      final_confidence_score: confidence,
    })
    .eq("id", rawIngredientId);

  if (error) throw error;
}

async function lockMaterial(materialId: string) {
  const { error } = await supabase
    .from("materials")
    .update({
      resolution_state: "locked_canonical",
      normalization_status: "canonical",
      confidence_score: 0.99,
    })
    .eq("id", materialId);

  if (error) throw error;
}

serve(async (req) => {
  try {
    const auth = req.headers.get("authorization") || "";
    if (!INTERNAL_FUNCTION_BEARER || auth !== `Bearer ${INTERNAL_FUNCTION_BEARER}`) {
      return json({ error: "unauthorized" }, 401);
    }

    const body = await req.json();
    const action = String(body.action || "");
    const queueId = String(body.queue_id || "");
    const reviewerNotes = String(body.reviewer_notes || "");

    if (!queueId) {
      return json({ error: "queue_id is required" }, 400);
    }

    const { data: reviewRow, error: reviewErr } = await supabase
      .from("material_review_queue")
      .select("*")
      .eq("id", queueId)
      .single();

    if (reviewErr || !reviewRow) {
      return json({ error: "review queue item not found" }, 404);
    }

    if (action === "approve_existing_material") {
      const materialId = String(body.material_id || "");
      if (!materialId) return json({ error: "material_id required" }, 400);

      const payload = {
        raw_ingredient_id: reviewRow.raw_ingredient_id,
        material_id: materialId,
        match_method: "manual_review",
        final_confidence_score: 0.99,
        resolution_state: "locked_canonical",
        review_status: "manually_approved",
        resolver_version: "reviewer-v1",
        aliases_to_add: [],
        identifiers_to_add: [],
        evidence: {
          reviewer_action: "approve_existing_material",
          queue_id: queueId,
          reviewer_notes: reviewerNotes,
        },
      };

      const { data, error } = await supabase.rpc("persist_resolution_result", {
        p_payload: payload,
      });
      if (error) throw error;

      await setReviewStatus(queueId, "approved", reviewerNotes);
      await lockRawIngredient(reviewRow.raw_ingredient_id, 0.99);
      await lockMaterial(materialId);

      return json({
        ok: true,
        action,
        material_id: materialId,
        mapping: data,
      });
    }

    if (action === "create_new_material_and_map") {
      const canonicalName = String(body.canonical_name || "").trim();
      const displayName = String(body.display_name || canonicalName).trim();
      const materialType = String(body.material_type || "chemical");

      if (!canonicalName) return json({ error: "canonical_name required" }, 400);

      const payload = {
        raw_ingredient_id: reviewRow.raw_ingredient_id,
        material_id: null,
        match_method: "manual_review",
        final_confidence_score: 0.99,
        resolution_state: "locked_canonical",
        review_status: "manually_approved",
        resolver_version: "reviewer-v1",
        aliases_to_add: [
          {
            alias: reviewRow.candidate_json?.[0]?.evidence?.alias || canonicalName,
            normalized_alias: canonicalName.toLowerCase(),
            alias_type: "label",
            source_code: "manual_review",
            confidence: 0.99,
            is_preferred: false,
          },
        ],
        identifiers_to_add: Array.isArray(body.identifiers) ? body.identifiers : [],
        new_material: {
          canonical_name: canonicalName,
          display_name: displayName,
          material_type: materialType,
        },
        evidence: {
          reviewer_action: "create_new_material_and_map",
          queue_id: queueId,
          reviewer_notes: reviewerNotes,
        },
      };

      const { data, error } = await supabase.rpc("persist_resolution_result", {
        p_payload: payload,
      });
      if (error) throw error;

      await setReviewStatus(queueId, "approved", reviewerNotes);
      await lockRawIngredient(reviewRow.raw_ingredient_id, 0.99);
      if (data?.material_id) await lockMaterial(data.material_id);

      return json({
        ok: true,
        action,
        material_id: data?.material_id,
        mapping: data,
      });
    }

    if (action === "reject_candidate") {
      await setReviewStatus(queueId, "rejected", reviewerNotes);
      return json({ ok: true, action });
    }

    if (action === "defer_review") {
      await setReviewStatus(queueId, "deferred", reviewerNotes);
      return json({ ok: true, action });
    }

    if (action === "merge_materials") {
      const fromMaterialId = String(body.from_material_id || "");
      const intoMaterialId = String(body.into_material_id || "");

      if (!fromMaterialId || !intoMaterialId) {
        return json({ error: "from_material_id and into_material_id required" }, 400);
      }

      // Move aliases
      {
        const { error } = await supabase
          .from("material_aliases")
          .update({ material_id: intoMaterialId })
          .eq("material_id", fromMaterialId);
        if (error) throw error;
      }

      // Move identifiers when conflict-free
      {
        const { data: ids, error } = await supabase
          .from("material_identifiers")
          .select("*")
          .eq("material_id", fromMaterialId);
        if (error) throw error;

        for (const id of ids ?? []) {
          const { error: upsertErr } = await supabase.from("material_identifiers").upsert({
            material_id: intoMaterialId,
            id_type: id.id_type,
            id_value: id.id_value,
            is_primary: id.is_primary,
            source_code: id.source_code,
            confidence: id.confidence,
            metadata: id.metadata,
          }, { onConflict: "id_type,id_value" });
          if (upsertErr) throw upsertErr;
        }
      }

      // Move mappings
      {
        const { error } = await supabase
          .from("ingredient_material_map")
          .update({ material_id: intoMaterialId })
          .eq("material_id", fromMaterialId);
        if (error) throw error;
      }

      // Add explicit same_as edge
      {
        const { error } = await supabase.from("material_relationships").upsert({
          material_id: fromMaterialId,
          related_material_id: intoMaterialId,
          relationship_type: "same_as",
          source_code: "manual_review",
          confidence: 0.99,
          metadata: { reviewer_action: "merge_materials" },
        }, { onConflict: "material_id,related_material_id,relationship_type" });

        if (error) throw error;
      }

      // Deprecate old material
      {
        const { error } = await supabase
          .from("materials")
          .update({
            normalization_status: "deprecated",
            resolution_state: "deprecated",
            is_active: false,
          })
          .eq("id", fromMaterialId);

        if (error) throw error;
      }

      await setReviewStatus(queueId, "approved", reviewerNotes);

      return json({
        ok: true,
        action,
        merged_from: fromMaterialId,
        merged_into: intoMaterialId,
      });
    }

    return json({ error: `unsupported action: ${action}` }, 400);
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
