// supabase/functions/reviewer-list-open-queue/index.ts

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

serve(async (req) => {
  try {
    const auth = req.headers.get("authorization") || "";
    if (!INTERNAL_FUNCTION_BEARER || auth !== `Bearer ${INTERNAL_FUNCTION_BEARER}`) {
      return json({ error: "unauthorized" }, 401);
    }

    const url = new URL(req.url);
    const status = url.searchParams.get("status") || "open";
    const reasonCode = url.searchParams.get("reason_code");
    const q = (url.searchParams.get("q") || "").trim().toLowerCase();
    const limit = Math.min(Number(url.searchParams.get("limit") || 50), 200);
    const offset = Math.max(Number(url.searchParams.get("offset") || 0), 0);

    let query = supabase
      .from("material_review_queue")
      .select("*", { count: "exact" })
      .eq("status", status)
      .order("created_at", { ascending: true })
      .range(offset, offset + limit - 1);

    if (reasonCode) query = query.eq("reason_code", reasonCode);

    const { data: queueRows, error: queueErr, count } = await query;
    if (queueErr) throw queueErr;

    const rawIngredientIds = unique((queueRows ?? []).map((r: any) => r.raw_ingredient_id).filter(Boolean));
    const proposedMaterialIds = unique((queueRows ?? []).map((r: any) => r.proposed_material_id).filter(Boolean));

    const [{ data: rawRows, error: rawErr }, { data: materials, error: matErr }] = await Promise.all([
      rawIngredientIds.length
        ? supabase
            .from("source_product_ingredients_raw")
            .select(`
              id,
              source_product_id,
              ingredient_raw,
              normalized_token,
              token_type,
              parent_group,
              parser_confidence,
              resolution_state,
              final_confidence_score
            `)
            .in("id", rawIngredientIds)
        : Promise.resolve({ data: [], error: null }),
      proposedMaterialIds.length
        ? supabase
            .from("materials")
            .select("id, canonical_name, display_name, material_type, confidence_score")
            .in("id", proposedMaterialIds)
        : Promise.resolve({ data: [], error: null }),
    ]);

    if (rawErr) throw rawErr;
    if (matErr) throw matErr;

    const sourceProductIds = unique((rawRows ?? []).map((r: any) => r.source_product_id).filter(Boolean));

    const { data: sourceProducts, error: spErr } = sourceProductIds.length
      ? await supabase
          .from("source_products_raw")
          .select("id, source, external_product_id")
          .in("id", sourceProductIds)
      : { data: [], error: null };

    if (spErr) throw spErr;

    const rawMap = new Map((rawRows ?? []).map((r: any) => [r.id, r]));
    const materialMap = new Map((materials ?? []).map((m: any) => [m.id, m]));
    const sourceProductMap = new Map((sourceProducts ?? []).map((p: any) => [p.id, p]));

    const enriched = (queueRows ?? [])
      .map((row: any) => {
        const raw = rawMap.get(row.raw_ingredient_id);
        const material = row.proposed_material_id ? materialMap.get(row.proposed_material_id) : null;
        const product = raw?.source_product_id ? sourceProductMap.get(raw.source_product_id) : null;

        return {
          queue_id: row.id,
          status: row.status,
          reason_code: row.reason_code,
          created_at: row.created_at,
          reviewer_notes: row.reviewer_notes,
          raw_ingredient: raw
            ? {
                id: raw.id,
                ingredient_raw: raw.ingredient_raw,
                normalized_token: raw.normalized_token,
                token_type: raw.token_type,
                parent_group: raw.parent_group,
                parser_confidence: raw.parser_confidence,
                resolution_state: raw.resolution_state,
                final_confidence_score: raw.final_confidence_score,
              }
            : null,
          source_product: product
            ? {
                id: product.id,
                source: product.source,
                external_product_id: product.external_product_id,
              }
            : null,
          proposed_material: material
            ? {
                id: material.id,
                canonical_name: material.canonical_name,
                display_name: material.display_name,
                material_type: material.material_type,
                confidence_score: material.confidence_score,
              }
            : null,
          candidate_json: row.candidate_json,
        };
      })
      .filter((item) => {
        if (!q) return true;
        const haystack = [
          item.raw_ingredient?.ingredient_raw,
          item.raw_ingredient?.normalized_token,
          item.proposed_material?.canonical_name,
          item.source_product?.source,
          item.reason_code,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();

        return haystack.includes(q);
      });

    return json({
      ok: true,
      count: count ?? enriched.length,
      limit,
      offset,
      items: enriched,
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
