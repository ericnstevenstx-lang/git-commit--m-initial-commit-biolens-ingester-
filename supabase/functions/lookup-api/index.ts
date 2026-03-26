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

function unauthorized() {
  return json({ error: "unauthorized" }, 401);
}

function normalizeText(input: string) {
  return input
    .toLowerCase()
    .normalize("NFKD")
    .replace(/\s*\(([^)]+)\)\s*/g, "")
    .replace(/[^\p{L}\p{N}\s\-\+\/]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function pubchemPropertyLookupByName(name: string) {
  const url =
    `https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/` +
    `${encodeURIComponent(name)}/property/` +
    `Title,IUPACName,InChI,InChIKey,CanonicalSMILES,MolecularFormula,MolecularWeight/JSON`;

  const res = await fetch(url);
  if (!res.ok) return [];
  const json = await res.json();
  return json?.PropertyTable?.Properties ?? [];
}

async function pubchemPropertyLookupByIdentifier(idType: string, idValue: string) {
  let namespace = "name";
  if (idType === "cas_rn") namespace = "xref/RN";
  else if (idType === "inchikey") namespace = "inchikey";
  else if (idType === "inchi") namespace = "inchi";
  else if (idType === "smiles") namespace = "smiles";

  const url =
    `https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/` +
    `${namespace}/${encodeURIComponent(idValue)}/property/` +
    `Title,IUPACName,InChI,InChIKey,CanonicalSMILES,MolecularFormula,MolecularWeight/JSON`;

  const res = await fetch(url);
  if (!res.ok) return [];
  const json = await res.json();
  return json?.PropertyTable?.Properties ?? [];
}

serve(async (req) => {
  try {
  const internalBearer = req.headers.get("x-internal-bearer") || "";
  if (!INTERNAL_FUNCTION_BEARER || internalBearer !== INTERNAL_FUNCTION_BEARER) {
  return unauthorized();
}


    const body = await req.json();
    const queryText = String(body.query_text || "").trim();
    const sourceCode = String(body.source_code || "resolver");
    const identifiers = Array.isArray(body.identifiers) ? body.identifiers : [];

    const normalized = normalizeText(queryText);
    const candidates: any[] = [];

    // 1) local alias candidates
    if (normalized) {
      const { data, error } = await supabase.rpc("find_material_candidates", {
        p_normalized_text: normalized,
        p_limit: 10,
      });
      if (error) throw error;

      for (const row of data ?? []) {
        candidates.push({
          material_id: row.material_id,
          canonical_name: row.matched_alias,
          display_name: row.matched_alias,
          material_type: "chemical",
          match_method: row.match_method,
          similarity_score: Number(row.similarity_score),
          identifier_strength: row.match_method === "alias_exact" ? 0.92 : 0.78,
          source_code: "local_alias",
          identifiers: [],
          aliases_to_add: [],
          evidence: {
            alias: row.matched_alias,
            normalized_alias: row.normalized_alias,
            alias_type: row.alias_type,
          },
        });
      }
    }

    // 2) PubChem lookup by identifiers first
    for (const id of identifiers) {
      if (!id?.idType || !id?.idValue) continue;
      const props = await pubchemPropertyLookupByIdentifier(id.idType, id.idValue);

      for (const p of props) {
        candidates.push({
          canonical_name: p.Title || p.IUPACName || queryText,
          display_name: p.Title || p.IUPACName || queryText,
          material_type: "chemical",
          match_method: "pubchem_identifier",
          similarity_score: 0.98,
          identifier_strength: 1.0,
          source_code: "pubchem",
          identifiers: [
            p.CID
              ? {
                  id_type: "pubchem_cid",
                  id_value: String(p.CID),
                  is_primary: true,
                  confidence: 0.99,
                  sourceCode: "pubchem",
                }
              : null,
            p.InChIKey
              ? {
                  id_type: "inchikey",
                  id_value: p.InChIKey,
                  confidence: 0.99,
                  sourceCode: "pubchem",
                }
              : null,
            p.InChI
              ? {
                  id_type: "inchi",
                  id_value: p.InChI,
                  confidence: 0.98,
                  sourceCode: "pubchem",
                }
              : null,
            p.CanonicalSMILES
              ? {
                  id_type: "smiles",
                  id_value: p.CanonicalSMILES,
                  confidence: 0.97,
                  sourceCode: "pubchem",
                }
              : null,
          ].filter(Boolean),
          aliases_to_add: normalized
            ? [
                {
                  alias: queryText,
                  normalized_alias: normalized,
                  alias_type: "pubchem_synonym",
                  source_code: "pubchem",
                  confidence: 0.95,
                  is_preferred: false,
                },
              ]
            : [],
          evidence: {
            pubchem_cid: p.CID,
            molecular_formula: p.MolecularFormula,
            molecular_weight: p.MolecularWeight,
            iupac_name: p.IUPACName,
          },
        });
      }
    }

    // 3) fallback PubChem lookup by name
    if (!candidates.some((c) => c.match_method === "pubchem_identifier") && normalized) {
      const props = await pubchemPropertyLookupByName(normalized);

      for (const p of props) {
        candidates.push({
          canonical_name: p.Title || p.IUPACName || queryText,
          display_name: p.Title || p.IUPACName || queryText,
          material_type: "chemical",
          match_method: "pubchem_synonym",
          similarity_score: 0.78,
          identifier_strength: p.InChIKey ? 0.98 : 0.88,
          source_code: "pubchem",
          identifiers: [
            p.CID
              ? {
                  id_type: "pubchem_cid",
                  id_value: String(p.CID),
                  is_primary: true,
                  confidence: 0.98,
                  sourceCode: "pubchem",
                }
              : null,
            p.InChIKey
              ? {
                  id_type: "inchikey",
                  id_value: p.InChIKey,
                  confidence: 0.98,
                  sourceCode: "pubchem",
                }
              : null,
            p.InChI
              ? {
                  id_type: "inchi",
                  id_value: p.InChI,
                  confidence: 0.97,
                  sourceCode: "pubchem",
                }
              : null,
            p.CanonicalSMILES
              ? {
                  id_type: "smiles",
                  id_value: p.CanonicalSMILES,
                  confidence: 0.96,
                  sourceCode: "pubchem",
                }
              : null,
          ].filter(Boolean),
          aliases_to_add: [
            {
              alias: queryText,
              normalized_alias: normalized,
              alias_type: "pubchem_synonym",
              source_code: "pubchem",
              confidence: 0.90,
              is_preferred: false,
            },
          ],
          evidence: {
            pubchem_cid: p.CID,
            source_code: sourceCode,
            molecular_formula: p.MolecularFormula,
            molecular_weight: p.MolecularWeight,
          },
        });
      }
    }

    // Deduplicate candidate outputs by canonical_name + primary identifier if possible
    const deduped = new Map<string, any>();
    for (const c of candidates) {
      const primaryId =
        c.identifiers?.find((x: any) => x.id_type === "pubchem_cid")?.id_value ||
        c.identifiers?.find((x: any) => x.id_type === "inchikey")?.id_value ||
        c.canonical_name;
      const key = `${c.match_method}::${primaryId}`;
      if (!deduped.has(key)) deduped.set(key, c);
    }

    return json({
      ok: true,
      normalized,
      candidates: Array.from(deduped.values()),
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
