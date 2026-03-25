 export type ResolutionState =
  | "raw"
  | "parsed"
  | "mapped_low_confidence"
  | "mapped_high_confidence"
  | "ambiguous"
  | "human_review_required"
  | "locked_canonical";

export type ReviewStatus =
  | "auto_accepted"
  | "needs_review"
  | "manually_approved"
  | "rejected";

export type MatchMethod =
  | "alias_exact"
  | "alias_fuzzy"
  | "alias_normalized"
  | "rule_based"
  | "pubchem_synonym"
  | "pubchem_identifier"
  | "manual_review"
  | "new_material";

export type MaterialType =
  | "chemical"
  | "mixture"
  | "extract"
  | "polymer"
  | "mineral"
  | "material"
  | "additive"
  | "label_term"
  | "unknown";

export interface RawIngredientRow {
  id: string;
  source_product_id: string;
  ingredient_raw: string;
  normalized_token: string | null;
  token_type: string | null;
  parent_group: string | null;
  raw_position: number | null;
  parser_confidence: number | null;
  resolution_state: ResolutionState | null;
  final_confidence_score: number | null;
}

export interface SourceProductRow {
  id: string;
  source: string;
  external_product_id: string | null;
  ingredient_list_text: string | null;
  inci_text: string | null;
  updated_at?: string | null;
  created_at?: string | null;
}

export interface IdentifierRecord {
  idType:
    | "pubchem_cid"
    | "cas_rn"
    | "inchikey"
    | "inchi"
    | "smiles"
    | "e_number"
    | "unii"
    | "dtxsid"
    | "wikidata"
    | "other";
  idValue: string;
  isPrimary?: boolean;
  confidence: number;
  sourceCode: string;
  metadata?: Record<string, unknown>;
}

export interface ResolveCandidate {
  materialId?: string;
  canonicalName: string;
  displayName?: string;
  materialType: MaterialType;
  matchMethod: MatchMethod;
  similarityScore: number;
  identifierStrength: number;
  sourceWeight: number;
  parserQuality: number;
  identifiers?: IdentifierRecord[];
  aliasesToAdd?: Array<{
    alias: string;
    normalized_alias: string;
    alias_type: string;
    source_code: string;
    confidence: number;
    is_preferred: boolean;
  }>;
  evidence: Record<string, unknown>;
  ambiguityGroup?: string;
}

export interface ResolutionDecision {
  rawIngredientId: string;
  normalized: string;
  winningCandidate?: ResolveCandidate;
  candidates: ResolveCandidate[];
  finalConfidenceScore: number;
  resolutionState: ResolutionState;
  reviewStatus: ReviewStatus;
  reasonCode?: string;
  needsReview: boolean;
  identifiersToAdd: IdentifierRecord[];
  aliasesToAdd: Array<{
    alias: string;
    normalized_alias: string;
    alias_type: string;
    source_code: string;
    confidence: number;
    is_preferred: boolean;
  }>;
}

export interface ResolverEnv {
  SUPABASE_URL: string;
  SUPABASE_SERVICE_ROLE_KEY: string;
  INTERNAL_FUNCTION_BEARER: string;
  LOOKUP_API_URL: string;
  RESOLVER_VERSION: string;
  WORKER_ID: string;
}
