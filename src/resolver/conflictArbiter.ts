 import {
  ResolveCandidate,
  ResolutionDecision,
  MatchMethod,
  IdentifierRecord,
} from "./types";

const METHOD_BASE_WEIGHT: Record<MatchMethod, number> = {
  pubchem_identifier: 0.97,
  alias_exact: 0.92,
  alias_normalized: 0.86,
  pubchem_synonym: 0.80,
  rule_based: 0.78,
  alias_fuzzy: 0.68,
  manual_review: 0.99,
  new_material: 0.55,
};

function clamp01(n: number) {
  return Math.max(0, Math.min(1, Number(n.toFixed(4))));
}

export function ambiguityPenalty(topScore: number, secondScore?: number): number {
  if (secondScore == null) return 1.0;
  const delta = topScore - secondScore;

  if (delta >= 0.15) return 1.0;
  if (delta >= 0.08) return 0.90;
  if (delta >= 0.04) return 0.78;
  return 0.64;
}

export function crossSourceAgreementFactor(distinctSourceCount: number): number {
  return Math.min(1.15, 1.0 + Math.max(0, distinctSourceCount - 1) * 0.05);
}

export function recencyFactor(daysOld: number): number {
  if (daysOld <= 365) return 1.0;
  if (daysOld <= 730) return 0.97;
  if (daysOld <= 1825) return 0.92;
  return 0.88;
}

export function methodBaseWeight(method: MatchMethod): number {
  return METHOD_BASE_WEIGHT[method] ?? 0.50;
}

export function computeMappingConfidence(input: {
  matchMethod: MatchMethod;
  sourceWeight: number;
  identifierStrength: number;
  parserQuality: number;
  distinctSourceCount: number;
  daysOld: number;
  topScore: number;
  secondScore?: number;
}): number {
  const base = methodBaseWeight(input.matchMethod);
  const agreement = crossSourceAgreementFactor(input.distinctSourceCount);
  const recency = recencyFactor(input.daysOld);
  const ambiguity = ambiguityPenalty(input.topScore, input.secondScore);

  const score =
    base *
    input.sourceWeight *
    input.identifierStrength *
    input.parserQuality *
    agreement *
    recency *
    ambiguity;

  return clamp01(score);
}

export function candidateRankScore(c: ResolveCandidate): number {
  const methodPriority =
    c.matchMethod === "pubchem_identifier" ? 100 :
    c.matchMethod === "alias_exact" ? 95 :
    c.matchMethod === "alias_normalized" ? 88 :
    c.matchMethod === "rule_based" ? 82 :
    c.matchMethod === "pubchem_synonym" ? 78 :
    c.matchMethod === "alias_fuzzy" ? 65 :
    c.matchMethod === "manual_review" ? 120 :
    40;

  const idCountBoost = c.identifiers?.length ?? 0;
  return methodPriority + c.similarityScore * 10 + idCountBoost;
}

export function rankCandidates(candidates: ResolveCandidate[]): ResolveCandidate[] {
  return [...candidates].sort((a, b) => candidateRankScore(b) - candidateRankScore(a));
}

export function detectIdentifierConflict(candidates: ResolveCandidate[]): {
  hasConflict: boolean;
  message?: string;
} {
  const byIdType = new Map<string, Set<string>>();

  for (const c of candidates) {
    for (const id of c.identifiers ?? []) {
      if (!byIdType.has(id.idType)) byIdType.set(id.idType, new Set());
      byIdType.get(id.idType)!.add(id.idValue);
    }
  }

  for (const [idType, values] of byIdType.entries()) {
    if (values.size > 1 && (idType === "pubchem_cid" || idType === "inchikey" || idType === "cas_rn")) {
      return {
        hasConflict: true,
        message: `Conflicting ${idType} values detected: ${Array.from(values).join(", ")}`,
      };
    }
  }

  return { hasConflict: false };
}

function dedupeIdentifiers(ids: IdentifierRecord[]): IdentifierRecord[] {
  const seen = new Set<string>();
  const out: IdentifierRecord[] = [];

  for (const id of ids) {
    const key = `${id.idType}::${id.idValue}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(id);
  }

  return out;
}

export function arbitrateResolution(params: {
  rawIngredientId: string;
  normalized: string;
  candidates: ResolveCandidate[];
  parserQuality: number;
  distinctSourceCount: number;
  daysOld: number;
}): ResolutionDecision {
  const ranked = rankCandidates(params.candidates);

  if (!ranked.length) {
    return {
      rawIngredientId: params.rawIngredientId,
      normalized: params.normalized,
      candidates: [],
      finalConfidenceScore: 0,
      resolutionState: "human_review_required",
      reviewStatus: "needs_review",
      reasonCode: "new_material_required",
      needsReview: true,
      identifiersToAdd: [],
      aliasesToAdd: [],
    };
  }

  const winner = ranked[0];
  const runnerUp = ranked[1];
  const conflict = detectIdentifierConflict(ranked);

  const finalConfidenceScore = computeMappingConfidence({
    matchMethod: winner.matchMethod,
    sourceWeight: winner.sourceWeight,
    identifierStrength: winner.identifierStrength,
    parserQuality: winner.parserQuality || params.parserQuality,
    distinctSourceCount: params.distinctSourceCount,
    daysOld: params.daysOld,
    topScore: winner.similarityScore,
    secondScore: runnerUp?.similarityScore,
  });

  let resolutionState: ResolutionDecision["resolutionState"];
  let reviewStatus: ResolutionDecision["reviewStatus"];
  let needsReview = false;
  let reasonCode: string | undefined;

  if (conflict.hasConflict) {
    resolutionState = "human_review_required";
    reviewStatus = "needs_review";
    needsReview = true;
    reasonCode = "identifier_conflict";
  } else if (finalConfidenceScore >= 0.85) {
    resolutionState = "mapped_high_confidence";
    reviewStatus = "auto_accepted";
  } else if (finalConfidenceScore >= 0.65) {
    resolutionState = "mapped_low_confidence";
    reviewStatus = "needs_review";
    needsReview = true;
    reasonCode = "low_confidence";
  } else if (runnerUp && Math.abs(winner.similarityScore - runnerUp.similarityScore) < 0.04) {
    resolutionState = "ambiguous";
    reviewStatus = "needs_review";
    needsReview = true;
    reasonCode = "ambiguous_candidates";
  } else {
    resolutionState = "human_review_required";
    reviewStatus = "needs_review";
    needsReview = true;
    reasonCode = "human_review_required";
  }

  return {
    rawIngredientId: params.rawIngredientId,
    normalized: params.normalized,
    winningCandidate: winner,
    candidates: ranked,
    finalConfidenceScore,
    resolutionState,
    reviewStatus,
    reasonCode,
    needsReview,
    identifiersToAdd: dedupeIdentifiers(winner.identifiers ?? []),
    aliasesToAdd: winner.aliasesToAdd ?? [],
  };
}
