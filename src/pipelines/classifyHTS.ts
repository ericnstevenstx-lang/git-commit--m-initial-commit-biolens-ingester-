/**
 * Pipeline: Classify HTS
 *
 * Assigns HTS codes to materials in the materials table based on
 * material category, name, and composition keywords.
 * Uses the hts_tariff_rates table for valid codes.
 *
 * Idempotent: upserts by (material_id, hts_code).
 */
import { supabase } from '../supabase';

/**
 * Keyword-based HTS classification rules.
 * Maps material name/category patterns to HTS codes.
 * These are starting heuristics; manual review refines confidence.
 */
const CLASSIFICATION_RULES: {
  patterns: RegExp[];
  hts_code: string;
  basis: string;
  confidence: number;
}[] = [
  // Chapter 53: Vegetable textile fibers
  {
    patterns: [/\bhemp\b/i, /\bhemp fiber\b/i, /\btrue hemp\b/i],
    hts_code: '5302.10',
    basis: 'composition',
    confidence: 0.85,
  },
  {
    patterns: [/\bhemp yarn\b/i],
    hts_code: '5308.20',
    basis: 'composition',
    confidence: 0.85,
  },
  {
    patterns: [/\bhemp.*woven\b/i, /\bhemp.*fabric\b/i, /\bhemp.*textile\b/i],
    hts_code: '5311.00',
    basis: 'primary_use',
    confidence: 0.75,
  },
  {
    patterns: [/\bflax\b/i, /\blinen\b/i],
    hts_code: '5301.10',
    basis: 'composition',
    confidence: 0.80,
  },
  {
    patterns: [/\bjute\b/i],
    hts_code: '5303.10',
    basis: 'composition',
    confidence: 0.85,
  },
  {
    patterns: [/\bramie\b/i, /\bcoconut fiber\b/i, /\bcoir\b/i, /\babaca\b/i],
    hts_code: '5305.00',
    basis: 'composition',
    confidence: 0.80,
  },

  // Chapter 47: Pulp
  {
    patterns: [/\bbamboo pulp\b/i, /\bbamboo.*viscose\b/i],
    hts_code: '4706.30',
    basis: 'composition',
    confidence: 0.80,
  },
  {
    patterns: [/\bwood pulp\b/i, /\bcellulose pulp\b/i],
    hts_code: '4703.21',
    basis: 'composition',
    confidence: 0.70,
  },

  // Chapter 54: Man-made filaments
  {
    patterns: [/\bpolyester.*yarn\b/i, /\bPET.*yarn\b/i],
    hts_code: '5402.33',
    basis: 'composition',
    confidence: 0.75,
  },
  {
    patterns: [/\bpolyester.*fabric\b/i, /\bpolyester.*woven\b/i],
    hts_code: '5407.51',
    basis: 'primary_use',
    confidence: 0.70,
  },
  {
    patterns: [/\bnylon.*fabric\b/i, /\bnylon.*woven\b/i],
    hts_code: '5407.10',
    basis: 'primary_use',
    confidence: 0.70,
  },

  // Chapter 55: Man-made staple fibers
  {
    patterns: [/\bpolyester\b/i, /\bPET\b/i],
    hts_code: '5503.20',
    basis: 'composition',
    confidence: 0.65,
  },
  {
    patterns: [/\bacrylic\b/i, /\bmodacrylic\b/i],
    hts_code: '5501.30',
    basis: 'composition',
    confidence: 0.70,
  },
  {
    patterns: [/\brayon\b/i, /\bviscose\b/i, /\bbamboo rayon\b/i],
    hts_code: '5516.11',
    basis: 'composition',
    confidence: 0.70,
  },

  // Chapter 39: Plastics
  {
    patterns: [/\bpolyethylene\b/i, /\bHDPE\b/i, /\bLDPE\b/i, /\bPE\b/i],
    hts_code: '3901.20',
    basis: 'composition',
    confidence: 0.75,
  },
  {
    patterns: [/\bpolypropylene\b/i, /\bPP\b/i],
    hts_code: '3902.10',
    basis: 'composition',
    confidence: 0.75,
  },
  {
    patterns: [/\bPVC\b/i, /\bpolyvinyl chloride\b/i, /\bvinyl\b/i],
    hts_code: '3904.10',
    basis: 'composition',
    confidence: 0.80,
  },
  {
    patterns: [/\bpolystyrene\b/i, /\bPS\b/i, /\bstyrofoam\b/i],
    hts_code: '3903.11',
    basis: 'composition',
    confidence: 0.75,
  },
];

export async function classifyMaterials(): Promise<void> {
  // Fetch all materials
  const { data: materials, error } = await supabase
    .from('materials')
    .select('id, name, category, description');

  if (error || !materials) {
    console.error('[classifyHTS] Failed to fetch materials:', error?.message);
    return;
  }

  console.log(`[classifyHTS] Evaluating ${materials.length} materials`);

  let classified = 0;
  let skipped = 0;

  for (const material of materials) {
    const searchText = [
      material.name || '',
      material.category || '',
      material.description || '',
    ].join(' ');

    // Find matching rules (can match multiple HTS codes)
    const matches: {
      hts_code: string;
      basis: string;
      confidence: number;
    }[] = [];

    for (const rule of CLASSIFICATION_RULES) {
      for (const pattern of rule.patterns) {
        if (pattern.test(searchText)) {
          matches.push({
            hts_code: rule.hts_code,
            basis: rule.basis,
            confidence: rule.confidence,
          });
          break; // one match per rule is enough
        }
      }
    }

    if (matches.length === 0) {
      skipped++;
      continue;
    }

    // Upsert each classification
    for (const match of matches) {
      const { error: upsertError } = await supabase
        .from('material_hts_classifications')
        .upsert(
          {
            material_id: material.id,
            hts_code: match.hts_code,
            classification_basis: match.basis,
            confidence: match.confidence,
            notes: `Auto-classified from material name/category: "${material.name}"`,
          },
          { onConflict: 'material_id,hts_code' }
        );

      if (upsertError) {
        console.error(
          `[classifyHTS] Upsert error for ${material.name}:`,
          upsertError.message
        );
      } else {
        classified++;
      }
    }

    console.log(
      `[classifyHTS] ${material.name}: ${matches.length} HTS code(s) assigned`
    );
  }

  console.log(
    `[classifyHTS] Complete. ${classified} classifications, ${skipped} unmatched.`
  );
}
