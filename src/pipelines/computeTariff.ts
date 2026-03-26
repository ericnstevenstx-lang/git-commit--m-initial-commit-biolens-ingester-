/**
 * Pipeline: Compute Tariff
 *
 * Takes material pairs and their origin countries, looks up HTS codes
 * and tariff rates, computes effective duty including Section 301,
 * checks FEOC/UFLPA flags, and caches results for FiberFoundry.
 *
 * Column mapping (materials table):
 *   material_name = product/ingredient name
 */
import { supabase } from '../supabase.js';

interface TariffLookup {
  material_id: string;
  material_name: string;
  hts_code: string;
  origin_country: string;
  general_rate_pct: number;
  section_301_rate_pct: number;
  effective_rate_pct: number;
  feoc_flagged: boolean;
  uflpa_risk: boolean;
  baba_eligible: boolean;
}

async function lookupTariff(
  materialId: string,
  originCountry: string
): Promise<TariffLookup | null> {
  const { data: material } = await supabase
    .from('materials')
    .select('id, material_name')
    .eq('id', materialId)
    .single();

  if (!material) return null;

  const { data: classification } = await supabase
    .from('material_hts_classifications')
    .select('hts_code, confidence')
    .eq('material_id', materialId)
    .order('confidence', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (!classification) {
    console.log(`[tariff] No HTS classification for ${material.material_name}`);
    return null;
  }

  const { data: rate } = await supabase
    .from('hts_tariff_rates')
    .select('*')
    .eq('hts_code', classification.hts_code)
    .maybeSingle();

  if (!rate) {
    console.log(`[tariff] No rate data for HTS ${classification.hts_code}`);
    return null;
  }

  const { data: countryFlags } = await supabase
    .from('gs1_country_prefixes')
    .select('feoc_flagged, uflpa_risk')
    .eq('country_code', originCountry)
    .limit(1)
    .maybeSingle();

  const feocFlagged = countryFlags?.feoc_flagged || false;
  const uflpaRisk = countryFlags?.uflpa_risk || false;

  const section301Applies = originCountry === 'CN';
  const section301Rate = section301Applies ? (rate.section_301_rate_pct || 0) : 0;
  const generalRate = rate.general_rate_pct || 0;
  const effectiveRate = generalRate + section301Rate;
  const babaEligible = originCountry === 'US' && !feocFlagged;

  return {
    material_id: materialId,
    material_name: material.material_name,
    hts_code: classification.hts_code,
    origin_country: originCountry,
    general_rate_pct: generalRate,
    section_301_rate_pct: section301Rate,
    effective_rate_pct: effectiveRate,
    feoc_flagged: feocFlagged,
    uflpa_risk: uflpaRisk,
    baba_eligible: babaEligible,
  };
}

export async function computeComparison(
  materialAId: string,
  originA: string,
  materialBId: string,
  originB: string
): Promise<void> {
  const tariffA = await lookupTariff(materialAId, originA);
  const tariffB = await lookupTariff(materialBId, originB);

  if (!tariffA || !tariffB) {
    console.log('[tariff] Cannot compute: missing tariff data for one or both materials');
    return;
  }

  const delta = tariffB.effective_rate_pct - tariffA.effective_rate_pct;

  const { error } = await supabase.from('tariff_comparisons').upsert(
    {
      material_a_id: materialAId,
      material_b_id: materialBId,
      origin_country_a: originA,
      origin_country_b: originB,
      hts_code_a: tariffA.hts_code,
      hts_code_b: tariffB.hts_code,
      duty_rate_a: tariffA.effective_rate_pct,
      duty_rate_b: tariffB.effective_rate_pct,
      section_301_applies_b: originB === 'CN',
      feoc_disqualified_b: tariffB.feoc_flagged,
      baba_eligible_a: tariffA.baba_eligible,
      uflpa_risk_b: tariffB.uflpa_risk,
      landed_cost_delta_pct: delta,
      computed_at: new Date().toISOString(),
    },
    { onConflict: 'material_a_id,material_b_id' }
  );

  if (error) {
    console.error('[tariff] Upsert error:', error.message);
    return;
  }

  console.log(
    `[tariff] ${tariffA.material_name} (${originA}, ${tariffA.effective_rate_pct}%) vs ` +
      `${tariffB.material_name} (${originB}, ${tariffB.effective_rate_pct}%) = ` +
      `${delta > 0 ? '+' : ''}${delta.toFixed(1)}% delta` +
      `${tariffB.feoc_flagged ? ' [FEOC]' : ''}` +
      `${tariffB.uflpa_risk ? ' [UFLPA]' : ''}` +
      `${tariffA.baba_eligible ? ' [BABA-A]' : ''}`
  );
}

export async function generateStandardComparisons(): Promise<void> {
  const { data: bioMaterials } = await supabase
    .from('material_hts_classifications')
    .select('material_id, hts_code')
    .or('hts_code.like.53%,hts_code.like.47%');

  const { data: petroMaterials } = await supabase
    .from('material_hts_classifications')
    .select('material_id, hts_code')
    .or('hts_code.like.39%,hts_code.like.54%,hts_code.like.55%');

  if (!bioMaterials?.length || !petroMaterials?.length) {
    console.log('[tariff] Not enough classified materials for comparisons');
    return;
  }

  console.log(
    `[tariff] Generating comparisons: ${bioMaterials.length} bio x ${petroMaterials.length} petro`
  );

  const originPairs = [
    { bioOrigin: 'US', petroOrigin: 'CN' },
    { bioOrigin: 'US', petroOrigin: 'VN' },
    { bioOrigin: 'US', petroOrigin: 'IN' },
    { bioOrigin: 'CA', petroOrigin: 'CN' },
  ];

  for (const bio of bioMaterials) {
    for (const petro of petroMaterials) {
      for (const pair of originPairs) {
        await computeComparison(
          bio.material_id,
          pair.bioOrigin,
          petro.material_id,
          pair.petroOrigin
        );
      }
    }
  }

  console.log('[tariff] Standard comparisons complete');
}
