/**
 * INCI naming normalization:
 * - lowercase
 * - trim spaces
 * - remove parenthetical dual names (e.g. "WATER (AQUA)" -> "water", "SOMETHING (OTHER)" -> "something")
 * Preserve original as ingredient_raw; normalized form used for alias matching.
 */
export function normalizeInci(raw: string): string {
  let s = raw.trim();
  // Remove parenthetical dual name: keep only the part before " (" or the whole string if no parens
  const paren = s.indexOf(" (");
  if (paren !== -1) {
    s = s.slice(0, paren).trim();
  }
  return s.toLowerCase().replace(/\s+/g, " ").trim();
}
