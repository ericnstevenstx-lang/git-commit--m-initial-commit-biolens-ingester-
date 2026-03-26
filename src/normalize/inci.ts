/**
 * INCI naming normalization:
 * - lowercase
 * - trim spaces
 * - strip trailing percentages (e.g. "niacinamide 10%" -> "niacinamide")
 * - normalize parenthetical dual names (e.g. "Aqua (Water)" -> "aqua", "Parfum (Fragrance)" -> "parfum")
 * - preserve only the "main" token for CI pigment groups (e.g. "(CI 19140, CI 42090)" -> "ci 19140")
 * Preserve original as ingredient_raw; normalized form used for alias matching.
 */
export function normalizeInci(raw: string): string {
  let s = raw.trim();
  if (!s) return "";

  // Collapse whitespace early to make parsing predictable.
  s = s.replace(/\s+/g, " ").trim();

  // Normalize parenthetical dual names better:
  // - "Aqua (Water)" -> "Aqua"
  // - "Parfum (Fragrance)" -> "Parfum"
  // - "Vitamin E (Tocopherol)" -> "Vitamin E"
  // - If the token starts with a parenthetical group, keep only the first CI pigment:
  //   "(CI 19140, CI 42090)" -> "CI 19140"
  const openIdx = s.indexOf("(");
  if (openIdx !== -1) {
    const before = s.slice(0, openIdx).trim();
    if (before) {
      s = before;
    } else {
      const inside = s.match(/\(([^)]*)\)/)?.[1] ?? "";
      if (inside) {
        s = inside.split(",")[0].trim();
      } else {
        // Fall back to removing any parentheses content.
        s = s.replace(/\([^)]*\)/g, "").trim();
      }
    }
    s = s.replace(/\s+/g, " ").trim();
  }

  // Strip trailing percentages:
  // "niacinamide 10%" -> "niacinamide"
  // "ascorbic acid 15%" -> "ascorbic acid"
  // "zinc oxide 20%" -> "zinc oxide"
  s = s.replace(/\s*\d+(?:\.\d+)?\s*%\s*$/i, "").trim();

  // Clean up leftover punctuation from earlier token splitting.
  s = s.replace(/^[()]+/, "").replace(/[()]+$/, "").trim();

  // Avoid creating standalone CI-code materials from comma-split pigment groups.
  // If the normalized token is only a CI pigment code, treat it as non-material.
  // Example: "CI 19140" -> ""
  if (/^ci\s*\d+$/i.test(s)) return "";

  return s.toLowerCase().replace(/\s+/g, " ").trim();
}
