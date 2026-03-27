export function buildNameIndex(rows: BulkChemicalRow[]): Map<string, BulkChemicalRow[]> {
  const index = new Map<string, BulkChemicalRow[]>();

  const normalizeChemicalName = (value: string): string =>
    value
      .toLowerCase()
      .trim()
      .replace(/[\/,()\-]/g, ' ')
      .replace(/\s+/g, ' ')
      .replace(/\bextract\b/g, '')
      .replace(/\boil\b/g, '')
      .replace(/\bleaf\b/g, '')
      .replace(/\broot\b/g, '')
      .replace(/\bfruit\b/g, '')
      .replace(/\bseed\b/g, '')
      .replace(/\bpeel\b/g, '')
      .replace(/\bjuice\b/g, '')
      .replace(/\bpowder\b/g, '')
      .replace(/\bwater\b/g, '')
      .replace(/\beau\b/g, '')
      .replace(/\baqua\b/g, '')
      .replace(/\s+/g, ' ')
      .trim();

  for (const row of rows) {
    if (!row.preferred_name) continue;

    const raw = row.preferred_name.trim();
    const rawKey = raw.toLowerCase();
    const normalizedKey = normalizeChemicalName(raw);

    for (const key of new Set([rawKey, normalizedKey])) {
      if (!key) continue;
      const existing = index.get(key) || [];
      existing.push(row);
      index.set(key, existing);
    }
  }

  return index;
}
