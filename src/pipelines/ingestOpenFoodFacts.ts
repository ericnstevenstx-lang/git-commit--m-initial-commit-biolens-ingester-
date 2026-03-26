function toArray(val: any): string[] | null {
  if (!val) return null;

  if (Array.isArray(val)) {
    return val.map((v) => String(v).trim()).filter(Boolean);
  }

  const str = String(val).trim();
  if (!str) return null;

  // handle comma-separated strings
  if (str.includes(',')) {
    return str.split(',').map((v) => v.trim()).filter(Boolean);
  }

  return [str];
}

function toRawRow(product: OFFProduct, registrySourceId: string) {
  const countryOfOrigin =
    product.origins ||
    (product.countries_tags && product.countries_tags.length > 0
      ? normalizeCountryTag(product.countries_tags[0])
      : null);

  const countriesSold = product.countries_tags
    ?.map(normalizeCountryTag)
    .filter(Boolean);

  return {
    registry_source_id: registrySourceId,
    source: 'off',
    external_product_id: `off-${product.code}`,
    barcode: product.code,
    gtin: product.code,

    product_name: product.product_name || null,
    brand: product.brands || null,

    category: product.categories_tags_en?.[0] || null,
    subcategory: product.categories_tags_en?.[1] || null,

    quantity: product.quantity || null,

    ingredient_list_text:
      product.ingredients_text || product.ingredients_text_en || null,

    inci_text:
      product.ingredients_text_en || product.ingredients_text || null,

    // ✅ FIXED ARRAY FIELDS
    country_of_origin: toArray(countryOfOrigin),
    countries_sold: toArray(countriesSold),
    manufacturing_places: toArray(product.manufacturing_places),
    labels_claims: toArray(product.labels),
    stores: toArray(product.stores),

    packaging_text: product.packaging || null,

    source_url: `https://world.openfoodfacts.org/product/${product.code}`,

    raw_payload: {
      nutriscore_grade: product.nutriscore_grade,
      ecoscore_grade: product.ecoscore_grade,
      nova_group: product.nova_group,
      image_url: product.image_url,
      categories_tags_en: product.categories_tags_en,
      countries_tags: product.countries_tags,
    },
  };
}
