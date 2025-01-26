MODEL (
  name bronze.snp__northwind__products,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (
    product_id
  )
);

SELECT
  product_id::BIGINT AS product_id,
  product_name::TEXT AS product_name,
  supplier_id::BIGINT AS supplier_id,
  category_id::BIGINT AS category_id,
  quantity_per_unit::TEXT AS quantity_per_unit,
  unit_price::DOUBLE AS unit_price,
  units_in_stock::BIGINT AS units_in_stock,
  units_on_order::BIGINT AS units_on_order,
  reorder_level::BIGINT AS reorder_level,
  discontinued::BOOLEAN AS discontinued,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @generate_surrogate_key(
    product_id,
    product_name,
    supplier_id,
    category_id,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw__northwind__products