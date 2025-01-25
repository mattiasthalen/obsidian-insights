MODEL (
  name bronze.snp__northwind__product_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (product_id),
);

SELECT
  CAST(product_id AS BIGINT) AS product_id,
  CAST(product_name AS TEXT) AS product_name,
  CAST(supplier_id AS BIGINT) AS supplier_id,
  CAST(category_id AS BIGINT) AS category_id,
  CAST(quantity_per_unit AS TEXT) AS quantity_per_unit,
  CAST(unit_price AS DOUBLE) AS unit_price,
  CAST(units_in_stock AS BIGINT) AS units_in_stock,
  CAST(units_on_order AS BIGINT) AS units_on_order,
  CAST(reorder_level AS BIGINT) AS reorder_level,
  CAST(discontinued AS BOOLEAN) AS discontinued,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
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
FROM
  bronze.raw__northwind__product_dto
