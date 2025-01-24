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
  CAST(c.product_id AS BIGINT) AS product_id,
  CAST(c.product_name AS TEXT) AS product_name,
  CAST(c.supplier_id AS BIGINT) AS supplier_id,
  CAST(c.category_id AS BIGINT) AS category_id,
  CAST(c.quantity_per_unit AS TEXT) AS quantity_per_unit,
  CAST(c.unit_price AS DOUBLE) AS unit_price,
  CAST(c.units_in_stock AS BIGINT) AS units_in_stock,
  CAST(c.units_on_order AS BIGINT) AS units_on_order,
  CAST(c.reorder_level AS BIGINT) AS reorder_level,
  CAST(c.discontinued AS BOOLEAN) AS discontinued,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    c.product_id,
    c.product_name,
    c.supplier_id,
    c.category_id,
    c.quantity_per_unit,
    c.unit_price,
    c.units_in_stock,
    c.units_on_order,
    c.reorder_level,
    c.discontinued
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__product_dto as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
