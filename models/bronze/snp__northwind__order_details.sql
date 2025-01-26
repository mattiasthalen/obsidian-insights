MODEL (
  name bronze.snp__northwind__order_details,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (order_id, product_id)
);

SELECT
  order_id::BIGINT AS order_id,
  product_id::BIGINT AS product_id,
  unit_price::DOUBLE AS unit_price,
  quantity::BIGINT AS quantity,
  discount::BIGINT AS discount,
  discount__v_double::DOUBLE AS discount__v_double,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @generate_surrogate_key(order_id, product_id, unit_price, quantity, discount, discount__v_double) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw__northwind__order_details