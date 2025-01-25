MODEL (
  name bronze.snp__northwind__order_detail_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
);

SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(product_id AS BIGINT) AS product_id,
  CAST(unit_price AS DOUBLE) AS unit_price,
  CAST(quantity AS BIGINT) AS quantity,
  CAST(discount AS BIGINT) AS discount,
  CAST(discount__v_double AS DOUBLE) AS discount__v_double,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    order_id,
    product_id,
    unit_price,
    quantity,
    discount,
    discount__v_double
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__order_detail_dto
