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
  CAST(c.order_id AS BIGINT) AS order_id,
  CAST(c.product_id AS BIGINT) AS product_id,
  CAST(c.unit_price AS DOUBLE) AS unit_price,
  CAST(c.quantity AS BIGINT) AS quantity,
  CAST(c.discount AS BIGINT) AS discount,
  CAST(c.discount__v_double AS DOUBLE) AS discount__v_double,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    c.order_id,
    c.product_id,
    c.unit_price,
    c.quantity,
    c.discount,
    c.discount__v_double
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__order_detail_dto as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
