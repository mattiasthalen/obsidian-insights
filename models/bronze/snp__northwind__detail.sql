MODEL (
  name bronze.snp__northwind__detail,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
);

SELECT
  CAST(c.value AS TEXT) AS value,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    c.value
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__detail as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
