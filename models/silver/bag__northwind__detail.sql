MODEL (
  name silver.bag__northwind__detail,
  kind VIEW,
);

SELECT
  value,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at
FROM
  bronze.snp__northwind__detail
