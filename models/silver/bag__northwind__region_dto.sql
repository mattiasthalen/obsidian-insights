MODEL (
  name silver.bag__northwind__region_dto,
  kind VIEW,
  grain (region_id),
);

SELECT
  region_id,
  region_description,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at
FROM
  bronze.snp__northwind__region_dto
