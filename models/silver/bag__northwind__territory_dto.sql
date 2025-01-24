MODEL (
  name silver.bag__northwind__territory_dto,
  kind VIEW,
  grain (territory_id),
);

SELECT
  territory_id,
  territory_description,
  region_id,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at
  
FROM
  bronze.snp__northwind__territory_dto
