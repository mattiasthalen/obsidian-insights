MODEL (
  name silver.bag__northwind__category_dto,
  kind VIEW,
  grain (category_id),
);

SELECT
  category_id,
  category_name,
  description,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at
FROM
  bronze.snp__northwind__category_dto
