MODEL (
  name silver.bag__northwind__shipper_dto,
  kind VIEW,
  grain (shipper_id),
);

SELECT
  shipper_id,
  company_name,
  phone,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at
FROM
  bronze.snp__northwind__shipper_dto
