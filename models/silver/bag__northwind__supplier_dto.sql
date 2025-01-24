MODEL (
  name silver.bag__northwind__supplier_dto,
  kind VIEW,
  grain (supplier_id),
);

SELECT
  supplier_id,
  company_name,
  contact_name,
  contact_title,
  address,
  city,
  postal_code,
  country,
  phone,
  region,
  home_page,
  fax,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at
FROM
  bronze.snp__northwind__supplier_dto
