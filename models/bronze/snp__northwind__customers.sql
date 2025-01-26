MODEL (
  name bronze.snp__northwind__customers,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (
    customer_id
  )
);

SELECT
  customer_id::TEXT AS customer_id,
  company_name::TEXT AS company_name,
  contact_name::TEXT AS contact_name,
  contact_title::TEXT AS contact_title,
  address::TEXT AS address,
  city::TEXT AS city,
  postal_code::TEXT AS postal_code,
  country::TEXT AS country,
  phone::TEXT AS phone,
  fax::TEXT AS fax,
  region::TEXT AS region,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @generate_surrogate_key(
    customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    postal_code,
    country,
    phone,
    fax,
    region
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw__northwind__customers