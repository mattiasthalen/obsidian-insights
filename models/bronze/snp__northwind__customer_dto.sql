MODEL (
  name bronze.snp__northwind__customer_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (customer_id),
);

SELECT
  CAST(customer_id AS TEXT) AS customer_id,
  CAST(company_name AS TEXT) AS company_name,
  CAST(contact_name AS TEXT) AS contact_name,
  CAST(contact_title AS TEXT) AS contact_title,
  CAST(address AS TEXT) AS address,
  CAST(city AS TEXT) AS city,
  CAST(postal_code AS TEXT) AS postal_code,
  CAST(country AS TEXT) AS country,
  CAST(phone AS TEXT) AS phone,
  CAST(fax AS TEXT) AS fax,
  CAST(region AS TEXT) AS region,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
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
FROM
  bronze.raw__northwind__customer_dto
