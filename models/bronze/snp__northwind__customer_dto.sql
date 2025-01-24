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
  CAST(c.customer_id AS TEXT) AS customer_id,
  CAST(c.company_name AS TEXT) AS company_name,
  CAST(c.contact_name AS TEXT) AS contact_name,
  CAST(c.contact_title AS TEXT) AS contact_title,
  CAST(c.address AS TEXT) AS address,
  CAST(c.city AS TEXT) AS city,
  CAST(c.postal_code AS TEXT) AS postal_code,
  CAST(c.country AS TEXT) AS country,
  CAST(c.phone AS TEXT) AS phone,
  CAST(c.fax AS TEXT) AS fax,
  CAST(c.region AS TEXT) AS region,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    c.customer_id,
    c.company_name,
    c.contact_name,
    c.contact_title,
    c.address,
    c.city,
    c.postal_code,
    c.country,
    c.phone,
    c.fax,
    c.region
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__customer_dto as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
