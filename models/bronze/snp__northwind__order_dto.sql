MODEL (
  name bronze.snp__northwind__order_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (order_id),
);

SELECT
  CAST(c.order_id AS BIGINT) AS order_id,
  CAST(c.customer_id AS TEXT) AS customer_id,
  CAST(c.employee_id AS BIGINT) AS employee_id,
  CAST(c.order_date AS TIMESTAMP) AS order_date,
  CAST(c.required_date AS TIMESTAMP) AS required_date,
  CAST(c.shipped_date AS TIMESTAMP) AS shipped_date,
  CAST(c.ship_via AS BIGINT) AS ship_via,
  CAST(c.freight AS DOUBLE) AS freight,
  CAST(c.ship_name AS TEXT) AS ship_name,
  CAST(c.ship_address AS TEXT) AS ship_address,
  CAST(c.ship_city AS TEXT) AS ship_city,
  CAST(c.ship_postal_code AS TEXT) AS ship_postal_code,
  CAST(c.ship_country AS TEXT) AS ship_country,
  CAST(c.ship_region AS TEXT) AS ship_region,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    c.order_id,
    c.customer_id,
    c.employee_id,
    c.order_date,
    c.required_date,
    c.shipped_date,
    c.ship_via,
    c.freight,
    c.ship_name,
    c.ship_address,
    c.ship_city,
    c.ship_postal_code,
    c.ship_country,
    c.ship_region
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__order_dto as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
