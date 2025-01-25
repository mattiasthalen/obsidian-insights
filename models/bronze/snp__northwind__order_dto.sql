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
  CAST(order_id AS BIGINT) AS order_id,
  CAST(customer_id AS TEXT) AS customer_id,
  CAST(employee_id AS BIGINT) AS employee_id,
  CAST(order_date AS TIMESTAMP) AS order_date,
  CAST(required_date AS TIMESTAMP) AS required_date,
  CAST(shipped_date AS TIMESTAMP) AS shipped_date,
  CAST(ship_via AS BIGINT) AS ship_via,
  CAST(freight AS DOUBLE) AS freight,
  CAST(ship_name AS TEXT) AS ship_name,
  CAST(ship_address AS TEXT) AS ship_address,
  CAST(ship_city AS TEXT) AS ship_city,
  CAST(ship_postal_code AS TEXT) AS ship_postal_code,
  CAST(ship_country AS TEXT) AS ship_country,
  CAST(ship_region AS TEXT) AS ship_region,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    order_id,
    customer_id,
    employee_id,
    order_date,
    required_date,
    shipped_date,
    ship_via,
    freight,
    ship_name,
    ship_address,
    ship_city,
    ship_postal_code,
    ship_country,
    ship_region
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__order_dto
