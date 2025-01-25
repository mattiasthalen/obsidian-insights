MODEL (
  name bronze.snp__northwind__order_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (
    order_id
  )
);

SELECT
  order_id::BIGINT AS order_id,
  customer_id::TEXT AS customer_id,
  employee_id::BIGINT AS employee_id,
  order_date::TIMESTAMP AS order_date,
  required_date::TIMESTAMP AS required_date,
  shipped_date::TIMESTAMP AS shipped_date,
  ship_via::BIGINT AS ship_via,
  freight::DOUBLE AS freight,
  ship_name::TEXT AS ship_name,
  ship_address::TEXT AS ship_address,
  ship_city::TEXT AS ship_city,
  ship_postal_code::TEXT AS ship_postal_code,
  ship_country::TEXT AS ship_country,
  ship_region::TEXT AS ship_region,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
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
FROM bronze.raw__northwind__order_dto