MODEL (
  name gold.orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order__record_loaded_at
  ),
  grain (
    _hook__order__valid_from
  )
);

SELECT
  _hook__order__valid_from,
  order_date AS order__order_date,
  required_date AS order__required_date,
  shipped_date AS order__shipped_date,
  ship_via AS order__ship_via,
  freight AS order__freight,
  ship_name AS order__ship_name,
  ship_address AS order__ship_address,
  ship_city AS order__ship_city,
  ship_postal_code AS order__ship_postal_code,
  ship_country AS order__ship_country,
  ship_region AS order__ship_region,
  _sqlmesh_loaded_at AS order__record_loaded_at,
  _sqlmesh_valid_from AS order__record_valid_from,
  _sqlmesh_valid_to AS order__record_valid_to,
  _sqlmesh_version AS order__record_version,
  _sqlmesh_is_current_record AS order__is_current_record
FROM silver.bag__northwind__orders
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts