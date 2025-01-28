MODEL (
  name gold.uss__orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__order__id
  )
);

SELECT
  _hook__order__id,
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
  ship_region,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__orders
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts