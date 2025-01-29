MODEL (
  name gold.uss__bridge__customers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH customers AS (
  SELECT
    _hook__customer__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__customers
)
SELECT
  'customers' as stage,
  *
FROM customers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts