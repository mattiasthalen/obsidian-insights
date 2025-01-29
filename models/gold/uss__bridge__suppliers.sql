MODEL (
  name gold.uss__bridge__suppliers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH suppliers AS (
  SELECT
    _hook__supplier__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__suppliers
)
SELECT
  'suppliers' AS stage,
  *
FROM suppliers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts