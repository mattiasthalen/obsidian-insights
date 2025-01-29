MODEL (
  name gold.uss__bridge__regions,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH regions AS (
  SELECT
    _hook__reference__region__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__regions
)
SELECT
  'regions' AS stage,
  *
FROM regions
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts