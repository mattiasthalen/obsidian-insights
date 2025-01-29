MODEL (
  name gold.uss__bridge__shippers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH shippers AS (
  SELECT
    _hook__shipper__id__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__shippers
)
SELECT
  'shippers' AS stage,
  *
FROM shippers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts