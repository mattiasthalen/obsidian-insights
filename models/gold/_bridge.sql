MODEL (
  name gold._bridge,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

SELECT
  *
FROM silver.int__uss_bridge
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts