MODEL (
  name gold.uss__bridge__categories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH categories AS (
  SELECT
    _hook__reference__category__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__categories
)
SELECT
  'categories' AS stage,
  *
FROM categories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts