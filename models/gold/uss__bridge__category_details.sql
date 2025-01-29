MODEL (
  name gold.uss__bridge__category_details,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH category_details AS (
  SELECT
    _hook__reference__category__detail__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__category_details
)
SELECT
  'category_details' as stage,
  *
FROM category_details
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts