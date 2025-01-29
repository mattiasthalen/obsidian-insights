MODEL (
  name gold.uss__bridge__categories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH categories AS (
  SELECT
    _hook__reference__category__valid_from,
    _hook__reference__category__detail__valid_from,
    GREATEST(
      bag__northwind__categories._sqlmesh_loaded_at,
      bag__northwind__category_details._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__categories._sqlmesh_valid_from,
      bag__northwind__category_details._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__categories._sqlmesh_valid_to,
      bag__northwind__category_details._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to
  FROM silver.bag__northwind__categories
  LEFT JOIN silver.bag__northwind__category_details
    ON bag__northwind__categories._hook__reference__category = bag__northwind__category_details._hook__reference__category__detail
    AND bag__northwind__categories._sqlmesh_valid_from < bag__northwind__category_details._sqlmesh_valid_to
    AND bag__northwind__categories._sqlmesh_valid_to > bag__northwind__category_details._sqlmesh_valid_from
)
SELECT
  'categories' as stage,
  *
FROM categories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts