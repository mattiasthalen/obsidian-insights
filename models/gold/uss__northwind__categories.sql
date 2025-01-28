MODEL (
  name gold.uss__northwind__categories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__reference__id__category
  )
);

SELECT
  hook__reference__id__category,
  category_name,
  description
FROM silver.bag__northwind__categories