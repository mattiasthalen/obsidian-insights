MODEL (
  name gold.uss__northwind__category_details,
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
  description,
  picture,
  product_names
FROM silver.bag__northwind__category_details