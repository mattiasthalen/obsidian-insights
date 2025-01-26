MODEL (
  name gold.uss__northwind__category_details,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__category__id
  )
);

SELECT
  hook__category__id,
  category_name,
  description,
  picture,
  product_names,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__category_details