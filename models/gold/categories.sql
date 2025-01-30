MODEL (
  name gold.categories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column category__record_loaded_at
  ),
  grain (
    _hook__reference__category__valid_from
  )
);

SELECT
  _hook__reference__category__valid_from,
  category_name AS category__name,
  description AS category__description,
  picture AS category__picture,
  product_names AS category__product_names,
  _sqlmesh_loaded_at AS category__record_loaded_at,
  _sqlmesh_valid_from AS category__record_valid_from,
  _sqlmesh_valid_to AS category__record_valid_to,
  _sqlmesh_version AS category__record_version,
  _sqlmesh_is_current_record AS category__is_current_record
FROM silver.bag__northwind__categories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts