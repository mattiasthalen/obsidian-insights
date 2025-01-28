MODEL (
  name gold.uss__product,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__product__id
  )
);

SELECT
  _hook__product__id,
  product_name,
  quantity_per_unit,
  unit_price,
  units_in_stock,
  units_on_order,
  reorder_level,
  discontinued,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__products
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts