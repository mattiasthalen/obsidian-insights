MODEL (
  name gold.uss__northwind__product,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__product__id
  )
);

SELECT
  hook__product__id,
  product_name,
  quantity_per_unit,
  unit_price,
  units_in_stock,
  units_on_order,
  reorder_level,
  discontinued
FROM silver.bag__northwind__products