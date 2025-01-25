MODEL (
  name gold.uss__northwind__product,
  kind FULL,
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
  discontinued,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__product_dto