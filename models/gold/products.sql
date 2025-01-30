MODEL (
  name gold.products,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column product__record_loaded_at
  ),
  grain (
    _hook__product__valid_from
  )
);

SELECT
  _hook__product__valid_from,
  product_name AS product__name,
  quantity_per_unit AS product__quantity_per_unit,
  unit_price AS product__unit_price,
  units_in_stock AS product__units_in_stock,
  units_on_order AS product__units_on_order,
  reorder_level AS product__reorder_level,
  discontinued AS product__discontinued,
  _sqlmesh_loaded_at AS product__record_loaded_at,
  _sqlmesh_valid_from AS product__record_valid_from,
  _sqlmesh_valid_to AS product__record_valid_to,
  _sqlmesh_version AS product__record_version,
  _sqlmesh_is_current_record AS product__is_current_record
FROM silver.bag__northwind__products
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts