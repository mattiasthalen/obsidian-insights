MODEL (
  name gold.uss__northwind__order_detail,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__order_detail__id
  )
);

SELECT
  _hook__order_detail__id,
  unit_price,
  quantity,
  discount
FROM silver.bag__northwind__order_details