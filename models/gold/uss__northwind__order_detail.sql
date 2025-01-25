MODEL (
  name gold.uss__northwind__order_detail,
  kind FULL,
  grain (
    hook__order_detail__id
  )
);

SELECT
  hook__order_detail__id,
  unit_price,
  quantity,
  discount,
  discount__v_double,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__order_detail_dto