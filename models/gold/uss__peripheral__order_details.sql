MODEL (
  name gold.uss__peripheral__order_details,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__order_detail__valid_from
  )
);

SELECT
  _hook__order_detail__valid_from,
  unit_price,
  quantity,
  discount,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__order_details
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts