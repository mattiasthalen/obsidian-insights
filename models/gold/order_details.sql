MODEL (
  name gold.order_details,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column order_detail__record_loaded_at
  ),
  grain (
    _hook__order_detail__valid_from
  )
);

SELECT
  _hook__order_detail__valid_from,
  unit_price AS order_detail__unit_price,
  quantity AS order_detail__quantity,
  discount AS order_detail__discount,
  _sqlmesh_loaded_at AS order_detail__record_loaded_at,
  _sqlmesh_valid_from AS order_detail__record_valid_from,
  _sqlmesh_valid_to AS order_detail__record_valid_to,
  _sqlmesh_version AS order_detail__record_version,
  _sqlmesh_is_current_record AS order_detail__is_current_record
FROM silver.bag__northwind__order_details
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts