MODEL (
  name gold._bridge,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

SELECT
  stage,
  _hook__customer__valid_from,
  _hook__employee__valid_from,
  _hook__order__valid_from,
  _hook__order_detail__valid_from,
  _hook__product__valid_from,
  _hook__reference__category__valid_from,
  _hook__reference__region__valid_from,
  _hook__reference__territory__valid_from,
  _hook__shipper__valid_from,
  _hook__supplier__valid_from,
  _key__date,
  measure__due_discounted_value,
  measure__due_lines,
  measure__due_quantity,
  measure__due_value,
  measure__ordered_discounted_value,
  measure__ordered_lines,
  measure__ordered_quantity,
  measure__ordered_value,
  measure__orders_placed,
  measure__orders_required,
  measure__orders_shipped,
  measure__shipped_discounted_value,
  measure__shipped_lines,
  measure__shipped_quantity,
  measure__shipped_value,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_source_system,
  _sqlmesh_source_table
FROM silver.int__uss_bridge
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts