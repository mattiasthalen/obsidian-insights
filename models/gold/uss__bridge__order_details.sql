MODEL (
  name gold.uss__bridge__order_details,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _key__puppini
  )
);

WITH order_details AS (
  SELECT
    bag__northwind__order_details._hook__order_detail__id__valid_from,
    bag__northwind__orders._hook__order__id__valid_from,
    bag__northwind__products._hook__product__id__valid_from,
    GREATEST(
      bag__northwind__order_details._sqlmesh_loaded_at,
      bag__northwind__orders._sqlmesh_loaded_at,
      bag__northwind__products._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__order_details._sqlmesh_valid_from,
      bag__northwind__orders._sqlmesh_valid_from,
      bag__northwind__products._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__order_details._sqlmesh_valid_to,
      bag__northwind__orders._sqlmesh_valid_to,
      bag__northwind__products._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to
  FROM silver.bag__northwind__order_details
  LEFT JOIN silver.bag__northwind__orders
    ON bag__northwind__order_details._hook__order__id = bag__northwind__orders._hook__order__id
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__orders._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__orders._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__products
    ON bag__northwind__order_details._hook__product__id = bag__northwind__products._hook__product__id
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__products._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__products._sqlmesh_valid_from
)
SELECT
  'order_details' AS stage,
  @generate_surrogate_key(
    stage,
    _hook__order_detail__id__valid_from,
    _hook__order__id__valid_from,
    _hook__product__id__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  ) AS _key__puppini,
  *
FROM order_details
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts