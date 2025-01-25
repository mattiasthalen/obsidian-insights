MODEL (
  name gold.uss__bridge,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    key__puppini
  )
);

WITH category AS (
  SELECT
    'category' AS stage,
    key__category_id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__category_dto
), customer AS (
  SELECT
    'customer' AS stage,
    hook__customer__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__customer_dto
), employee AS (
  SELECT
    'employee' AS stage,
    hook__employee__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__employee_dto
), order_detail AS (
  SELECT
    'order_detail' AS stage,
    hook__order_detail__id,
    hook__order__id,
    hook__product__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__order_detail_dto
), "order" AS (
  SELECT
    'order' AS stage,
    hook__order__id,
    hook__customer__id,
    hook__employee__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__order_dto
), product AS (
  SELECT
    'product' AS stage,
    hook__product__id,
    hook__supplier__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__product_dto
), region AS (
  SELECT
    'region' AS stage,
    key__region_id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__region_dto
), shipper AS (
  SELECT
    'shipper' AS stage,
    hook__shipper__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__shipper_dto
), supplier AS (
  SELECT
    'supplier' AS stage,
    hook__supplier__id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__supplier_dto
), territory AS (
  SELECT
    'territory' AS stage,
    key__territory_id,
    key__region_id,
    _sqlmesh_loaded_at
  FROM silver.bag__northwind__territory_dto
), puppini_bridge AS (
  SELECT
    *
  FROM category
  UNION ALL BY NAME
  SELECT
    *
  FROM customer
  UNION ALL BY NAME
  SELECT
    *
  FROM employee
  UNION ALL BY NAME
  SELECT
    *
  FROM order_detail
  UNION ALL BY NAME
  SELECT
    *
  FROM "order"
  UNION ALL BY NAME
  SELECT
    *
  FROM product
  UNION ALL BY NAME
  SELECT
    *
  FROM region
  UNION ALL BY NAME
  SELECT
    *
  FROM shipper
  UNION ALL BY NAME
  SELECT
    *
  FROM supplier
  UNION ALL BY NAME
  SELECT
    *
  FROM territory
)
SELECT
  stage,
  @generate_surrogate_key(
    stage,
    hook__customer__id,
    hook__employee__id,
    hook__order_detail__id,
    hook__order__id,
    hook__product__id,
    hook__shipper__id,
    hook__supplier__id,
    key__category_id,
    key__region_id,
    key__territory_id,
    hash_function := 'SHA256'
  ) AS key__puppini,
  hook__customer__id,
  hook__employee__id,
  hook__order_detail__id,
  hook__order__id,
  hook__product__id,
  hook__shipper__id,
  hook__supplier__id,
  key__category_id,
  key__region_id,
  key__territory_id,
  _sqlmesh_loaded_at
FROM puppini_bridge
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts