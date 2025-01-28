MODEL (
  name gold.uss__bridge,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    key__puppini
  )
);

WITH categories AS (
  SELECT
    'categories' AS stage,
    _hook__reference__id__category,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__categories
), category_details AS (
  SELECT
    'category_details' AS stage,
    _hook__reference__id__category,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__category_details
), customers AS (
  SELECT
    'customers' AS stage,
    _hook__customer__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__customers
), employees AS (
  SELECT
    'employees' AS stage,
    _hook__employee__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__employees
), employee_territories AS (
  SELECT
    'employee_territories' AS stage,
    _hook__employee__id,
    _hook__reference__id__territory,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__employee_territories
), order_details AS (
  SELECT
    'order_details' AS stage,
    _hook__order_detail__id,
    _hook__order__id,
    _hook__product__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__order_details
), orders AS (
  SELECT
    'orders' AS stage,
    _hook__order__id,
    _hook__customer__id,
    _hook__employee__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__orders
), products AS (
  SELECT
    'products' AS stage,
    _hook__product__id,
    _hook__supplier__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__products
), regions AS (
  SELECT
    'regions' AS stage,
    _hook__reference__id__region,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__regions
), shippers AS (
  SELECT
    'shippers' AS stage,
    _hook__shipper__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__shippers
), suppliers AS (
  SELECT
    'suppliers' AS stage,
    _hook__supplier__id,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__suppliers
), territories AS (
  SELECT
    'territories' AS stage,
    _hook__reference__id__territory,
    _hook__reference__id__region,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to,
    _sqlmesh_version,
    _sqlmesh_is_current_record
  FROM silver.bag__northwind__territories
), bridge AS (
  SELECT
    *
  FROM categories
  UNION ALL BY NAME
  SELECT
    *
  FROM category_details
  UNION ALL BY NAME
  SELECT
    *
  FROM customers
  UNION ALL BY NAME
  SELECT
    *
  FROM employees
  UNION ALL BY NAME
  SELECT
    *
  FROM employee_territories
  UNION ALL BY NAME
  SELECT
    *
  FROM order_details
  UNION ALL BY NAME
  SELECT
    *
  FROM orders
  UNION ALL BY NAME
  SELECT
    *
  FROM products
  UNION ALL BY NAME
  SELECT
    *
  FROM regions
  UNION ALL BY NAME
  SELECT
    *
  FROM shippers
  UNION ALL BY NAME
  SELECT
    *
  FROM suppliers
  UNION ALL BY NAME
  SELECT
    *
  FROM territories
)
SELECT
  stage,
  @generate_surrogate_key(
    stage,
    _hook__reference__id__category,
    _hook__customer__id,
    _hook__employee__id,
    _hook__order_detail__id,
    _hook__order__id,
    _hook__product__id,
    _hook__reference__id__region,
    _hook__shipper__id,
    _hook__supplier__id,
    _hook__reference__id__territory,
    hash_function := 'SHA256'
  ) AS key__puppini,
  _hook__reference__id__category,
  _hook__customer__id,
  _hook__employee__id,
  _hook__order_detail__id,
  _hook__order__id,
  _hook__product__id,
  _hook__reference__id__region,
  _hook__shipper__id,
  _hook__supplier__id,
  _hook__reference__id__territory,
  _sqlmesh_loaded_at
FROM bridge
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts