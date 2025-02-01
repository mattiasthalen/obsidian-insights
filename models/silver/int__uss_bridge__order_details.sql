MODEL (
  name silver.int__uss_bridge__order_details,
  kind VIEW
);

WITH order_details AS (
  SELECT
    bag__northwind__order_details._hook__order_detail__valid_from,
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__employees._hook__employee__valid_from,
    bag__northwind__customers._hook__customer__valid_from,
    bag__northwind__shippers._hook__shipper__valid_from,
    bag__northwind__products._hook__product__valid_from,
    bag__northwind__categories._hook__reference__category__valid_from,
    bag__northwind__suppliers._hook__supplier__valid_from,
    GREATEST(
      bag__northwind__order_details._sqlmesh_loaded_at,
      bag__northwind__orders._sqlmesh_loaded_at,
      bag__northwind__employees._sqlmesh_loaded_at,
      bag__northwind__customers._sqlmesh_loaded_at,
      bag__northwind__shippers._sqlmesh_loaded_at,
      bag__northwind__products._sqlmesh_loaded_at,
      bag__northwind__categories._sqlmesh_loaded_at,
      bag__northwind__suppliers._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__order_details._sqlmesh_valid_from,
      bag__northwind__orders._sqlmesh_valid_from,
      bag__northwind__employees._sqlmesh_valid_from,
      bag__northwind__customers._sqlmesh_valid_from,
      bag__northwind__shippers._sqlmesh_valid_from,
      bag__northwind__products._sqlmesh_valid_from,
      bag__northwind__categories._sqlmesh_valid_from,
      bag__northwind__suppliers._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__order_details._sqlmesh_valid_to,
      bag__northwind__orders._sqlmesh_valid_to,
      bag__northwind__employees._sqlmesh_valid_to,
      bag__northwind__customers._sqlmesh_valid_to,
      bag__northwind__shippers._sqlmesh_valid_to,
      bag__northwind__products._sqlmesh_valid_to,
      bag__northwind__categories._sqlmesh_valid_to,
      bag__northwind__suppliers._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to
  FROM silver.bag__northwind__order_details
  LEFT JOIN silver.bag__northwind__orders
    ON bag__northwind__order_details._hook__order = bag__northwind__orders._hook__order
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__orders._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__orders._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__employees
    ON bag__northwind__orders._hook__employee = bag__northwind__employees._hook__employee
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__customers
    ON bag__northwind__orders._hook__customer = bag__northwind__customers._hook__customer
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__customers._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__customers._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__shippers
    ON bag__northwind__orders._hook__shipper = bag__northwind__shippers._hook__shipper
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__shippers._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__shippers._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__products
    ON bag__northwind__order_details._hook__product = bag__northwind__products._hook__product
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__products._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__products._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__categories
    ON bag__northwind__products._hook__reference__category = bag__northwind__categories._hook__reference__category
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__categories._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__categories._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__suppliers
    ON bag__northwind__products._hook__supplier = bag__northwind__suppliers._hook__supplier
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__suppliers._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__suppliers._sqlmesh_valid_from
)
SELECT
  'order_details' AS stage,
  order_details.*
FROM order_details