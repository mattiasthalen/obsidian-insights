MODEL (
  name silver.int__uss_bridge__orders,
  kind VIEW
);

WITH orders AS (
  SELECT
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__customers._hook__customer__valid_from,
    bag__northwind__employees._hook__employee__valid_from,
    bag__northwind__shippers._hook__shipper__valid_from,
    GREATEST(
      bag__northwind__orders._sqlmesh_loaded_at,
      bag__northwind__customers._sqlmesh_loaded_at,
      bag__northwind__employees._sqlmesh_loaded_at,
      bag__northwind__shippers._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__orders._sqlmesh_valid_from,
      bag__northwind__customers._sqlmesh_valid_from,
      bag__northwind__employees._sqlmesh_valid_from,
      bag__northwind__shippers._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__orders._sqlmesh_valid_to,
      bag__northwind__customers._sqlmesh_valid_to,
      bag__northwind__employees._sqlmesh_valid_to,
      bag__northwind__shippers._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to,
    bag__northwind__orders._sqlmesh_source_system,
    bag__northwind__orders._sqlmesh_source_table
  FROM silver.bag__northwind__orders
  LEFT JOIN silver.bag__northwind__customers
    ON bag__northwind__orders._hook__customer = bag__northwind__customers._hook__customer
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__customers._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__customers._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__employees
    ON bag__northwind__orders._hook__employee = bag__northwind__employees._hook__employee
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__shippers
    ON bag__northwind__orders._hook__shipper = bag__northwind__shippers._hook__shipper
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__shippers._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__shippers._sqlmesh_valid_from
)
SELECT
  'orders' AS stage,
  orders.*,
  int__measures__orders.measure__orders_placed,
  int__measures__orders.measure__orders_required,
  int__measures__orders.measure__orders_shipped,
  int__measures__orders._key__date
FROM orders
LEFT JOIN silver.int__measures__orders
  ON orders._hook__order__valid_from = int__measures__orders._hook__order__valid_from