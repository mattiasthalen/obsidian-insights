MODEL (
  name silver.int__uss_bridge,
  kind VIEW
);

SELECT
  *
FROM silver.int__uss_bridge__categories
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__customers
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__employees
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__employee_territories
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__order_details
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__orders
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__products
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__regions
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__shippers
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__suppliers
UNION ALL BY NAME
SELECT
  *
FROM silver.int__uss_bridge__territories