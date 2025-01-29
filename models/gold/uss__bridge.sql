MODEL (
  name gold.uss__bridge,
  kind VIEW,
  grain (
    _key__puppini
  )
);

SELECT
  *
FROM gold.uss__bridge__customers
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__employees
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__employee_territories
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__order_details
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__orders
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__products
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__shippers
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__suppliers
UNION ALL BY NAME
SELECT
  *
FROM gold.uss__bridge__sales_territories