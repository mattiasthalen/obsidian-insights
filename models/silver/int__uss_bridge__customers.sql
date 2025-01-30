MODEL (
  name silver.int__uss_bridge__customers,
  kind VIEW
);

WITH customers AS (
  SELECT
    _hook__customer__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__customers
)
SELECT
  'customers' AS stage,
  *
FROM customers