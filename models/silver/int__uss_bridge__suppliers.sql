MODEL (
  name silver.int__uss_bridge__suppliers,
  kind VIEW
);

WITH suppliers AS (
  SELECT
    _hook__supplier__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__suppliers
)
SELECT
  'suppliers' AS stage,
  *
FROM suppliers