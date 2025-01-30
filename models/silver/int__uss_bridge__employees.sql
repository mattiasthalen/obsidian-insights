MODEL (
  name silver.int__uss_bridge__employees,
  kind VIEW
);

WITH employees AS (
  SELECT
    _hook__employee__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__employees
)
SELECT
  'employees' AS stage,
  *
FROM employees