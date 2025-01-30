MODEL (
  name silver.int__uss_bridge__regions,
  kind VIEW
);

WITH regions AS (
  SELECT
    _hook__reference__region__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__regions
)
SELECT
  'regions' AS stage,
  *
FROM regions