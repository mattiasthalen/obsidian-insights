MODEL (
  name silver.int__uss_bridge__shippers,
  kind VIEW
);

WITH shippers AS (
  SELECT
    _hook__shipper__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__shippers
)
SELECT
  'shippers' AS stage,
  *
FROM shippers