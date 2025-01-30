MODEL (
  name silver.int__uss_bridge__categories,
  kind VIEW
);

WITH categories AS (
  SELECT
    _hook__reference__category__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__categories
)
SELECT
  'categories' AS stage,
  *
FROM categories