MODEL (
  name silver.int__uss_bridge__products,
  kind VIEW
);

WITH products AS (
  SELECT
    bag__northwind__products._hook__product__valid_from,
    bag__northwind__suppliers._hook__supplier__valid_from,
    GREATEST(
      bag__northwind__products._sqlmesh_loaded_at,
      bag__northwind__suppliers._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__products._sqlmesh_valid_from,
      bag__northwind__suppliers._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__products._sqlmesh_valid_to,
      bag__northwind__suppliers._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to
  FROM silver.bag__northwind__products
  LEFT JOIN silver.bag__northwind__suppliers
    ON bag__northwind__products._hook__supplier = bag__northwind__suppliers._hook__supplier
    AND bag__northwind__products._sqlmesh_valid_from < bag__northwind__suppliers._sqlmesh_valid_to
    AND bag__northwind__products._sqlmesh_valid_to > bag__northwind__suppliers._sqlmesh_valid_from
)
SELECT
  'products' AS stage,
  *
FROM products