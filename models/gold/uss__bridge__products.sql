MODEL (
  name gold.uss__bridge__products,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _key__puppini
  )
);

WITH products AS (
  SELECT
    bag__northwind__products._hook__product__id__valid_from,
    bag__northwind__suppliers._hook__supplier__id__valid_from,
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
    ON bag__northwind__products._hook__supplier__id = bag__northwind__suppliers._hook__supplier__id
    AND bag__northwind__products._sqlmesh_valid_from < bag__northwind__suppliers._sqlmesh_valid_to
    AND bag__northwind__products._sqlmesh_valid_to > bag__northwind__suppliers._sqlmesh_valid_from
)
SELECT
  'products' AS stage,
  *
FROM products
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts