MODEL (
  name gold.uss__bridge__sales_territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _key__puppini
  )
);

WITH sales_territories AS (
  SELECT
    bag__northwind__territories._hook__reference__id__territory__valid_from,
    bag__northwind__regions._hook__reference__id__region__valid_from,
    GREATEST(
      bag__northwind__territories._sqlmesh_loaded_at,
      bag__northwind__regions._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__territories._sqlmesh_valid_from,
      bag__northwind__regions._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__territories._sqlmesh_valid_to,
      bag__northwind__regions._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to
  FROM silver.bag__northwind__territories
  LEFT JOIN silver.bag__northwind__regions
    ON bag__northwind__territories._hook__reference__id__region = bag__northwind__regions._hook__reference__id__region
    AND bag__northwind__territories._sqlmesh_valid_from < bag__northwind__regions._sqlmesh_valid_to
    AND bag__northwind__territories._sqlmesh_valid_to > bag__northwind__regions._sqlmesh_valid_from
)
SELECT
  'sales_territories' AS stage,
  @generate_surrogate_key(
    stage,
    _hook__reference__id__territory__valid_from,
    _hook__reference__id__region__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  ) AS _key__puppini,
  *
FROM sales_territories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts