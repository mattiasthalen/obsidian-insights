MODEL (
  name gold.uss__northwind__territory,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__reference__id__territory
  )
);

SELECT
  _hook__reference__id__territory,
  territory_description
FROM silver.bag__northwind__territories