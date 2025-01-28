MODEL (
  name gold.uss__northwind__territory,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__reference__id__territory
  )
);

SELECT
  hook__reference__id__territory,
  territory_description
FROM silver.bag__northwind__territories