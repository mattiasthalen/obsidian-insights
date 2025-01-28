MODEL (
  name gold.uss__northwind__region,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__reference__id__region
  )
);

SELECT
  _hook__reference__id__region,
  region_description
FROM silver.bag__northwind__regions