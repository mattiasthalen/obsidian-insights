MODEL (
  name gold.uss__peripheral__regions,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__reference__id__region__valid_from
  )
);

SELECT
  _hook__reference__id__region__valid_from,
  region_description,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__regions
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts