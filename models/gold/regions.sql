MODEL (
  name gold.regions,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column region__record_loaded_at
  ),
  grain (
    _hook__reference__region__valid_from
  )
);

SELECT
  _hook__reference__region__valid_from,
  region_description AS region__description,
  _sqlmesh_loaded_at AS region__record_loaded_at,
  _sqlmesh_valid_from AS region__record_valid_from,
  _sqlmesh_valid_to AS region__record_valid_to,
  _sqlmesh_version AS region__record_version,
  _sqlmesh_is_current_record AS region__is_current_record
FROM silver.bag__northwind__regions
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts