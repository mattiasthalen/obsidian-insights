MODEL (
  name gold.territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column territory__record_loaded_at
  ),
  grain (
    _hook__reference__territory__valid_from
  )
);

SELECT
  _hook__reference__territory__valid_from,
  territory_description AS territory__description,
  _sqlmesh_loaded_at AS territory__record_loaded_at,
  _sqlmesh_valid_from AS territory__record_valid_from,
  _sqlmesh_valid_to AS territory__record_valid_to,
  _sqlmesh_version AS territory__record_version,
  _sqlmesh_is_current_record AS territory__is_current_record
FROM silver.bag__northwind__territories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts