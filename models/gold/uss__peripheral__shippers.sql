MODEL (
  name gold.uss__peripheral__shippers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__shipper__id__valid_from
  )
);

SELECT
  @generate_surrogate_key(_hook__shipper__id, _sqlmesh_valid_from)  As _pit__shipper,
  _hook__shipper__id__valid_from,
  company_name,
  phone,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__shippers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts