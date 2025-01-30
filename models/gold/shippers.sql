MODEL (
  name gold.shippers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column shipper__record_loaded_at
  ),
  grain (
    _hook__shipper__valid_from
  )
);

SELECT
  _hook__shipper__valid_from,
  company_name AS shipper__company_name,
  phone AS shipper__phone,
  _sqlmesh_loaded_at AS shipper__record_loaded_at,
  _sqlmesh_valid_from AS shipper__record_valid_from,
  _sqlmesh_valid_to AS shipper__record_valid_to,
  _sqlmesh_version AS shipper__record_version,
  _sqlmesh_is_current_record AS shipper__is_current_record
FROM silver.bag__northwind__shippers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts