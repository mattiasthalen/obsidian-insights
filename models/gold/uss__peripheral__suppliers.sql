MODEL (
  name gold.uss__peripheral__suppliers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__supplier__valid_from
  )
);

SELECT
  _hook__supplier__valid_from,
  contact_name,
  contact_title,
  address,
  city,
  postal_code,
  country,
  phone,
  region,
  home_page,
  fax,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__suppliers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts