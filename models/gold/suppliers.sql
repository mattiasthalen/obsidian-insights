MODEL (
  name gold.suppliers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column supplier__record_loaded_at
  ),
  grain (
    _hook__supplier__valid_from
  )
);

SELECT
  _hook__supplier__valid_from,
  contact_name AS supplier__contact_name,
  contact_title AS supplier__contact_title,
  address AS supplier__address,
  city AS supplier__city,
  postal_code AS supplier__postal_code,
  country AS supplier__country,
  phone AS supplier__phone,
  region AS supplier__region,
  home_page AS supplier__home_page,
  fax AS supplier__fax,
  _sqlmesh_loaded_at AS supplier__record_loaded_at,
  _sqlmesh_valid_from AS supplier__record_valid_from,
  _sqlmesh_valid_to AS supplier__record_valid_to,
  _sqlmesh_version AS supplier__record_version,
  _sqlmesh_is_current_record AS supplier__is_current_record
FROM silver.bag__northwind__suppliers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts