MODEL (
  name gold.customers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column customer__record_loaded_at
  ),
  grain (
    _hook__customer__valid_from
  )
);

SELECT
  _hook__customer__valid_from,
  company_name AS customer__company_name,
  contact_name AS customer__contact_name,
  contact_title AS customer__contact_title,
  address AS customer__address,
  city AS customer__city,
  postal_code AS customer__postal_code,
  country AS customer__country,
  phone AS customer__phone,
  fax AS customer__fax,
  region AS customer__region,
  _sqlmesh_loaded_at AS customer__record_loaded_at,
  _sqlmesh_valid_from AS customer__record_valid_from,
  _sqlmesh_valid_to AS customer__record_valid_to,
  _sqlmesh_version AS customer__record_version,
  _sqlmesh_is_current_record AS customer__is_current_record
FROM silver.bag__northwind__customers
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts