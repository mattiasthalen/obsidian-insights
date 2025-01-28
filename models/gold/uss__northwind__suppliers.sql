MODEL (
  name gold.uss__northwind__supplier,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__supplier__id
  )
);

SELECT
  _hook__supplier__id,
  company_name,
  contact_name,
  contact_title,
  address,
  city,
  postal_code,
  country,
  phone,
  region,
  home_page,
  fax
FROM silver.bag__northwind__suppliers