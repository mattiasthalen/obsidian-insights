MODEL (
  name gold.uss__northwind__customer,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__customer__id
  )
);

SELECT
  hook__customer__id,
  company_name,
  contact_name,
  contact_title,
  address,
  city,
  postal_code,
  country,
  phone,
  fax,
  region
FROM silver.bag__northwind__customers