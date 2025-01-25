MODEL (
  name gold.uss__northwind__customer,
  kind FULL,
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
  region,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__customer_dto