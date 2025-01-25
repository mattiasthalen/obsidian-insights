MODEL (
  name gold.uss__northwind__shipper,
  kind FULL,
  grain (
    hook__shipper__id
  )
);

SELECT
  hook__shipper__id,
  company_name,
  phone,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__shipper_dto