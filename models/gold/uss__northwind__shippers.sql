MODEL (
  name gold.uss__northwind__shipper,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__shipper__id
  )
);

SELECT
  hook__shipper__id,
  company_name,
  phone
FROM silver.bag__northwind__shippers