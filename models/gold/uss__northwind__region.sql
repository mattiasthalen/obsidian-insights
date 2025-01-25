MODEL (
  name gold.uss__northwind__region,
  kind FULL,
  grain (
    key__region_id
  )
);

SELECT
  key__region_id,
  region_description,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__region_dto