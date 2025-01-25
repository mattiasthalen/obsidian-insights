MODEL (
  name gold.uss__northwind__territory,
  kind FULL,
  grain (
    key__territory_id
  )
);

SELECT
  key__territory_id,
  territory_description,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__territory_dto