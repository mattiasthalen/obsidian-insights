MODEL (
  name gold.uss__northwind__territory,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    key__territory_id
  )
);

SELECT
  key__territory_id,
  territory_description,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__territory_dto