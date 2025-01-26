MODEL (
  name gold.uss__northwind__employee_territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    hook__employee__id
  )
);

SELECT
  hook__employee_territory__id,
  employee_id,
  territory_id,
  territory_description,
  region_id,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__employee_territories