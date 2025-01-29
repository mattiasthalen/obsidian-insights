MODEL (
  name silver.pit__northwind__employee_territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (_hook__employee__id__valid_from, _hook__reference__id__territory__valid_from)
);

SELECT
  bag__northwind__employees._hook__employee__id__valid_from,
  bag__northwind__territories._hook__reference__id__territory__valid_from,
  bag__northwind__employee_territories._hook__employee__id,
  bag__northwind__employee_territories._hook__reference__id__territory,
  bag__northwind__employee_territories.employee_id,
  bag__northwind__employee_territories.territory_id,
  bag__northwind__employee_territories._dlt_load_id,
  bag__northwind__employee_territories._dlt_id,
  bag__northwind__employee_territories._dlt_extracted_at,
  bag__northwind__employee_territories._sqlmesh_hash_diff,
  bag__northwind__employee_territories._sqlmesh_loaded_at,

  GREATEST(
    bag__northwind__employee_territories._sqlmesh_valid_from,
    bag__northwind__employees._sqlmesh_valid_from,
    bag__northwind__territories._sqlmesh_valid_from
  ) AS _sqlmesh_valid_from,
  LEAST(
    bag__northwind__employee_territories._sqlmesh_valid_to,
    bag__northwind__employees._sqlmesh_valid_to,
    bag__northwind__territories._sqlmesh_valid_to
  ) AS _sqlmesh_valid_to,

  --bag__northwind__employee_territories._sqlmesh_version,
  --bag__northwind__employee_territories._sqlmesh_is_current_record,
  bag__northwind__employee_territories._sqlmesh_source_system,
  bag__northwind__employee_territories._sqlmesh_source_table
FROM silver.bag__northwind__employee_territories
LEFT JOIN silver.bag__northwind__employees
  ON bag__northwind__employee_territories._hook__employee__id = bag__northwind__employees._hook__employee__id
  AND bag__northwind__employee_territories._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
  AND bag__northwind__employee_territories._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
LEFT JOIN silver.bag__northwind__territories
  ON bag__northwind__employee_territories._hook__reference__id__territory = bag__northwind__territories._hook__reference__id__territory
  AND bag__northwind__employee_territories._sqlmesh_valid_from < bag__northwind__territories._sqlmesh_valid_to
  AND bag__northwind__employee_territories._sqlmesh_valid_to > bag__northwind__territories._sqlmesh_valid_from
WHERE
  bag__northwind__employee_territories._sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts