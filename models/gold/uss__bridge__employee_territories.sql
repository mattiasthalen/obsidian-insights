MODEL (
  name gold.uss__bridge__employee_territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH employee_territories AS (
  SELECT
    bag__northwind__employees._hook__employee__valid_from,
    bag__northwind__territories._hook__reference__territory__valid_from,
    GREATEST(
      bag__northwind__employee_territories._sqlmesh_loaded_at,
      bag__northwind__employees._sqlmesh_loaded_at,
      bag__northwind__territories._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__employee_territories._sqlmesh_valid_from,
      bag__northwind__employees._sqlmesh_valid_from,
      bag__northwind__territories._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__employee_territories._sqlmesh_valid_to,
      bag__northwind__employees._sqlmesh_valid_to,
      bag__northwind__territories._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to
  FROM silver.bag__northwind__employee_territories
  LEFT JOIN silver.bag__northwind__employees
    ON bag__northwind__employee_territories._hook__employee = bag__northwind__employees._hook__employee
    AND bag__northwind__employee_territories._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
    AND bag__northwind__employee_territories._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__territories
    ON bag__northwind__employee_territories._hook__reference__territory = bag__northwind__territories._hook__reference__territory
    AND bag__northwind__employee_territories._sqlmesh_valid_from < bag__northwind__territories._sqlmesh_valid_to
    AND bag__northwind__employee_territories._sqlmesh_valid_to > bag__northwind__territories._sqlmesh_valid_from
)
SELECT
  'employee_territories' AS stage,
  *
FROM employee_territories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts