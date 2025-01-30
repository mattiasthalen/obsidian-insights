MODEL (
  name silver.int__uss_bridge__employee_territories,
  kind VIEW
);

WITH employee_territories AS (
  SELECT
    bag__northwind__employees._hook__employee__valid_from,
    bag__northwind__territories._hook__reference__territory__valid_from,
    bag__northwind__regions._hook__reference__region__valid_from,
    GREATEST(
      bag__northwind__employee_territories._sqlmesh_loaded_at,
      bag__northwind__employees._sqlmesh_loaded_at,
      bag__northwind__territories._sqlmesh_loaded_at,
      bag__northwind__regions._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__employee_territories._sqlmesh_valid_from,
      bag__northwind__employees._sqlmesh_valid_from,
      bag__northwind__territories._sqlmesh_valid_from,
      bag__northwind__regions._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__employee_territories._sqlmesh_valid_to,
      bag__northwind__employees._sqlmesh_valid_to,
      bag__northwind__territories._sqlmesh_valid_to,
      bag__northwind__regions._sqlmesh_valid_to
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
  LEFT JOIN silver.bag__northwind__regions
    ON bag__northwind__territories._hook__reference__region = bag__northwind__regions._hook__reference__region
    AND bag__northwind__employee_territories._sqlmesh_valid_from < bag__northwind__regions._sqlmesh_valid_to
    AND bag__northwind__employee_territories._sqlmesh_valid_to > bag__northwind__regions._sqlmesh_valid_from
)
SELECT
  'employee_territories' AS stage,
  *
FROM employee_territories