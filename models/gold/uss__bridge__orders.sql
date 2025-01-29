MODEL (
  name gold.uss__bridge__orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH orders AS (
  SELECT
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__customers._hook__customer__valid_from,
    bag__northwind__employees._hook__employee__valid_from,
    GREATEST(
      bag__northwind__orders._sqlmesh_loaded_at,
      bag__northwind__customers._sqlmesh_loaded_at,
      bag__northwind__employees._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__orders._sqlmesh_valid_from,
      bag__northwind__customers._sqlmesh_valid_from,
      bag__northwind__employees._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__orders._sqlmesh_valid_to,
      bag__northwind__customers._sqlmesh_valid_to,
      bag__northwind__employees._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to,
    bag__northwind__orders._sqlmesh_source_system,
    bag__northwind__orders._sqlmesh_source_table
  FROM silver.bag__northwind__orders
  LEFT JOIN silver.bag__northwind__customers
    ON bag__northwind__orders._hook__customer = bag__northwind__customers._hook__customer
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__customers._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__customers._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__employees
    ON bag__northwind__orders._hook__employee = bag__northwind__employees._hook__employee
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
)
SELECT
  'orders' AS stage,
  *
FROM orders
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts