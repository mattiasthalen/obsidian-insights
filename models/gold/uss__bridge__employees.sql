MODEL (
  name gold.uss__bridge__employees,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH employees AS (
  SELECT
    _hook__employee__id__valid_from,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from,
    _sqlmesh_valid_to
  FROM silver.bag__northwind__employees
)
SELECT
  'employees' AS stage,
  *
FROM employees
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts