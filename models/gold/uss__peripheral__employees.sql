MODEL (
  name gold.uss__peripheral__employees,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__employee__valid_from
  )
);

SELECT
  _hook__employee__valid_from,
  last_name,
  first_name,
  title,
  title_of_courtesy,
  birth_date,
  hire_date,
  address,
  city,
  region,
  postal_code,
  country,
  home_phone,
  extension,
  photo,
  notes,
  reports_to,
  photo_path,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__employees
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts