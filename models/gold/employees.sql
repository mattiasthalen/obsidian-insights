MODEL (
  name gold.employees,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column employee__record_loaded_at
  ),
  grain (
    _hook__employee__valid_from
  )
);

SELECT
  _hook__employee__valid_from,
  last_name AS employee__last_name,
  first_name AS employee__first_name,
  title AS employee__title,
  title_of_courtesy AS employee__title_of_courtesy,
  birth_date AS employee__birth_date,
  hire_date AS employee__hire_date,
  address AS employee__address,
  city AS employee__city,
  region AS employee__region,
  postal_code AS employee__postal_code,
  country AS employee__country,
  home_phone AS employee__home_phone,
  extension AS employee__extension,
  photo AS employee__photo,
  notes AS employee__notes,
  reports_to AS employee__reports_to,
  photo_path AS employee__photo_path,
  _sqlmesh_loaded_at AS employee__record_loaded_at,
  _sqlmesh_valid_from AS employee__record_valid_from,
  _sqlmesh_valid_to AS employee__record_valid_to,
  _sqlmesh_version AS employee__record_version,
  _sqlmesh_is_current_record AS employee__is_current_record
FROM silver.bag__northwind__employees
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts