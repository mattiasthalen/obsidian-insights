MODEL (
  name gold.uss__northwind__employees,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__employee__id
  )
);

SELECT
  _hook__employee__id,
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
  photo_path
FROM silver.bag__northwind__employees