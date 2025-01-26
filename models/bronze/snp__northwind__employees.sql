MODEL (
  name bronze.snp__northwind__employees,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (
    employee_id
  )
);

SELECT
  employee_id::BIGINT AS employee_id,
  last_name::TEXT AS last_name,
  first_name::TEXT AS first_name,
  title::TEXT AS title,
  title_of_courtesy::TEXT AS title_of_courtesy,
  birth_date::TIMESTAMP AS birth_date,
  hire_date::TIMESTAMP AS hire_date,
  address::TEXT AS address,
  city::TEXT AS city,
  region::TEXT AS region,
  postal_code::TEXT AS postal_code,
  country::TEXT AS country,
  home_phone::TEXT AS home_phone,
  extension::TEXT AS extension,
  photo::TEXT AS photo,
  notes::TEXT AS notes,
  reports_to::BIGINT AS reports_to,
  photo_path::TEXT AS photo_path,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @generate_surrogate_key(
    employee_id,
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
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw__northwind__employees