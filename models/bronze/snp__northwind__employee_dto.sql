MODEL (
  name bronze.snp__northwind__employee_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (employee_id),
);

SELECT
  CAST(employee_id AS BIGINT) AS employee_id,
  CAST(last_name AS TEXT) AS last_name,
  CAST(first_name AS TEXT) AS first_name,
  CAST(title AS TEXT) AS title,
  CAST(title_of_courtesy AS TEXT) AS title_of_courtesy,
  CAST(birth_date AS TIMESTAMP) AS birth_date,
  CAST(hire_date AS TIMESTAMP) AS hire_date,
  CAST(address AS TEXT) AS address,
  CAST(city AS TEXT) AS city,
  CAST(region AS TEXT) AS region,
  CAST(postal_code AS TEXT) AS postal_code,
  CAST(country AS TEXT) AS country,
  CAST(home_phone AS TEXT) AS home_phone,
  CAST(extension AS TEXT) AS extension,
  CAST(photo AS TEXT) AS photo,
  CAST(notes AS TEXT) AS notes,
  CAST(reports_to AS BIGINT) AS reports_to,
  CAST(photo_path AS TEXT) AS photo_path,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
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
FROM
  bronze.raw__northwind__employee_dto
