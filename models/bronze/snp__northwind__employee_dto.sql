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
  CAST(c.employee_id AS BIGINT) AS employee_id,
  CAST(c.last_name AS TEXT) AS last_name,
  CAST(c.first_name AS TEXT) AS first_name,
  CAST(c.title AS TEXT) AS title,
  CAST(c.title_of_courtesy AS TEXT) AS title_of_courtesy,
  CAST(c.birth_date AS TIMESTAMP) AS birth_date,
  CAST(c.hire_date AS TIMESTAMP) AS hire_date,
  CAST(c.address AS TEXT) AS address,
  CAST(c.city AS TEXT) AS city,
  CAST(c.region AS TEXT) AS region,
  CAST(c.postal_code AS TEXT) AS postal_code,
  CAST(c.country AS TEXT) AS country,
  CAST(c.home_phone AS TEXT) AS home_phone,
  CAST(c.extension AS TEXT) AS extension,
  CAST(c.photo AS TEXT) AS photo,
  CAST(c.notes AS TEXT) AS notes,
  CAST(c.reports_to AS BIGINT) AS reports_to,
  CAST(c.photo_path AS TEXT) AS photo_path,
  CAST(c._dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(c._dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    c.employee_id,
    c.last_name,
    c.first_name,
    c.title,
    c.title_of_courtesy,
    c.birth_date,
    c.hire_date,
    c.address,
    c.city,
    c.region,
    c.postal_code,
    c.country,
    c.home_phone,
    c.extension,
    c.photo,
    c.notes,
    c.reports_to,
    c.photo_path
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM
  bronze.raw__northwind__employee_dto as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
