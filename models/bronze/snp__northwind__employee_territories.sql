MODEL (
  name bronze.snp__northwind__employee_territories,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (employee_id, territory_id)
);

SELECT
  _get_northwindapiv_1_employees_employee_id::BIGINT AS employee_id,
  territory_id::TEXT AS territory_id,
  territory_description::TEXT AS territory_description,
  region_id::BIGINT AS region_id,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @generate_surrogate_key(employee_id, territory_id, territory_description, region_id) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw__northwind__employee_territories