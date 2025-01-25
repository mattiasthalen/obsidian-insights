MODEL (
  name bronze.snp__northwind__territory_dto,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  ),
  grain (territory_id),
);

SELECT
  CAST(territory_id AS TEXT) AS territory_id,
  CAST(territory_description AS TEXT) AS territory_description,
  CAST(region_id AS BIGINT) AS region_id,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_extracted_at,
  
  @generate_surrogate_key(
    territory_id,
    territory_description,
    region_id
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
  
FROM
  bronze.raw__northwind__territory_dto
