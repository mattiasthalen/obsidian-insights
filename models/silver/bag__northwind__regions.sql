MODEL (
  name silver.bag__northwind__regions,
  kind VIEW,
  grain (
    _hook__reference__region__valid_from
  )
);

SELECT
  CONCAT('northwind|region|', region_id::TEXT, '~epoch|valid_from|', _sqlmesh_valid_from::TEXT)::BLOB AS _hook__reference__region__valid_from,
  CONCAT('northwind|region|', region_id::TEXT)::BLOB AS _hook__reference__region,
  region_id,
  region_description,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from::TIMESTAMP AS _sqlmesh_valid_from,
  COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59.999999')::TIMESTAMP AS _sqlmesh_valid_to,
  ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
  _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record,
  'northwind' AS _sqlmesh_source_system,
  'regions' AS _sqlmesh_source_table
FROM bronze.snp__northwind__regions