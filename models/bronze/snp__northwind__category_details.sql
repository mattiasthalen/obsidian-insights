MODEL (
  name bronze.snp__northwind__category_details,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  )
);

SELECT
  category_id::BIGINT AS category_id,
  category_name::TEXT AS category_name,
  description::TEXT AS description,
  picture::TEXT AS picture,
  product_names::TEXT AS product_names,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @generate_surrogate_key(category_id, category_name, description, picture, product_names) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw__northwind__category_details