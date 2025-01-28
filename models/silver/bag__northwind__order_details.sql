MODEL (
  name silver.bag__northwind__order_details,
  kind VIEW,
  grain (
    _hook__order_detail__id
  )
);

SELECT
  CONCAT('northwind|order|', order_id::TEXT, '~northwind|product|', product_id::TEXT)::BLOB AS _hook__order_detail__id,
  CONCAT('northwind|order|', order_id::TEXT)::BLOB AS _hook__order__id,
  CONCAT('northwind|product|', product_id::TEXT)::BLOB AS _hook__product__id,
  order_id,
  product_id,
  unit_price,
  quantity,
  discount,
  discount__v_double,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from::TIMESTAMP AS _sqlmesh_valid_from,
  COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59.999999')::TIMESTAMP AS _sqlmesh_valid_to,
  ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
  _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record,
  'northwind' AS _sqlmesh_source_system,
  'order_details' AS _sqlmesh_source_table
FROM bronze.snp__northwind__order_details
