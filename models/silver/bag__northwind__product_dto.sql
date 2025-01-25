MODEL (
  name silver.bag__northwind__product_dto,
  kind VIEW,
  grain (
    product_id
  )
);

SELECT
  CONCAT('northwind|product|', product_id::TEXT)::BLOB AS hook__product__id,
  CONCAT('northwind|supplier|', supplier_id::TEXT)::BLOB AS hook__supplier__id,
  category_id AS key__category_id,
  product_id,
  supplier_id,
  category_id,
  product_name,
  quantity_per_unit,
  unit_price,
  units_in_stock,
  units_on_order,
  reorder_level,
  discontinued,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from::TIMESTAMP AS _sqlmesh_valid_from,
  COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59.999999')::TIMESTAMP AS _sqlmesh_valid_to,
  ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
  _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record,
  'northwind' AS _sqlmesh_source_system,
  'product' AS _sqlmesh_source_table
FROM bronze.snp__northwind__product_dto