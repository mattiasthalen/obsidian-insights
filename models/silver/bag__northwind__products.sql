MODEL (
  name silver.bag__northwind__products,
  kind VIEW,
  grain (
    _hook__product__valid_from
  )
);

SELECT
  CONCAT(
    'northwind|product|',
    product_id::TEXT,
    '~epoch|valid_from|',
    _sqlmesh_valid_from::TEXT
  )::BLOB AS _hook__product__valid_from,
  CONCAT('northwind|product|', product_id::TEXT)::BLOB AS _hook__product,
  CONCAT('northwind|supplier|', supplier_id::TEXT)::BLOB AS _hook__supplier,
  CONCAT('northwind|category|', category_id) AS _hook__reference__category,
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
  'products' AS _sqlmesh_source_table
FROM bronze.snp__northwind__products