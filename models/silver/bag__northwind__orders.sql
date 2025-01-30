MODEL (
  name silver.bag__northwind__orders,
  kind VIEW,
  grain (
    _hook__order__valid_from
  )
);

SELECT
  CONCAT('northwind|order|', order_id::TEXT, '~epoch|valid_from|', _sqlmesh_valid_from::TEXT)::BLOB AS _hook__order__valid_from,
  CONCAT('northwind|order|', order_id::TEXT)::BLOB AS _hook__order,
  CONCAT('northwind|customer|', customer_id::TEXT)::BLOB AS _hook__customer,
  CONCAT('northwind|employee|', employee_id::TEXT)::BLOB AS _hook__employee,
  CONCAT('northwind|shipper|', ship_via::TEXT)::BLOB AS _hook__shipper,
  order_id,
  customer_id,
  employee_id,
  order_date,
  required_date,
  shipped_date,
  ship_via,
  freight,
  ship_name,
  ship_address,
  ship_city,
  ship_postal_code,
  ship_country,
  ship_region,
  _dlt_load_id,
  _dlt_id,
  _dlt_extracted_at,
  _sqlmesh_hash_diff,
  _sqlmesh_loaded_at,
  _sqlmesh_valid_from::TIMESTAMP AS _sqlmesh_valid_from,
  COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59.999999')::TIMESTAMP AS _sqlmesh_valid_to,
  ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
  _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record,
  'northwind' AS _sqlmesh_source_system,
  'orders' AS _sqlmesh_source_table
FROM bronze.snp__northwind__orders