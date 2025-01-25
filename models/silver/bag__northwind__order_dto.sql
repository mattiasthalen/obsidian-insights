MODEL (
  name silver.bag__northwind__order_dto,
  kind VIEW,
  grain (
    order_id
  )
);

WITH source AS (
  SELECT
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
    'order' AS _sqlmesh_source_table
  FROM bronze.snp__northwind__order_dto
), hook_keys AS (
  SELECT
    CONCAT('northwind|order|', order_id::TEXT)::BLOB AS hook__order__id,
    CONCAT('northwind|customer|', customer_id::TEXT)::BLOB AS hook__customer__id,
    CONCAT('northwind|employee|', employee_id::TEXT)::BLOB AS hook__employee__id,
    *
  FROM source
)
SELECT
  *
FROM hook_keys