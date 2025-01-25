MODEL (
  name silver.bag__northwind__customer_dto,
  kind VIEW,
  grain (
    customer_id
  )
);

WITH source AS (
  SELECT
    customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    postal_code,
    country,
    phone,
    fax,
    region,
    _dlt_load_id,
    _dlt_id,
    _dlt_extracted_at,
    _sqlmesh_hash_diff,
    _sqlmesh_loaded_at,
    _sqlmesh_valid_from::TIMESTAMP AS _sqlmesh_valid_from,
    COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59.999999')::TIMESTAMP AS _sqlmesh_valid_to,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
    _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record,
    'northwind' AS _sqlmesh_source_system,
    'customer' AS _sqlmesh_source_table
  FROM bronze.snp__northwind__customer_dto
), hook_keys AS (
  SELECT
    CONCAT('northwind|customer|', customer_id::TEXT)::BLOB AS hook__customer__id,
    *
  FROM source
)
SELECT
  *
FROM hook_keys