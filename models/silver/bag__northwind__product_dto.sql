MODEL (
  name silver.bag__northwind__product_dto,
  kind VIEW,
  grain (product_id),
);

SELECT
  product_id,
  product_name,
  supplier_id,
  category_id,
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
  _sqlmesh_loaded_at
FROM
  bronze.snp__northwind__product_dto
