MODEL (
  name gold.uss__northwind__category,
  kind FULL,
  grain (
    key__category_id
  )
);

SELECT
  key__category_id,
  category_name,
  description,
  _sqlmesh_valid_from,
  _sqlmesh_valid_to,
  _sqlmesh_version,
  _sqlmesh_is_current_record
FROM silver.bag__northwind__category_dto