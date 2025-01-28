MODEL (
  name gold.uss__product,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__product__id
  )
);

WITH products AS(
SELECT
  bag__northwind__products._hook__product__id,
  bag__northwind__products.product_name,
  bag__northwind__products.quantity_per_unit,
  bag__northwind__products.unit_price,
  bag__northwind__products.units_in_stock,
  bag__northwind__products.units_on_order,
  bag__northwind__products.reorder_level,
  bag__northwind__products.discontinued,
  bag__northwind__categories.category_name,
  bag__northwind__categories.description AS category_description,
  bag__northwind__category_details.picture AS category_picture,
  bag__northwind__products._sqlmesh_loaded_at,
  bag__northwind__products._sqlmesh_valid_from,
  bag__northwind__products._sqlmesh_valid_to,
  bag__northwind__products._sqlmesh_version,
  bag__northwind__products._sqlmesh_is_current_record
FROM silver.bag__northwind__products

LEFT JOIN silver.bag__northwind__categories
  ON bag__northwind__products._hook__reference__id__category = bag__northwind__categories._hook__reference__id__category

LEFT JOIN silver.bag__northwind__category_details
    ON bag__northwind__products._hook__reference__id__category = bag__northwind__category_details._hook__reference__id__category
)

SELECT
  *
FROM products
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts