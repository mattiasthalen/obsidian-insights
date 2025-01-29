MODEL (
  name gold.uss__peripheral__products,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _pit__product
  )
);

WITH products AS (
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
    bag__northwind__categories.description,
    bag__northwind__category_details.picture,

    GREATEST(
      bag__northwind__products._sqlmesh_loaded_at,
      bag__northwind__categories._sqlmesh_loaded_at,
      bag__northwind__category_details._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__products._sqlmesh_valid_from,
      bag__northwind__categories._sqlmesh_valid_from,
      bag__northwind__category_details._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__products._sqlmesh_valid_to,
      bag__northwind__categories._sqlmesh_valid_to,
      bag__northwind__category_details._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to

  FROM silver.bag__northwind__products
  LEFT JOIN silver.bag__northwind__categories
    ON bag__northwind__products._hook__reference__id__category = bag__northwind__categories._hook__reference__id__category
    AND bag__northwind__products._sqlmesh_valid_from < bag__northwind__categories._sqlmesh_valid_to
    AND bag__northwind__products._sqlmesh_valid_to > bag__northwind__categories._sqlmesh_valid_from

  LEFT JOIN silver.bag__northwind__category_details
    ON bag__northwind__products._hook__reference__id__category = bag__northwind__category_details._hook__reference__id__category
    AND bag__northwind__products._sqlmesh_valid_from < bag__northwind__category_details._sqlmesh_valid_to
    AND bag__northwind__products._sqlmesh_valid_to > bag__northwind__category_details._sqlmesh_valid_from

)
SELECT
  @generate_surrogate_key(_hook__product__id, _sqlmesh_valid_from)  As _pit__product,
  *
FROM products
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts