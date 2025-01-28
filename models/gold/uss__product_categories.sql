MODEL (
  name gold.uss__product_categories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__reference__id__category
  )
);

WITH product_categories AS(
SELECT
  bag__northwind__categories._hook__reference__id__category,
  bag__northwind__categories.category_name,
  bag__northwind__categories.description,
  bag__northwind__category_details.picture,
  bag__northwind__category_details.product_names,
  bag__northwind__categories._sqlmesh_loaded_at,
  bag__northwind__categories._sqlmesh_valid_from,
  bag__northwind__categories._sqlmesh_valid_to,
  bag__northwind__categories._sqlmesh_version,
  bag__northwind__categories._sqlmesh_is_current_record
FROM silver.bag__northwind__categories

LEFT JOIN silver.bag__northwind__category_details
    ON bag__northwind__categories._hook__reference__id__category = bag__northwind__category_details._hook__reference__id__category
)

SELECT
    *
FROM product_categories

    WHERE
      _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts