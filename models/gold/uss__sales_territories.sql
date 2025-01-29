MODEL (
  name gold.uss__sales_territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__reference__id__territory
  )
);

WITH sales_territories AS (
  SELECT
    bag__northwind__territories._hook__reference__id__territory,
    bag__northwind__territories.territory_description,
    bag__northwind__regions.region_description,
    bag__northwind__territories._sqlmesh_loaded_at,
    bag__northwind__territories._sqlmesh_valid_from,
    bag__northwind__territories._sqlmesh_valid_to,
    bag__northwind__territories._sqlmesh_version,
    bag__northwind__territories._sqlmesh_is_current_record
  FROM silver.bag__northwind__territories
  LEFT JOIN silver.bag__northwind__regions
    ON bag__northwind__territories._hook__reference__id__region = bag__northwind__regions._hook__reference__id__region
)
SELECT
  *
FROM sales_territories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts