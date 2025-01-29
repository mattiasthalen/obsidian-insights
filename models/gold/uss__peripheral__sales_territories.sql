MODEL (
  name gold.uss__peripheral__sales_territories,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _pit__territory
  )
);

WITH sales_territories AS (
  SELECT
    bag__northwind__territories._hook__reference__id__territory,
    bag__northwind__territories.territory_description,
    bag__northwind__regions.region_description,

    GREATEST(
      bag__northwind__territories._sqlmesh_loaded_at,
      bag__northwind__regions._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__territories._sqlmesh_valid_from,
      bag__northwind__regions._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__territories._sqlmesh_valid_to,
      bag__northwind__regions._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to

  FROM silver.bag__northwind__territories
  LEFT JOIN silver.bag__northwind__regions
    ON bag__northwind__territories._hook__reference__id__region = bag__northwind__regions._hook__reference__id__region
    AND bag__northwind__territories._sqlmesh_valid_from < bag__northwind__regions._sqlmesh_valid_to
    AND bag__northwind__territories._sqlmesh_valid_to > bag__northwind__regions._sqlmesh_valid_from
)
SELECT
  @generate_surrogate_key(_hook__reference__id__territory, _sqlmesh_valid_from)  As _pit__territory,
  *
FROM sales_territories
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts