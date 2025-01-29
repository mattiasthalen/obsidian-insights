MODEL (
  name silver.pit__northwind__orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    _hook__order__id__valid_from, _hook__customer__id__valid_from, _hook__employee__id__valid_from
  )
);

SELECT
  bag__northwind__orders._hook__order__id__valid_from,
  bag__northwind__customers._hook__customer__id__valid_from,
  bag__northwind__employees._hook__employee__id__valid_from,
  bag__northwind__orders._hook__order__id,
  bag__northwind__orders._hook__customer__id,
  bag__northwind__orders._hook__employee__id,
  bag__northwind__orders.order_id,
  bag__northwind__orders.customer_id,
  bag__northwind__orders.employee_id,
  bag__northwind__orders.order_date,
  bag__northwind__orders.required_date,
  bag__northwind__orders.shipped_date,
  bag__northwind__orders.ship_via,
  bag__northwind__orders.freight,
  bag__northwind__orders.ship_name,
  bag__northwind__orders.ship_address,
  bag__northwind__orders.ship_city,
  bag__northwind__orders.ship_postal_code,
  bag__northwind__orders.ship_country,
  bag__northwind__orders.ship_region,
  bag__northwind__orders._dlt_load_id,
  bag__northwind__orders._dlt_id,
  bag__northwind__orders._dlt_extracted_at,
  bag__northwind__orders._sqlmesh_hash_diff,
  bag__northwind__orders._sqlmesh_loaded_at,
  GREATEST(
    bag__northwind__orders._sqlmesh_valid_from,
    bag__northwind__customers._sqlmesh_valid_from,
    bag__northwind__employees._sqlmesh_valid_from
  ) AS _sqlmesh_valid_from,
  LEAST(
    bag__northwind__orders._sqlmesh_valid_to,
    bag__northwind__customers._sqlmesh_valid_to,
    bag__northwind__employees._sqlmesh_valid_to
  ) AS _sqlmesh_valid_to,
  --ROW_NUMBER() OVER (PARTITION BY bag__northwind__orders._hook__order__id ORDER BY _sqlmesh_valid_from) AS _sqlmesh_version,
  --ROW_NUMBER() OVER (PARTITION BY bag__northwind__orders._hook__order__id ORDER BY _sqlmesh_valid_from DESC) = 1 AS _sqlmesh_is_current_record,
  bag__northwind__orders._sqlmesh_source_system,
  bag__northwind__orders._sqlmesh_source_table
FROM silver.bag__northwind__orders
LEFT JOIN silver.bag__northwind__customers
  ON bag__northwind__orders._hook__customer__id = bag__northwind__customers._hook__customer__id
  AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__customers._sqlmesh_valid_to
  AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__customers._sqlmesh_valid_from
LEFT JOIN silver.bag__northwind__employees
  ON bag__northwind__orders._hook__employee__id = bag__northwind__employees._hook__employee__id
  AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
  AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
WHERE
  bag__northwind__orders._sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts