MODEL (
  name gold.uss__bridge__orders,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  )
);

WITH orders AS (
  SELECT
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__customers._hook__customer__valid_from,
    bag__northwind__employees._hook__employee__valid_from,
    bag__northwind__shippers._hook__shipper__valid_from,
    GREATEST(
      bag__northwind__orders._sqlmesh_loaded_at,
      bag__northwind__customers._sqlmesh_loaded_at,
      bag__northwind__employees._sqlmesh_loaded_at,
      bag__northwind__shippers._sqlmesh_loaded_at
    ) AS _sqlmesh_loaded_at,
    GREATEST(
      bag__northwind__orders._sqlmesh_valid_from,
      bag__northwind__customers._sqlmesh_valid_from,
      bag__northwind__employees._sqlmesh_valid_from,
      bag__northwind__shippers._sqlmesh_valid_from
    ) AS _sqlmesh_valid_from,
    LEAST(
      bag__northwind__orders._sqlmesh_valid_to,
      bag__northwind__customers._sqlmesh_valid_to,
      bag__northwind__employees._sqlmesh_valid_to,
      bag__northwind__shippers._sqlmesh_valid_to
    ) AS _sqlmesh_valid_to,
    bag__northwind__orders._sqlmesh_source_system,
    bag__northwind__orders._sqlmesh_source_table
  FROM silver.bag__northwind__orders
  LEFT JOIN silver.bag__northwind__customers
    ON bag__northwind__orders._hook__customer = bag__northwind__customers._hook__customer
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__customers._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__customers._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__employees
    ON bag__northwind__orders._hook__employee = bag__northwind__employees._hook__employee
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__employees._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__employees._sqlmesh_valid_from
  LEFT JOIN silver.bag__northwind__shippers
    ON bag__northwind__orders._hook__shipper = bag__northwind__shippers._hook__shipper
    AND bag__northwind__orders._sqlmesh_valid_from < bag__northwind__shippers._sqlmesh_valid_to
    AND bag__northwind__orders._sqlmesh_valid_to > bag__northwind__shippers._sqlmesh_valid_from
), orders_placed AS (
    SELECT
        _hook__order__valid_from,
        1 As measure__orders_placed,
        order_date AS date
    FROM silver.bag__northwind__orders
    WHERE bag__northwind__orders.order_date IS NOT NULL
    ), orders_required AS (
    SELECT
        _hook__order__valid_from,
        1 As measure__orders_required,
        required_date AS date
    FROM silver.bag__northwind__orders
    WHERE bag__northwind__orders.required_date IS NOT NULL
    ), orders_shipped AS (
    SELECT
        _hook__order__valid_from,
        1 As measure__orders_shipped,
        shipped_date AS date
    FROM silver.bag__northwind__orders
    WHERE bag__northwind__orders.shipped_date IS NOT NULL
), order_measurements AS (
    SELECT
        COALESCE(
            orders_required._hook__order__valid_from,
            orders_shipped._hook__order__valid_from,
            orders_shipped._hook__order__valid_from
        ) AS _hook__order__valid_from,
        orders_placed.measure__orders_placed,
        orders_required.measure__orders_required,
        orders_shipped.measure__orders_shipped,
        COALESCE(
            orders_required.date,
            orders_shipped.date,
            orders_shipped.date
        ) AS date
    
    FROM orders_placed
    FULL OUTER JOIN orders_required
        ON orders_placed._hook__order__valid_from = orders_required._hook__order__valid_from
        AND orders_placed.date = orders_required.date
    FULL OUTER JOIN orders_shipped
        ON orders_placed._hook__order__valid_from = orders_shipped._hook__order__valid_from
        AND orders_placed.date = orders_shipped.date
)
SELECT
  'orders' AS stage,
  orders.*,
  order_measurements.measure__orders_placed,
  order_measurements.measure__orders_required,
  order_measurements.measure__orders_shipped,
  order_measurements.date
FROM orders
LEFT JOIN order_measurements
    ON orders._hook__order__valid_from = order_measurements._hook__order__valid_from
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts