MODEL (
  name silver.int__measures__orders,
  kind VIEW
);

WITH order_date AS (
  SELECT
    _hook__order__valid_from,
    order_date,
    1 AS measure__order_placed
  FROM silver.bag__northwind__orders
  WHERE
    NOT bag__northwind__orders.order_date IS NULL
), required_date AS (
  SELECT
    _hook__order__valid_from,
    required_date,
    1 AS measure__order_due,
    CASE
      WHEN 1 = 1
      AND NOT shipped_date IS NULL
      AND required_date - INTERVAL '5' DAYS <= shipped_date
      AND shipped_date <= required_date - INTERVAL '3' DAYS
      THEN 1
    END AS measure__order_shipped_on_time
  FROM silver.bag__northwind__orders
  WHERE
    NOT bag__northwind__orders.required_date IS NULL
), shipped_date AS (
  SELECT
    _hook__order__valid_from,
    shipped_date,
    1 AS measure__order_shipped,
    shipped_date - order_date AS measure__order_processing_time
  FROM silver.bag__northwind__orders
  WHERE
    NOT bag__northwind__orders.shipped_date IS NULL
)
SELECT
  COALESCE(
    order_date._hook__order__valid_from,
    required_date._hook__order__valid_from,
    shipped_date._hook__order__valid_from
  ) AS _hook__order__valid_from,
  COALESCE(order_date.order_date, required_date.required_date, shipped_date.shipped_date)::TEXT::BLOB AS _key__date,
  order_date.measure__order_placed,
  required_date.measure__order_due,
  required_date.measure__order_shipped_on_time,
  shipped_date.measure__order_shipped,
  shipped_date.measure__order_processing_time
FROM order_date
FULL OUTER JOIN required_date
  ON order_date._hook__order__valid_from = required_date._hook__order__valid_from
  AND order_date.order_date = required_date.required_date
FULL OUTER JOIN shipped_date
  ON order_date._hook__order__valid_from = shipped_date._hook__order__valid_from
  AND order_date.order_date = shipped_date.shipped_date