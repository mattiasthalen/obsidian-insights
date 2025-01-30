MODEL (
  name silver.int__measures__orders,
  kind VIEW
);

WITH orders_placed AS (
  SELECT
    _hook__order__valid_from,
    1 AS measure__orders_placed,
    order_date AS _key__date
  FROM silver.bag__northwind__orders
  WHERE
    NOT bag__northwind__orders.order_date IS NULL
), orders_required AS (
  SELECT
    _hook__order__valid_from,
    1 AS measure__orders_required,
    required_date AS _key__date
  FROM silver.bag__northwind__orders
  WHERE
    NOT bag__northwind__orders.required_date IS NULL
), orders_shipped AS (
  SELECT
    _hook__order__valid_from,
    1 AS measure__orders_shipped,
    shipped_date AS _key__date
  FROM silver.bag__northwind__orders
  WHERE
    NOT bag__northwind__orders.shipped_date IS NULL
)
SELECT
  COALESCE(
    orders_required._hook__order__valid_from,
    orders_shipped._hook__order__valid_from,
    orders_shipped._hook__order__valid_from
  ) AS _hook__order__valid_from,
  orders_placed.measure__orders_placed,
  orders_required.measure__orders_required,
  orders_shipped.measure__orders_shipped,
  COALESCE(orders_required._key__date, orders_shipped._key__date, orders_shipped._key__date) AS _key__date
FROM orders_placed
FULL OUTER JOIN orders_required
  ON orders_placed._hook__order__valid_from = orders_required._hook__order__valid_from
  AND orders_placed._key__date = orders_required._key__date
FULL OUTER JOIN orders_shipped
  ON orders_placed._hook__order__valid_from = orders_shipped._hook__order__valid_from
  AND orders_placed._key__date = orders_shipped._key__date