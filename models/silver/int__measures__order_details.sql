MODEL (
  name silver.int__measures__order_details,
  kind VIEW
);

WITH order_date AS (
  SELECT
    bag__northwind__order_details._hook__order_detail__valid_from,
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__orders.order_date,
    1 AS measure__ordered_lines,
    bag__northwind__order_details.quantity AS measure__ordered_quantity,
    bag__northwind__order_details.line_value AS measure__ordered_value,
    bag__northwind__order_details.discounted_line_value AS measure__ordered_discounted_value
  FROM silver.bag__northwind__order_details
  INNER JOIN silver.bag__northwind__orders
    ON bag__northwind__order_details._hook__order = bag__northwind__orders._hook__order
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__orders._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__orders._sqlmesh_valid_from
  WHERE
    NOT bag__northwind__orders.order_date IS NULL
), required_date AS (
  SELECT
    bag__northwind__order_details._hook__order_detail__valid_from,
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__orders.required_date,
    1 AS measure__due_lines,
    bag__northwind__order_details.quantity AS measure__due_quantity,
    bag__northwind__order_details.line_value AS measure__due_value,
    bag__northwind__order_details.discounted_line_value AS measure__due_discounted_value
  FROM silver.bag__northwind__order_details
  INNER JOIN silver.bag__northwind__orders
    ON bag__northwind__order_details._hook__order = bag__northwind__orders._hook__order
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__orders._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__orders._sqlmesh_valid_from
  WHERE
    NOT bag__northwind__orders.required_date IS NULL
), shipped_date AS (
  SELECT
    bag__northwind__order_details._hook__order_detail__valid_from,
    bag__northwind__orders._hook__order__valid_from,
    bag__northwind__orders.shipped_date,
    1 AS measure__shipped_lines,
    bag__northwind__order_details.quantity AS measure__shipped_quantity,
    bag__northwind__order_details.line_value AS measure__shipped_value,
    bag__northwind__order_details.discounted_line_value AS measure__shipped_discounted_value
  FROM silver.bag__northwind__order_details
  INNER JOIN silver.bag__northwind__orders
    ON bag__northwind__order_details._hook__order = bag__northwind__orders._hook__order
    AND bag__northwind__order_details._sqlmesh_valid_from < bag__northwind__orders._sqlmesh_valid_to
    AND bag__northwind__order_details._sqlmesh_valid_to > bag__northwind__orders._sqlmesh_valid_from
  WHERE
    NOT bag__northwind__orders.shipped_date IS NULL
)
SELECT
  COALESCE(
    order_date._hook__order_detail__valid_from,
    required_date._hook__order_detail__valid_from,
    shipped_date._hook__order_detail__valid_from
  ) AS _hook__order_detail__valid_from,
  COALESCE(
    order_date._hook__order__valid_from,
    required_date._hook__order__valid_from,
    shipped_date._hook__order__valid_from
  ) AS _hook__order__valid_from,
  COALESCE(order_date.order_date, required_date.required_date, shipped_date.shipped_date) AS _key__date,
  order_date.measure__ordered_lines,
  order_date.measure__ordered_quantity,
  order_date.measure__ordered_value,
  order_date.measure__ordered_discounted_value,
  required_date.measure__due_lines,
  required_date.measure__due_quantity,
  required_date.measure__due_value,
  required_date.measure__due_discounted_value,
  shipped_date.measure__shipped_lines,
  shipped_date.measure__shipped_quantity,
  shipped_date.measure__shipped_value,
  shipped_date.measure__shipped_discounted_value
FROM order_date
FULL OUTER JOIN required_date
  ON order_date._hook__order_detail__valid_from = required_date._hook__order_detail__valid_from
  AND order_date._hook__order__valid_from = required_date._hook__order__valid_from
  AND order_date.order_date = required_date.required_date
FULL OUTER JOIN shipped_date
  ON order_date._hook__order_detail__valid_from = shipped_date._hook__order_detail__valid_from
  AND order_date._hook__order__valid_from = shipped_date._hook__order__valid_from
  AND order_date.order_date = shipped_date.shipped_date