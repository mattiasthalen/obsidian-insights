MODEL (
  name gold.report__perfomance,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _sqlmesh_loaded_at
  ),
  grain (
    date
  )
);

SELECT
  date,
  COALESCE(SUM(measure__order_placed), 0) AS orders_placed,
  COALESCE(SUM(measure__order_shipped), 0) AS orders_shipped,
  COALESCE(SUM(measure__order_due), 0) AS orders_due,
  COALESCE(SUM(measure__order_shipped_on_time), 0) AS orders_shipped_on_time,
  COALESCE(SUM(measure__order_processing_time), 0) AS total_order_processing_time,
  orders_shipped / orders_placed AS order_fill_rate,
  orders_shipped_on_time / orders_due AS on_time_delivery,
  total_order_processing_time / orders_shipped AS average_order_processing_time,
  _bridge._sqlmesh_loaded_at
FROM gold._bridge
FULL OUTER JOIN gold.calendar
  USING (_key__date)
WHERE
  _sqlmesh_loaded_at BETWEEN @start_ts AND @end_ts
GROUP BY ALL
ORDER BY
  date DESC