MODEL (
  name gold.uss__peripheral__calendar,
  kind FULL,
  cron '0 0 1 1 *'
);

WITH date_spine AS (
  @date_spine('day', '2000-01-01', '2050-01-16')
), calendar AS (
  SELECT
    date_day AS _key__date,
    date_day AS date,
    DATE_PART('year', date_day) AS year,
    CONCAT(DATE_PART('year', date_day), '-Q', DATE_PART('quarter', date_day)) AS year_quarter,
    STRFTIME(date_day, '%Y-%m') AS year_month,
    CONCAT(
      DATE_PART('isoyear', date_day),
      '-W',
      LPAD(DATE_PART('week', date_day)::TEXT, 2, '0')
    ) AS year_week,
    LEFT(year_week, 4)::INT AS year_of_week,
    DATE_PART('quarter', date_day) AS quarter,
    DATE_PART('month', date_day) AS month,
    DATE_PART('week', date_day) AS week,
    DATE_PART('isodow', date_day) AS weekday,
    year_week || '-' || weekday AS iso_week_date
  FROM date_spine
)
SELECT
  *
FROM calendar