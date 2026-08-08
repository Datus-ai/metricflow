-- Compute Metrics via Expressions
SELECT
  metric_time
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_14.ds AS metric_time
    , subq_12.bookings_5_days_ago AS bookings_5_days_ago
  FROM (
    SELECT EXPLODE(
      SEQUENCE(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-12-31 00:00:00', INTERVAL 1 DAY)
    ) AS ds
  ) subq_14
  INNER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(bookings) AS bookings_5_days_ago
    FROM (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time']
      SELECT
        ds AS metric_time
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_10
    GROUP BY
      metric_time
  ) subq_12
  ON
    DATEADD(day, -5, subq_14.ds) = subq_12.metric_time
) subq_15
