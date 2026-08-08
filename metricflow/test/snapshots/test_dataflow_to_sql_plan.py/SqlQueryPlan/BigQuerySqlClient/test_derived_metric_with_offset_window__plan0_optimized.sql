-- Compute Metrics via Expressions
SELECT
  metric_time
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_18.metric_time, subq_26.metric_time) AS metric_time
    , subq_18.bookings AS bookings
    , subq_26.bookings_2_weeks_ago AS bookings_2_weeks_ago
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(bookings) AS bookings
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
    ) subq_16
    GROUP BY
      metric_time
  ) subq_18
  INNER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_25.ds AS metric_time
      , subq_23.bookings_2_weeks_ago AS bookings_2_weeks_ago
    FROM (
      SELECT CAST(ds AS DATETIME) AS ds
      FROM UNNEST(
        GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('2020-01-01 00:00:00'), TIMESTAMP('2020-12-31 00:00:00'), INTERVAL 1 DAY)
      ) AS ds
    ) subq_25
    INNER JOIN (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time
        , SUM(bookings) AS bookings_2_weeks_ago
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
      ) subq_21
      GROUP BY
        metric_time
    ) subq_23
    ON
      DATE_SUB(CAST(subq_25.ds AS DATETIME), INTERVAL 14 day) = subq_23.metric_time
  ) subq_26
  ON
    (
      subq_18.metric_time = subq_26.metric_time
    ) OR (
      (subq_18.metric_time IS NULL) AND (subq_26.metric_time IS NULL)
    )
) subq_27
