AUDIT (
  name event_not_in_future
);

SELECT
  *
FROM @this_model
WHERE
  @column > CURRENT_DATE;

AUDIT (
  name event_not_in_future_non_blocking,
  blocking FALSE
);

SELECT
  *
FROM @this_model
WHERE
  @column > CURRENT_DATE
