MODEL (
  name bronze.visit_occurrence,
  kind FULL,
  cron '@monthly',
  grain visit_occurrence_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

/* @UNION('all', bronze_lth.visit_occurrence, bronze_uhmb.visit_occurrence) */
SELECT
  *
FROM bronze_lth.visit_occurrence
