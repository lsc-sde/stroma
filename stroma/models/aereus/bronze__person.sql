MODEL (
  name bronze.person,
  kind FULL,
  cron '@monthly',
  grain person_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

-- @UNION('all', bronze_lth.person, bronze_uhmb.person) /* This is the patient table. */

select * from bronze_lth.person;
