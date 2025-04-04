
MODEL (
  name @temp_schema.achilles__temp_dates_1409,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 1409	Number of persons with continuous payer plan in each year
-- Note: using temp table instead of nested query because this gives vastly improved
select distinct YEAR(payer_plan_period_start_date)::INT as obs_year
from
  @src_schema.payer_plan_period
