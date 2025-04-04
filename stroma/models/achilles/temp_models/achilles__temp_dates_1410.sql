
MODEL (
  name @temp_schema.achilles__temp_dates_1410,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 1410	Number of persons with continuous payer plan in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
select distinct
  YEAR(payer_plan_period_start_date) * 100
  + MONTH(payer_plan_period_start_date)::INT as obs_month,
  MAKE_DATE(
    YEAR(payer_plan_period_start_date), MONTH(payer_plan_period_start_date), 1
  )::TIMESTAMP as obs_month_start,
  LAST_DAY(payer_plan_period_start_date)::TIMESTAMP as obs_month_end
from
  @src_schema.payer_plan_period
