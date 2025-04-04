
MODEL (
  name @temp_schema.achilles__statsView_105,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

select
  count_value,
  count(*) as total,
  row_number() over (order by count_value) as rn
from @temp_schema.achilles__tempObs_105
group by count_value
