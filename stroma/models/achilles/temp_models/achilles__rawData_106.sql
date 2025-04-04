
MODEL (
  name @temp_schema.achilles__rawData_106,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 106	Length of observation (days) of first observation period by gender
select
  p.gender_concept_id,
  op.count_value
from
  (
    select
      person_id,
      datediff( 'day',  op.observation_period_end_date, op.observation_period_start_date
      )::FLOAT as count_value,
      ROW_NUMBER() over (
        partition by op.person_id order by op.observation_period_start_date asc
      ) as rn
    from @src_schema.observation_period as op
  ) as op
inner join @src_schema.person as p on op.person_id = p.person_id
where op.rn = 1
