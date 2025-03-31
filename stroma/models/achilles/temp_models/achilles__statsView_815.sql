
MODEL (
  name @temp_schema.achilles__statsView_815,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

select
  o.subject_id as stratum1_id,
  o.unit_concept_id as stratum2_id,
  o.count_value,
  count(*) as total,
  row_number() over (
    partition by o.subject_id, o.unit_concept_id order by o.count_value
  ) as rn
from (
  select
    o.observation_concept_id as subject_id,
    o.unit_concept_id,
    o.value_as_number::FLOAT as count_value
  from
    @src_schema.observation as o
  inner join
    @src_schema.observation_period as op
    on
      o.person_id = op.person_id
      and
      o.observation_date >= op.observation_period_start_date
      and
      o.observation_date <= op.observation_period_end_date
  where
    o.unit_concept_id is not NULL
    and
    o.value_as_number is not NULL
) as o
group by
  o.subject_id,
  o.unit_concept_id,
  o.count_value
