
MODEL (
  name @temp_schema.achilles__s_tmpach_2000,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 2000	patients with at least 1 Dx and 1 Rx
select
  2000 as analysis_id,
  CAST(NULL as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  d.cnt::FLOAT as count_value
from (
  select COUNT(*) as cnt
  from (
    select distinct person_id
    from (
      select co.person_id
      from
        @src_schema.condition_occurrence as co
      inner join
        @src_schema.observation_period as op
        on
          co.person_id = op.person_id
          and
          co.condition_start_date >= op.observation_period_start_date
          and
          co.condition_start_date <= op.observation_period_end_date
    ) as a
    intersect
    select distinct person_id
    from (
      select de.person_id
      from
        @src_schema.drug_exposure as de
      inner join
        @src_schema.observation_period as op
        on
          de.person_id = op.person_id
          and
          de.drug_exposure_start_date >= op.observation_period_start_date
          and
          de.drug_exposure_start_date <= op.observation_period_end_date
    ) as b
  ) as c
) as d
