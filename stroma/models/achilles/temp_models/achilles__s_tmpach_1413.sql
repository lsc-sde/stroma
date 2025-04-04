
MODEL (
  name @temp_schema.achilles__s_tmpach_1413,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 1413	Number of persons by number of payer plan periods
select
  1413 as analysis_id,
  cast(ppp1.num_periods as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(distinct p1.PERSON_ID)::FLOAT as count_value
from
  @src_schema.person as p1
inner join
  (select
    person_id,
    count(payer_plan_period_start_date) as num_periods
  from @src_schema.payer_plan_period group by PERSON_ID) as ppp1
  on p1.person_id = ppp1.person_id
group by ppp1.num_periods
