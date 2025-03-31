
MODEL (
  name @temp_schema.achilles__s_tmpach_1411,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 1411	Number of persons by payer plan period start month
select
  1411 as analysis_id,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  make_date(
    year(payer_plan_period_start_date), month(payer_plan_period_start_date), 1
  ) as stratum_1,
  count(distinct p1.PERSON_ID)::FLOAT as count_value
from
  @src_schema.person as p1
inner join @src_schema.payer_plan_period as ppp1
  on p1.person_id = ppp1.person_id
group by
  make_date(
    year(payer_plan_period_start_date), month(payer_plan_period_start_date), 1
  )
