
MODEL (
  name @temp_schema.achilles__s_tmpach_226,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 226	Number of records by domain by visit concept id
select
  226 as analysis_id,
  cast(v.visit_concept_id as VARCHAR(255)) as stratum_1,
  v.cdm_table as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  v.record_count::FLOAT as count_value
from (
  select
    'drug_exposure' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from @src_schema.drug_exposure as t
  left join
    @src_schema.visit_occurrence as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'condition_occurrence' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from @src_schema.condition_occurrence as t
  left join
    @src_schema.visit_occurrence as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'device_exposure' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from @src_schema.device_exposure as t
  left join
    @src_schema.visit_occurrence as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'procedure_occurrence' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from @src_schema.procedure_occurrence as t
  left join
    @src_schema.visit_occurrence as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'measurement' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from @src_schema.measurement as t
  left join
    @src_schema.visit_occurrence as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
  union
  select
    'observation' as cdm_table,
    coalesce(visit_concept_id, 0) as visit_concept_id,
    count(*) as record_count
  from @src_schema.observation as t
  left join
    @src_schema.visit_occurrence as v
    on t.visit_occurrence_id = v.visit_occurrence_id
  group by visit_concept_id
) as v
