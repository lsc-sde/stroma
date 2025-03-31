
MODEL (
  name @schema_achilles.achilles_results_dist,
  kind FULL,
  cron '@monthly',
   blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);


select
  analysis_id,
  stratum_1,
  stratum_2,
  stratum_3,
  stratum_4,
  stratum_5,
  count_value,
  min_value,
  max_value,
  avg_value,
  stdev_value,
  median_value,
  p10_value,
  p25_value,
  p75_value,
  p90_value

from
  (
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_0
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_103
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_104
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_105
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_106
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_107
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_203
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_206
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_213
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_403
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_406
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_506
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_511
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_512
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_513
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_514
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_515
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_603
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_606
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_703
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_706
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_715
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_716
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_717
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_803
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_806
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_815
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_903
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_906
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_907
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1003
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1006
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1007
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1303
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1306
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1313
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1406
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1407
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1803
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1806
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1815
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1816
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_1817
    union all
    select
      analysis_id::INT,
      stratum_1::TEXT,
      stratum_2::TEXT,
      stratum_3::TEXT,
      stratum_4::TEXT,
      stratum_5::TEXT,
      count_value::FLOAT ,
      min_value::FLOAT ,
      max_value::FLOAT ,
      avg_value::FLOAT ,
      stdev_value::FLOAT ,
      median_value::FLOAT ,
      p10_value::FLOAT ,
      p25_value::FLOAT ,
      p75_value::FLOAT ,
      p90_value::FLOAT
    from
      @temp_schema.achilles__s_tmpach_dist_2106
  )

where count_value > 5
