/* Code taken from: */ /* https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_drug_era_non_stockpile.sql */
MODEL (
  name silver.drug_era,
  kind FULL,
  cron '@monthly',
  grain drug_era_id,
  dialect 'databricks'
);

@DEF(schema, 'gold');

@DEF(schema_vocab, 'gold');

WITH ctepredrugtarget AS (
  /* Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date */
  SELECT
    d.drug_exposure_id,
    d.person_id,
    c.concept_id AS ingredient_concept_id,
    d.drug_exposure_start_date AS drug_exposure_start_date,
    d.days_supply AS days_supply,
    coalesce(
      nullif(drug_exposure_end_date, NULL) /* -NULLIF returns NULL if both values are the same, otherwise it returns the first parameter */,
      nullif(dateadd(DAY, d.days_supply, drug_exposure_start_date), drug_exposure_start_date) /* -If drug_exposure_end_date != NULL, return drug_exposure_end_date, otherwise go to next case */,
      dateadd(DAY, 1, drug_exposure_start_date) /* -If days_supply != NULL or 0, return drug_exposure_start_date + days_supply, otherwise go to next case */
    ) AS drug_exposure_end_date /* -Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply */
  FROM @schema.drug_exposure AS d
  INNER JOIN @schema_vocab.concept_ancestor AS ca
    ON d.drug_concept_id = ca.descendant_concept_id
  INNER JOIN @schema_vocab.concept AS c
    ON ca.ancestor_concept_id = c.concept_id
  WHERE
    c.vocabulary_id = 'RxNorm' /* -8 selects RxNorm from the vocabulary_id */
    AND c.concept_class_id = 'Ingredient'
    AND d.drug_concept_id <> 0 /* -Our unmapped drug_concept_id's are set to 0, so we don't want different drugs wrapped up in the same era */
    AND coalesce(d.days_supply, 0) >= 0 /* -We have cases where days_supply is negative, and this can set the end_date before the start_date, which we don't want. So we're just looking over those rows. This is a data-quality issue. */
), ctesubexposureenddates AS (
  /* - A preliminary sorting that groups all of the overlapping exposures into one exposure so that we don't double-count non-gap-days( */
  SELECT
    person_id,
    ingredient_concept_id,
    event_date AS end_date
  FROM (
    SELECT
      person_id,
      ingredient_concept_id,
      event_date,
      event_type,
      max(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS start_ordinal,
      row_number() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord /* this pulls the current START down from the prior rows so that the NULLs */ /* from the END DATES will contain a value we can compare with */
    /* this re-numbers the inner UNION so all rows are numbered ordered by the event date */
    FROM (
      /* select the start dates, assigning a row number to each */
      SELECT
        person_id,
        ingredient_concept_id,
        drug_exposure_start_date AS event_date,
        -1 AS event_type,
        row_number() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
      FROM ctepredrugtarget
      UNION ALL
      SELECT
        person_id,
        ingredient_concept_id,
        drug_exposure_end_date,
        1 AS event_type,
        NULL
      FROM ctepredrugtarget
    ) AS rawdata
  ) AS e
  WHERE
    (
      2 * e.start_ordinal
    ) - e.overall_ord = 0
), ctedrugexposureends AS (
  SELECT
    dt.person_id,
    dt.ingredient_concept_id,
    dt.drug_exposure_start_date,
    min(e.end_date) AS drug_sub_exposure_end_date
  FROM ctepredrugtarget AS dt
  INNER JOIN ctesubexposureenddates AS e
    ON dt.person_id = e.person_id
    AND dt.ingredient_concept_id = e.ingredient_concept_id
    AND dt.drug_exposure_start_date <= e.end_date
  GROUP BY
    dt.drug_exposure_id,
    dt.person_id,
    dt.ingredient_concept_id,
    dt.drug_exposure_start_date
), ctesubexposures(row_number, person_id, ingredient_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count) /* ORDER BY person_id, drug_concept_id */ AS (
  SELECT
    row_number() OVER (PARTITION BY person_id, ingredient_concept_id, drug_sub_exposure_end_date ORDER BY person_id),
    person_id,
    ingredient_concept_id,
    min(drug_exposure_start_date) AS drug_sub_exposure_start_date,
    drug_sub_exposure_end_date,
    count(*) AS drug_exposure_count
  FROM ctedrugexposureends
  GROUP BY
    person_id,
    ingredient_concept_id,
    drug_sub_exposure_end_date
), ctefinaltarget(row_number, person_id, ingredient_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count, days_exposed) /* ------------------------------------------------------------------------------------------------------------ */ /* Everything above grouped exposures into sub_exposures if there was overlap between exposures.
 *So there was no persistence window. Now we can add the persistence window to calculate eras.
 */ /* ------------------------------------------------------------------------------------------------------------ */ AS (
  SELECT
    row_number,
    person_id,
    ingredient_concept_id,
    drug_sub_exposure_start_date,
    drug_sub_exposure_end_date,
    drug_exposure_count,
    datediff(DAY, drug_sub_exposure_start_date, drug_sub_exposure_end_date) AS days_exposed
  FROM ctesubexposures
), cteenddates(person_id, ingredient_concept_id, end_date) /* ------------------------------------------------------------------------------------------------------------ */ AS (
  /* the magic */ /* unpad the end date */
  SELECT
    person_id,
    ingredient_concept_id,
    dateadd(DAY, -30, event_date) AS end_date
  FROM (
    SELECT
      person_id,
      ingredient_concept_id,
      event_date,
      event_type,
      max(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS start_ordinal,
      row_number() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord /* this pulls the current START down from the prior rows so that the NULLs */ /* from the END DATES will contain a value we can compare with */
    /* this re-numbers the inner UNION so all rows are numbered ordered by the event date */
    FROM (
      /* select the start dates, assigning a row number to each */
      SELECT
        person_id,
        ingredient_concept_id,
        drug_sub_exposure_start_date AS event_date,
        -1 AS event_type,
        row_number() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_sub_exposure_start_date) AS start_ordinal
      FROM ctefinaltarget
      UNION ALL
      /* pad the end dates by 30 to allow a grace period for overlapping ranges. */
      SELECT
        person_id,
        ingredient_concept_id,
        dateadd(DAY, 30, drug_sub_exposure_end_date) AS event_date,
        1 AS event_type,
        NULL
      FROM ctefinaltarget
    ) AS rawdata
  ) AS e
  WHERE
    (
      2 * e.start_ordinal
    ) - e.overall_ord = 0
), ctedrugeraends AS (
  SELECT
    ft.person_id,
    ft.ingredient_concept_id AS drug_concept_id,
    ft.drug_sub_exposure_start_date,
    min(e.end_date) AS drug_era_end_date,
    drug_exposure_count,
    days_exposed
  FROM ctefinaltarget AS ft
  INNER JOIN cteenddates AS e
    ON ft.person_id = e.person_id
    AND ft.ingredient_concept_id = e.ingredient_concept_id
    AND ft.drug_sub_exposure_start_date <= e.end_date
  GROUP BY
    ft.person_id,
    ft.ingredient_concept_id,
    ft.drug_sub_exposure_start_date,
    drug_exposure_count,
    days_exposed
)
SELECT
  row_number() OVER (ORDER BY person_id) AS drug_era_id,
  person_id,
  drug_concept_id,
  min(drug_sub_exposure_start_date) AS drug_era_start_date,
  drug_era_end_date,
  sum(drug_exposure_count) AS drug_exposure_count,
  datediff(DAY, min(drug_sub_exposure_start_date), drug_era_end_date) - sum(days_exposed) AS gap_days
FROM ctedrugeraends
GROUP BY
  person_id,
  drug_concept_id,
  drug_era_end_date
