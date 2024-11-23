MODEL (
  name silver.condition_era,
  kind FULL,
  cron '@monthly',
  grain condition_era_id
);

@DEF(schema, 'silver');

WITH cteconditiontarget /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to our unmapped condition_concept_id's,
	 * and since we don't want different conditions put in the same era, we put in the filter below.
 	 */ /* -WHERE condition_concept_id != 0 */ AS (
  SELECT
    co.condition_occurrence_id,
    co.person_id,
    co.condition_concept_id,
    co.condition_start_date,
    coalesce(nullif(co.condition_end_date, NULL), condition_start_date + INTERVAL 1 DAY) AS condition_end_date
  FROM @schema.condition_occurrence AS co
), cteenddates AS (
  SELECT
    person_id,
    condition_concept_id,
    event_date + INTERVAL (-30) DAY AS end_date /* unpad the end date */
  FROM (
    SELECT
      person_id,
      condition_concept_id,
      event_date,
      event_type,
      max(start_ordinal) OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS start_ordinal, /* this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with */
      row_number() OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type) AS overall_ord /* this re-numbers the inner UNION so all rows are numbered ordered by the event date */
    FROM (
      /* select the start dates, assigning a row number to each */
      SELECT
        person_id,
        condition_concept_id,
        condition_start_date AS event_date,
        -1 AS event_type,
        row_number() OVER (PARTITION BY person_id, condition_concept_id ORDER BY condition_start_date) AS start_ordinal
      FROM cteconditiontarget
      UNION ALL
      /* pad the end dates by 30 to allow a grace period for overlapping ranges. */
      SELECT
        person_id,
        condition_concept_id,
        condition_end_date + INTERVAL 30 DAY AS event_date,
        1 AS event_type,
        NULL
      FROM cteconditiontarget
    ) AS rawdata
  ) AS e
  WHERE
    (
      2 * e.start_ordinal
    ) - e.overall_ord = 0
), cteconditionends AS (
  SELECT
    c.person_id,
    c.condition_concept_id,
    c.condition_start_date,
    min(e.end_date) AS era_end_date
  FROM cteconditiontarget AS c
  INNER JOIN cteenddates AS e
    ON c.person_id = e.person_id
    AND c.condition_concept_id = e.condition_concept_id
    AND c.condition_start_date <= e.end_date
  GROUP BY
    c.condition_occurrence_id,
    c.person_id,
    c.condition_concept_id,
    c.condition_start_date
)
SELECT
  row_number() OVER (ORDER BY person_id) AS condition_era_id,
  person_id,
  condition_concept_id,
  min(condition_start_date) AS condition_era_start_date,
  era_end_date AS condition_era_end_date,
  count(*) AS condition_occurrence_count
FROM cteconditionends
GROUP BY
  person_id,
  condition_concept_id,
  era_end_date
