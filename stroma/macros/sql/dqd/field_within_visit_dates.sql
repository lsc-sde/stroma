/*********
FIELD LEVEL check:
WITHIN_VISIT_DATES - find events that occur one week before the corresponding visit_start_date or one week after the corresponding visit_end_date

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
{@cohort & '@runForCohort' == 'Yes'}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
**********/


SELECT num_violated_rows,
    CASE
        WHEN denominator.num_rows = 0 THEN 0
        ELSE 1.0*num_violated_rows/denominator.num_rows
    END AS pct_violated_rows,
    denominator.num_rows AS num_denominator_rows
FROM (
    SELECT
        COUNT_BIG(violated_rows.violating_field) AS num_violated_rows
    FROM (
        /*violatedRowsBegin*/
        SELECT
            '@cdmTableName.@cdmFieldName' AS violating_field,
            cdmTable.*
        FROM @cdmDatabaseSchema.@cdmTableName cdmTable
        {@cohort & '@runForCohort' == 'Yes'}?{
            JOIN @cohortDatabaseSchema.@cohortTableName c
                ON cdmTable.person_id = c.subject_id
                AND c.cohort_definition_id = @cohortDefinitionId
        }
        JOIN @cdmDatabaseSchema.visit_occurrence vo
            ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
        WHERE cdmTable.@cdmFieldName < DATEADD(DAY, -7, vo.visit_start_date)
            OR cdmTable.@cdmFieldName > DATEADD(DAY, 7, vo.visit_end_date)
        /*violatedRowsEnd*/
    ) violated_rows
) violated_row_count,
(
    SELECT
        COUNT_BIG(*) AS num_rows
    FROM @cdmDatabaseSchema.@cdmTableName cdmTable
    {@cohort & '@runForCohort' == 'Yes'}?{
        JOIN @cohortDatabaseSchema.@cohortTableName c
            ON cdmTable.person_id = c.subject_id
            AND c.cohort_definition_id = @cohortDefinitionId
    }
    JOIN @cdmDatabaseSchema.visit_occurrence vo
        ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
) denominator
;
