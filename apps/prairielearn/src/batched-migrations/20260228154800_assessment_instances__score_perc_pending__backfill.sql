-- BLOCK select_bounds
SELECT
  MAX(id)
FROM
  assessment_instances;

-- BLOCK update_assessment_instances_score_perc_pending
WITH
  target_assessment_instances AS (
    SELECT
      ai.id,
      ai.max_points
    FROM
      assessment_instances AS ai
    WHERE
      ai.id >= $start
      AND ai.id <= $end
  ),
  all_questions AS (
    SELECT
      tai.id AS assessment_instance_id,
      iq.id AS iq_id,
      z.id AS zone_id,
      aq.max_points,
      aq.max_manual_points,
      iq.requires_manual_grading,
      row_number() OVER (
        PARTITION BY
          tai.id,
          z.id
        ORDER BY
          aq.max_points DESC
      ) AS max_points_rank,
      z.best_questions,
      z.max_points AS zone_max_points
    FROM
      target_assessment_instances AS tai
      JOIN instance_questions AS iq ON (iq.assessment_instance_id = tai.id)
      JOIN assessment_questions AS aq ON (aq.id = iq.assessment_question_id)
      JOIN alternative_groups AS ag ON (ag.id = aq.alternative_group_id)
      JOIN zones AS z ON (z.id = ag.zone_id)
      JOIN assessments AS a ON (a.id = aq.assessment_id)
    WHERE
      -- drop deleted questions unless assessment type is Exam
      (
        aq.deleted_at IS NULL
        OR a.type = 'Exam'
      )
  ),
  max_points_questions AS (
    SELECT
      *
    FROM
      all_questions AS allq
    WHERE
      (
        (allq.max_points_rank <= allq.best_questions)
        OR (allq.best_questions IS NULL)
      )
  ),
  pending_by_zone AS (
    SELECT
      mpq.assessment_instance_id,
      mpq.zone_id,
      CASE
        WHEN mpq.zone_max_points IS NULL THEN sum(
          CASE
            WHEN COALESCE(mpq.max_manual_points, 0) > 0
            AND mpq.requires_manual_grading THEN COALESCE(mpq.max_manual_points, 0)
            ELSE 0
          END
        )
        ELSE LEAST(
          sum(
            CASE
              WHEN COALESCE(mpq.max_manual_points, 0) > 0
              AND mpq.requires_manual_grading THEN COALESCE(mpq.max_manual_points, 0)
              ELSE 0
            END
          ),
          mpq.zone_max_points
        )
      END AS pending_points
    FROM
      max_points_questions AS mpq
    GROUP BY
      mpq.assessment_instance_id,
      mpq.zone_id,
      mpq.zone_max_points
  ),
  pending_total AS (
    SELECT
      assessment_instance_id,
      COALESCE(sum(pending_points), 0) AS pending_points
    FROM
      pending_by_zone
    GROUP BY
      assessment_instance_id
  ),
  new_pending AS (
    SELECT
      tai.id,
      CASE
        WHEN tai.max_points IS NULL
        OR tai.max_points <= 0 THEN 0
        ELSE LEAST(
          100,
          GREATEST(
            0,
            (COALESCE(pt.pending_points, 0) * 100) / tai.max_points
          )
        )
      END AS score_perc_pending
    FROM
      target_assessment_instances AS tai
      LEFT JOIN pending_total AS pt ON (pt.assessment_instance_id = tai.id)
  )
UPDATE assessment_instances AS ai
SET
  score_perc_pending = np.score_perc_pending,
  modified_at = now()
FROM
  new_pending AS np
WHERE
  ai.id = np.id
  AND ai.score_perc_pending IS DISTINCT FROM np.score_perc_pending;
