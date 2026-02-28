import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import * as sqldb from '@prairielearn/postgres';

import * as helperDb from '../tests/helperDb.js';

import migration from './20260228154800_assessment_instances__score_perc_pending__backfill.js';

const MIGRATION_NAME = '20260228154800_assessment_instances__score_perc_pending__backfill';

describe('assessment_instances score_perc_pending backfill migration', { timeout: 60_000 }, () => {
  it('backfills pending score and is idempotent', async () => {
    await helperDb.testMigration({
      name: MIGRATION_NAME,
      beforeMigration: async () => {
        const courseId = await sqldb.queryRow(
          `INSERT INTO courses (path, short_name, title, display_timezone, institution_id)
           VALUES ('score-pending-backfill', 'SPB', 'Score pending backfill', 'America/Chicago', 1)
           RETURNING id`,
          {},
          z.bigint({ coerce: true }),
        );

        const courseInstanceId = await sqldb.queryRow(
          `INSERT INTO course_instances (course_id, short_name, long_name, display_timezone, enrollment_code)
           VALUES ($course_id, 'SPB CI', 'Score Pending Backfill CI', 'America/Chicago', 'spb-code')
           RETURNING id`,
          { course_id: courseId },
          z.bigint({ coerce: true }),
        );

        const assessmentId = await sqldb.queryRow(
          `INSERT INTO assessments (course_instance_id, title)
           VALUES ($course_instance_id, 'Backfill test assessment')
           RETURNING id`,
          { course_instance_id: courseInstanceId },
          z.bigint({ coerce: true }),
        );

        const zoneId = await sqldb.queryRow(
          `INSERT INTO zones (assessment_id, number, title)
           VALUES ($assessment_id, 1, 'Zone 1')
           RETURNING id`,
          { assessment_id: assessmentId },
          z.bigint({ coerce: true }),
        );

        const alternativeGroupId = await sqldb.queryRow(
          `INSERT INTO alternative_groups (assessment_id, zone_id, number)
           VALUES ($assessment_id, $zone_id, 1)
           RETURNING id`,
          { assessment_id: assessmentId, zone_id: zoneId },
          z.bigint({ coerce: true }),
        );

        const questionId = await sqldb.queryRow(
          `INSERT INTO questions (course_id, qid, title)
           VALUES ($course_id, 'spb-q1', 'Backfill question')
           RETURNING id`,
          { course_id: courseId },
          z.bigint({ coerce: true }),
        );

        const assessmentQuestionId = await sqldb.queryRow(
          `INSERT INTO assessment_questions (
              assessment_id,
              question_id,
              alternative_group_id,
              allow_real_time_grading,
              max_points,
              max_manual_points,
              max_auto_points
            )
           VALUES ($assessment_id, $question_id, $alternative_group_id, TRUE, 50, 20, 30)
           RETURNING id`,
          {
            assessment_id: assessmentId,
            question_id: questionId,
            alternative_group_id: alternativeGroupId,
          },
          z.bigint({ coerce: true }),
        );

        const userId = await sqldb.queryRow(
          `INSERT INTO users (uid, name, institution_id)
           VALUES ('spb-user', 'Score Pending Backfill User', 1)
           RETURNING id`,
          {},
          z.bigint({ coerce: true }),
        );

        const assessmentInstanceId = await sqldb.queryRow(
          `INSERT INTO assessment_instances (assessment_id, user_id, number, max_points, score_perc_pending)
           VALUES ($assessment_id, $user_id, 1, 50, 0)
           RETURNING id`,
          { assessment_id: assessmentId, user_id: userId },
          z.bigint({ coerce: true }),
        );

        const zeroMaxAssessmentInstanceId = await sqldb.queryRow(
          `INSERT INTO assessment_instances (assessment_id, user_id, number, max_points, score_perc_pending)
           VALUES ($assessment_id, $user_id, 2, 0, 0)
           RETURNING id`,
          { assessment_id: assessmentId, user_id: userId },
          z.bigint({ coerce: true }),
        );

        await sqldb.execute(
          `INSERT INTO instance_questions (
              assessment_instance_id,
              assessment_question_id,
              requires_manual_grading
            )
           VALUES
             ($assessment_instance_id, $assessment_question_id, TRUE),
             ($zero_max_assessment_instance_id, $assessment_question_id, TRUE)`,
          {
            assessment_instance_id: assessmentInstanceId,
            zero_max_assessment_instance_id: zeroMaxAssessmentInstanceId,
            assessment_question_id: assessmentQuestionId,
          },
        );

        return { assessmentInstanceId, zeroMaxAssessmentInstanceId };
      },
      afterMigration: async ({ assessmentInstanceId, zeroMaxAssessmentInstanceId }) => {
        let start = assessmentInstanceId;
        let end = zeroMaxAssessmentInstanceId;
        if (start > end) {
          const tmp = start;
          start = end;
          end = tmp;
        }

        await migration.execute(start, end);

        const firstAssessmentInstancePending = await sqldb.queryRow(
          'SELECT score_perc_pending FROM assessment_instances WHERE id = $id',
          { id: assessmentInstanceId },
          z.number(),
        );
        const zeroMaxAssessmentInstancePending = await sqldb.queryRow(
          'SELECT score_perc_pending FROM assessment_instances WHERE id = $id',
          { id: zeroMaxAssessmentInstanceId },
          z.number(),
        );

        expect(firstAssessmentInstancePending).toBeCloseTo(40, 4);
        expect(zeroMaxAssessmentInstancePending).toBe(0);

        await migration.execute(start, end);

        const secondAssessmentInstancePending = await sqldb.queryRow(
          'SELECT score_perc_pending FROM assessment_instances WHERE id = $id',
          { id: assessmentInstanceId },
          z.number(),
        );
        const secondZeroMaxAssessmentInstancePending = await sqldb.queryRow(
          'SELECT score_perc_pending FROM assessment_instances WHERE id = $id',
          { id: zeroMaxAssessmentInstanceId },
          z.number(),
        );

        expect(secondAssessmentInstancePending).toBeCloseTo(40, 4);
        expect(secondZeroMaxAssessmentInstancePending).toBe(0);
      },
    });
  });
});
