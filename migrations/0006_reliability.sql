-- M1-M6 reliability fixes: pending decisions do not have a decider yet.
BEGIN;

ALTER TABLE decisions ALTER COLUMN decider_id DROP NOT NULL;

COMMIT;
