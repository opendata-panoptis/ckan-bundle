/*
Find and soft-delete broken package relationships.

Problem signature:
- active package_relationship row
- object_package_id is NULL
- comment is NULL or empty

These rows cannot point to either a CKAN dataset or an external URI.
Run the SELECT queries first, inspect affected datasets, then run the UPDATE
inside a transaction.
*/

-- 1. Summary: how many broken active relationships exist.
SELECT
    count(*) AS broken_active_relationship_count
FROM package_relationship pr
WHERE pr.state = 'active'
  AND pr.object_package_id IS NULL
  AND nullif(btrim(pr.comment), '') IS NULL;

-- 2. Details: relationships and affected subject datasets.
SELECT
    pr.id AS relationship_id,
    pr.type AS relationship_type,
    pr.state AS relationship_state,
    pr.subject_package_id,
    subject.name AS subject_name,
    subject.title AS subject_title,
    subject.state AS subject_state,
    subject.type AS subject_type,
    pr.object_package_id,
    pr.comment
FROM package_relationship pr
LEFT JOIN package subject
    ON subject.id = pr.subject_package_id
WHERE pr.state = 'active'
  AND pr.object_package_id IS NULL
  AND nullif(btrim(pr.comment), '') IS NULL
ORDER BY subject.name, pr.type, pr.id;

-- 3. Dataset URLs to open and verify the relationships page before cleanup.
-- Replace https://example.com with the portal base URL.
SELECT
    subject.name AS subject_name,
    'https://example.com/dataset/relationships/' || subject.name AS relationships_url,
    pr.id AS relationship_id,
    pr.type AS relationship_type
FROM package_relationship pr
JOIN package subject
    ON subject.id = pr.subject_package_id
WHERE pr.state = 'active'
  AND pr.object_package_id IS NULL
  AND nullif(btrim(pr.comment), '') IS NULL
ORDER BY subject.name, pr.type, pr.id;

-- 4. Cleanup: soft-delete broken rows.
-- Keep this in a transaction. Inspect the RETURNING rows before COMMIT.
BEGIN;

UPDATE package_relationship pr
SET state = 'deleted'
WHERE pr.state = 'active'
  AND pr.object_package_id IS NULL
  AND nullif(btrim(pr.comment), '') IS NULL
RETURNING
    pr.id AS relationship_id,
    pr.subject_package_id,
    pr.type,
    pr.state,
    pr.object_package_id,
    pr.comment;

-- Use ROLLBACK while testing/reviewing.
-- Change to COMMIT only after the RETURNING rows match what you expect.
ROLLBACK;
-- COMMIT;
