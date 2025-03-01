--------------------------------------------------------------
-- v7: Fixes
--------------------------------------------------------------

-- Remove NOT NULL on transactions_acceptances.transaction_id (missing in v6_to_v7.sql)
ALTER TABLE transactions_acceptances ALTER COLUMN transaction_id DROP NOT NULL;

-- Update schema_version
UPDATE vars SET value = '9' WHERE key = 'schema_version';
