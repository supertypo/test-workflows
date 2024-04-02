-- Your SQL goes here
CREATE TABLE IF NOT EXISTS "blocks"
(
    hash                    BYTEA PRIMARY KEY,
    accepted_id_merkle_root BYTEA,
    difficulty              DOUBLE PRECISION,
    is_chain_block          BOOLEAN,
    merge_set_blues_hashes  BYTEA[],
    merge_set_reds_hashes   BYTEA[],
    selected_parent_hash    BYTEA,
    bits                    INTEGER,
    blue_score              BIGINT,
    blue_work               BYTEA,
    daa_score               BIGINT,
    hash_merkle_root        BYTEA,
    nonce                   NUMERIC(32, 0),
    parents                 BYTEA[],
    pruning_point           BYTEA,
    "timestamp"             INTEGER,
    utxo_commitment         BYTEA,
    version                 SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_block_is_chain_block ON blocks (is_chain_block);
CREATE INDEX IF NOT EXISTS idx_blue_score ON blocks (blue_score);
CREATE INDEX IF NOT EXISTS idx_daa_score ON blocks (daa_score);
