// @generated automatically by Diesel CLI.

diesel::table! {
    blocks (hash) {
        hash -> Bytea,
        accepted_id_merkle_root -> Nullable<Bytea>,
        difficulty -> Nullable<Float8>,
        is_chain_block -> Nullable<Bool>,
        merge_set_blues_hashes -> Nullable<Array<Nullable<Bytea>>>,
        merge_set_reds_hashes -> Nullable<Array<Nullable<Bytea>>>,
        selected_parent_hash -> Nullable<Bytea>,
        bits -> Nullable<Int4>,
        blue_score -> Nullable<Int8>,
        blue_work -> Nullable<Bytea>,
        daa_score -> Nullable<Int8>,
        hash_merkle_root -> Nullable<Bytea>,
        nonce -> Nullable<Numeric>,
        parents -> Nullable<Array<Nullable<Bytea>>>,
        pruning_point -> Nullable<Bytea>,
        timestamp -> Nullable<Int4>,
        utxo_commitment -> Nullable<Bytea>,
        version -> Nullable<Int2>,
    }
}
