use crate::settings::Settings;
use crate::vars::save_checkpoint;
use crossbeam_queue::ArrayQueue;
use log::{debug, error, info, warn};
use simply_kaspa_cli::cli_args::CliDisable;
use simply_kaspa_database::client::KaspaDbClient;
use simply_kaspa_database::models::types::hash::Hash as SqlHash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointOrigin {
    Blocks,
    Transactions,
    Vcp,
}

pub async fn process_checkpoints(
    settings: Settings,
    run: Arc<AtomicBool>,
    checkpoint_queue: Arc<ArrayQueue<(CheckpointOrigin, SqlHash)>>,
    database: KaspaDbClient,
) {
    let disable_virtual_chain_processing = settings.cli_args.is_disabled(CliDisable::VirtualChainProcessing);
    let disable_transaction_processing = settings.cli_args.is_disabled(CliDisable::TransactionProcessing);

    const CHECKPOINT_SAVE_INTERVAL: u64 = 60;
    const CHECKPOINT_WARN_INTERVAL: u64 = 60;
    const CHECKPOINT_FAILED_TIMEOUT: u64 = 600;
    let mut checkpoint_last_saved = Instant::now();
    let mut checkpoint_last_warned = Instant::now();
    let mut checkpoint_candidate = None;

    let mut blocks_processed: HashSet<SqlHash> = HashSet::new();
    let mut txs_processed: HashSet<SqlHash> = HashSet::new();

    let mut cp_ok_blocks: bool = false;
    let mut cp_ok_txs: bool = false;

    while run.load(Ordering::Relaxed) {
        if let Some((origin, block_hash)) = checkpoint_queue.pop() {
            match origin {
                CheckpointOrigin::Blocks => {
                    if disable_virtual_chain_processing {
                        if checkpoint_candidate.is_none()
                            && Instant::now().duration_since(checkpoint_last_saved).as_secs() > CHECKPOINT_SAVE_INTERVAL
                        {
                            debug!("Selected block_checkpoint candidate {}", hex::encode(block_hash.as_bytes()));
                            checkpoint_candidate = Some(block_hash.clone());
                            checkpoint_last_warned = Instant::now();
                            cp_ok_blocks = true;
                        }
                    } else {
                        blocks_processed.insert(block_hash);
                    }
                }
                CheckpointOrigin::Transactions => {
                    txs_processed.insert(block_hash);
                }
                CheckpointOrigin::Vcp => {
                    if checkpoint_candidate.is_none()
                        && Instant::now().duration_since(checkpoint_last_saved).as_secs() > CHECKPOINT_SAVE_INTERVAL
                    {
                        debug!("Selected block_checkpoint candidate {}", hex::encode(block_hash.as_bytes()));
                        checkpoint_candidate = Some(block_hash.clone());
                        checkpoint_last_warned = Instant::now();
                    }
                }
            }
            if let Some(checkpoint) = checkpoint_candidate.clone() {
                if !cp_ok_blocks && blocks_processed.contains(&checkpoint) {
                    cp_ok_blocks = true;
                    blocks_processed = HashSet::new();
                }
                if !cp_ok_txs && (disable_transaction_processing || txs_processed.contains(&checkpoint)) {
                    cp_ok_txs = true;
                    txs_processed = HashSet::new();
                }
                if cp_ok_blocks && cp_ok_txs {
                    let checkpoint_string = hex::encode(checkpoint.as_bytes());
                    info!("Saving block_checkpoint {}", checkpoint_string);
                    save_checkpoint(&checkpoint_string, &database).await.unwrap();
                    checkpoint_last_saved = Instant::now();
                    checkpoint_candidate = None;
                } else if Instant::now().duration_since(checkpoint_last_warned).as_secs() > CHECKPOINT_WARN_INTERVAL {
                    warn!("Still unable to save block_checkpoint {}", hex::encode(checkpoint.as_bytes()));
                    checkpoint_last_warned = Instant::now();
                } else if Instant::now().duration_since(checkpoint_last_saved).as_secs() > CHECKPOINT_FAILED_TIMEOUT {
                    // Failsafe in the unlikely scenario that vcp is more than CHECKPOINT_SAVE_INTERVAL behind blocks/txs processing
                    // or, in the case of vcp-disabled, that blocks is equally far behind tx processing.
                    // Selecting a new candidate without clearing the processed hashmaps should allow it to eventually succeed,
                    // although it will be at the expense of increased memory use.
                    error!("Failed to synchronize on block_checkpoint {}", hex::encode(checkpoint.as_bytes()));
                    checkpoint_candidate = None;
                }
            }
        } else {
            sleep(Duration::from_millis(100)).await;
        }
    }
}
