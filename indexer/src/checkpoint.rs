use crate::settings::Settings;
use crate::vars::save_checkpoint;
use clap::ValueEnum;
use crossbeam_queue::ArrayQueue;
use log::{info, warn};
use simply_kaspa_cli::cli_args::CliDisable;
use simply_kaspa_database::client::KaspaDbClient;
use simply_kaspa_database::models::types::hash::Hash as SqlHash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Clone, Debug, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "snake_case")]
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
    const CHECKPOINT_TIMEOUT: u64 = CHECKPOINT_SAVE_INTERVAL + CHECKPOINT_SAVE_INTERVAL * 3;
    let mut checkpoint_last_saved = Instant::now();
    let mut checkpoint_candidate = None;

    let mut blocks_processed: HashSet<SqlHash> = HashSet::new();
    let mut txs_processed: HashSet<SqlHash> = HashSet::new();
    let mut vcp_processed: HashSet<SqlHash> = HashSet::new();

    while run.load(Ordering::Relaxed) {
        if let Some((origin, block_hash)) = checkpoint_queue.pop() {
            match origin {
                CheckpointOrigin::Blocks => {
                    if disable_virtual_chain_processing
                        && checkpoint_candidate.is_none()
                        && Instant::now().duration_since(checkpoint_last_saved).as_secs() > CHECKPOINT_SAVE_INTERVAL
                    {
                        checkpoint_candidate = Some(block_hash.clone());
                    }
                    blocks_processed.insert(block_hash);
                }
                CheckpointOrigin::Transactions => {
                    txs_processed.insert(block_hash);
                }
                CheckpointOrigin::Vcp => {
                    if checkpoint_candidate.is_none()
                        && Instant::now().duration_since(checkpoint_last_saved).as_secs() > CHECKPOINT_SAVE_INTERVAL
                    {
                        checkpoint_candidate = Some(block_hash.clone());
                    }
                    vcp_processed.insert(block_hash);
                }
            }
        } else {
            sleep(Duration::from_millis(100)).await;
        }
        if let Some(checkpoint) = checkpoint_candidate.clone() {
            let mut checkpoint_complete = true;
            if !blocks_processed.contains(&checkpoint) {
                checkpoint_complete = false;
            } else if !disable_transaction_processing && !txs_processed.contains(&checkpoint) {
                checkpoint_complete = false;
            } else if !disable_virtual_chain_processing && !vcp_processed.contains(&checkpoint) {
                checkpoint_complete = false;
            }
            if checkpoint_complete {
                let checkpoint_string = hex::encode(checkpoint.as_bytes());
                info!("Saving block_checkpoint {}", checkpoint_string);
                save_checkpoint(&checkpoint_string, &database).await.unwrap();
                checkpoint_last_saved = Instant::now();
                checkpoint_candidate = None;
                blocks_processed = HashSet::new();
                txs_processed = HashSet::new();
                vcp_processed = HashSet::new();
            } else if Instant::now().duration_since(checkpoint_last_saved).as_secs() > CHECKPOINT_TIMEOUT {
                warn!("Unable to save block_checkpoint {}, retrying...", hex::encode(checkpoint.as_bytes()));
                checkpoint_last_saved = Instant::now();
                checkpoint_candidate = None;
                blocks_processed = HashSet::new();
                txs_processed = HashSet::new();
                vcp_processed = HashSet::new();
            }
        }
    }
}
