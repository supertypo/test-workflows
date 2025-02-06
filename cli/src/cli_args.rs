use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, PartialEq, Eq, ValueEnum, ToSchema, Serialize, Deserialize)]
#[clap(rename_all = "snake_case")]
pub enum CliDisable {
    None,
    /// Disables the virtual chain processor / the transactions_acceptances table
    VirtualChainProcessing,
    /// Disables transaction processing / all transaction related tables
    TransactionProcessing,
    /// Disables the blocks table
    BlocksTable,
    /// Disables the block_parent table
    BlockParentTable,
    /// Disables the blocks_transactions table
    BlocksTransactionsTable,
    /// Disables the transactions table
    TransactionsTable,
    /// Disables the transactions_inputs table
    TransactionsInputsTable,
    /// Disables the transactions_outputs table
    TransactionsOutputsTable,
    /// Disables the addresses_transactions table
    AddressesTransactionsTable,
    /// Start VCP as soon as the filler has passed the previous run. Use with care
    VcpWaitForSync,
}

#[derive(Clone, Debug, PartialEq, Eq, ValueEnum, ToSchema, Serialize, Deserialize)]
#[clap(rename_all = "snake_case")]
pub enum CliField {
    None,
    BlockAcceptedIdMerkleRoot,
    BlockMergeSetBluesHashes,
    BlockMergeSetRedsHashes,
    BlockSelectedParentHash,
    BlockBits,
    BlockBlueWork,
    /// NB! Used for sorting blocks
    BlockBlueScore,
    BlockDaaScore,
    BlockHashMerkleRoot,
    BlockNonce,
    BlockPruningPoint,
    BlockTimestamp,
    BlockUtxoCommitment,
    BlockVersion,
    /// NB! Used for identifying tx type (coinbase/regular)
    TxSubnetworkId,
    TxHash,
    TxMass,
    TxPayload,
    /// NB! Used for sorting transactions
    TxBlockTime,
    /// NB! Used for identifying wallet address of sender
    TxInPreviousOutpoint,
    TxInSignatureScript,
    TxInSigOpCount,
    TxInBlockTime,
    TxOutAmount,
    /// NB! Used for identifying wallet addresses
    TxOutScriptPublicKey,
    /// NB! Used for identifying wallet addresses
    TxOutScriptPublicKeyAddress,
    TxOutBlockTime,
}

#[derive(Parser, Clone, Debug, ToSchema, Serialize, Deserialize)]
#[command(version = env!("VERGEN_GIT_DESCRIBE"))]
#[serde(rename_all = "camelCase")]
pub struct CliArgs {
    #[clap(short = 's', long, help = "The url to a kaspad instance, e.g 'ws://localhost:17110'. Leave empty to use the Kaspa PNN")]
    pub rpc_url: Option<String>,
    #[clap(short, long, default_value = "mainnet", help = "The network type and suffix, e.g. 'testnet-11'")]
    pub network: String,
    #[clap(short, long, default_value = "postgres://postgres:postgres@localhost:5432/postgres", help = "PostgreSQL url")]
    pub database_url: String,
    #[clap(short, long, default_value = "localhost:8500", help = "Socket address for web server")]
    pub listen: String,
    #[clap(long, default_value = "info", help = "error, warn, info, debug, trace, off")]
    pub log_level: String,
    #[clap(long, help = "Disable colored output")]
    pub log_no_color: bool,
    #[clap(short, long, default_value = "1.0", help = "Batch size factor [0.1-10]. Adjusts internal queues and database batch sizes")]
    pub batch_scale: f64,
    #[clap(short = 't', long, default_value = "60", help = "Cache ttl (secs). Adjusts tx/block caches for in-memory de-duplication")]
    pub cache_ttl: u64,
    #[clap(short, long, help = "Ignore checkpoint and start from a specified block, 'p' for pruning point or 'v' for virtual")]
    pub ignore_checkpoint: Option<String>,
    #[clap(short, long, help = "Auto-upgrades older db schemas. Use with care")]
    pub upgrade_db: bool,
    #[clap(short = 'c', long, help = "(Re-)initializes the database schema. Use with care")]
    pub initialize_db: bool,
    #[clap(long, help = "Disable specific functionality", value_enum, use_value_delimiter = true)]
    pub disable: Option<Vec<CliDisable>>,
    #[clap(
        long,
        help = "Exclude specific fields. If include_fields is specified this argument is ignored.",
        value_enum,
        use_value_delimiter = true
    )]
    pub exclude_fields: Option<Vec<CliField>>,
}

impl CliArgs {
    pub fn is_disabled(&self, feature: CliDisable) -> bool {
        self.disable.as_ref().map_or(false, |disable| disable.contains(&feature))
    }

    pub fn is_excluded(&self, field: CliField) -> bool {
        if let Some(exclude_fields) = self.exclude_fields.clone() {
            exclude_fields.contains(&field)
        } else {
            false
        }
    }
}
