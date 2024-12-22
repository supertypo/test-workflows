use kaspa_hashes::Hash as KaspaHash;
use simply_kaspa_cli::cli_args::CliArgs;

#[derive(Clone)]
pub struct Settings {
    pub cli_args: CliArgs,
    pub net_bps: u8,
    pub net_tps_max: u16,
    pub checkpoint: KaspaHash,
}
