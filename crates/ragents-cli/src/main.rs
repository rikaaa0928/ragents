use clap::Parser;
use ragents_cli::{run_cli, Cli};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    ragents_core::ensure_rustls_crypto_provider();
    let cli = Cli::parse();
    run_cli(cli).await
}
