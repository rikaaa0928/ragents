use clap::Parser;
use ragents_cli::{run_cli, Cli};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run_cli(cli).await
}
