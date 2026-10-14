use ragents_core::subprocess::run_subprocess_worker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut ipc_addr = None;
    let mut ipc_token = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "internal-core-worker" => {}
            "--ipc-addr" => ipc_addr = args.next(),
            "--ipc-token" => ipc_token = args.next(),
            _ => {}
        }
    }

    let addr = ipc_addr.ok_or_else(|| anyhow::anyhow!("missing --ipc-addr"))?;
    let token = ipc_token.ok_or_else(|| anyhow::anyhow!("missing --ipc-token"))?;
    run_subprocess_worker(&addr, &token).await?;
    Ok(())
}
