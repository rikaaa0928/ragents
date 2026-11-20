pub mod context;
pub mod control_plugin;
pub mod error;
pub mod hooks;
pub mod host;
pub mod model;
pub mod process;
pub mod runner;
pub mod sandbox;
pub mod subprocess;

pub use context::*;
pub use control_plugin::*;
pub use error::*;
pub use hooks::*;
pub use host::*;
pub use model::*;
pub use process::*;
pub use ragents_protocol;
pub use runner::*;
pub use sandbox::*;
pub use subprocess::*;

/// Ensures a process-wide default `rustls` `CryptoProvider` is installed before
/// `wasmtime-wasi-http` or other `rustls::ClientConfig::builder()` callers run.
pub fn ensure_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests;
