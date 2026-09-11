//! The excode terminal.
//!
//! Layering, outermost first:
//! - [`ui`]: application-agnostic primitives (theme, components, scroll, diff).
//! - [`session`]: sandbox-pool state and operations, with no terminal types.
//! - [`dashboard`], [`command`]: excode-specific views and the slash-command
//!   grammar.
//! - [`app`]: layout, focus, and the event loop that ties the three together.

mod app;
mod args;
mod clipboard;
mod command;
mod dashboard;
mod harness;
mod session;
pub mod ui;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use excode::{CodingAgent, CodingAgentConfig};
use executor::RouterModelClient;

use crate::HarnessSelection;

pub use args::ExcodeArgs;

/// Start the pool, run the terminal, then drain the pool.
pub async fn run(root: &Path, args: ExcodeArgs, env_vars: HashMap<String, String>) -> Result<()> {
    let session = session::Session::start(
        root,
        args.backend,
        args.image.clone(),
        args.workers,
        args.max_total(),
    )
    .await?;
    let pool = session.pool();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let reconciler = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { pool.run_reconciler(receiver).await }
    });

    let agent = match &args.harness {
        Some(raw) => {
            let selection: HarnessSelection = raw.parse().map_err(|error| anyhow!("{error}"))?;
            harness::ChatDriver::Harness(
                harness::HarnessChatDriver::build(root, &selection, &args, env_vars).await?,
            )
        }
        None => harness::ChatDriver::Builtin(CodingAgent::new(
            Arc::new(RouterModelClient::new(env_vars)),
            Arc::clone(&pool) as Arc<dyn excode::ManagedSandboxPool>,
            CodingAgentConfig {
                model: args
                    .model
                    .clone()
                    .unwrap_or_else(|| "gpt-4o-mini".to_string()),
                ..Default::default()
            },
        )),
    };
    let result = app::App::new(session, agent).run().await;

    shutdown
        .send(true)
        .context("signalling the sandbox pool reconciler to stop")?;
    reconciler
        .await
        .context("sandbox pool reconciler task failed")?;
    pool.drain().await.context("draining sandbox pool")?;
    result
}
