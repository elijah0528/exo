use std::collections::HashMap;
use std::sync::Arc;

use executor::{
    BraintrustRuntimeConfig, ExecutorHarnessRuntime, ModelClient, RouterModelClient, SharedHarness,
    SharedHarnessBacked,
};
use exoharness::{BasicExoHarness, BasicExoHarnessConfig, ExoHarness, Result};

use super::{executor::CodingExecutor, runtime::CodingToolRuntime};

pub struct CodingHarness<M> {
    inner: SharedHarness<ExecutorHarnessRuntime<CodingExecutor<M>>>,
}

impl<M> CodingHarness<M> {
    pub fn new(exoharness: Arc<dyn ExoHarness>, model: Arc<M>) -> Self
    where
        M: ModelClient + 'static,
    {
        let executor =
            CodingExecutor::new(model, Arc::new(CodingToolRuntime::with_default_tools()));
        let runtime = ExecutorHarnessRuntime::new(executor, None);
        Self {
            inner: SharedHarness::new(exoharness, runtime),
        }
    }
}

impl CodingHarness<RouterModelClient> {
    pub fn from_exoharness(
        exoharness: Arc<dyn ExoHarness>,
        runtime_config: Option<BraintrustRuntimeConfig>,
        env: HashMap<String, String>,
    ) -> Self {
        let model = Arc::new(RouterModelClient::new(env));
        let executor =
            CodingExecutor::new(model, Arc::new(CodingToolRuntime::with_default_tools()));
        let runtime = ExecutorHarnessRuntime::new(executor, runtime_config);
        Self {
            inner: SharedHarness::new(exoharness, runtime),
        }
    }

    pub async fn from_config(
        exo_config: BasicExoHarnessConfig,
        runtime_config: Option<BraintrustRuntimeConfig>,
        env: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self::from_exoharness(
            Arc::new(BasicExoHarness::new(exo_config).await?),
            runtime_config,
            env,
        ))
    }
}

impl<M> SharedHarnessBacked for CodingHarness<M>
where
    M: ModelClient + 'static,
{
    type Runtime = ExecutorHarnessRuntime<CodingExecutor<M>>;

    fn shared_harness(&self) -> &SharedHarness<Self::Runtime> {
        &self.inner
    }
}
