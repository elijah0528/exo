//! Application-level services for managed Exo Code agents.

pub mod agent;
mod recipe;
mod sandbox_pool;
mod snapshot_store;

pub use agent::{CodingAgent, CodingAgentConfig, CodingAgentEvent, CodingResult, CodingTask};
pub use recipe::{
    CreateSandboxFromRecipeRequest, RecipePolicy, RecipeService, SandboxRecipe, SandboxRecipeStep,
    SecretResolver,
};
pub use sandbox_pool::*;
pub use snapshot_store::*;
