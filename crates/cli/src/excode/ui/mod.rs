//! Reusable TUI primitives.
//!
//! Everything here is application-agnostic: no module knows about sandboxes,
//! pools, or agents. Views compose these primitives, and only the theme
//! decides colors.
//!
//! The layering is:
//! - [`theme`] and [`component`]: styling roles and the render/event contract.
//! - [`layout`], [`wrap`], [`scroll`]: geometry, styled text wrapping, viewport.
//! - [`panel`], [`list`], [`text_input`], [`status`], [`gauge`], [`overlay`]:
//!   drawable widgets built on the above.
//! - [`transcript`], [`cells`], [`diff`]: append-only content and its entries.

// These primitives are a component library that happens to live in a binary
// crate, so parts of the surface have no in-tree caller yet.
#![allow(dead_code)]

pub mod cells;
pub mod component;
pub mod diff;
pub mod gauge;
pub mod layout;
pub mod list;
pub mod overlay;
pub mod panel;
pub mod scroll;
pub mod status;
pub mod text_input;
pub mod theme;
pub mod transcript;
pub mod wrap;
