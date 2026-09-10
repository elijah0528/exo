//! The one contract every excode widget implements.
//!
//! A component owns its state, draws itself into a [`Rect`], and decides
//! whether it consumed an input event. Composition is plain Rust: a parent owns
//! its children, splits its area, and forwards events to the child that has
//! focus. There is no global registry, no dynamic layout engine, and no
//! implicit redraw: the app loop redraws after any consumed event.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;

use super::theme::Theme;

/// Everything a component needs from its parent to draw itself.
#[derive(Debug, Clone, Copy)]
pub struct RenderCtx<'a> {
    pub theme: &'a Theme,
    /// Whether this component currently owns keyboard focus.
    pub focused: bool,
}

impl<'a> RenderCtx<'a> {
    pub fn new(theme: &'a Theme) -> Self {
        Self {
            theme,
            focused: false,
        }
    }

    pub fn focused(self, focused: bool) -> Self {
        Self { focused, ..self }
    }
}

/// Whether an event was handled, and therefore should not bubble up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventFlow {
    Consumed,
    Ignored,
}

impl EventFlow {
    pub fn is_consumed(self) -> bool {
        self == EventFlow::Consumed
    }

    /// `Consumed` when the predicate holds, so handlers can stay expression-shaped.
    pub fn consumed_if(consumed: bool) -> Self {
        if consumed {
            EventFlow::Consumed
        } else {
            EventFlow::Ignored
        }
    }
}

pub trait Component {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>);

    /// Rows this component wants at `width`. Parents use it for content-sized
    /// layout; components that always fill their area can keep the default.
    fn desired_height(&self, _width: u16) -> u16 {
        0
    }

    fn handle_key(&mut self, _key: crossterm::event::KeyEvent) -> EventFlow {
        EventFlow::Ignored
    }

    fn handle_mouse(&mut self, _mouse: crossterm::event::MouseEvent) -> EventFlow {
        EventFlow::Ignored
    }

    /// Terminal cursor position while this component is focused, if it wants a
    /// visible cursor (text inputs do; lists do not).
    fn cursor(&self, _area: Rect) -> Option<(u16, u16)> {
        None
    }
}
