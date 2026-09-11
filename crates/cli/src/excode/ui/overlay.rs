//! A centered modal surface drawn over the main view.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Clear, Widget};

use super::component::{Component, RenderCtx};
use super::layout::RectExt;
use super::panel::Panel;

/// Draw `body` inside a bordered panel covering `width_pct`/`height_pct` of
/// `area`, clearing whatever was underneath.
pub fn render_overlay(
    area: Rect,
    buf: &mut Buffer,
    ctx: RenderCtx<'_>,
    title: Line<'static>,
    footer: Option<Line<'static>>,
    width_pct: u16,
    height_pct: u16,
    body: &dyn Component,
) {
    let width = area.width * width_pct / 100;
    let height = area.height * height_pct / 100;
    let surface = area.centered(width, height);
    if surface.width == 0 || surface.height == 0 {
        return;
    }
    Clear.render(surface, buf);
    let mut panel = Panel::new(title);
    if let Some(footer) = footer {
        panel = panel.footer(footer);
    }
    let inner = panel.inner(surface);
    panel.render(surface, buf, ctx);
    body.render(inner, buf, ctx);
}
