//! A titled, bordered container.
//!
//! Every framed surface in the app goes through this so focus highlighting and
//! title placement stay identical everywhere.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Widget};

use super::component::RenderCtx;

pub struct Panel<'a> {
    title: Line<'a>,
    footer: Option<Line<'a>>,
}

impl<'a> Panel<'a> {
    pub fn new(title: impl Into<Line<'a>>) -> Self {
        Self {
            title: title.into(),
            footer: None,
        }
    }

    /// A right-aligned hint drawn on the bottom border.
    pub fn footer(mut self, footer: impl Into<Line<'a>>) -> Self {
        self.footer = Some(footer.into());
        self
    }

    /// The area left for content once the border is drawn.
    pub fn inner(&self, area: Rect) -> Rect {
        Block::default().borders(Borders::ALL).inner(area)
    }

    pub fn render(self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) -> Rect {
        let border_style = if ctx.focused {
            ctx.theme.border_focused
        } else {
            ctx.theme.border
        };
        let title = Line::from(
            std::iter::once(Span::raw(" "))
                .chain(self.title.spans)
                .chain(std::iter::once(Span::raw(" ")))
                .collect::<Vec<_>>(),
        )
        .style(ctx.theme.title);
        let mut block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(title);
        if let Some(footer) = self.footer {
            block = block.title_bottom(footer.right_aligned().style(ctx.theme.dim));
        }
        let inner = block.inner(area);
        block.render(area, buf);
        inner
    }
}
