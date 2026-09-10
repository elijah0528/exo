//! Small rectangle helpers used instead of ad-hoc arithmetic at call sites.

use ratatui::layout::Rect;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Insets {
    pub top: u16,
    pub right: u16,
    pub bottom: u16,
    pub left: u16,
}

impl Insets {
    pub fn all(value: u16) -> Self {
        Self {
            top: value,
            right: value,
            bottom: value,
            left: value,
        }
    }

    pub fn horizontal(value: u16) -> Self {
        Self {
            right: value,
            left: value,
            ..Self::default()
        }
    }

    pub fn vertical(value: u16) -> Self {
        Self {
            top: value,
            bottom: value,
            ..Self::default()
        }
    }
}

pub trait RectExt {
    /// Shrink a rect by `insets`, saturating to an empty rect.
    fn inset(self, insets: Insets) -> Rect;
    /// A centered rect of at most `width` x `height`.
    fn centered(self, width: u16, height: u16) -> Rect;
}

impl RectExt for Rect {
    fn inset(self, insets: Insets) -> Rect {
        let width = self
            .width
            .saturating_sub(insets.left.saturating_add(insets.right));
        let height = self
            .height
            .saturating_sub(insets.top.saturating_add(insets.bottom));
        Rect {
            x: self.x.saturating_add(insets.left),
            y: self.y.saturating_add(insets.top),
            width,
            height,
        }
    }

    fn centered(self, width: u16, height: u16) -> Rect {
        let width = width.min(self.width);
        let height = height.min(self.height);
        Rect {
            x: self.x + (self.width - width) / 2,
            y: self.y + (self.height - height) / 2,
            width,
            height,
        }
    }
}

#[cfg(test)]
#[path = "layout_tests.rs"]
mod tests;
