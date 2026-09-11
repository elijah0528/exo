//! Viewport scrolling shared by every scrollable surface.
//!
//! The state is just "which row is at the top" plus a sticky flag for
//! following new output. Content length and viewport height are passed in on
//! every call: the scroll state never caches them, so a component can re-wrap,
//! filter, or resize without invalidating anything.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scroll {
    top: usize,
    target: usize,
    /// While pinned, the viewport follows the end of the content.
    pinned_to_bottom: bool,
}

impl Default for Scroll {
    fn default() -> Self {
        Self {
            top: 0,
            target: 0,
            pinned_to_bottom: true,
        }
    }
}

impl Scroll {
    /// The first visible row, clamped to the current content and viewport.
    pub fn top(&self, total: usize, viewport: usize) -> usize {
        let max = max_top(total, viewport);
        if self.pinned_to_bottom {
            max
        } else {
            self.top.min(max)
        }
    }

    pub fn is_pinned_to_bottom(&self) -> bool {
        self.pinned_to_bottom
    }

    /// Scroll by `delta` rows; negative scrolls towards the top.
    pub fn scroll_by(&mut self, delta: isize, total: usize, viewport: usize) {
        let max = max_top(total, viewport);
        let current = if self.pinned_to_bottom {
            max
        } else {
            self.target.min(max)
        } as isize;
        let next = current.saturating_add(delta).clamp(0, max as isize) as usize;
        self.target = next;
        if self.pinned_to_bottom && next < max {
            self.top = max;
        }
        self.pinned_to_bottom = next >= max;
        if self.pinned_to_bottom {
            self.top = max;
        }
    }

    /// Advance one smooth scroll step toward the requested position.
    pub fn tick(&mut self, total: usize, viewport: usize) {
        if self.pinned_to_bottom {
            self.top = max_top(total, viewport);
            self.target = self.top;
            return;
        }
        self.top = match self.top.cmp(&self.target) {
            std::cmp::Ordering::Less => self.top + 1,
            std::cmp::Ordering::Greater => self.top.saturating_sub(1),
            std::cmp::Ordering::Equal => self.top,
        };
        self.top = self.top.min(max_top(total, viewport));
    }

    pub fn page_up(&mut self, total: usize, viewport: usize) {
        self.scroll_by(-(viewport.max(1) as isize), total, viewport);
    }

    pub fn page_down(&mut self, total: usize, viewport: usize) {
        self.scroll_by(viewport.max(1) as isize, total, viewport);
    }

    pub fn scroll_to_top(&mut self) {
        self.top = 0;
        self.target = 0;
        self.pinned_to_bottom = false;
    }

    pub fn scroll_to_bottom(&mut self) {
        self.pinned_to_bottom = true;
    }
}

fn max_top(total: usize, viewport: usize) -> usize {
    total.saturating_sub(viewport)
}

#[cfg(test)]
#[path = "scroll_tests.rs"]
mod tests;
