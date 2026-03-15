
// In memory raft log.
// Entries are indexed starting at base_index + 1.
// The LogEntry itself no longer stores its own index, it's calculated from the vec offset. 
use crate::types::{LogEntry, LogIndex, LogPosition, Term};

#[derive(Debug)]
pub struct Log {
    entries: Vec<LogEntry>,
    base_index: LogIndex,
}

impl Log {
    pub fn new() -> Self {
        Self { entries: Vec::new(), base_index: 0 }
    }

    pub fn with_base(base_index: LogIndex) -> Self {
        Self { entries: Vec::new(), base_index }
    }

    // priv idx helpers

    fn offset(&self, index: LogIndex) -> Option<usize> {
        if index <= self.base_index {
            return None;
        }
        let off = (index - self.base_index - 1) as usize;
        if off < self.entries.len() { Some(off) } else { None }
    }

    fn index_of(&self, offset: usize) -> LogIndex {
        self.base_index + 1 + offset as u64
    }

    // read

    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        self.offset(index).map(|i| &self.entries[i])
    }

    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.get(index).map(|e| e.term)
    }

    pub fn last_index(&self) -> LogIndex {
        self.base_index + self.entries.len() as u64
    }

    pub fn first_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            0
        } else {
            self.base_index + 1
        }
    }

    pub fn last_position(&self) -> LogPosition {
        LogPosition {
            index: self.last_index(),
            term: self.entries.last().map_or(0, |e| e.term),
        }
    }

    pub fn base(&self) -> LogIndex {
        self.base_index
    }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
    pub fn len(&self) -> usize { self.entries.len() }

    // Borrow a slice of entries from `start` up to (not including) `end`.
    // Clamps to available range.
    pub fn slice(&self, start: LogIndex, end: LogIndex) -> &[LogEntry] {
        if start >= end || self.entries.is_empty() {
            return &[];
        }
        let s = start.saturating_sub(self.base_index + 1) as usize;
        let e = end.saturating_sub(self.base_index + 1) as usize;
        let s = s.min(self.entries.len());
        let e = e.min(self.entries.len());
        &self.entries[s..e]
    }

    // write

    pub fn push(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn extend(&mut self, entries: impl IntoIterator<Item = LogEntry>) {
        self.entries.extend(entries);
    }

    // Remove all entries after `index` (keep entries up to and including `index`).
    pub fn truncate_after(&mut self, index: LogIndex) {
        if index <= self.base_index {
            self.entries.clear();
        } else {
            let keep = (index - self.base_index) as usize;
            self.entries.truncate(keep);
        }
    }

    // Remove entries up to and including `through`, advancing base_index.
    pub fn compact(&mut self, through: LogIndex) {
        if through <= self.base_index {
            return;
        }
        let drain = (through - self.base_index) as usize;
        if drain >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(..drain);
        }
        self.base_index = through;
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn reset(&mut self, base: LogIndex) {
        self.entries.clear();
        self.base_index = base;
    }

    // term searching helpers and whatnot

    pub fn find_first_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries.iter()
            .position(|e| e.term == term)
            .map(|off| self.index_of(off))
    }

    pub fn find_last_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries.iter()
            .rposition(|e| e.term == term)
            .map(|off| self.index_of(off))
    }
}

impl Default for Log {
    fn default() -> Self { Self::new() }
}