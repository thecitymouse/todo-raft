// Perhaps I should split this

// ============================================================================
// Primitives
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogPosition {
    pub index: LogIndex,
    pub term: Term,
}

pub type Term = u64;
pub type LogIndex = u64;

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RaftError {
    #[error("not leader")]
    NotLeader,
    #[error("snapshot not found")]
    SnapshotNotFound,
    #[error("snapshot in progress")]
    SnapshotInProgress,
    #[error("log compacted")]
    LogCompacted,
    #[error("shutdown")]
    Shutdown,
    #[error("queue full")]
    QueueFull,
    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, RaftError>;

// ============================================================================
// Config
// ============================================================================

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Features: u32 {
        const PREVOTE       = 1 << 0;
        const CHECK_QUORUM  = 1 << 1;
        const LEARNER       = 1 << 2;
        const AUTO_SNAPSHOT = 1 << 3;
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub features: Features,
    pub election_timeout_min_ms: u32,
    pub election_timeout_max_ms: u32,
    pub heartbeat_interval_ms: u32,
    pub max_entries_per_msg: usize,
    pub max_bytes_per_msg: Option<usize>,
    pub snapshot_threshold: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            features: Features::PREVOTE,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            heartbeat_interval_ms: 100,
            max_entries_per_msg: 64,
            max_bytes_per_msg: None,
            snapshot_threshold: Some(10_000),
        }
    }
}

// ============================================================================
// Snapshot state
// ============================================================================

#[derive(Debug)]
pub enum SnapshotState {
    Idle,
    InProgress {
        pending_index: LogIndex,
        pending_term: Term,
    },
}

impl Default for SnapshotState {
    fn default() -> Self { Self::Idle }
}

// ============================================================================
// Log Entry
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryPayload {
    Data(Vec<u8>),
    Noop,
    Config(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub payload: EntryPayload,
}

impl LogEntry {
    pub fn data(term: Term, data: Vec<u8>) -> Self {
        Self { term, payload: EntryPayload::Data(data) }
    }

    pub fn noop(term: Term) -> Self {
        Self { term, payload: EntryPayload::Noop }
    }

    pub fn is_noop(&self) -> bool {
        matches!(self.payload, EntryPayload::Noop)
    }

    pub fn payload_len(&self) -> usize {
        match &self.payload {
            EntryPayload::Data(d) | EntryPayload::Config(d) => d.len(),
            EntryPayload::Noop => 0,
        }
    }
}

// ============================================================================
// RPC Messages
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteKind {
    PreVote,
    Vote,
}

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log: LogPosition,
    pub kind: VoteKind,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResp {
    pub term: Term,
    pub granted: bool,
    pub kind: VoteKind,
}

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log: LogPosition,
    pub leader_commit: LogIndex,
    pub seq: u64,
    pub entries: Vec<LogEntry>,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    pub conflict: Option<LogPosition>,
    pub match_index: LogIndex,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included: LogPosition,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotResp {
    pub term: Term,
    pub success: bool,
    pub next_offset: u64,
    pub done: bool,
}

#[derive(Debug, Clone)]
pub struct ReadIndexReq {
    pub req_id: u64,
    pub from: NodeId,
}

#[derive(Debug, Clone)]
pub struct ReadIndexResp {
    pub req_id: u64,
    pub read_index: Option<LogIndex>,
}

// Three traits (LogStore, StateMachine, Transport) instead of one so Rust
// can borrow them independently, e.g. read from log while calling apply().
pub trait LogStore: Send {
    fn persist_vote(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<()>;
    fn load_vote(&mut self) -> Result<(Term, Option<NodeId>)>;

    fn append(&mut self, index: LogIndex, entry: &LogEntry) -> Result<()>;
    fn append_batch(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<()> {
        for (i, e) in entries.iter().enumerate() {
            self.append(start + i as u64, e)?;
        }
        Ok(())
    }
    fn truncate_after(&mut self, index: LogIndex) -> Result<()>;

    fn fsync(&mut self) -> Result<()>;
    /// Submit async fsync.  Default falls back to blocking.
    fn fsync_submit(&mut self) -> Result<()> { self.fsync() }
    /// Poll async fsync completion.  None = still pending, Some = done.
    fn fsync_poll(&mut self) -> Option<Result<()>> { None }
}

/// State machine application + snapshot operations.
pub trait StateMachine: Send {
    fn apply(&mut self, index: LogIndex, entry: &LogEntry) -> Result<()>;
    fn apply_batch(&mut self, start: LogIndex, entries: &[LogEntry]) -> Result<()> {
        for (i, e) in entries.iter().enumerate() {
            self.apply(start + i as u64, e)?;
        }
        Ok(())
    }

    fn snapshot_create(&mut self, pos: LogPosition) -> Result<()>;
    /// Returns true when async snapshot is complete.  Default: synchronous.
    fn snapshot_poll(&self) -> bool { true }
    fn snapshot_load(&mut self) -> Result<Option<LogPosition>>;
    fn snapshot_read(&self, offset: u64, buf: &mut [u8]) -> Result<(usize, bool)>;
    fn snapshot_write(&mut self, offset: u64, data: &[u8], done: bool) -> Result<()>;
    fn snapshot_restore(&mut self, pos: LogPosition) -> Result<()>;
}

/// Network transport for sending RPCs.
///
/// Takes &self (not &mut self) so you can call transport methods while
/// holding mutable borrows on other parts of RaftCore.  Implementations
/// typically use interior mutability (channels, Arc<Mutex<...>>, etc).
pub trait Transport: Send {
    fn send_request_vote(&self, to: NodeId, msg: &RequestVote) -> Result<()>;
    fn send_append_entries(&self, to: NodeId, msg: &AppendEntries) -> Result<()>;
    fn send_install_snapshot(&self, to: NodeId, msg: &InstallSnapshot) -> Result<()>;
    fn send_read_index(&self, to: NodeId, msg: &ReadIndexReq) -> Result<()>;
    fn send_read_index_resp(&self, to: NodeId, resp: &ReadIndexResp) -> Result<()>;
    fn on_read_index_complete(&self, req_id: u64, read_index: Option<LogIndex>);
}