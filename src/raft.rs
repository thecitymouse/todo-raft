use std::collections::{HashMap, HashSet};
use std::time::Instant;
use crate::types::*;
use crate::log::Log;

// ============================================================================
// Per-Peer State
// ============================================================================

#[derive(Debug)]
pub struct Peer {
    pub next_index: LogIndex,
    pub match_index: LogIndex,
    pub in_flight: bool,
    pub last_sent: Option<Instant>,
}

#[derive(Debug)]
pub struct SnapshotTransfer {
    pub offset: u64,
    pub last_sent: Option<Instant>,
}

// ============================================================================
// ReadIndex State
// ============================================================================

#[derive(Debug)]
pub struct PendingRead {
    pub req_id: u64,
    pub from: Option<NodeId>,
    pub commit_index: LogIndex,
    pub term: Term,
    pub acks: HashSet<NodeId>,
    pub required_seq: u64,
}

// ============================================================================
// Leader State
// ============================================================================

#[derive(Debug)]
pub struct LeaderState {
    pub peers: HashMap<NodeId, Peer>,
    pub snapshot_transfers: HashMap<NodeId, SnapshotTransfer>,
    pub pending_reads: Vec<PendingRead>,
    pub heartbeat_seq: u64,
}

impl LeaderState {
    pub fn new(my_id: NodeId, peer_ids: &[NodeId], last_index: LogIndex) -> Self {
        let peers = peer_ids
            .iter()
            .filter(|&&id| id != my_id)
            .map(|&id| (id, Peer {
                next_index: last_index + 1,
                match_index: 0,
                in_flight: false,
                last_sent: None,
            }))
            .collect();

        Self {
            peers,
            snapshot_transfers: HashMap::new(),
            pending_reads: Vec::new(),
            heartbeat_seq: 0,
        }
    }
}

// ============================================================================
// Election State
// ============================================================================

#[derive(Debug)]
pub struct Election {
    pub votes: HashSet<NodeId>,
}

impl Election {
    pub fn new(self_vote: NodeId) -> Self {
        let mut votes = HashSet::new();
        votes.insert(self_vote);
        Self { votes }
    }

    pub fn tally(&self) -> usize {
        self.votes.len()
    }
}

// ============================================================================
// Role
// ============================================================================

#[derive(Debug)]
pub enum Role {
    Follower {
        leader: Option<NodeId>,
    },
    PreCandidate {
        leader: Option<NodeId>,
        election: Election,
    },
    Candidate {
        election: Election,
    },
    Leader(LeaderState),
}

// ============================================================================
// Core state
//
// Three separate trait-object fields so the borrow checker lets you do things
// like read self.log while calling self.state_machine.apply().
// ============================================================================

pub struct RaftCore {
    pub my_id: NodeId,
    pub peers: Vec<NodeId>,     // all node IDs including self
    pub config: Config,

    // Persistent state 
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Log,

    // Volatile state 
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    // Snapshot state
    pub snapshot_last: LogPosition,
    pub snapshot_state: SnapshotState,

    // Lifecycle
    pub shutdown: bool,
    pub skip_next_fsync: bool,

    // Async fsync tracking
    pub durable_index: LogIndex,
    pub fsync_pending: bool,

    // Other
    pub log_store: Box<dyn LogStore>,
    pub state_machine: Box<dyn StateMachine>,
    pub transport: Box<dyn Transport>,
}

impl RaftCore {
    // Total nodes in the cluster (peers includes self).
    pub fn num_nodes(&self) -> usize {
        self.peers.len()
    }

    // Majority quorum size. 
    pub fn quorum(&self) -> usize {
        self.num_nodes() / 2 + 1
    }

    // Get term at index, falling back to snapshot boundary.
    pub fn term_at_or_snapshot(&self, index: LogIndex) -> Option<Term> {
        self.log.term_at(index).or_else(|| {
            if index == self.snapshot_last.index && self.snapshot_last.index > 0 {
                Some(self.snapshot_last.term)
            } else {
                None
            }
        })
    }
}

// ============================================================================
// Raft Node
// ============================================================================

pub struct Raft {
    pub core: RaftCore,
    pub role: Role,

    pub election_deadline: Instant,
    pub last_tick: Instant,
    pub prng: u32,
}

impl Raft {
    pub fn new(
        my_id: NodeId,
        peers: Vec<NodeId>,
        config: Config,
        mut log_store: Box<dyn LogStore>,
        mut state_machine: Box<dyn StateMachine>,
        transport: Box<dyn Transport>,
    ) -> Result<Self> {
        let now = Instant::now();

        // Load persistent state
        let (term, voted_for) = log_store.load_vote()?;
        let snapshot_last = state_machine.snapshot_load()?
            .unwrap_or(LogPosition { index: 0, term: 0 });

        let mut log = Log::new();

        // If snapshot exists, set log base so new entries start after it
        if snapshot_last.index > 0 {
            log = Log::with_base(snapshot_last.index);
        }

        let mut raft = Self {
            core: RaftCore {
                my_id,
                peers,
                config,
                current_term: term,
                voted_for,
                log,
                commit_index: snapshot_last.index,
                last_applied: snapshot_last.index,
                snapshot_last,
                snapshot_state: SnapshotState::default(),
                shutdown: false,
                skip_next_fsync: false,
                durable_index: 0,
                fsync_pending: false,
                log_store,
                state_machine,
                transport,
            },
            role: Role::Follower { leader: None },
            election_deadline: now,
            last_tick: now,
            // This is Knuth multiplicative hash I believe
            // This is just done to make sure nodes election timeouts aren't the same
            prng: my_id.0.wrapping_mul(2654435761).max(1),
        };

        raft.reset_election_deadline();
        Ok(raft)
    }

    // === Accessors ===

    pub fn id(&self) -> NodeId { self.core.my_id }
    pub fn term(&self) -> Term { self.core.current_term }
    pub fn role(&self) -> &Role { &self.role }
    pub fn commit_index(&self) -> LogIndex { self.core.commit_index }
    pub fn last_applied(&self) -> LogIndex { self.core.last_applied }
    pub fn log(&self) -> &Log { &self.core.log }
    pub fn num_nodes(&self) -> usize { self.core.num_nodes() }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader(_))
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        match &self.role {
            Role::Follower { leader } | Role::PreCandidate { leader, .. } => *leader,
            Role::Candidate { .. } => None,
            Role::Leader(_) => Some(self.core.my_id),
        }
    }

    pub fn leader_state(&self) -> Option<&LeaderState> {
        match &self.role {
            Role::Leader(s) => Some(s),
            _ => None,
        }
    }

    pub fn leader_state_mut(&mut self) -> Option<&mut LeaderState> {
        match &mut self.role {
            Role::Leader(s) => Some(s),
            _ => None,
        }
    }

    pub fn quorum(&self) -> usize {
        self.core.quorum()
    }

    // === State transitions ===

    pub fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        self.clear_pending_reads();

        if term > self.core.current_term {
            self.core.current_term = term;
            self.core.voted_for = None;
        }


        self.role = Role::Follower { leader };
        self.reset_election_deadline();

        let _ = self.core.log_store.persist_vote(
            self.core.current_term,
            self.core.voted_for,
        );
    }

    pub fn become_pre_candidate(&mut self) {
        let leader = self.leader_id();
        self.role = Role::PreCandidate {
            leader,
            election: Election::new(self.core.my_id),
        };
        self.reset_election_deadline();
    }

    pub fn become_candidate(&mut self) {
        self.core.current_term += 1;
        self.core.voted_for = Some(self.core.my_id);
        self.role = Role::Candidate {
            election: Election::new(self.core.my_id),
        };
        self.reset_election_deadline();

        let _ = self.core.log_store.persist_vote(
            self.core.current_term,
            self.core.voted_for,
        );
        
        self.send_requestvote_all();

        // Single-node: already have quorum
        if let Role::Candidate { ref election } = self.role {
            if election.tally() >= self.quorum() {
                self.become_leader();
            }
        }
    }

    pub fn become_leader(&mut self) {
        let last_index = self.core.log.last_index();
        self.role = Role::Leader(
            LeaderState::new(self.core.my_id, &self.core.peers, last_index),
        );

        // Immediate heartbeat
        self.send_heartbeats();

        // NOOP to commit entries from previous terms
        let _ = self.propose_noop();
    }

    // === Tick ===

    pub fn tick(&mut self) {
        if self.core.shutdown {
            return;
        }

        // Poll async fsync
        if self.core.fsync_pending {
            if let Some(result) = self.core.log_store.fsync_poll() {
                match result {
                    Ok(()) => {
                        self.core.durable_index = self.core.log.last_index();
                        self.core.fsync_pending = false;

                        // Single node: can now commit
                        if self.core.num_nodes() == 1 && self.is_leader() {
                            self.core.commit_index = self.core.durable_index;
                        }
                    }
                    Err(e) => {
                        eprintln!("async fsync failed: {:?}", e); 
                        self.core.fsync_pending = false;
                    }
                }
            }
        }

        // Poll async snapshot
        if matches!(self.core.snapshot_state, SnapshotState::InProgress { .. }) {
            self.snapshot_poll();
        }

        let now = Instant::now();

        match &self.role {
            Role::Follower { .. }
            | Role::Candidate { .. }
            | Role::PreCandidate { .. } => {
                if now >= self.election_deadline {
                    if self.core.config.features.contains(Features::PREVOTE) {
                        self.start_prevote();
                    } else {
                        // become_candidate now sends RequestVote internally
                        self.become_candidate();
                    }
                }
            }
            Role::Leader(_) => {
                let heartbeat_interval = std::time::Duration::from_millis(
                    self.core.config.heartbeat_interval_ms as u64,
                );
                if now.duration_since(self.last_tick) >= heartbeat_interval {
                    self.send_heartbeats();
                    self.last_tick = now;
                }
            }
        }

        // Apply committed entries
        while self.core.last_applied < self.core.commit_index {
            let next = self.core.last_applied + 1;
            let entry = match self.core.log.get(next) {
                Some(e) => e.clone(),
                None => break, // gap, snapshot might fill it unsure.
            };

            self.core.last_applied = next;

            if let Err(e) = self.core.state_machine.apply(next, &entry) {
                eprintln!("apply failed at index {}: {:?}", next, e);
            }
        }

        // Auto-snapshot
        if self.core.config.features.contains(Features::AUTO_SNAPSHOT) {
            self.maybe_snapshot();
        }
    }

    // === Shutdown ===

    pub fn shutdown(&mut self) {
        self.core.shutdown = true;
    }

    pub fn is_shutdown(&self) -> bool {
        self.core.shutdown
    }

    // === Timers ===

    pub(crate) fn reset_election_deadline(&mut self) {
        let timeout = self.random_election_timeout();
        self.election_deadline = Instant::now()
            + std::time::Duration::from_millis(timeout);
    }

    pub(crate) fn random_election_timeout(&mut self) -> u64 {
        let min = self.core.config.election_timeout_min_ms as u64;
        let max = self.core.config.election_timeout_max_ms as u64;
        let range = max.saturating_sub(min);
        if range == 0 {
            return min;
        }

        // xorshift32 (Thanks Marsaglia!)
        self.prng ^= self.prng << 13;
        self.prng ^= self.prng >> 17;
        self.prng ^= self.prng << 5;

        min + (self.prng as u64 % range)
    }

    // === Helpers ===
    
    pub fn skip_next_fsync(&mut self) {
        self.core.skip_next_fsync = true;
    }
}

impl std::fmt::Debug for Raft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Raft")
            .field("id", &self.core.my_id)
            .field("term", &self.core.current_term)
            .field("role", &self.role)
            .field("commit_index", &self.core.commit_index)
            .field("log_len", &self.core.log.len())
            .finish()
    }
}