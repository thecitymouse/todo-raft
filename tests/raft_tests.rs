// tests/harness.rs — Simulated Raft cluster for testing
//
// The key idea: instead of real networking, all messages go into
// a shared queue. We manually deliver them between Raft nodes.
// This gives us total control — we can drop messages, delay them,
// partition nodes, etc.

use std::sync::{Arc, Mutex};

use lil_raft::*;

// ============================================================================
// Mock LogStore — in-memory, no disk
// ============================================================================

#[derive(Default)]
pub struct MemLogStore {
    term: Term,
    voted_for: Option<NodeId>,
    entries: Vec<(LogIndex, LogEntry)>,
    fsynced: bool,
}

impl LogStore for MemLogStore {
    fn persist_vote(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<()> {
        self.term = term;
        self.voted_for = voted_for;
        Ok(())
    }

    fn load_vote(&mut self) -> Result<(Term, Option<NodeId>)> {
        Ok((self.term, self.voted_for))
    }

    fn append(&mut self, index: LogIndex, entry: &LogEntry) -> Result<()> {
        self.entries.push((index, entry.clone()));
        self.fsynced = false;
        Ok(())
    }

    fn truncate_after(&mut self, index: LogIndex) -> Result<()> {
        self.entries.retain(|(idx, _)| *idx <= index);
        Ok(())
    }

    fn fsync(&mut self) -> Result<()> {
        self.fsynced = true;
        Ok(())
    }
}

// ============================================================================
// Mock StateMachine — records what was applied
// ============================================================================

#[derive(Default)]
pub struct MemStateMachine {
    pub applied: Vec<(LogIndex, LogEntry)>,
    pub snapshot: Option<LogPosition>,
}

impl StateMachine for MemStateMachine {
    fn apply(&mut self, index: LogIndex, entry: &LogEntry) -> Result<()> {
        self.applied.push((index, entry.clone()));
        Ok(())
    }

    fn snapshot_create(&mut self, pos: LogPosition) -> Result<()> {
        self.snapshot = Some(pos);
        Ok(())
    }

    fn snapshot_load(&mut self) -> Result<Option<LogPosition>> {
        Ok(self.snapshot)
    }

    fn snapshot_read(&self, _offset: u64, _buf: &mut [u8]) -> Result<(usize, bool)> {
        Ok((0, true))
    }

    fn snapshot_write(&mut self, _offset: u64, _data: &[u8], _done: bool) -> Result<()> {
        Ok(())
    }

    fn snapshot_restore(&mut self, pos: LogPosition) -> Result<()> {
        self.snapshot = Some(pos);
        Ok(())
    }
}

// ============================================================================
// Message envelope — everything that goes "over the network"
// ============================================================================

#[derive(Debug, Clone)]
pub enum Message {
    RequestVote(NodeId, RequestVote),
    RequestVoteResp(NodeId, RequestVoteResp),
    AppendEntries(NodeId, AppendEntries),
    AppendEntriesResp(NodeId, AppendEntriesResp),
    InstallSnapshot(NodeId, InstallSnapshot),
    InstallSnapshotResp(NodeId, InstallSnapshotResp),
    ReadIndex(NodeId, ReadIndexReq),
    ReadIndexResp(NodeId, ReadIndexResp),
    ReadIndexComplete(u64, Option<LogIndex>),
}

// ============================================================================
// Mock Transport — captures outgoing messages in a shared queue
// ============================================================================

pub struct MemTransport {
    pub from: NodeId,
    pub outbox: Arc<Mutex<Vec<(NodeId, Message)>>>,
}

impl Transport for MemTransport {
    fn send_request_vote(&self, to: NodeId, msg: &RequestVote) -> Result<()> {
        self.outbox.lock().unwrap().push((to, Message::RequestVote(self.from, msg.clone())));
        Ok(())
    }

    fn send_append_entries(&self, to: NodeId, msg: &AppendEntries) -> Result<()> {
        self.outbox.lock().unwrap().push((to, Message::AppendEntries(self.from, msg.clone())));
        Ok(())
    }

    fn send_install_snapshot(&self, to: NodeId, msg: &InstallSnapshot) -> Result<()> {
        self.outbox.lock().unwrap().push((to, Message::InstallSnapshot(self.from, msg.clone())));
        Ok(())
    }

    fn send_read_index(&self, to: NodeId, msg: &ReadIndexReq) -> Result<()> {
        self.outbox.lock().unwrap().push((to, Message::ReadIndex(self.from, msg.clone())));
        Ok(())
    }

    fn send_read_index_resp(&self, to: NodeId, resp: &ReadIndexResp) -> Result<()> {
        self.outbox.lock().unwrap().push((to, Message::ReadIndexResp(self.from, resp.clone())));
        Ok(())
    }

    fn on_read_index_complete(&self, req_id: u64, read_index: Option<LogIndex>) {
        self.outbox.lock().unwrap().push((
            self.from,
            Message::ReadIndexComplete(req_id, read_index),
        ));
    }
}

// ============================================================================
// Test Cluster — N raft nodes with message delivery
// ============================================================================

pub struct Cluster {
    pub nodes: Vec<Raft>,
    pub outbox: Arc<Mutex<Vec<(NodeId, Message)>>>,
}

impl Cluster {
    /// Create a cluster of `n` nodes with default config.
    pub fn new(n: u32) -> Self {
        let mut config = Config::default();
        // Fast elections for tests
        config.election_timeout_min_ms = 150;
        config.election_timeout_max_ms = 300;
        config.heartbeat_interval_ms = 50;

        Self::with_config(n, config)
    }

    pub fn with_config(n: u32, config: Config) -> Self {
        let outbox = Arc::new(Mutex::new(Vec::new()));
        let peers: Vec<NodeId> = (0..n).map(NodeId).collect();

        let mut nodes = Vec::new();
        for i in 0..n {
            let id = NodeId(i);
            let transport = MemTransport {
                from: id,
                outbox: Arc::clone(&outbox),
            };

            let raft = Raft::new(
                id,
                peers.clone(),
                config.clone(),
                Box::new(MemLogStore::default()),
                Box::new(MemStateMachine::default()),
                Box::new(transport),
            ).unwrap();

            nodes.push(raft);
        }

        Self { nodes, outbox }
    }

    /// Deliver all pending messages to their destinations.
    /// Returns the number of messages delivered.
    pub fn deliver_all(&mut self) -> usize {
        let mut total = 0;
        loop {
            let msgs: Vec<(NodeId, Message)> = self.outbox.lock().unwrap().drain(..).collect();
            if msgs.is_empty() { break; }
            total += msgs.len();

            for (to, msg) in msgs {
                let idx = to.0 as usize;
                if idx >= self.nodes.len() { continue; }
                let node = &mut self.nodes[idx];

                match msg {
                    Message::RequestVote(from, req) => {
                        let resp = node.recv_request_vote(&req);
                        self.outbox.lock().unwrap().push((
                            from,
                            Message::RequestVoteResp(to, resp),
                        ));
                    }
                    Message::RequestVoteResp(from, resp) => {
                        node.recv_requestvote_response(from, &resp);
                    }
                    Message::AppendEntries(from, req) => {
                        let resp = node.recv_append_entries(&req);
                        self.outbox.lock().unwrap().push((
                            from,
                            Message::AppendEntriesResp(to, resp),
                        ));
                    }
                    Message::AppendEntriesResp(from, resp) => {
                        node.recv_append_entries_response(from, &resp);
                    }
                    Message::InstallSnapshot(_, req) => {
                        let _resp = node.recv_install_snapshot(&req);
                    }
                    Message::InstallSnapshotResp(_, resp) => {
                        node.recv_install_snapshot_response(to, &resp);
                    }
                    Message::ReadIndex(_, req) => {
                        let _resp = node.recv_read_index(&req);
                    }
                    Message::ReadIndexResp(_, resp) => {
                        node.recv_read_index_response(&resp);
                    }
                    Message::ReadIndexComplete(..) => {
                        // captured for assertions — not delivered to raft
                    }
                }
            }
        }
        total
    }

    /// Trigger election timeout on a specific node.
    pub fn trigger_election(&mut self, id: NodeId) {
        let node = &mut self.nodes[id.0 as usize];
        // Force election by setting deadline to now
        node.election_deadline = std::time::Instant::now();
        node.tick();
    }

    /// Elect a specific node as leader. Triggers election + delivers messages.
    pub fn elect_leader(&mut self, id: NodeId) {
        self.trigger_election(id);
        // Deliver RequestVote + responses + initial heartbeat + responses
        for _ in 0..10 {
            if self.deliver_all() == 0 { break; }
        }
        assert!(
            self.nodes[id.0 as usize].is_leader(),
            "node {} failed to become leader", id.0
        );
    }

    /// Find current leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes.iter()
            .find(|n| n.is_leader())
            .map(|n| n.id())
    }

    /// Tick all nodes.
    pub fn tick_all(&mut self) {
        for node in &mut self.nodes {
            node.tick();
        }
    }

    /// Propose data on the leader. Panics if no leader.
    pub fn propose(&mut self, data: &[u8]) -> LogIndex {
        let leader_id = self.leader().expect("no leader");
        let idx = leader_id.0 as usize;
        self.nodes[idx].propose(data.to_vec()).expect("propose failed")
    }

    /// Propose + deliver until committed on majority.
    pub fn propose_and_commit(&mut self, data: &[u8]) -> LogIndex {
        let index = self.propose(data);
        for _ in 0..10 {
            if self.deliver_all() == 0 { break; }
        }
        // Tick to apply
        self.tick_all();
        index
    }
}