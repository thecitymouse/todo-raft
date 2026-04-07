

use crate::types::*;
use crate::raft::{Raft, Role};

const REPLICATION_BATCH_MAX: usize = 64;

// ============================================================================
// Heartbeats / AppendEntries — leader side
// ============================================================================

impl Raft {
    pub fn send_heartbeats(&mut self) {
        let Role::Leader(ref mut leader) = self.role else { return };

        leader.heartbeat_seq += 1;

        // Collect what we need from leader state before we borrow core.
        let seq = leader.heartbeat_seq;
        let peer_next: Vec<(NodeId, LogIndex)> = leader.peers
            .iter()
            .map(|(&id, p)| (id, p.next_index))
            .collect();

        let snapshot_in_progress: Vec<NodeId> = leader.snapshot_transfers
            .iter()
            .filter(|(_, t)| t.last_sent.is_some())
            .map(|(&id, _)| id)
            .collect();

        let max_entries = self.core.config.max_entries_per_msg
            .min(REPLICATION_BATCH_MAX);

        for (peer_id, next_idx) in peer_next {
            // peer_ids includes self in the vec, skip it
            if peer_id == self.core.my_id { continue; }

            // Peer too far behind we need a snapshot
            if self.peer_needs_snapshot(peer_id) {
                if !snapshot_in_progress.contains(&peer_id) {
                    let _ = self.send_install_snapshot(peer_id);
                }
                continue;
            }

            let prev_idx = next_idx.saturating_sub(1);
            let prev_term = if prev_idx == 0 {
                0
            } else {
                self.core.term_at_or_snapshot(prev_idx).unwrap_or(0)
            };

            let entries: Vec<LogEntry> = self.core.log
                .slice(next_idx, next_idx + max_entries as u64)
                .to_vec();

            let req = AppendEntries {
                term: self.core.current_term,
                leader_id: self.core.my_id,
                prev_log: LogPosition { index: prev_idx, term: prev_term },
                leader_commit: self.core.commit_index,
                seq,
                entries,
            };

            let _ = self.core.transport.send_append_entries(peer_id, &req);
        }
    }
}

// ============================================================================
// Handle incoming AppendEntries — follower side
// ============================================================================

impl Raft {
    pub fn recv_append_entries(&mut self, req: &AppendEntries) -> AppendEntriesResp {
        let mut resp = AppendEntriesResp {
            term: self.core.current_term,
            success: false,
            conflict: None,
            match_index: 0,
            seq: req.seq,
        };

        // stale term
        if req.term < self.core.current_term {
            return resp;
        }

        // Recognize leader
        if req.term > self.core.current_term
            || !matches!(self.role, Role::Follower { .. })
        {
            self.become_follower(req.term, Some(req.leader_id));
        } else if let Role::Follower { ref mut leader } = self.role {
            *leader = Some(req.leader_id);
        }
        self.reset_election_deadline();
        resp.term = self.core.current_term;

        // Rule 2: prev log consistency
        if req.prev_log.index > 0 {
            match self.check_prev_log(&req.prev_log) {
                PrevLogCheck::Ok => {}
                PrevLogCheck::SnapshotMismatch => {
                    resp.match_index = 0;
                    return resp;
                }
                PrevLogCheck::BeforeSnapshot => {
                    resp.match_index = self.core.snapshot_last.index;
                    return resp;
                }
                PrevLogCheck::Missing => {
                    resp.match_index = self.core.log.last_index();
                    return resp;
                }
                PrevLogCheck::Conflict { term, first_index } => {
                    resp.conflict = Some(LogPosition { index: first_index, term });
                    self.core.log.truncate_after(req.prev_log.index - 1);
                    resp.match_index = req.prev_log.index - 1;
                    return resp;
                }
            }
        }

        // Rules 3 & 4: append new entries
        let mut idx = req.prev_log.index + 1;
        let mut persist_failed = false;

        for entry in &req.entries {
            if let Some(existing_term) = self.core.log.term_at(idx) {
                if existing_term == entry.term {
                    idx += 1;
                    continue; // already have this entry
                }
                self.core.log.truncate_after(idx - 1);
            }
            self.core.log.push(entry.clone());

            if let Err(e) = self.core.log_store.append(idx, entry) {
                eprintln!("log_append failed at {}: {:?}", idx, e);
                // Roll back this entry from in-memory log so it stays
                // consistent with what's actually persisted, then bail.
                self.core.log.truncate_after(idx - 1);
                persist_failed = true;
                break;
            }
            idx += 1;
        }

        // If persistence failed, DON'T tell the leader we have these entries.
        // They'll retry on the next heartbeat. This prevents counting an
        // unreliable follower toward the commit quorum.
        if persist_failed {
            resp.match_index = self.core.log.last_index();
            return resp; // success stays false
        }

        if !req.entries.is_empty() {
            if let Err(e) = self.core.log_store.fsync() {
                // fsync failed — same deal: don't claim we have durable entries.
                eprintln!("fsync failed: {:?}", e);
                resp.match_index = self.core.log.last_index();
                return resp;
            }
        }

        // Rule 5: advance commit index
        if req.leader_commit > self.core.commit_index {
            let last_new = req.prev_log.index + req.entries.len() as u64;
            self.core.commit_index = req.leader_commit
                .min(last_new)
                .min(self.core.log.last_index());
        }

        resp.success = true;
        resp.match_index = self.core.log.last_index();
        resp
    }

    /// Check whether our log matches the leader's prev_log entry.
    fn check_prev_log(&self, prev: &LogPosition) -> PrevLogCheck {
        // Check snapshot boundary
        if prev.index == self.core.snapshot_last.index {
            return if self.core.snapshot_last.term == prev.term {
                PrevLogCheck::Ok
            } else {
                PrevLogCheck::SnapshotMismatch
            };
        }

        if prev.index < self.core.snapshot_last.index {
            return PrevLogCheck::BeforeSnapshot;
        }

        match self.core.log.term_at(prev.index) {
            None => PrevLogCheck::Missing,
            Some(t) if t != prev.term => {
                let first = self.core.log.find_first_of_term(t)
                    .unwrap_or(prev.index);
                PrevLogCheck::Conflict { term: t, first_index: first }
            }
            Some(_) => PrevLogCheck::Ok,
        }
    }
}

enum PrevLogCheck {
    Ok,
    SnapshotMismatch,
    BeforeSnapshot,
    Missing,
    Conflict { term: Term, first_index: LogIndex },
}

// ============================================================================
// Handle AppendEntries response — leader side
// ============================================================================

impl Raft {
    pub fn recv_append_entries_response(
        &mut self,
        peer_id: NodeId,
        resp: &AppendEntriesResp,
    ) {
        if !self.is_leader() { return; }

        if resp.term > self.core.current_term {
            self.become_follower(resp.term, None);
            return;
        }
        if resp.term < self.core.current_term { return; } // stale

        if resp.success {
            self.handle_successful_ae(peer_id, resp);
        } else {
            self.handle_failed_ae(peer_id, resp);
        }
    }

    fn handle_successful_ae(&mut self, peer_id: NodeId, resp: &AppendEntriesResp) {
        let Role::Leader(ref mut leader) = self.role else { return };

        // Update peer progress
        if let Some(peer) = leader.peers.get_mut(&peer_id) {
            if resp.match_index > peer.match_index {
                peer.match_index = resp.match_index;
                peer.next_index = resp.match_index + 1;
            }
        }

        // Clear snapshot transfer — peer is caught up
        if let Some(st) = leader.snapshot_transfers.get_mut(&peer_id) {
            st.offset = 0;
            st.last_sent = None;
        }

        // Record ack for ReadIndex quorum tracking
        self.record_read_index_ack(peer_id, resp.seq);

        // Advance commit index
        self.try_advance_commit();
    }

    fn handle_failed_ae(&mut self, peer_id: NodeId, resp: &AppendEntriesResp) {
        // Fast rollback using conflict info
        let new_next = resp.conflict.and_then(|conflict| {
            if conflict.term == 0 { return None; }
            match self.core.log.find_last_of_term(conflict.term) {
                Some(our_last) => Some(our_last + 1),
                None => Some(conflict.index),
            }
        });

        let Role::Leader(ref mut leader) = self.role else { return };
        let Some(peer) = leader.peers.get_mut(&peer_id) else { return };

        if let Some(idx) = new_next {
            peer.next_index = idx;
        } else if resp.match_index > 0 && resp.match_index < peer.next_index {
            peer.next_index = resp.match_index + 1;
        } else if peer.next_index > 1 {
            peer.next_index -= 1;
        }
    }
    
    // Find the highest N such that a majority has match_index >= N and log[N].term == current_term.  Then set commit_index = N.
    fn try_advance_commit(&mut self) {
        let Role::Leader(ref leader) = self.role else { return };
        let quorum = self.core.quorum();

        for n in (self.core.commit_index + 1)..=self.core.log.last_index() {
            let Some(term) = self.core.log.term_at(n) else { continue };
            if term != self.core.current_term { continue; }

            // Count self + peers
            let count = 1 + leader.peers.values()
                .filter(|p| p.match_index >= n)
                .count();

            if count >= quorum {
                self.core.commit_index = n;
            }
        }
    }
}

// ============================================================================
// Propose (client writes)
// ============================================================================

impl Raft {
    pub fn propose(&mut self, data: Vec<u8>) -> Result<LogIndex> {
        self.propose_batch(&[data])
    }

    pub fn propose_batch(&mut self, entries: &[Vec<u8>]) -> Result<LogIndex> {
        if self.core.shutdown { return Err(RaftError::Shutdown); }
        if !self.is_leader() { return Err(RaftError::NotLeader); }
        if entries.is_empty() { return Err(RaftError::Storage("empty batch".into())); }

        let first_index = self.core.log.last_index() + 1;

        for (i, data) in entries.iter().enumerate() {
            let idx = first_index + i as u64;
            let entry = LogEntry::data(self.core.current_term, data.clone());
            self.core.log.push(entry);

            let stored = self.core.log.get(idx).unwrap();
            if let Err(e) = self.core.log_store.append(idx, stored) {
                // Rollback BOTH in-memory and durable log so they stay in sync.
                // Without the durable truncate, a restart could surface entries
                // that the in-memory log doesn't know about — causing log
                // divergence or duplicate applies.
                self.core.log.truncate_after(first_index - 1);
                let _ = self.core.log_store.truncate_after(first_index - 1);
                return Err(e);
            }
        }

        let last_index = self.core.log.last_index();
        self.do_fsync(last_index)?;

        // Single node — commit immediately (if durable)
        if self.core.num_nodes() == 1 && self.core.durable_index >= last_index {
            self.core.commit_index = last_index;
        }

        self.send_heartbeats();
        Ok(last_index)
    }

    pub fn propose_noop(&mut self) -> Result<LogIndex> {
        if self.core.shutdown { return Err(RaftError::Shutdown); }
        if !self.is_leader() { return Err(RaftError::NotLeader); }

        let index = self.core.log.last_index() + 1;
        let entry = LogEntry::noop(self.core.current_term);
        self.core.log.push(entry);

        let stored = self.core.log.get(index).unwrap();
        if let Err(e) = self.core.log_store.append(index, stored) {
            self.core.log.truncate_after(index - 1);
            return Err(e);
        }

        self.do_fsync(index)?;

        if self.core.num_nodes() == 1 && self.core.durable_index >= index {
            self.core.commit_index = index;
        }

        self.send_heartbeats();
        Ok(index)
    }

    /// Handle fsync: async if available, blocking fallback.
    /// Respects skip_next_fsync for drain proposals.
    fn do_fsync(&mut self, last_index: LogIndex) -> Result<()> {
        if self.core.skip_next_fsync {
            self.core.skip_next_fsync = false;
            self.core.durable_index = last_index;
            return Ok(());
        }

        // Try async submit first
        match self.core.log_store.fsync_submit() {
            Ok(()) => {
                self.core.fsync_pending = true;
                Ok(())
            }
            Err(_) => {
                // Fallback: blocking fsync
                self.core.log_store.fsync()?;
                self.core.durable_index = last_index;
                Ok(())
            }
        }
    }
}