// snapshot.rs — Snapshot creation, transfer, and recovery

use crate::types::*;
use crate::raft::{Raft, Role, SnapshotTransfer};

pub const SNAPSHOT_CHUNK_SIZE: usize = 65536; // 64 KB

impl Raft {
    // ====================================================================
    // Async snapshot polling (called from tick)
    // ====================================================================

    pub(crate) fn snapshot_poll(&mut self) {
        let SnapshotState::InProgress { .. } = self.core.snapshot_state else {
            return;
        };
        if self.core.state_machine.snapshot_poll() {
            self.finish_snapshot();
        }
    }

    fn finish_snapshot(&mut self) {
        let SnapshotState::InProgress { pending_index, pending_term } =
            self.core.snapshot_state else { return };

        self.core.snapshot_last = LogPosition {
            index: pending_index,
            term: pending_term,
        };
        self.core.log.compact(pending_index);
        self.core.snapshot_state = SnapshotState::Idle;
    }

    // ====================================================================
    // Maybe create snapshot (called from tick when AUTO_SNAPSHOT)
    // ====================================================================

    pub(crate) fn maybe_snapshot(&mut self) {
        if matches!(self.core.snapshot_state, SnapshotState::InProgress { .. }) {
            return;
        }

        let threshold = match self.core.config.snapshot_threshold {
            Some(t) => t,
            None => return,
        };

        let log_growth = self.core.log.last_index()
            .saturating_sub(self.core.snapshot_last.index);
        if log_growth < threshold { return; }

        if self.core.last_applied <= self.core.snapshot_last.index {
            return; // nothing new to snapshot
        }

        let _ = self.create_snapshot();
    }

    // ====================================================================
    // Create snapshot at current last_applied
    // ====================================================================

    pub fn create_snapshot(&mut self) -> Result<()> {
        if matches!(self.core.snapshot_state, SnapshotState::InProgress { .. }) {
            return Err(RaftError::SnapshotInProgress);
        }

        let snap_index = self.core.last_applied;

        // Get term — check in-memory log first, then snapshot boundary
        let snap_term = self.core.log.term_at(snap_index)
            .or_else(|| {
                if snap_index == self.core.snapshot_last.index {
                    Some(self.core.snapshot_last.term)
                } else {
                    None
                }
            })
            .ok_or_else(|| RaftError::Storage(
                format!("cannot snapshot: entry {} not found", snap_index),
            ))?;

        let pos = LogPosition { index: snap_index, term: snap_term };

        self.core.snapshot_state = SnapshotState::InProgress {
            pending_index: snap_index,
            pending_term: snap_term,
        };

        if let Err(e) = self.core.state_machine.snapshot_create(pos) {
            self.core.snapshot_state = SnapshotState::Idle;
            return Err(e);
        }

        // If synchronous (snapshot_poll returns true immediately), finish now
        if self.core.state_machine.snapshot_poll() {
            self.finish_snapshot();
        }

        Ok(())
    }

    // ====================================================================
    // Check if peer needs snapshot (leader only)
    // ====================================================================

    pub(crate) fn peer_needs_snapshot(&self, peer_id: NodeId) -> bool {
        let Role::Leader(ref leader) = self.role else { return false };
        let Some(peer) = leader.peers.get(&peer_id) else { return false };

        peer.next_index <= self.core.log.base()
    }
    // ====================================================================
    // Send InstallSnapshot (leader → follower)
    // ====================================================================

    pub fn send_install_snapshot(&mut self, peer_id: NodeId) -> Result<()> {
        if !self.is_leader() { return Err(RaftError::NotLeader); }

        let offset = if let Role::Leader(ref leader) = self.role {
            leader.snapshot_transfers.get(&peer_id).map_or(0, |t| t.offset)
        } else {
            0
        };

        let mut buf = vec![0u8; SNAPSHOT_CHUNK_SIZE];
        let (n, done) = self.core.state_machine.snapshot_read(offset, &mut buf)?;
        buf.truncate(n);

        let req = InstallSnapshot {
            term: self.core.current_term,
            leader_id: self.core.my_id,
            last_included: self.core.snapshot_last,
            offset,
            data: buf,
            done,
        };

        self.core.transport.send_install_snapshot(peer_id, &req)?;

        if let Role::Leader(ref mut leader) = self.role {
            let transfer = leader.snapshot_transfers.entry(peer_id)
                .or_insert(SnapshotTransfer { offset: 0, last_sent: None });
            transfer.offset = offset;
            transfer.last_sent = Some(std::time::Instant::now());
        }

        Ok(())
    }

    // ====================================================================
    // Handle InstallSnapshot (follower)
    // ====================================================================

    pub fn recv_install_snapshot(&mut self, req: &InstallSnapshot) -> InstallSnapshotResp {
        let mut resp = InstallSnapshotResp {
            term: self.core.current_term,
            success: false,
            next_offset: req.offset,
            done: false,
        };

        // Stale term
        if req.term < self.core.current_term { return resp; }

        // Update term / recognize leader
        if req.term > self.core.current_term
            || !matches!(self.role, Role::Follower { .. })
        {
            self.become_follower(req.term, Some(req.leader_id));
        } else if let Role::Follower { ref mut leader } = self.role {
            *leader = Some(req.leader_id);
        }
        self.reset_election_deadline();
        resp.term = self.core.current_term;

        // Already have this or newer snapshot
        if req.last_included.index <= self.core.snapshot_last.index {
            resp.success = true;
            return resp;
        }

        // Write chunk
        if let Err(e) = self.core.state_machine.snapshot_write(
            req.offset, &req.data, req.done,
        ) {
            eprintln!("snapshot_write failed: {:?}", e);
            return resp;
        }

        resp.success = true;
        resp.next_offset = req.offset + req.data.len() as u64;
        resp.done = req.done;

        // Last chunk — restore state machine
        if req.done {
            if let Err(e) = self.core.state_machine.snapshot_restore(req.last_included) {
                eprintln!("snapshot_restore failed: {:?}", e);
                resp.success = false;
                return resp;
            }

            self.core.snapshot_last = req.last_included;
            self.core.log.reset(req.last_included.index);

            if req.last_included.index > self.core.commit_index {
                self.core.commit_index = req.last_included.index;
            }
            if req.last_included.index > self.core.last_applied {
                self.core.last_applied = req.last_included.index;
            }
        }

        resp
    }

    // ====================================================================
    // Handle InstallSnapshot response (leader)
    // ====================================================================

    pub fn recv_install_snapshot_response(
        &mut self,
        peer_id: NodeId,
        resp: &InstallSnapshotResp,
    ) {
        if !self.is_leader() { return; }

        if resp.term > self.core.current_term {
            self.become_follower(resp.term, None);
            return;
        }

        let Role::Leader(ref mut leader) = self.role else { return };

        if resp.success {
            if let Some(transfer) = leader.snapshot_transfers.get_mut(&peer_id) {
                transfer.offset = resp.next_offset;
                if resp.done {
                    transfer.offset = 0;
                    transfer.last_sent = None;
                    if let Some(peer) = leader.peers.get_mut(&peer_id) {
                        peer.next_index = self.core.snapshot_last.index + 1;
                        peer.match_index = self.core.snapshot_last.index;
                    }
                }
            }
        } else if let Some(transfer) = leader.snapshot_transfers.get_mut(&peer_id) {
            transfer.offset = 0;
            transfer.last_sent = None;
        }
    }

    // ====================================================================
    // Query
    // ====================================================================

    pub fn snapshot_info(&self) -> LogPosition {
        self.core.snapshot_last
    }
}