// readindex.rs — ReadIndex for linearizable reads 

use std::collections::HashSet;
use crate::types::*;
use crate::raft::{Raft, Role, LeaderState, PendingRead};

const MAX_PENDING_READS: usize = 64;

impl Raft {
    /// Queue a read on the leader.  Takes &mut LeaderState directly
    /// so we don't borrow all of self.
    fn try_queue_read(
        leader: &mut LeaderState,
        my_id: NodeId,
        current_term: Term,
        commit_index: LogIndex,
        req_id: u64,
        from: Option<NodeId>,
    ) -> bool {
        if leader.pending_reads.len() >= MAX_PENDING_READS {
            return false;
        }

        let mut acks = HashSet::new();
        acks.insert(my_id);

        leader.pending_reads.push(PendingRead {
            req_id,
            from,
            commit_index,
            term: current_term,
            acks,
            required_seq: leader.heartbeat_seq + 1,
        });

        true
    }

    // ====================================================================
    // Request read index its async and can work from leader and followers.
    // ====================================================================

    pub fn request_read_index(&mut self, req_id: u64) -> Result<()> {
        if self.is_leader() {
            // Single node — complete immediately
            if self.num_nodes() == 1 {
                self.core.transport.on_read_index_complete(
                    req_id,
                    Some(self.core.commit_index),
                );
                return Ok(());
            }
            
            let queued = {
                let Role::Leader(ref mut leader) = self.role else {
                    return Err(RaftError::NotLeader);
                };
                Self::try_queue_read(
                    leader,
                    self.core.my_id,
                    self.core.current_term,
                    self.core.commit_index,
                    req_id,
                    None, // None = local leader request
                )
            };

            if !queued {
                return Err(RaftError::QueueFull);
            }

            self.send_heartbeats();
            return Ok(());
        }

        // Follower path, forward to leader
        let leader_id = self.leader_id().ok_or(RaftError::NotLeader)?;
        let req = ReadIndexReq { req_id, from: self.core.my_id };
        self.core.transport.send_read_index(leader_id, &req)
    }

    // ====================================================================
    // Handle incoming ReadIndex request (leader only)
    // ====================================================================

    pub fn recv_read_index(&mut self, req: &ReadIndexReq) -> ReadIndexResp {
        let mut resp = ReadIndexResp {
            req_id: req.req_id,
            read_index: None,
        };

        if !self.is_leader() {
            return resp;
        }

        // Single node, respond immediately because we are the quorum or well majority 
        if self.num_nodes() == 1 {
            resp.read_index = Some(self.core.commit_index);
            return resp;
        }

        if let Role::Leader(ref mut leader) = self.role {
            Self::try_queue_read(
                leader,
                self.core.my_id,
                self.core.current_term,
                self.core.commit_index,
                req.req_id,
                Some(req.from),
            );
        }

        // async sent after heartbeat queue
        resp
    }

    // ====================================================================
    // Handle ReadIndex response (follower only)
    // ====================================================================

    pub fn recv_read_index_response(&mut self, resp: &ReadIndexResp) {
        self.core.transport.on_read_index_complete(
            resp.req_id,
            resp.read_index,
        );
    }

    // ====================================================================
    // Record heartbeat ack for ReadIndex quorum tracking
    // ====================================================================

    pub fn record_read_index_ack(&mut self, peer_id: NodeId, seq: u64) {
        let current_term = self.core.current_term;

        if let Role::Leader(ref mut leader) = self.role {
            for pending in &mut leader.pending_reads {
                if pending.term == current_term && seq >= pending.required_seq {
                    pending.acks.insert(peer_id);
                }
            }
        }

        self.check_read_index_quorum();
    }

    // ====================================================================
    // Check and complete pending reads that have quorum
    // ====================================================================

    pub fn check_read_index_quorum(&mut self) {
        if !self.is_leader() { return; }

        let quorum = self.quorum();
        let current_term = self.core.current_term;
        let commit_index = self.core.commit_index;

        let Role::Leader(ref mut leader) = self.role else { return };

        // Walk the pending_reads vec using swap_remove for completed ones.
        // swap_remove is O(1) but changes ordering but it should be fine since pending reads have no ordering guarantees.
        let mut i = 0;
        while i < leader.pending_reads.len() {
            let p = &leader.pending_reads[i];

            // Stale term, fail
            if p.term != current_term {
                let from = p.from;
                let req_id = p.req_id;
                leader.pending_reads.swap_remove(i);

                match from {
                    Some(peer) => {
                        let resp = ReadIndexResp { req_id, read_index: None };
                        let _ = self.core.transport.send_read_index_resp(peer, &resp);
                    }
                    None => {
                        self.core.transport.on_read_index_complete(req_id, None);
                    }
                }
                continue; // swap_remove moved a new element to [i]
            }

            // Have quorum?
            if p.acks.len() >= quorum {
                let from = p.from;
                let req_id = p.req_id;
                leader.pending_reads.swap_remove(i);

                match from {
                    Some(peer) => {
                        let resp = ReadIndexResp {
                            req_id,
                            read_index: Some(commit_index),
                        };
                        let _ = self.core.transport.send_read_index_resp(peer, &resp);
                    }
                    None => {
                        self.core.transport.on_read_index_complete(
                            req_id,
                            Some(commit_index),
                        );
                    }
                }
                continue;
            }

            i += 1;
        }
    }

    // ====================================================================
    // Clear pending reads ie (on term change / step down)
    //
    // Called from become_follower().  Fails all in-flight reads with None.
    // ====================================================================

    pub fn clear_pending_reads(&mut self) {
        let Role::Leader(ref mut leader) = self.role else { return };

        for p in leader.pending_reads.drain(..) {
            match p.from {
                Some(peer) => {
                    let resp = ReadIndexResp {
                        req_id: p.req_id,
                        read_index: None,
                    };
                    let _ = self.core.transport.send_read_index_resp(peer, &resp);
                }
                None => {
                    self.core.transport.on_read_index_complete(p.req_id, None);
                }
            }
        }
    }
}