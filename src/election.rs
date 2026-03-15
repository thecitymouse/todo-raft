// election.rs — RequestVote RPC logic (including pre-vote)

use crate::types::*;
use crate::raft::{Raft, Role, Election};
use std::time::Instant;

/// Helper: rejection response to avoid borrowing self
fn reject(term: Term, kind: VoteKind) -> RequestVoteResp {
    RequestVoteResp { term, granted: false, kind }
}

/// this feels wrong because no return but whatever
fn log_is_up_to_date(ours: LogPosition, theirs: LogPosition) -> bool {
    (theirs.term, theirs.index) >= (ours.term, ours.index)
}

impl Raft {
    // ================================================================
    // Pre-vote
    // ================================================================

    pub fn start_prevote(&mut self) {
        if self.is_leader() {
            return;
        }

        let leader = self.leader_id();
        self.role = Role::PreCandidate {
            leader,
            election: Election::new(self.core.my_id),
        };
        self.reset_election_deadline();

        // Snapshot-aware last log position
        let mut last_log = self.core.log.last_position();
        if last_log.term == 0 && self.core.snapshot_last.term > 0 {
            last_log.term = self.core.snapshot_last.term;
        }

        let msg = RequestVote {
            term: self.core.current_term + 1, // hypothetical next term
            candidate_id: self.core.my_id,
            last_log,
            kind: VoteKind::PreVote,
        };

        for &peer_id in &self.core.peers {
            if peer_id != self.core.my_id {
                let _ = self.core.transport.send_request_vote(peer_id, &msg);
            }
        }

        // Single node cluster (we have majority)
        if let Role::PreCandidate { ref election, .. } = self.role {
            if election.tally() >= self.quorum() {
                self.become_candidate();
            }
        }
    }


    // This sends a request vote to all peers and is called automatically, but its still part of the crate for testing. 
    pub(crate) fn send_requestvote_all(&mut self) {
        // Snapshot-aware last log position
        let mut last_log = self.core.log.last_position();
        if last_log.term == 0 && self.core.snapshot_last.term > 0 {
            last_log.term = self.core.snapshot_last.term;
        }

        let req = RequestVote {
            term: self.core.current_term,
            candidate_id: self.core.my_id,
            last_log,
            kind: VoteKind::Vote,
        };

        for &peer_id in &self.core.peers {
            if peer_id != self.core.my_id {
                let _ = self.core.transport.send_request_vote(peer_id, &req);
            }
        }
    }

    // ================================================================
    // Handle incoming RequestVote (pre-vote or real)
    // ================================================================

    pub fn recv_request_vote(&mut self, req: &RequestVote) -> RequestVoteResp {
        // ---- Pre-vote ----
        if req.kind == VoteKind::PreVote {
            if req.term < self.core.current_term {
                return reject(self.core.current_term, VoteKind::PreVote);
            }

            let ours = self.core.log.last_position();
            if !log_is_up_to_date(ours, req.last_log) {
                return reject(self.core.current_term, VoteKind::PreVote);
            }

            // Reject if live leader
            if self.leader_id().is_some() && Instant::now() < self.election_deadline {
                return reject(self.core.current_term, VoteKind::PreVote);
            }

            return RequestVoteResp {
                term: self.core.current_term,
                granted: true,
                kind: VoteKind::PreVote,
            };
        }

        // ---- Real vote ----
        if req.term < self.core.current_term {
            return reject(self.core.current_term, VoteKind::Vote);
        }

        if req.term > self.core.current_term {
            self.become_follower(req.term, None);
        }

        let can_vote = self.core.voted_for.is_none()
            || self.core.voted_for == Some(req.candidate_id);

        if !can_vote {
            return reject(self.core.current_term, VoteKind::Vote);
        }

        let ours = self.core.log.last_position();
        if !log_is_up_to_date(ours, req.last_log) {
            return reject(self.core.current_term, VoteKind::Vote);
        }

        // Grant vote
        self.core.voted_for = Some(req.candidate_id);
        self.reset_election_deadline();
        let _ = self.core.log_store.persist_vote(
            self.core.current_term,
            self.core.voted_for,
        );

        RequestVoteResp {
            term: self.core.current_term,
            granted: true,
            kind: VoteKind::Vote,
        }
    }

    // ================================================================
    // Handle RequestVote response (pre-vote or real)
    // ================================================================

    pub fn recv_requestvote_response(
        &mut self,
        peer_id: NodeId,
        resp: &RequestVoteResp,
    ) {
        // prevote response
        if resp.kind == VoteKind::PreVote {
            if resp.term > self.core.current_term {
                self.become_follower(resp.term, None);
                return;
            }

            let Role::PreCandidate { ref mut election, .. } = self.role else {
                return;
            };

            if resp.granted {
                election.votes.insert(peer_id);
                if election.tally() >= self.quorum() {
                    // become_candidate sends RequestVote internally
                    self.become_candidate();
                }
            }
            return;
        }

        // real vote response
        if resp.term > self.core.current_term {
            self.become_follower(resp.term, None);
            return;
        }

        if resp.term < self.core.current_term {
            return; // stale term
        }

        let Role::Candidate { ref mut election, .. } = self.role else {
            return;
        };

        if resp.granted {
            election.votes.insert(peer_id);
            if election.tally() >= self.quorum() {
                self.become_leader();
            }
        }
    }
}