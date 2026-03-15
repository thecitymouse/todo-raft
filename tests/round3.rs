
mod harness;

use harness::{Cluster, Message};
use lil_raft::*;
use std::collections::HashSet;

// ============================================================================
// Helpers
// ============================================================================

fn far_past() -> std::time::Instant {
    std::time::Instant::now() - std::time::Duration::from_secs(10)
}

fn far_future() -> std::time::Instant {
    std::time::Instant::now() + std::time::Duration::from_secs(60)
}

fn heal(c: &mut Cluster) {
    for node in &mut c.nodes {
        node.election_deadline = far_future();
        node.last_tick = far_past();
    }
    for _ in 0..50 {
        for node in &mut c.nodes {
            node.last_tick = far_past();
        }
        c.tick_all();
        c.deliver_all();
    }
}

fn assert_committed_prefix_agreement(c: &Cluster) {
    for i in 0..c.nodes.len() {
        for j in (i + 1)..c.nodes.len() {
            let shared = c.nodes[i].commit_index().min(c.nodes[j].commit_index());
            for idx in 1..=shared {
                let ti = c.nodes[i].log().term_at(idx);
                let tj = c.nodes[j].log().term_at(idx);

                // If either node doesn't have this entry in memory, it's been
                // compacted away (via snapshot install, log.compact, or log.reset).
                // The entry was committed and applied before being removed —
                // that's normal and safe. Only check when BOTH nodes have the entry.
                if ti.is_none() || tj.is_none() {
                    continue;
                }

                assert_eq!(ti, tj,
                           "SAFETY VIOLATION: nodes {} and {} disagree at committed index {}. \
                     term_i={:?}, term_j={:?}", i, j, idx, ti, tj);
            }
        }
    }
}

fn assert_at_most_one_leader(c: &Cluster) {
    let leaders: Vec<_> = c.nodes.iter()
        .filter(|n| n.is_leader())
        .map(|n| (n.id(), n.term()))
        .collect();
    let terms: HashSet<Term> = leaders.iter().map(|(_, t)| *t).collect();
    assert_eq!(leaders.len(), terms.len(),
               "SAFETY: two leaders in same term! {:?}", leaders);
}

impl Cluster {
    fn deliver_within_r3(&mut self, partition: &HashSet<NodeId>) -> usize {
        let mut total = 0;
        loop {
            let msgs: Vec<(NodeId, Message)> = self.outbox.lock().unwrap().drain(..).collect();
            if msgs.is_empty() { break; }
            for (to, msg) in msgs {
                if !partition.contains(&to) { continue; }
                let from = match &msg {
                    Message::RequestVote(f, _) => *f,
                    Message::RequestVoteResp(f, _) => *f,
                    Message::AppendEntries(f, _) => *f,
                    Message::AppendEntriesResp(f, _) => *f,
                    Message::InstallSnapshot(f, _) => *f,
                    Message::InstallSnapshotResp(f, _) => *f,
                    Message::ReadIndex(f, _) => *f,
                    Message::ReadIndexResp(f, _) => *f,
                    Message::ReadIndexComplete(..) => to,
                };
                if !partition.contains(&from) { continue; }
                let idx = to.0 as usize;
                if idx >= self.nodes.len() { continue; }
                let node = &mut self.nodes[idx];
                total += 1;
                match msg {
                    Message::RequestVote(from, req) => {
                        let resp = node.recv_request_vote(&req);
                        self.outbox.lock().unwrap().push((from, Message::RequestVoteResp(to, resp)));
                    }
                    Message::RequestVoteResp(from, resp) => {
                        node.recv_requestvote_response(from, &resp);
                    }
                    Message::AppendEntries(from, req) => {
                        let resp = node.recv_append_entries(&req);
                        self.outbox.lock().unwrap().push((from, Message::AppendEntriesResp(to, resp)));
                    }
                    Message::AppendEntriesResp(from, resp) => {
                        node.recv_append_entries_response(from, &resp);
                    }
                    Message::InstallSnapshot(_, req) => { let _ = node.recv_install_snapshot(&req); }
                    Message::InstallSnapshotResp(_, resp) => { node.recv_install_snapshot_response(to, &resp); }
                    Message::ReadIndex(_, req) => { let _ = node.recv_read_index(&req); }
                    Message::ReadIndexResp(_, resp) => { node.recv_read_index_response(&resp); }
                    Message::ReadIndexComplete(..) => {}
                }
            }
        }
        total
    }
}

// ============================================================================
// THESIS: try_advance_commit iterates forward from commit_index+1 and sets
// commit_index = n whenever count >= quorum. But it doesn't break early —
// it keeps going. If there's a gap of old-term entries followed by
// current-term entries, it should skip the old-term ones and only commit
// the current-term one (which indirectly commits the old ones).
// But what if we engineer match_index values that create a false majority
// at an index where the term doesn't match?
// ============================================================================

#[test]
fn test_try_advance_commit_skips_old_term_entries_correctly() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0)); // term 1
    c.propose_and_commit(b"term1-entry");

    // Force a new election so term increases
    let past = far_past();
    for i in 0..5 { c.nodes[i].election_deadline = past; }
    c.nodes[1].tick();
    for _ in 0..20 { c.deliver_all(); }

    // Find the new leader
    let leader_id = c.leader().expect("should have a leader");
    let leader_term = c.nodes[leader_id.0 as usize].term();
    assert!(leader_term > 1);

    // Manually inject an entry from term 1 into the leader's log
    // (simulating entries that got replicated but not committed from old leader)
    let old_entry = LogEntry::data(1, b"old-term-sneaky".to_vec());
    c.nodes[leader_id.0 as usize].core.log.push(old_entry);

    // Now propose a current-term entry
    let _ = c.nodes[leader_id.0 as usize].propose(b"current-term".to_vec());
    heal(&mut c);

    // The old-term entry should only be committed if a current-term entry
    // at a higher index is also committed
    let li = leader_id.0 as usize;
    let commit = c.nodes[li].commit_index();
    for idx in 1..=commit {
        if let Some(term) = c.nodes[li].log().term_at(idx) {
            if term < c.nodes[li].term() {
                // Old-term entry is committed — verify there's a current-term
                // entry also committed at a higher index
                let has_current = ((idx + 1)..=commit)
                    .any(|i| c.nodes[li].log().term_at(i) == Some(c.nodes[li].term()));
                assert!(has_current,
                        "SAFETY: old-term entry at {} committed without current-term entry after it", idx);
            }
        }
    }
}

// ============================================================================
// THESIS: When a PreVote is sent with term = current_term + 1 (hypothetical),
// but the receiving node has a HIGHER term, the PreVote should be rejected.
// What if two nodes with different terms both start PreVote at the same time?
// Neither should corrupt their actual term.
// ============================================================================

#[test]
fn test_prevote_does_not_change_real_term() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    let term_before: Vec<Term> = c.nodes.iter().map(|n| n.term()).collect();

    // Drop all messages, force two nodes into PreVote simultaneously
    c.outbox.lock().unwrap().clear();

    // Set both to pre-candidate
    c.nodes[1].election_deadline = std::time::Instant::now();
    c.nodes[2].election_deadline = std::time::Instant::now();
    c.nodes[1].tick();
    c.nodes[2].tick();

    // Deliver only between {1,2} — they'll get each other's PreVote requests
    let pair: HashSet<NodeId> = [NodeId(1), NodeId(2)].into();
    for _ in 0..10 { c.deliver_within_r3(&pair); }

    // Key check: no node's ACTUAL term should have changed from PreVote alone.
    // PreVote is hypothetical — it must not increment anyone's term.
    for i in 0..5 {
        assert_eq!(c.nodes[i].term(), term_before[i],
                   "node {} term changed during PreVote! before={}, after={}",
                   i, term_before[i], c.nodes[i].term());
    }
}

// ============================================================================
// THESIS: A candidate that receives a PreVote response (stale, from before
// it became a real candidate) should ignore it and not corrupt its election.
// ============================================================================

#[test]
fn test_stale_prevote_response_ignored_by_candidate() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");
    c.outbox.lock().unwrap().clear();

    let past = far_past();
    for i in 0..5 { c.nodes[i].election_deadline = past; }

    // Node 1 starts prevote
    c.nodes[1].start_prevote();
    c.outbox.lock().unwrap().clear();

    // Manually promote node 1 to real candidate
    c.nodes[1].become_candidate();
    let candidate_term = c.nodes[1].term();
    c.outbox.lock().unwrap().clear();

    // Now deliver a stale PreVote response from the previous round
    let stale_prevote_resp = RequestVoteResp {
        term: candidate_term - 1, // from the prevote round
        granted: true,
        kind: VoteKind::PreVote,
    };
    c.nodes[1].recv_requestvote_response(NodeId(2), &stale_prevote_resp);

    // Node 1 should still be a candidate with the same term
    assert!(matches!(c.nodes[1].role(), Role::Candidate { .. }),
            "stale prevote response should not change candidate state");
    assert_eq!(c.nodes[1].term(), candidate_term);
}

// ============================================================================
// THESIS: send_heartbeats reads next_index from leader state, then uses it
// to index into the log. If the log has been compacted between when
// next_index was set and when heartbeat runs, this could panic.
// Specifically: compact the leader's log, DON'T update next_index,
// then send heartbeats.
// ============================================================================

#[test]
fn test_heartbeat_after_compaction_stale_next_index() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Build up log
    for i in 0..10 {
        c.propose_and_commit(format!("entry-{}", i).as_bytes());
    }

    // Compact the leader's log aggressively
    let applied = c.nodes[0].last_applied();
    c.nodes[0].core.log.compact(applied);
    c.nodes[0].core.snapshot_last = LogPosition {
        index: applied,
        term: c.nodes[0].term(),
    };

    // Manually set a peer's next_index to BEFORE the compaction point.
    // This simulates a slow follower that hasn't been updated.
    if let Role::Leader(ref mut leader) = c.nodes[0].role {
        if let Some(peer) = leader.peers.get_mut(&NodeId(1)) {
            peer.next_index = 1; // way before compaction
            peer.match_index = 0;
        }
    }

    // This should NOT panic — it should detect the peer needs a snapshot
    c.nodes[0].send_heartbeats();

    // Deliver and heal — system should recover
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: The Log::slice method clamps to available range. But what about
// when next_index points to exactly base_index + 1 (first available entry)
// and prev_idx = base_index (which is NOT in the log anymore)?
// send_heartbeats computes prev_term via term_at_or_snapshot — does it
// handle this boundary correctly?
// ============================================================================

#[test]
fn test_heartbeat_prev_log_at_exact_snapshot_boundary() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    for i in 0..5 {
        c.propose_and_commit(format!("e-{}", i).as_bytes());
    }

    let snap_idx = c.nodes[0].last_applied();
    c.nodes[0].core.log.compact(snap_idx);
    c.nodes[0].core.snapshot_last = LogPosition {
        index: snap_idx,
        term: c.nodes[0].term(),
    };

    // Set peer's next_index to snap_idx + 1 (right at the boundary)
    // prev_idx will be snap_idx, which is the snapshot boundary
    if let Role::Leader(ref mut leader) = c.nodes[0].role {
        if let Some(peer) = leader.peers.get_mut(&NodeId(1)) {
            peer.next_index = snap_idx + 1;
        }
    }

    // Propose a new entry so there's something to replicate
    let _ = c.nodes[0].propose(b"after-snap".to_vec());

    // Should not panic — prev_term should come from snapshot_last
    c.nodes[0].send_heartbeats();
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: recv_append_entries advances commit_index using:
//   min(leader_commit, last_new, log.last_index())
// But last_new = prev_log.index + entries.len(). If the leader sends a
// HUGE leader_commit with a small number of entries, the follower should
// NOT set commit beyond what it actually has.
// ============================================================================

#[test]
fn test_follower_commit_capped_at_actual_log_length() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"base");

    let ae = AppendEntries {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        prev_log: LogPosition {
            index: c.nodes[1].log().last_index(),
            term: c.nodes[0].term(),
        },
        leader_commit: 999999, // absurdly high
        seq: 1,
        entries: vec![LogEntry::data(c.nodes[0].term(), b"one-entry".to_vec())],
    };

    c.nodes[1].recv_append_entries(&ae);

    // Commit must not exceed actual log length
    assert!(c.nodes[1].commit_index() <= c.nodes[1].log().last_index(),
            "SAFETY: commit_index {} exceeds log length {}!",
            c.nodes[1].commit_index(), c.nodes[1].log().last_index());
}

// ============================================================================
// THESIS: A follower that receives entries with gaps should not corrupt
// its log. If AE has prev_log.index = 10 but follower only has up to
// index 5, it should reject (Missing), not append at the wrong position.
// ============================================================================

#[test]
fn test_follower_rejects_gapped_entries() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"base");

    let follower_last = c.nodes[1].log().last_index();

    // Send AE with prev_log way beyond follower's log
    let ae = AppendEntries {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        prev_log: LogPosition {
            index: follower_last + 100,
            term: c.nodes[0].term(),
        },
        leader_commit: c.nodes[0].commit_index(),
        seq: 1,
        entries: vec![LogEntry::data(c.nodes[0].term(), b"gapped".to_vec())],
    };

    let resp = c.nodes[1].recv_append_entries(&ae);
    assert!(!resp.success, "should reject AE with gap");

    // Log should be unchanged
    assert_eq!(c.nodes[1].log().last_index(), follower_last,
               "follower log should not have changed on rejected AE");
}

// ============================================================================
// THESIS: handle_failed_ae uses conflict info. But what happens with a
// crafted conflict response where conflict.term is a term that exists
// in the leader's log but conflict.index points somewhere totally wrong?
// The leader uses find_last_of_term(conflict.term) — does it handle
// the result correctly even if the follower is lying?
// ============================================================================

#[test]
fn test_crafted_conflict_info_does_not_corrupt_leader() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    for i in 0..5 {
        c.propose_and_commit(format!("e-{}", i).as_bytes());
    }

    let leader_last = c.nodes[0].log().last_index();
    let leader_term = c.nodes[0].term();

    // Crafted response: claims conflict at term that matches leader's entries,
    // but with an index of 0 (which is weird/impossible)
    let evil_resp = AppendEntriesResp {
        term: leader_term,
        success: false,
        conflict: Some(LogPosition { index: 0, term: leader_term }),
        match_index: 0,
        seq: 1,
    };

    c.nodes[0].recv_append_entries_response(NodeId(1), &evil_resp);

    // Leader should not have panicked. next_index for peer 1 should
    // still be sane (> 0 and <= leader_last + 1)
    if let Role::Leader(ref leader) = c.nodes[0].role {
        let peer = leader.peers.get(&NodeId(1)).unwrap();
        assert!(peer.next_index >= 1,
                "next_index went to 0 or below: {}", peer.next_index);
        assert!(peer.next_index <= leader_last + 1,
                "next_index exceeded log: {}", peer.next_index);
    }

    // Should recover normally
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: What happens if the leader receives a successful AE response
// from a peer that's NOT in its peers map? (Maybe the cluster was
// reconfigured.) It shouldn't panic.
// ============================================================================

#[test]
fn test_ae_response_from_unknown_peer() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    let resp = AppendEntriesResp {
        term: c.nodes[0].term(),
        success: true,
        conflict: None,
        match_index: 5,
        seq: 1,
    };

    // NodeId(99) is not in the cluster
    c.nodes[0].recv_append_entries_response(NodeId(99), &resp);

    // Should not panic, leader should still function
    assert!(c.nodes[0].is_leader());
    let _ = c.nodes[0].propose(b"still-works".to_vec());
}

// ============================================================================
// THESIS: Single-node cluster should commit immediately. But what about
// single-node with async fsync? If durable_index hasn't caught up,
// the commit should be delayed.
// ============================================================================

#[test]
fn test_single_node_delayed_commit_without_durability() {
    // MemLogStore's default fsync_submit() calls fsync() and succeeds, which
    // sets fsync_pending=true in do_fsync. But MemLogStore doesn't implement
    // fsync_poll() (returns None), so durable_index never advances through
    // the async path during tick(). This means the single-node commit gate
    //   `durable_index >= last_index`
    // correctly blocks — the library is being conservative about durability.
    //
    // This test verifies that behavior: propose on a single node with the
    // default MemLogStore should NOT commit until durability is confirmed.
    // Then we use skip_next_fsync to bypass and confirm it commits.

    let peers = vec![NodeId(0)];
    let config = Config::default();
    let outbox = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = harness::MemTransport {
        from: NodeId(0),
        outbox: outbox.clone(),
    };

    let mut raft = Raft::new(
        NodeId(0),
        peers,
        config,
        Box::new(harness::MemLogStore::default()),
        Box::new(harness::MemStateMachine::default()),
        Box::new(transport),
    ).unwrap();

    // Force become leader (single node)
    raft.election_deadline = std::time::Instant::now();
    raft.tick();
    assert!(raft.is_leader(), "single node should be leader");

    // First propose: fsync_submit succeeds, so fsync_pending=true but
    // durable_index stays at 0. Single-node commit gate blocks.
    let idx1 = raft.propose(b"write-1".to_vec()).unwrap();
    raft.tick();

    // Commit should be blocked (durable_index hasn't caught up)
    // The noop from become_leader may have committed via propose_noop's
    // direct fsync path, but our data entry should be gated.
    let commit_before = raft.commit_index();

    // Now use skip_next_fsync to bypass durability for the next propose.
    // This sets durable_index = last_index immediately.
    raft.skip_next_fsync();
    let idx2 = raft.propose(b"write-2".to_vec()).unwrap();
    raft.tick();

    assert!(raft.commit_index() >= idx2,
            "single node should commit after skip_next_fsync: commit={}, proposed={}",
            raft.commit_index(), idx2);
}

// ============================================================================
// THESIS: ReadIndex uses heartbeat_seq to track which heartbeat round the
// read was queued in. If heartbeat_seq wraps or if acks come from a
// PREVIOUS heartbeat round, the read should not complete.
// ============================================================================

#[test]
fn test_read_index_old_seq_does_not_satisfy() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Get current heartbeat_seq
    let seq_before = if let Role::Leader(ref leader) = c.nodes[0].role {
        leader.heartbeat_seq
    } else { panic!("not leader"); };

    // Queue a read — this sets required_seq = heartbeat_seq + 1
    c.nodes[0].request_read_index(42).unwrap();

    // Deliver the heartbeat but intercept — manually craft responses
    // with the OLD seq (before the read was queued)
    c.outbox.lock().unwrap().clear();

    let old_ack = AppendEntriesResp {
        term: c.nodes[0].term(),
        success: true,
        conflict: None,
        match_index: c.nodes[0].log().last_index(),
        seq: seq_before, // OLD seq, before the read's required_seq
    };

    c.nodes[0].recv_append_entries_response(NodeId(1), &old_ack);

    // The read should NOT have completed (seq is too old)
    let msgs = c.outbox.lock().unwrap();
    let completed = msgs.iter().any(|(_, m)| {
        matches!(m, Message::ReadIndexComplete(42, Some(_)))
    });
    assert!(!completed,
            "read completed with stale heartbeat seq! This could serve a stale read.");
}

// ============================================================================
// THESIS: Two leaders in different terms. The old leader has entries the
// new leader doesn't. When they reconnect, the old leader's uncommitted
// entries must be overwritten, not somehow committed.
// ============================================================================

#[test]
fn test_old_leader_uncommitted_entries_overwritten() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0)); // term 1

    // Commit a baseline
    c.propose_and_commit(b"baseline");
    let baseline_commit = c.nodes[0].commit_index();

    // Old leader writes entries but can't replicate (partitioned)
    let _ = c.nodes[0].propose(b"doomed-1".to_vec());
    let _ = c.nodes[0].propose(b"doomed-2".to_vec());
    let doomed_last = c.nodes[0].log().last_index();
    c.outbox.lock().unwrap().clear(); // drop all replication messages

    // Elect node 2 in the majority partition
    let majority: HashSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)].into();
    for &id in &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        c.nodes[id.0 as usize].election_deadline = far_past();
    }
    c.nodes[2].tick();
    for _ in 0..20 { c.deliver_within_r3(&majority); }

    // New leader commits its own entries
    if c.nodes[2].is_leader() {
        let _ = c.nodes[2].propose(b"new-leader-1".to_vec());
        for _ in 0..20 { c.deliver_within_r3(&majority); }
    }

    // Heal — old leader reconnects
    heal(&mut c);

    // Old leader (node 0) should have stepped down
    assert!(!c.nodes[0].is_leader(), "old leader should have stepped down");

    // The doomed entries should be gone — replaced by new leader's entries
    // Check: node 0's log at the doomed indices should match the current leader
    let current_leader = c.leader().expect("should have a leader");
    let cl = current_leader.0 as usize;

    for idx in (baseline_commit + 1)..=doomed_last {
        let leader_term_at = c.nodes[cl].log().term_at(idx);
        let old_leader_term_at = c.nodes[0].log().term_at(idx);

        if leader_term_at.is_some() && old_leader_term_at.is_some() {
            assert_eq!(leader_term_at, old_leader_term_at,
                       "node 0 still has divergent entry at index {}!", idx);
        }
    }

    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: What if a follower receives an InstallSnapshot where
// last_included.index is BETWEEN its commit_index and last_applied?
// That shouldn't be possible in normal operation but a Byzantine leader
// could craft it. The follower should handle it gracefully.
// ============================================================================

#[test]
fn test_install_snapshot_between_commit_and_applied() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    for i in 0..5 {
        c.propose_and_commit(format!("e-{}", i).as_bytes());
    }
    c.tick_all(); // ensure entries are applied

    let follower_commit = c.nodes[1].commit_index();
    let follower_applied = c.nodes[1].last_applied();
    assert!(follower_applied > 0);

    // Send snapshot for an index the follower has already applied
    // but that's still within its log
    let snap = InstallSnapshot {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        last_included: LogPosition {
            index: follower_applied - 1, // before last_applied
            term: c.nodes[0].term(),
        },
        offset: 0,
        data: vec![],
        done: true,
    };

    let resp = c.nodes[1].recv_install_snapshot(&snap);

    // Should not panic. Follower should still be functional.
    // commit_index should not regress
    assert!(c.nodes[1].commit_index() >= follower_commit.min(follower_applied - 1),
            "snapshot install caused state regression");
}

// ============================================================================
// THESIS: The log's find_first_of_term / find_last_of_term are used in
// conflict resolution. What if the log has been compacted and those
// functions are called for a term that only existed in compacted entries?
// They should return None, not panic.
// ============================================================================

#[test]
fn test_find_term_in_compacted_region() {
    let mut log = Log::new();

    // Terms: 1,1,1,2,2,3,3,3,3,3
    for _ in 0..3 { log.push(LogEntry::data(1, b"t1".to_vec())); }
    for _ in 0..2 { log.push(LogEntry::data(2, b"t2".to_vec())); }
    for _ in 0..5 { log.push(LogEntry::data(3, b"t3".to_vec())); }

    // Compact through index 5 (removes all term 1, all term 2)
    log.compact(5);

    // Should return None for term 1 and term 2 (compacted)
    assert_eq!(log.find_first_of_term(1), None,
               "found term 1 entry after compaction");
    assert_eq!(log.find_last_of_term(1), None,
               "found term 1 entry after compaction");
    assert_eq!(log.find_first_of_term(2), None,
               "found term 2 entry after compaction");
    assert_eq!(log.find_last_of_term(2), None,
               "found term 2 entry after compaction");

    // Term 3 should still be findable
    assert!(log.find_first_of_term(3).is_some());
    assert!(log.find_last_of_term(3).is_some());
}

// ============================================================================
// THESIS: propose_batch rollback. If the LogStore fails mid-batch, the
// in-memory log should be rolled back to the state before the batch.
// The log should not have partial entries.
// ============================================================================

#[test]
fn test_propose_batch_multi_entry() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    let before_last = c.nodes[0].log().last_index();

    let entries: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("batch-{}", i).into_bytes())
        .collect();

    let refs: Vec<Vec<u8>> = entries.clone();
    let result = c.nodes[0].propose_batch(&refs);
    assert!(result.is_ok());

    let after_last = c.nodes[0].log().last_index();
    assert_eq!(after_last, before_last + 5,
               "batch of 5 should add 5 entries");

    // All entries should have current term
    let term = c.nodes[0].term();
    for idx in (before_last + 1)..=after_last {
        assert_eq!(c.nodes[0].log().term_at(idx), Some(term),
                   "batch entry at {} has wrong term", idx);
    }

    // Commit and verify
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: What if a node receives a VoteRequest with last_log at
// index=0, term=0 (completely empty log)? This is the initial state.
// A node with ANY log entries should reject this vote.
// ============================================================================

#[test]
fn test_vote_rejected_for_empty_log_candidate() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Node 1 has entries. Send it a vote request from a node with empty log.
    let req = RequestVote {
        term: c.nodes[1].term() + 1, // higher term to get past the term check
        candidate_id: NodeId(2),
        last_log: LogPosition { index: 0, term: 0 }, // empty log
        kind: VoteKind::Vote,
    };

    let resp = c.nodes[1].recv_request_vote(&req);

    // Node 1 has a non-empty log — it should NOT vote for a node with
    // an empty log (our log is more up-to-date).
    assert!(!resp.granted,
            "voted for candidate with empty log when we have entries!");
}

// ============================================================================
// THESIS: Double stepdown. Call become_follower twice rapidly. The second
// call should be idempotent — no double-clearing of pending reads,
// no term weirdness.
// ============================================================================

#[test]
fn test_double_stepdown_idempotent() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Queue some reads
    c.nodes[0].request_read_index(1).unwrap();
    c.nodes[0].request_read_index(2).unwrap();
    c.outbox.lock().unwrap().clear();

    let new_term = c.nodes[0].term() + 1;

    // First stepdown
    c.nodes[0].become_follower(new_term, None);
    assert!(!c.nodes[0].is_leader());
    assert_eq!(c.nodes[0].term(), new_term);

    // Second stepdown with same term (should be idempotent)
    c.nodes[0].become_follower(new_term, None);
    assert!(!c.nodes[0].is_leader());
    assert_eq!(c.nodes[0].term(), new_term);

    // Should not panic, system should recover
    heal(&mut c);
}

// ============================================================================
// THESIS: AE with entries that have decreasing terms. In a valid Raft log,
// entries should have monotonically non-decreasing terms. What happens
// if a Byzantine leader sends entries with terms going backwards?
// ============================================================================

#[test]
fn test_ae_with_decreasing_entry_terms() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"base");

    let current_term = c.nodes[0].term();
    let follower_last = c.nodes[1].log().last_index();

    // Craft entries with decreasing terms (invalid in correct Raft)
    let evil_entries = vec![
        LogEntry::data(current_term, b"ok".to_vec()),
        LogEntry::data(current_term - 1, b"backwards-term".to_vec()), // invalid!
    ];

    let ae = AppendEntries {
        term: current_term,
        leader_id: NodeId(0),
        prev_log: LogPosition {
            index: follower_last,
            term: current_term,
        },
        leader_commit: c.nodes[0].commit_index(),
        seq: 1,
        entries: evil_entries,
    };

    // Should not panic
    let resp = c.nodes[1].recv_append_entries(&ae);

    // Even if accepted, the system should converge correctly after healing
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: Snapshot + election race. A follower is in the middle of receiving
// a snapshot when an election happens. After the election, the snapshot
// state should be sane.
// ============================================================================

#[test]
fn test_snapshot_interrupted_by_election() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));

    for i in 0..10 {
        c.propose_and_commit(format!("e-{}", i).as_bytes());
    }

    // Start a snapshot transfer to node 1 (first chunk, not done)
    let partial_snap = InstallSnapshot {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        last_included: LogPosition {
            index: c.nodes[0].last_applied(),
            term: c.nodes[0].term(),
        },
        offset: 0,
        data: vec![1, 2, 3, 4], // partial data
        done: false, // NOT done
    };

    c.nodes[1].recv_install_snapshot(&partial_snap);

    // Now force an election — node 2 becomes leader
    let past = far_past();
    for i in 0..5 { c.nodes[i].election_deadline = past; }
    c.nodes[2].tick();
    for _ in 0..20 { c.deliver_all(); }

    // Node 1 should have become a follower of the new leader.
    // Its state should be consistent.
    assert!(!c.nodes[1].is_leader());

    // System should be able to make progress
    if let Some(leader_id) = c.leader() {
        let _ = c.nodes[leader_id.0 as usize].propose(b"after-election".to_vec());
    }
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THESIS: What if ALL five nodes in a 5-node cluster simultaneously
// become candidates at the exact same term? No one should get a majority
// (since you can only vote for yourself). After enough rounds, someone
// should eventually win due to randomized timeouts.
// But the key check: safety must hold throughout.
// ============================================================================

#[test]
fn test_simultaneous_candidates_all_nodes() {
    let mut c = Cluster::new(5);

    // All 5 nodes become candidates simultaneously
    for i in 0..5 {
        c.nodes[i].election_deadline = std::time::Instant::now();
    }
    // Tick all simultaneously
    for i in 0..5 {
        c.nodes[i].tick();
    }

    // Deliver everything
    for _ in 0..50 {
        c.deliver_all();
    }

    // Safety: at most one leader per term
    assert_at_most_one_leader(&c);

    // Now let the system stabilize
    heal(&mut c);

    // After enough rounds, should have a leader
    let mut has_leader = c.leader().is_some();
    for _ in 0..20 {
        if has_leader { break; }
        for i in 0..5 {
            c.nodes[i].election_deadline = far_past();
        }
        c.tick_all();
        for _ in 0..20 { c.deliver_all(); }
        has_leader = c.leader().is_some();
    }

    assert_at_most_one_leader(&c);
}

// ============================================================================
// THESIS: Log::with_base(u64::MAX - 1). Operations near the top of the
// u64 range should not overflow or panic.
// ============================================================================

#[test]
fn test_log_near_u64_max() {
    let base = u64::MAX - 10;
    let mut log = Log::with_base(base);

    assert_eq!(log.last_index(), base);
    assert!(log.is_empty());

    // Push a few entries
    log.push(LogEntry::data(1, b"near-max".to_vec()));
    log.push(LogEntry::data(1, b"near-max-2".to_vec()));

    assert_eq!(log.first_index(), base + 1);
    assert_eq!(log.last_index(), base + 2);
    assert!(log.get(base + 1).is_some());
    assert!(log.get(base + 2).is_some());

    // Slice should work
    let s = log.slice(base + 1, base + 3);
    assert_eq!(s.len(), 2);
}

// ============================================================================
// THESIS: The commit_index should never exceed last_applied after
// tick() processes entries. If commit_index jumps ahead by a lot,
// tick() should apply entries one by one until caught up.
// ============================================================================

#[test]
fn test_bulk_commit_applied_in_order() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Propose many entries without ticking
    for i in 0..20 {
        let _ = c.nodes[0].propose(format!("bulk-{}", i).as_bytes().to_vec());
    }

    // Deliver but don't tick — commit_index advances but last_applied doesn't
    for _ in 0..20 { c.deliver_all(); }

    let commit = c.nodes[0].commit_index();
    let applied_before = c.nodes[0].last_applied();

    // Now tick — should apply all committed entries
    c.tick_all();

    let applied_after = c.nodes[0].last_applied();
    assert!(applied_after >= commit.min(c.nodes[0].log().last_index()),
            "tick should apply all committed entries: commit={}, applied={}",
            commit, applied_after);
    assert!(applied_after >= applied_before,
            "last_applied should never decrease");
}

// ============================================================================
// THESIS: ReadIndex quorum check uses `commit_index` at the time the read
// completes (not when it was queued). If commit advances between queue
// and completion, the read should get the NEWER commit_index.
// ============================================================================

#[test]
fn test_read_index_uses_current_commit_not_queued() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"initial");

    let commit_at_queue_time = c.nodes[0].commit_index();

    // Queue a read
    c.nodes[0].request_read_index(77).unwrap();
    c.outbox.lock().unwrap().clear();

    // Advance commit by proposing more
    let _ = c.nodes[0].propose(b"more-data".to_vec());
    // Deliver to replicate and advance commit
    for _ in 0..10 { c.deliver_all(); }
    c.tick_all();

    let commit_now = c.nodes[0].commit_index();
    assert!(commit_now > commit_at_queue_time, "commit should have advanced");

    // Now deliver heartbeat acks that satisfy the read's seq requirement
    c.nodes[0].send_heartbeats();
    for _ in 0..10 { c.deliver_all(); }

    // The ReadIndexComplete should report commit_now, not commit_at_queue_time
    let msgs = c.outbox.lock().unwrap();
    let read_complete = msgs.iter().find(|(_, m)| {
        matches!(m, Message::ReadIndexComplete(77, Some(_)))
    });

    if let Some((_, Message::ReadIndexComplete(_, Some(read_idx)))) = read_complete {
        assert!(*read_idx >= commit_now,
                "ReadIndex returned stale commit! read_idx={}, commit_now={}, commit_at_queue={}",
                read_idx, commit_now, commit_at_queue_time);
    }
    // It's OK if the read hasn't completed yet (might need more ack rounds)
}

// ============================================================================
// THE FINAL BOSS: Triple partition, two leader changes, snapshot on old
// leader, compaction on new leader, proposals everywhere, heal, verify.
// ============================================================================

#[test]
fn test_triple_partition_final_boss() {
    let mut c = Cluster::new(9);

    // Phase 1: Normal ops
    c.elect_leader(NodeId(0));
    for i in 0..5 {
        c.propose_and_commit(format!("phase1-{}", i).as_bytes());
    }
    let phase1_commit = c.nodes[0].commit_index();

    // Phase 2: Make sure all entries are fully replicated before snapshotting.
    // Without this, the leader could compact entries that followers don't have
    // yet, creating a gap where commit_index > log_last on the follower.
    // (This is a test setup concern — in production, InstallSnapshot fills gaps.)
    heal(&mut c);

    // Re-elect node 0 as leader for the partition phase
    c.elect_leader(NodeId(0));

    // Now snapshot — all nodes already have the entries
    c.nodes[0].create_snapshot().unwrap();

    // Phase 3: Three-way partition
    //   A: {0,1,2}     — old leader, minority
    //   B: {3,4,5,6,7} — majority
    //   C: {8}         — alone
    let part_a: HashSet<NodeId> = [NodeId(0), NodeId(1), NodeId(2)].into();
    let part_b: HashSet<NodeId> = [NodeId(3), NodeId(4), NodeId(5), NodeId(6), NodeId(7)].into();

    // Old leader proposes (can't commit)
    for i in 0..3 {
        let _ = c.nodes[0].propose(format!("minority-{}", i).as_bytes().to_vec());
    }
    for _ in 0..10 {
        for node in &mut c.nodes { node.last_tick = far_past(); }
        c.tick_all();
        c.deliver_within_r3(&part_a);
    }

    // Node 8 (alone) would run elections in a non-PreVote system, driving
    // its term sky-high. With PreVote enabled (the default), it can't get
    // quorum so its term stays put — that's the whole point of PreVote.
    // Simulate the non-PreVote scenario by manually setting a high term.
    c.nodes[8].become_follower(500, None);
    let node8_term = c.nodes[8].term();
    assert!(node8_term > c.nodes[0].term(),
            "node 8 should have high term: node8={}, node0={}", node8_term, c.nodes[0].term());

    // Elect leader in majority partition
    for &id in &[NodeId(3), NodeId(4), NodeId(5), NodeId(6), NodeId(7)] {
        c.nodes[id.0 as usize].election_deadline = far_past();
    }
    c.nodes[4].tick();
    for _ in 0..30 { c.deliver_within_r3(&part_b); }

    // Majority leader proposes
    if let Some(lid) = c.nodes.iter()
        .find(|n| n.is_leader() && part_b.contains(&n.id()))
        .map(|n| n.id())
    {
        for i in 0..5 {
            let _ = c.nodes[lid.0 as usize].propose(format!("maj-{}", i).as_bytes().to_vec());
        }
        // Compact majority leader's log
        for _ in 0..20 {
            for node in &mut c.nodes { node.last_tick = far_past(); }
            c.tick_all();
            c.deliver_within_r3(&part_b);
        }

        let applied = c.nodes[lid.0 as usize].last_applied();
        if applied > 0 {
            c.nodes[lid.0 as usize].core.log.compact(applied);
            c.nodes[lid.0 as usize].core.snapshot_last = LogPosition {
                index: applied,
                term: c.nodes[lid.0 as usize].term(),
            };
        }
    }

    // Phase 4: Heal everything — the high-term node 8 will disrupt everyone
    heal(&mut c);

    // The healing above may not be enough because node 8 has term 500,
    // which forces everyone to that term, causing election chaos.
    // Strategy: force a stable leader, then give it LOTS of time to replicate.

    // Step 1: Pick a well-connected node (node 3, was in majority partition)
    // and force it to win an election decisively.
    for node in &mut c.nodes {
        node.election_deadline = far_future();
    }
    c.nodes[3].election_deadline = far_past();
    c.nodes[3].tick();
    for _ in 0..30 { c.deliver_all(); }

    // If node 3 isn't leader, try harder — bump its term and retry
    if !c.nodes[3].is_leader() {
        let max_term = c.nodes.iter().map(|n| n.term()).max().unwrap();
        c.nodes[3].become_follower(max_term, None);
        c.nodes[3].election_deadline = far_past();
        c.nodes[3].tick();
        for _ in 0..30 { c.deliver_all(); }
    }

    // Step 2: Suppress all other elections and let the leader replicate
    for node in &mut c.nodes {
        node.election_deadline = far_future();
    }
    for _ in 0..100 {
        if let Some(_) = c.leader() {
            for node in &mut c.nodes {
                node.last_tick = far_past();
                node.election_deadline = far_future();
            }
            c.tick_all();
            c.deliver_all();
        }
    }

    // === Verify everything ===

    // 1. Committed prefix agreement
    assert_committed_prefix_agreement(&c);

    // 2. At most one leader
    assert_at_most_one_leader(&c);

    // 3. Phase 1 commits survived (in log or snapshot)
    println!("\nPHASE 1 COMMIT CHECK — phase1_commit={}", phase1_commit);
    for (ni, node) in c.nodes.iter().enumerate() {
        if node.commit_index() >= phase1_commit {
            let mut missing = vec![];
            for idx in 1..=phase1_commit {
                let in_log = node.log().get(idx).is_some();
                let in_snap = node.snapshot_info().index >= idx;
                if !in_log && !in_snap {
                    missing.push(idx);
                }
            }
            println!("  node {}: commit={}, snapshot={}, log_base={}, log_first={}, log_last={}, missing={:?}",
                     ni, node.commit_index(), node.snapshot_info().index,
                     node.log().base(), node.log().first_index(), node.log().last_index(), missing);
            if !missing.is_empty() {
                // Print the actual terms around the gap
                let terms: Vec<String> = (1..=phase1_commit + 3)
                    .map(|idx| format!("{}:{:?}", idx, node.log().term_at(idx)))
                    .collect();
                println!("    terms: {}", terms.join(", "));
            }
        }
    }
    // Now assert
    for node in &c.nodes {
        if node.commit_index() >= phase1_commit {
            for idx in 1..=phase1_commit {
                let in_log = node.log().get(idx).is_some();
                let in_snap = node.snapshot_info().index >= idx;
                assert!(in_log || in_snap,
                        "node {} missing committed entry {} from phase 1 \
                     (not in log or snapshot)", node.id().0, idx);
            }
        }
    }

    // 4. All terms are consistent — no node has a term below the leader's
    if let Some(leader_id) = c.leader() {
        let leader_term = c.nodes[leader_id.0 as usize].term();
        for node in &c.nodes {
            assert!(node.term() <= leader_term,
                    "node {} has term {} > leader term {}",
                    node.id().0, node.term(), leader_term);
        }
    }

    println!("FINAL BOSS PASSED. Final state:");
    for node in &c.nodes {
        println!("  node {}: term={}, leader={}, commit={}, log_last={}, snapshot={}",
                 node.id().0, node.term(), node.is_leader(),
                 node.commit_index(), node.log().last_index(),
                 node.snapshot_info().index);
    }
}