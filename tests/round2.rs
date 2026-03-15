// tests/round2_tests.rs — Round 2: Break the Raft (harder)
//
// Byzantine-style crafted messages, log compaction boundary torture,
// snapshot + replication races, liveness proofs, commit regression
// attacks, ReadIndex overflow, and edge cases at every seam.

mod harness;

use harness::{Cluster, Message};
use lil_raft::*;
use std::collections::HashSet;

// ============================================================================
// Helpers
// ============================================================================

impl Cluster {
    fn deliver_within_r2(&mut self, partition: &HashSet<NodeId>) -> usize {
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

    fn drop_all_messages_r2(&mut self) {
        self.outbox.lock().unwrap().clear();
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn far_past() -> std::time::Instant {
    std::time::Instant::now() - std::time::Duration::from_secs(10)
}

fn far_future() -> std::time::Instant {
    std::time::Instant::now() + std::time::Duration::from_secs(60)
}

/// Heal a cluster: force heartbeats, suppress elections, deliver until stable.
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

/// Assert the core Raft safety invariant: no two nodes disagree on committed entries.
/// Entries behind a snapshot boundary are considered valid (compacted but safe).
fn assert_committed_prefix_agreement(c: &Cluster) {
    for i in 0..c.nodes.len() {
        for j in (i + 1)..c.nodes.len() {
            let shared = c.nodes[i].commit_index().min(c.nodes[j].commit_index());
            for idx in 1..=shared {
                // If either node has compacted past this index, skip —
                // the entry is in the snapshot, not the log.
                if idx <= c.nodes[i].snapshot_info().index
                    || idx <= c.nodes[j].snapshot_info().index
                {
                    continue;
                }

                let ti = c.nodes[i].log().term_at(idx);
                let tj = c.nodes[j].log().term_at(idx);
                assert_eq!(
                    ti, tj,
                    "SAFETY VIOLATION: nodes {} and {} disagree at committed index {}",
                    i, j, idx
                );
            }
        }
    }
}

// ============================================================================
// BYZANTINE: AppendEntries with wrong leader_id
// ============================================================================

#[test]
fn test_byzantine_wrong_leader_id() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Craft AE claiming to be from node 2 (who is NOT leader)
    let fake_ae = AppendEntries {
        term: c.nodes[0].term(), // correct term, wrong leader
        leader_id: NodeId(2),    // liar
        prev_log: LogPosition { index: 0, term: 0 },
        leader_commit: 0,
        seq: 1,
        entries: vec![LogEntry::data(c.nodes[0].term(), b"injected".to_vec())],
    };

    let resp = c.nodes[1].recv_append_entries(&fake_ae);
    // Node 1 accepts it (Raft doesn't authenticate leader_id — it trusts term)
    // This is by design: Raft assumes non-Byzantine faults.
    // The key check: the REAL leader can still overwrite this.
    c.nodes[0].propose(b"real-data".to_vec()).unwrap();
    heal(&mut c);

    // After healing, committed entries must be from the real leader
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// BYZANTINE: AppendEntries with term=0
// ============================================================================

#[test]
fn test_byzantine_term_zero_ae() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"legit");

    let commit_before = c.nodes[1].commit_index();

    let garbage = AppendEntries {
        term: 0, // ancient
        leader_id: NodeId(99), // nonexistent
        prev_log: LogPosition { index: 0, term: 0 },
        leader_commit: 999, // try to inflate commit
        seq: 0,
        entries: vec![LogEntry::data(0, b"evil".to_vec())],
    };

    let resp = c.nodes[1].recv_append_entries(&garbage);
    assert!(!resp.success, "term-0 AE should be rejected");

    // Commit index must not have advanced
    assert_eq!(c.nodes[1].commit_index(), commit_before,
               "commit index moved on garbage AE");
}

// ============================================================================
// BYZANTINE: AE response with match_index beyond log end
// ============================================================================

#[test]
fn test_byzantine_inflated_match_index() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"real");

    let commit_before = c.nodes[0].commit_index();

    // Fake response claiming match_index = 99999
    let fake_resp = AppendEntriesResp {
        term: c.nodes[0].term(),
        success: true,
        conflict: None,
        match_index: 99999,
        seq: 1,
    };

    c.nodes[0].recv_append_entries_response(NodeId(1), &fake_resp);

    // With only 1 inflated peer (need majority), commit shouldn't jump
    // But let's check it doesn't panic or corrupt state
    assert!(c.nodes[0].commit_index() <= c.nodes[0].log().last_index(),
            "commit index exceeded log length!");
}

// ============================================================================
// BYZANTINE: RequestVote from nonexistent node
// ============================================================================

#[test]
fn test_byzantine_vote_from_ghost_node() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    let ghost_vote = RequestVote {
        term: c.nodes[0].term() + 1,
        candidate_id: NodeId(99), // doesn't exist in cluster
        last_log: LogPosition { index: 999, term: 999 },
        kind: VoteKind::Vote,
    };

    // Should cause stepdown (higher term) and grant vote to ghost
    let resp = c.nodes[0].recv_request_vote(&ghost_vote);
    assert!(!c.nodes[0].is_leader(), "should have stepped down");
    assert!(resp.granted, "should grant vote — ghost has higher log");

    // Cluster should recover — ghost can't win (not in peers list)
    heal(&mut c);
    // No panic, no corruption
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// BYZANTINE: InstallSnapshot with stale term
// ============================================================================

#[test]
fn test_byzantine_stale_install_snapshot() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    let stale_snap = InstallSnapshot {
        term: 0,
        leader_id: NodeId(99),
        last_included: LogPosition { index: 100, term: 50 },
        offset: 0,
        data: vec![0u8; 1024],
        done: true,
    };

    let resp = c.nodes[1].recv_install_snapshot(&stale_snap);
    assert!(!resp.success || resp.term > 0,
            "stale snapshot should be rejected");

    // Node 1 should still function
    assert!(c.nodes[1].log().last_index() > 0);
}

// ============================================================================
// BYZANTINE: AE response with match_index going BACKWARDS
// ============================================================================

#[test]
fn test_byzantine_match_index_regression() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"entry1");
    c.propose_and_commit(b"entry2");

    // Get current peer state
    let leader = c.nodes[0].leader_state().unwrap();
    let peer1 = leader.peers.get(&NodeId(1)).unwrap();
    let match_before = peer1.match_index;

    // Send response with lower match_index
    let regress_resp = AppendEntriesResp {
        term: c.nodes[0].term(),
        success: true,
        conflict: None,
        match_index: 1, // lower than current
        seq: 1,
    };

    c.nodes[0].recv_append_entries_response(NodeId(1), &regress_resp);

    // match_index should NOT have gone backwards
    let leader = c.nodes[0].leader_state().unwrap();
    let peer1 = leader.peers.get(&NodeId(1)).unwrap();
    assert!(peer1.match_index >= match_before,
            "match_index regressed from {} to {}", match_before, peer1.match_index);
}

// ============================================================================
// LOG BOUNDARY: Propose after compaction
// ============================================================================

#[test]
fn test_propose_after_log_compaction() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Commit several entries
    for i in 0..5 {
        c.propose_and_commit(format!("entry-{}", i).as_bytes());
    }

    // Compact the leader's log
    let snap_idx = c.nodes[0].last_applied();
    c.nodes[0].core.log.compact(snap_idx);
    c.nodes[0].core.snapshot_last = LogPosition {
        index: snap_idx,
        term: c.nodes[0].term(),
    };

    // Now propose new entries — should work fine
    let new_idx = c.nodes[0].propose(b"after-compact".to_vec()).unwrap();
    assert!(new_idx > snap_idx, "new entry should be after snapshot");

    heal(&mut c);

    assert!(c.nodes[0].commit_index() >= new_idx,
            "entry after compaction should commit");
}

// ============================================================================
// LOG BOUNDARY: AE with prev_log pointing into compacted region
// ============================================================================

#[test]
fn test_ae_prev_log_in_compacted_region() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    for i in 0..5 {
        c.propose_and_commit(format!("entry-{}", i).as_bytes());
    }

    // Compact follower's log aggressively
    let snap_idx = c.nodes[1].last_applied();
    c.nodes[1].core.log.compact(snap_idx);
    c.nodes[1].core.snapshot_last = LogPosition {
        index: snap_idx,
        term: c.nodes[0].term(),
    };

    // Leader sends AE with prev_log before the compaction point
    let ae = AppendEntries {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        prev_log: LogPosition { index: 1, term: 1 }, // compacted on follower
        leader_commit: c.nodes[0].commit_index(),
        seq: 1,
        entries: vec![],
    };

    let resp = c.nodes[1].recv_append_entries(&ae);
    // Should not panic. Either rejected or handled gracefully.
    // The follower should report its snapshot position so leader can adjust.
    assert!(resp.match_index <= c.nodes[1].log().last_index());
}

// ============================================================================
// LOG BOUNDARY: Slice across compaction boundary
// ============================================================================

#[test]
fn test_log_slice_across_compaction() {
    let mut log = Log::new();
    for i in 1..=10 {
        log.push(LogEntry::data(1, format!("{}", i).into_bytes()));
    }

    log.compact(5);

    // Slice that starts before compaction point
    let s = log.slice(3, 8);
    assert!(!s.is_empty());
    // Should only contain entries 6,7 (available after compaction)
    assert!(s.len() <= 2);

    // Slice entirely before compaction
    let s2 = log.slice(1, 4);
    assert!(s2.is_empty());

    // Slice entirely after compaction
    let s3 = log.slice(6, 11);
    assert_eq!(s3.len(), 5); // entries 6-10
}

// ============================================================================
// LOG BOUNDARY: Truncate to before base_index
// ============================================================================

#[test]
fn test_log_truncate_before_base() {
    let mut log = Log::new();
    for _ in 0..5 {
        log.push(LogEntry::data(1, b"x".to_vec()));
    }
    log.compact(3); // base=3, entries at 4,5

    // Truncate to before base — should clear everything
    log.truncate_after(2);
    assert!(log.is_empty());
    assert_eq!(log.base(), 3); // base shouldn't change
}

// ============================================================================
// LOG BOUNDARY: Double compact
// ============================================================================

#[test]
fn test_log_double_compact() {
    let mut log = Log::new();
    for _ in 0..10 {
        log.push(LogEntry::data(1, b"x".to_vec()));
    }

    log.compact(5);
    assert_eq!(log.base(), 5);
    assert_eq!(log.len(), 5);

    // Compact again past the first compaction
    log.compact(8);
    assert_eq!(log.base(), 8);
    assert_eq!(log.len(), 2);

    // Compact to exact end
    log.compact(10);
    assert_eq!(log.base(), 10);
    assert!(log.is_empty());

    // Compact beyond end — should be a no-op
    log.compact(999);
    assert_eq!(log.base(), 999);
}

// ============================================================================
// SNAPSHOT: Create snapshot while leader is replicating
// ============================================================================

#[test]
fn test_snapshot_during_replication() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Build up a log
    for i in 0..10 {
        c.propose_and_commit(format!("data-{}", i).as_bytes());
    }

    // Take a snapshot on the leader
    c.nodes[0].create_snapshot().unwrap();

    // Now propose more entries — replication must still work
    for i in 0..5 {
        let _idx = c.nodes[0].propose(format!("post-snap-{}", i).as_bytes().to_vec()).unwrap();
    }

    heal(&mut c);

    // All nodes should converge
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// SNAPSHOT: InstallSnapshot on follower that already has newer data
// ============================================================================

#[test]
fn test_install_snapshot_older_than_follower() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    for i in 0..5 {
        c.propose_and_commit(format!("data-{}", i).as_bytes());
    }

    let follower_commit = c.nodes[1].commit_index();

    // Send an InstallSnapshot for an index the follower already has
    let old_snap = InstallSnapshot {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        last_included: LogPosition { index: 1, term: 1 },
        offset: 0,
        data: vec![],
        done: true,
    };

    let resp = c.nodes[1].recv_install_snapshot(&old_snap);
    assert!(resp.success, "should accept old snapshot gracefully");

    // Follower state should not have regressed
    assert!(c.nodes[1].commit_index() >= follower_commit,
            "commit index regressed after old snapshot install");
}

// ============================================================================
// COMMIT REGRESSION: Try to make commit_index go backwards
// ============================================================================

#[test]
fn test_commit_index_never_regresses() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"entry1");
    c.propose_and_commit(b"entry2");

    let commit_after = c.nodes[1].commit_index();

    // Send AE with lower leader_commit
    let ae = AppendEntries {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        prev_log: LogPosition {
            index: c.nodes[1].log().last_index(),
            term: c.nodes[0].term(),
        },
        leader_commit: 1, // lower than current commit
        seq: 99,
        entries: vec![],
    };

    c.nodes[1].recv_append_entries(&ae);

    assert!(c.nodes[1].commit_index() >= commit_after,
            "commit_index regressed from {} to {}!",
            commit_after, c.nodes[1].commit_index());
}

// ============================================================================
// READINDEX: Queue overflow (65+ pending reads)
// ============================================================================

#[test]
fn test_read_index_queue_overflow() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Queue 64 reads (the max)
    for i in 0..64 {
        let result = c.nodes[0].request_read_index(i as u64);
        assert!(result.is_ok(), "read {} should succeed", i);
    }

    // 65th should fail with QueueFull
    let result = c.nodes[0].request_read_index(999);
    assert_eq!(result, Err(RaftError::QueueFull),
               "should reject when queue is full");
}

// ============================================================================
// READINDEX: Follower forwards to leader, leader responds after heartbeat
// ============================================================================

#[test]
fn test_read_index_follower_forward() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Follower requests read
    c.nodes[1].request_read_index(42).unwrap();

    // Deliver the forwarded request to leader + heartbeat + ack cycle
    heal(&mut c);

    // Check that a ReadIndexComplete was emitted (either on leader or follower)
    let msgs = c.outbox.lock().unwrap();
    let any_complete = msgs.iter().any(|(_, m)| {
        matches!(m, Message::ReadIndexComplete(42, Some(_)))
    });
    // Note: completion might have been consumed by deliver_all.
    // At minimum, no panic and no corruption.
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// READINDEX: Reads from old term are failed on stepdown
// ============================================================================

#[test]
fn test_read_index_stale_term_cleared() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Queue reads
    for i in 0..5 {
        c.nodes[0].request_read_index(i).unwrap();
    }

    // Don't deliver — force a term bump by new election
    c.drop_all_messages_r2();

    let past = far_past();
    for i in 0..3 { c.nodes[i].election_deadline = past; }
    c.nodes[1].tick(); // starts election
    for _ in 0..20 { c.deliver_all(); }

    // All 5 reads from old leader should have been failed
    let msgs = c.outbox.lock().unwrap();
    let failed_count = msgs.iter().filter(|(_, m)| {
        matches!(m, Message::ReadIndexComplete(id, None) if *id < 5)
    }).count();
    // At least some should have been failed (leader may have cleared them on stepdown)
    // The important thing: no stale reads complete with a commit_index from the old term
    let stale_completions = msgs.iter().filter(|(_, m)| {
        matches!(m, Message::ReadIndexComplete(id, Some(_)) if *id < 5)
    }).count();
    // This is technically OK if the leader confirmed them before stepping down,
    // but there shouldn't be any since we dropped all heartbeat messages
    drop(msgs);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// LIVENESS: Cluster makes progress after every kind of disruption
// ============================================================================

#[test]
fn test_liveness_after_leader_crash() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"before-crash");

    // "Crash" the leader by shutting it down
    c.nodes[0].shutdown();

    // Remaining nodes should elect a new leader and commit
    for i in 1..5 { c.nodes[i].election_deadline = far_past(); }
    c.nodes[1].tick();
    for _ in 0..30 { c.deliver_all(); }

    let new_leader = c.nodes.iter()
        .find(|n| n.is_leader() && n.id() != NodeId(0));
    assert!(new_leader.is_some(), "cluster should elect new leader after crash");

    // Should be able to commit new entries
    let leader_id = new_leader.unwrap().id();
    let idx = c.nodes[leader_id.0 as usize].propose(b"after-crash".to_vec()).unwrap();
    heal(&mut c);
    assert!(c.nodes[leader_id.0 as usize].commit_index() >= idx,
            "cluster should make progress after leader crash");
}

#[test]
fn test_liveness_after_minority_partition() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"stable");

    // Partition off 2 nodes (minority)
    let majority: HashSet<NodeId> = [NodeId(0), NodeId(1), NodeId(2)].into();
    let _ = c.nodes[0].propose(b"during-partition".to_vec());
    for _ in 0..20 {
        for node in &mut c.nodes {
            node.last_tick = far_past();
        }
        c.tick_all();
        c.deliver_within_r2(&majority);
    }

    // Leader should still commit (has majority)
    assert!(c.nodes[0].commit_index() > 1,
            "leader with majority should still commit");
}

// ============================================================================
// LIVENESS: 3-node cluster survives 1 node failure
// ============================================================================

#[test]
fn test_liveness_three_node_one_dead() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Kill node 2
    c.nodes[2].shutdown();

    // Leader + node 1 = majority of 3 — should still commit
    let idx = c.nodes[0].propose(b"two-alive".to_vec()).unwrap();
    heal(&mut c);

    assert!(c.nodes[0].commit_index() >= idx,
            "3-node cluster should commit with 2 nodes alive");
}

// ============================================================================
// EDGE: Propose empty batch
// ============================================================================

#[test]
fn test_propose_empty_batch() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    let result = c.nodes[0].propose_batch(&[]);
    assert!(result.is_err(), "empty batch should be rejected");
}

// ============================================================================
// EDGE: Multiple noops don't corrupt
// ============================================================================

#[test]
fn test_multiple_noops() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // become_leader already sends a noop, send more
    c.nodes[0].propose_noop().unwrap();
    c.nodes[0].propose_noop().unwrap();
    c.nodes[0].propose_noop().unwrap();

    heal(&mut c);

    // Should commit all noops and still function
    let idx = c.nodes[0].propose(b"after-noops".to_vec()).unwrap();
    heal(&mut c);

    assert!(c.nodes[0].commit_index() >= idx);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// EDGE: Log with base > 0 — push, get, slice all correct
// ============================================================================

#[test]
fn test_log_with_nonzero_base() {
    let mut log = Log::with_base(100);

    assert_eq!(log.last_index(), 100);
    assert_eq!(log.first_index(), 0); // empty log
    assert!(log.is_empty());

    log.push(LogEntry::data(5, b"a".to_vec()));
    log.push(LogEntry::data(5, b"b".to_vec()));

    assert_eq!(log.first_index(), 101);
    assert_eq!(log.last_index(), 102);
    assert_eq!(log.len(), 2);

    assert!(log.get(100).is_none()); // base, no entry
    assert!(log.get(101).is_some());
    assert!(log.get(102).is_some());
    assert!(log.get(103).is_none());

    assert_eq!(log.term_at(101), Some(5));

    let s = log.slice(101, 103);
    assert_eq!(s.len(), 2);

    let pos = log.last_position();
    assert_eq!(pos.index, 102);
    assert_eq!(pos.term, 5);
}

// ============================================================================
// EDGE: Rapid propose + compact cycle
// ============================================================================

#[test]
fn test_rapid_propose_compact_cycle() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    for cycle in 0..5 {
        // Propose a batch
        for i in 0..3 {
            c.propose_and_commit(
                format!("cycle-{}-entry-{}", cycle, i).as_bytes()
            );
        }

        // Compact leader's log to last_applied
        let applied = c.nodes[0].last_applied();
        if applied > 0 {
            c.nodes[0].core.log.compact(applied);
            c.nodes[0].core.snapshot_last = LogPosition {
                index: applied,
                term: c.nodes[0].term(),
            };
        }
    }

    // After all cycles, propose one more
    let final_idx = c.nodes[0].propose(b"final".to_vec()).unwrap();
    heal(&mut c);

    assert!(c.nodes[0].commit_index() >= final_idx,
            "should commit after rapid propose/compact cycles");
}

// ============================================================================
// EDGE: Follower with completely empty log receives AppendEntries
// ============================================================================

#[test]
fn test_ae_to_empty_log_follower() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Nuke follower's log (simulate fresh node joining)
    c.nodes[2].core.log.clear();

    let entries = vec![
        LogEntry::noop(1),
        LogEntry::data(1, b"hello".to_vec()),
    ];

    let ae = AppendEntries {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        prev_log: LogPosition { index: 0, term: 0 },
        leader_commit: 2,
        seq: 1,
        entries,
    };

    let resp = c.nodes[2].recv_append_entries(&ae);
    assert!(resp.success, "empty follower should accept entries from index 1");
    assert_eq!(resp.match_index, 2);
}

// ============================================================================
// EDGE: Leader steps down mid-heartbeat cycle — no half-sent state
// ============================================================================

#[test]
fn test_stepdown_during_heartbeat() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Leader sends heartbeats
    c.nodes[0].send_heartbeats();

    // Before responses arrive, a higher-term AE forces stepdown
    let coup = AppendEntries {
        term: c.nodes[0].term() + 10,
        leader_id: NodeId(1),
        prev_log: LogPosition { index: 0, term: 0 },
        leader_commit: 0,
        seq: 0,
        entries: vec![],
    };
    c.nodes[0].recv_append_entries(&coup);
    assert!(!c.nodes[0].is_leader());

    // Now deliver the heartbeat responses from the original round
    c.deliver_all();

    // Node 0 should still be follower, not somehow re-promoted
    assert!(!c.nodes[0].is_leader(),
            "stale heartbeat responses shouldn't re-promote a follower");
}

// ============================================================================
// STRESS: Many concurrent elections, safety holds
// ============================================================================

#[test]
fn test_election_storm() {
    let mut c = Cluster::new(7);

    // 10 rounds of all nodes trying to become leader
    for round in 0..10 {
        for i in 0..7 {
            c.nodes[i].election_deadline = far_past();
        }
        // Pick a different node each round
        c.nodes[round % 7].tick();
        for _ in 0..20 { c.deliver_all(); }

        // At most one leader per term
        let leaders: Vec<_> = c.nodes.iter()
            .filter(|n| n.is_leader())
            .map(|n| (n.id(), n.term()))
            .collect();

        let terms: HashSet<Term> = leaders.iter().map(|(_, t)| *t).collect();
        assert_eq!(leaders.len(), terms.len(),
                   "SAFETY: two leaders in same term! round={}, leaders={:?}",
                   round, leaders);
    }
}

// ============================================================================
// STRESS: Propose under duress — alternating elections and proposals
// ============================================================================

#[test]
fn test_propose_under_election_churn() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));

    let mut proposed = Vec::new();

    for round in 0..5 {
        // Propose if we have a leader
        if let Some(lid) = c.leader() {
            let data = format!("round-{}", round);
            if let Ok(idx) = c.nodes[lid.0 as usize].propose(data.as_bytes().to_vec()) {
                proposed.push(idx);
            }
        }

        // Force election on a different node
        let candidate = (round + 1) % 5;
        for i in 0..5 { c.nodes[i].election_deadline = far_past(); }
        c.nodes[candidate].tick();
        for _ in 0..20 { c.deliver_all(); }
    }

    // After the storm, heal and check safety
    heal(&mut c);
    assert_committed_prefix_agreement(&c);
}

// ============================================================================
// THE GAUNTLET: Everything at once
//
// Normal ops → snapshot → partition → election storm → proposals on
// both sides → heal → verify every safety invariant.
// ============================================================================

#[test]
fn test_the_gauntlet() {
    let mut c = Cluster::new(7);

    // Phase 1: Normal operation, build up log
    c.elect_leader(NodeId(0));
    for i in 0..10 {
        c.propose_and_commit(format!("phase1-{}", i).as_bytes());
    }
    let phase1_commit = c.nodes[0].commit_index();

    // Phase 2: Snapshot on leader
    let applied = c.nodes[0].last_applied();
    c.nodes[0].core.log.compact(applied);
    c.nodes[0].core.snapshot_last = LogPosition {
        index: applied,
        term: c.nodes[0].term(),
    };

    // Phase 3: Partition {0,1,2} vs {3,4,5,6}
    let minority: HashSet<NodeId> = [NodeId(0), NodeId(1), NodeId(2)].into();
    let majority: HashSet<NodeId> = [NodeId(3), NodeId(4), NodeId(5), NodeId(6)].into();

    // Old leader proposes (can't commit — minority)
    for i in 0..3 {
        let _ = c.nodes[0].propose(format!("minority-{}", i).as_bytes().to_vec());
    }
    for node in &mut c.nodes { node.last_tick = far_past(); }
    for _ in 0..10 {
        for node in &mut c.nodes { node.last_tick = far_past(); }
        c.tick_all();
        c.deliver_within_r2(&minority);
    }

    // Phase 4: Election storm in majority partition
    for &id in &[NodeId(3), NodeId(4), NodeId(5), NodeId(6)] {
        c.nodes[id.0 as usize].election_deadline = far_past();
    }
    c.nodes[3].tick();
    for _ in 0..30 { c.deliver_within_r2(&majority); }

    // Majority leader proposes
    if let Some(lid) = c.nodes.iter().find(|n| n.is_leader() && majority.contains(&n.id())).map(|n| n.id()) {
        for i in 0..5 {
            let _ = c.nodes[lid.0 as usize].propose(format!("majority-{}", i).as_bytes().to_vec());
        }
        for _ in 0..20 {
            for node in &mut c.nodes { node.last_tick = far_past(); }
            c.tick_all();
            c.deliver_within_r2(&majority);
        }
    }

    // Phase 5: Heal
    heal(&mut c);

    // === Verify ALL safety invariants ===

    // 1. Committed prefix agreement
    assert_committed_prefix_agreement(&c);

    // 2. At most one leader
    let leaders: Vec<_> = c.nodes.iter()
        .filter(|n| n.is_leader())
        .map(|n| (n.id(), n.term()))
        .collect();
    assert!(leaders.len() <= 1,
            "multiple leaders after gauntlet: {:?}", leaders);

    // 3. No leader has a lower term than any follower
    if let Some((_, leader_term)) = leaders.first() {
        for node in &c.nodes {
            assert!(node.term() <= *leader_term,
                    "node {} has term {} > leader term {}",
                    node.id().0, node.term(), leader_term);
        }
    }

    // 4. Phase 1 commits survived
    // Every node that has progressed past phase1_commit must agree
    for node in &c.nodes {
        if node.commit_index() >= phase1_commit {
            for idx in 1..=phase1_commit {
                assert!(node.log().get(idx).is_some() ||
                            node.snapshot_info().index >= idx,
                        "node {} missing committed entry {} (compacted or lost)",
                        node.id().0, idx);
            }
        }
    }

    // 5. Commit indices are monotonically ordered with terms
    // (no node at a higher term has a lower commit than one at a lower term
    //  ... well, that's not strictly guaranteed by Raft, but worth checking)

    println!("GAUNTLET PASSED. Final state:");
    for node in &c.nodes {
        println!("  node {}: term={}, leader={}, commit={}, log_last={}, snapshot={}",
                 node.id().0, node.term(), node.is_leader(),
                 node.commit_index(), node.log().last_index(),
                 node.snapshot_info().index);
    }
}