// tests/adversarial_tests.rs — Break the Raft
//
// These tests simulate network partitions, message reordering, stale leaders,
// log divergence, and other adversarial scenarios that Jepsen would throw at
// a consensus system. The goal is to violate safety properties:
//
//   1. Election Safety:   at most one leader per term
//   2. Leader Append-Only: leader never overwrites or deletes its own entries
//   3. Log Matching:       if two logs have an entry with the same index+term,
//                          all preceding entries are identical
//   4. Leader Completeness: if an entry is committed in term T, it's present
//                           in the log of every leader for terms > T
//   5. State Machine Safety: if a server has applied entry at index i,
//                            no other server will ever apply a different entry at i

mod harness;

use harness::{Cluster, Message};
use lil_raft::*;
use std::collections::HashSet;

// ============================================================================
// Helper: selective message delivery (simulate partitions)
// ============================================================================

impl Cluster {
    /// Deliver messages only between nodes in `partition`.
    /// Messages to/from nodes outside the partition are dropped.
    /// Returns number of messages delivered.
    fn deliver_within(&mut self, partition: &HashSet<NodeId>) -> usize {
        let mut total = 0;
        loop {
            let msgs: Vec<(NodeId, Message)> = self.outbox.lock().unwrap().drain(..).collect();
            if msgs.is_empty() { break; }

            for (to, msg) in msgs {
                // Drop if destination not in partition
                if !partition.contains(&to) { continue; }

                // Drop if source not in partition
                let from = match &msg {
                    Message::RequestVote(from, _) => *from,
                    Message::RequestVoteResp(from, _) => *from,
                    Message::AppendEntries(from, _) => *from,
                    Message::AppendEntriesResp(from, _) => *from,
                    Message::InstallSnapshot(from, _) => *from,
                    Message::InstallSnapshotResp(from, _) => *from,
                    Message::ReadIndex(from, _) => *from,
                    Message::ReadIndexResp(from, _) => *from,
                    Message::ReadIndexComplete(..) => to, // local
                };
                if !partition.contains(&from) { continue; }

                let idx = to.0 as usize;
                if idx >= self.nodes.len() { continue; }
                let node = &mut self.nodes[idx];
                total += 1;

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
                    Message::ReadIndexComplete(..) => {}
                }
            }
        }
        total
    }

    /// Drop all pending messages.
    fn drop_all_messages(&mut self) {
        self.outbox.lock().unwrap().clear();
    }
}

// ============================================================================
// SAFETY INVARIANT: At most one leader per term
// ============================================================================

#[test]
fn test_no_two_leaders_same_term() {
    let mut c = Cluster::new(5);

    // Trigger elections on two nodes simultaneously
    c.nodes[0].election_deadline = std::time::Instant::now();
    c.nodes[1].election_deadline = std::time::Instant::now();
    c.nodes[0].tick();
    c.nodes[1].tick();

    for _ in 0..20 {
        c.deliver_all();
    }

    // Check: no two leaders share the same term
    let leaders: Vec<_> = c.nodes.iter()
        .filter(|n| n.is_leader())
        .collect();

    if leaders.len() > 1 {
        let terms: Vec<_> = leaders.iter().map(|l| l.term()).collect();
        let unique_terms: HashSet<_> = terms.iter().collect();
        assert_eq!(
            terms.len(), unique_terms.len(),
            "SAFETY VIOLATION: two leaders in same term! terms={:?}", terms
        );
    }
}

// ============================================================================
// PARTITION: Stale leader can't commit after losing quorum
// ============================================================================

#[test]
fn test_partitioned_leader_cannot_commit() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));

    // Commit something so we have a baseline
    c.propose_and_commit(b"before-partition");
    let commit_before = c.nodes[0].commit_index();

    // Partition: node 0 is alone, nodes 1-4 are together
    // Propose on the stale leader — it writes to its own log but
    // can't get acks from anyone
    let stale_index = c.nodes[0].propose(b"stale-write".to_vec()).unwrap();

    // Only deliver within the minority (just node 0)
    let minority: HashSet<NodeId> = [NodeId(0)].into();
    for _ in 0..10 {
        c.deliver_within(&minority);
    }

    // Stale leader should NOT have advanced commit_index
    assert!(
        c.nodes[0].commit_index() <= commit_before,
        "SAFETY VIOLATION: partitioned leader committed without quorum! \
         commit_before={}, commit_now={}, stale_index={}",
        commit_before, c.nodes[0].commit_index(), stale_index
    );
}

// ============================================================================
// PARTITION: Majority partition elects new leader, old leader steps down
// ============================================================================

#[test]
fn test_partition_heal_old_leader_steps_down() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"epoch-1");
    let old_term = c.nodes[0].term();

    // Partition: {1,2,3} are majority, {0,4} are minority
    let majority: HashSet<NodeId> = [NodeId(1), NodeId(2), NodeId(3)].into();

    // Expire election deadlines on all majority-partition nodes.
    // Without this, prevote is rejected because nodes still think
    // the old leader is alive (their deadline hasn't elapsed).
    let past = std::time::Instant::now() - std::time::Duration::from_secs(10);
    for &id in &[NodeId(1), NodeId(2), NodeId(3)] {
        c.nodes[id.0 as usize].election_deadline = past;
    }

    // Trigger election on node 1
    c.nodes[1].tick();
    for _ in 0..20 {
        c.deliver_within(&majority);
    }

    assert!(c.nodes[1].is_leader(), "node 1 should be leader in majority partition");
    let new_term = c.nodes[1].term();
    assert!(new_term > old_term, "new term should be higher");

    // Now heal the partition — deliver all messages
    // The old leader (node 0) must step down when it sees the higher term
    c.nodes[1].send_heartbeats();
    for _ in 0..20 {
        c.deliver_all();
    }

    assert!(
        !c.nodes[0].is_leader(),
        "old leader should have stepped down after partition heal"
    );
    assert!(
        c.nodes[0].term() >= new_term,
        "old leader should have updated its term"
    );
}

// ============================================================================
// SPLIT BRAIN: Both sides propose, only majority commits survive
// ============================================================================

#[test]
fn test_split_brain_only_majority_commits_survive() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"agreed");

    // Partition: {0,1} minority, {2,3,4} majority
    let majority: HashSet<NodeId> = [NodeId(2), NodeId(3), NodeId(4)].into();

    // Old leader (node 0) proposes — can't commit
    let _ = c.nodes[0].propose(b"minority-write".to_vec());
    let minority: HashSet<NodeId> = [NodeId(0), NodeId(1)].into();
    for _ in 0..10 { c.deliver_within(&minority); }

    // Elect leader in majority — expire deadlines so prevote succeeds
    let past = std::time::Instant::now() - std::time::Duration::from_secs(10);
    for &id in &[NodeId(2), NodeId(3), NodeId(4)] {
        c.nodes[id.0 as usize].election_deadline = past;
    }
    c.nodes[2].tick();
    for _ in 0..20 { c.deliver_within(&majority); }

    // Majority leader proposes and commits
    if c.nodes[2].is_leader() {
        let _ = c.nodes[2].propose(b"majority-write".to_vec());
        for _ in 0..20 { c.deliver_within(&majority); }
    }

    // Heal partition — force heartbeats by setting last_tick to the past,
    // and prevent spurious re-elections with a far-future deadline.
    let far_future = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let far_past = std::time::Instant::now() - std::time::Duration::from_secs(10);
    for node in &mut c.nodes {
        node.election_deadline = far_future;
        node.last_tick = far_past;
    }
    for _ in 0..30 {
        for node in &mut c.nodes {
            node.last_tick = far_past;
        }
        c.tick_all();
        c.deliver_all();
    }

    // The real Raft safety invariant: if two nodes both consider index i
    // committed, they must agree on the entry at index i.
    // We check: for every pair of nodes, their committed ranges must agree.
    for i in 0..c.nodes.len() {
        for j in (i + 1)..c.nodes.len() {
            let ci = c.nodes[i].commit_index();
            let cj = c.nodes[j].commit_index();
            let shared_commit = ci.min(cj);

            for idx in 1..=shared_commit {
                let ti = c.nodes[i].log().term_at(idx);
                let tj = c.nodes[j].log().term_at(idx);
                assert_eq!(
                    ti, tj,
                    "SAFETY VIOLATION: nodes {} and {} disagree at committed index {}! \
                     terms={:?} vs {:?}",
                    i, j, idx, ti, tj
                );
            }
        }
    }
}

// ============================================================================
// LOG DIVERGENCE: Follower has extra uncommitted entries from old term
// ============================================================================

#[test]
fn test_follower_extra_entries_get_truncated() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"base");

    let committed_index = c.nodes[0].commit_index();

    // Manually append stale entries to node 1 (as if it was a briefly-elected
    // leader that wrote but never replicated)
    for i in 0..5 {
        let entry = LogEntry::data(
            c.nodes[0].term() + 100, // bogus high term
            format!("stale-{}", i).into_bytes(),
        );
        c.nodes[1].core.log.push(entry);
    }

    let follower_last = c.nodes[1].log().last_index();
    assert!(follower_last > c.nodes[0].log().last_index());

    // Leader sends heartbeats — should detect mismatch and fix follower
    c.nodes[0].send_heartbeats();
    for _ in 0..30 { c.deliver_all(); }

    // After convergence, follower's committed entries must match leader's
    for idx in 1..=committed_index {
        let leader_term = c.nodes[0].log().term_at(idx);
        let follower_term = c.nodes[1].log().term_at(idx);
        assert_eq!(
            leader_term, follower_term,
            "Log mismatch at committed index {}", idx
        );
    }
}

// ============================================================================
// COMMIT SAFETY: Can't commit entries from prior terms via counting alone
// (Raft §5.4.2 — the Figure 8 problem)
// ============================================================================

#[test]
fn test_figure8_no_commit_old_term_entries() {
    // This is the classic Raft safety scenario.
    // An entry from an old term that's replicated to a majority
    // must NOT be committed until the leader also commits an entry
    // from its own term.

    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0)); // term 1

    // Leader appends entry but only replicates to node 1 (not a majority)
    let _ = c.nodes[0].propose(b"old-term-entry".to_vec());

    // Manually deliver only to node 1
    let msgs: Vec<_> = c.outbox.lock().unwrap().drain(..).collect();
    for (to, msg) in &msgs {
        if *to == NodeId(1) {
            if let Message::AppendEntries(from, req) = msg {
                let resp = c.nodes[1].recv_append_entries(req);
                c.outbox.lock().unwrap().push((*from, Message::AppendEntriesResp(*to, resp)));
            }
        }
    }
    c.deliver_all();

    // Now suppose node 0 dies. Node 2 gets elected in term 2.
    // The old-term entry is on nodes 0 and 1 but NOT committed.
    c.nodes[2].election_deadline = std::time::Instant::now();
    c.nodes[2].tick();
    for _ in 0..20 { c.deliver_all(); }

    // If node 2 is leader, it should not have committed the old-term entry
    // just because it might have been replicated. The entry needs a new-term
    // noop to push it through.
    if c.nodes[2].is_leader() {
        // The key check from try_advance_commit:
        // it only commits entries where term == current_term.
        // So old-term entries are only indirectly committed after a
        // new-term entry gets committed.
        let old_entry_idx = 2; // noop at 1, data at 2
        if let Some(entry) = c.nodes[2].log().get(old_entry_idx) {
            if entry.term < c.nodes[2].term() {
                // This entry is from an old term — it should only be
                // committed if there's also a current-term entry committed
                // at a higher index.
                let commit = c.nodes[2].commit_index();
                if commit >= old_entry_idx {
                    // Committed! Check that there's a current-term entry
                    // at or above the commit point
                    let has_current_term_entry = (old_entry_idx..=commit)
                        .any(|i| {
                            c.nodes[2].log().term_at(i) == Some(c.nodes[2].term())
                        });
                    assert!(
                        has_current_term_entry,
                        "SAFETY VIOLATION: committed old-term entry without \
                         committing a current-term entry first (Figure 8)"
                    );
                }
            }
        }
    }
}

// ============================================================================
// STALE MESSAGES: Responses from dead terms shouldn't corrupt state
// ============================================================================

#[test]
fn test_stale_append_entries_response_ignored() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"epoch1");
    let commit1 = c.nodes[0].commit_index();

    // Craft a stale response from an old term
    let stale_resp = AppendEntriesResp {
        term: 0, // ancient term
        success: true,
        conflict: None,
        match_index: 999, // bogus high match
        seq: 0,
    };

    c.nodes[0].recv_append_entries_response(NodeId(1), &stale_resp);

    // Commit index should not have moved
    assert_eq!(
        c.nodes[0].commit_index(), commit1,
        "stale AE response corrupted commit index"
    );
}

#[test]
fn test_stale_vote_response_after_becoming_leader() {
    let mut c = Cluster::new(5);
    c.elect_leader(NodeId(0));

    // Now node 0 is already leader. Send it a stale vote grant.
    let stale_vote = RequestVoteResp {
        term: 0,
        granted: true,
        kind: VoteKind::Vote,
    };
    c.nodes[0].recv_requestvote_response(NodeId(3), &stale_vote);

    // Should still be leader, nothing broken
    assert!(c.nodes[0].is_leader());
}

// ============================================================================
// REORDER: Out-of-order AppendEntries
// ============================================================================

#[test]
fn test_reordered_append_entries() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    // Propose two entries
    let _ = c.nodes[0].propose(b"first".to_vec()).unwrap();
    let _ = c.nodes[0].propose(b"second".to_vec()).unwrap();

    // Grab all messages, deliver them in reverse order to node 1
    let msgs: Vec<_> = c.outbox.lock().unwrap().drain(..).collect();

    let ae_to_node1: Vec<_> = msgs.iter()
        .filter(|(to, msg)| *to == NodeId(1) && matches!(msg, Message::AppendEntries(..)))
        .cloned()
        .collect();

    // Deliver in reverse
    for (to, msg) in ae_to_node1.into_iter().rev() {
        if let Message::AppendEntries(from, req) = msg {
            let resp = c.nodes[to.0 as usize].recv_append_entries(&req);
            c.outbox.lock().unwrap().push((from, Message::AppendEntriesResp(to, resp)));
        }
    }

    // Deliver remaining + responses
    for _ in 0..20 { c.deliver_all(); }

    // After convergence, logs should match
    let leader_last = c.nodes[0].log().last_index();
    for idx in 1..=leader_last {
        assert_eq!(
            c.nodes[0].log().term_at(idx),
            c.nodes[1].log().term_at(idx),
            "log divergence at index {} after reordering", idx
        );
    }
}

// ============================================================================
// RAPID ELECTIONS: Many candidates, terms should converge
// ============================================================================

#[test]
fn test_rapid_elections_converge() {
    let mut c = Cluster::new(5);

    // All 5 nodes try to become leader simultaneously
    for i in 0..5 {
        c.nodes[i].election_deadline = std::time::Instant::now();
        c.nodes[i].tick();
    }

    // Deliver a bunch of rounds
    for _ in 0..50 {
        c.deliver_all();
    }

    // At most one leader
    let leaders: Vec<_> = c.nodes.iter()
        .filter(|n| n.is_leader())
        .map(|n| (n.id(), n.term()))
        .collect();

    assert!(
        leaders.len() <= 1,
        "multiple leaders after rapid elections: {:?}", leaders
    );
}

// ============================================================================
// TERM EXPLOSION: Partitioned node rejoins with very high term
// ============================================================================

#[test]
fn test_high_term_node_rejoins() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"before");

    let committed_before = c.nodes[0].commit_index();

    // Node 2 was partitioned and had many failed elections,
    // driving its term way up
    c.nodes[2].become_follower(1000, None);

    // Now it sends a vote request with term 1000
    // This should cause the leader to step down
    c.nodes[2].election_deadline = std::time::Instant::now();
    c.nodes[2].tick();

    for _ in 0..30 { c.deliver_all(); }

    // Eventually some node becomes leader
    // The key safety check: committed entries must survive
    let mut found_leader = false;
    for _ in 0..10 {
        let past = std::time::Instant::now() - std::time::Duration::from_secs(10);
        for i in 0..3 {
            c.nodes[i].election_deadline = past;
            c.nodes[i].tick();
        }
        for _ in 0..20 { c.deliver_all(); }
        if c.leader().is_some() {
            found_leader = true;
            break;
        }
    }

    if found_leader {
        let leader_id = c.leader().unwrap();
        let leader = &c.nodes[leader_id.0 as usize];

        // All previously committed entries must be in the new leader's log
        for idx in 1..=committed_before {
            assert!(
                leader.log().get(idx).is_some(),
                "SAFETY VIOLATION: committed entry at index {} missing from \
                 new leader (node {})", idx, leader_id.0
            );
        }
    }
}

// ============================================================================
// DUPLICATE MESSAGES: Same AppendEntries delivered twice
// ============================================================================

#[test]
fn test_duplicate_append_entries_idempotent() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));

    let _ = c.nodes[0].propose(b"dup-test".to_vec()).unwrap();

    // Grab the AE message to node 1
    let msgs: Vec<_> = c.outbox.lock().unwrap().drain(..).collect();
    let ae_msg = msgs.iter()
        .find(|(to, msg)| *to == NodeId(1) && matches!(msg, Message::AppendEntries(..)))
        .cloned();

    // Deliver it twice
    if let Some((to, Message::AppendEntries(from, req))) = ae_msg {
        let resp1 = c.nodes[to.0 as usize].recv_append_entries(&req);
        let resp2 = c.nodes[to.0 as usize].recv_append_entries(&req);

        // Both should succeed
        assert!(resp1.success);
        assert!(resp2.success);

        // Log length should be the same (not doubled)
        assert_eq!(resp1.match_index, resp2.match_index,
                   "duplicate AE caused log to grow");
    }

    // Deliver everything else
    for (to, msg) in msgs {
        let idx = to.0 as usize;
        if idx < c.nodes.len() {
            if let Message::AppendEntries(from, req) = &msg {
                let resp = c.nodes[idx].recv_append_entries(req);
                c.outbox.lock().unwrap().push((*from, Message::AppendEntriesResp(to, resp)));
            }
        }
    }
    for _ in 0..10 { c.deliver_all(); }
}

// ============================================================================
// EMPTY HEARTBEATS: Leader sends empty AE, follower shouldn't regress
// ============================================================================

#[test]
fn test_empty_heartbeat_no_regression() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    let follower_commit = c.nodes[1].commit_index();
    let follower_last = c.nodes[1].log().last_index();

    // Send empty heartbeat
    let hb = AppendEntries {
        term: c.nodes[0].term(),
        leader_id: NodeId(0),
        prev_log: LogPosition {
            index: follower_last,
            term: c.nodes[0].term(),
        },
        leader_commit: c.nodes[0].commit_index(),
        seq: 99,
        entries: vec![],
    };

    let resp = c.nodes[1].recv_append_entries(&hb);
    assert!(resp.success);

    // Log should not have shrunk
    assert!(c.nodes[1].log().last_index() >= follower_last);
    // Commit should not have regressed
    assert!(c.nodes[1].commit_index() >= follower_commit);
}

// ============================================================================
// READINDEX: Stale leader's pending reads are cleared on stepdown
// ============================================================================

#[test]
fn test_read_index_cleared_on_stepdown() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"data");

    // Queue a read
    c.nodes[0].request_read_index(100).unwrap();

    // Don't deliver — instead force stepdown
    c.drop_all_messages();
    let new_term = c.nodes[0].term() + 1;
    c.nodes[0].become_follower(new_term, None);

    // The pending read should have been failed (None)
    let msgs = c.outbox.lock().unwrap();
    let failed_read = msgs.iter().find(|(_, m)| {
        matches!(m, Message::ReadIndexComplete(100, None))
    });
    assert!(
        failed_read.is_some(),
        "pending read should be failed with None on stepdown"
    );
}

// ============================================================================
// PROPOSE AFTER SHUTDOWN: Multiple propose variants
// ============================================================================

#[test]
fn test_all_operations_rejected_after_shutdown() {
    let mut c = Cluster::new(3);
    c.elect_leader(NodeId(0));
    c.nodes[0].shutdown();

    assert_eq!(c.nodes[0].propose(b"x".to_vec()), Err(RaftError::Shutdown));
    assert_eq!(
        c.nodes[0].propose_batch(&[b"a".to_vec()]),
        Err(RaftError::Shutdown)
    );
    assert_eq!(c.nodes[0].propose_noop(), Err(RaftError::Shutdown));
}

// ============================================================================
// LOG COMPACTION: Entries before snapshot are gone
// ============================================================================

#[test]
fn test_log_compact_then_append() {
    let mut log = Log::new();
    for i in 1..=10 {
        log.push(LogEntry::data(1, format!("entry-{}", i).into_bytes()));
    }

    // Compact through index 7
    log.compact(7);

    assert_eq!(log.base(), 7);
    assert_eq!(log.first_index(), 8);
    assert_eq!(log.len(), 3);
    assert!(log.get(7).is_none()); // compacted
    assert!(log.get(8).is_some()); // still there

    // Can still append
    log.push(LogEntry::data(2, b"new".to_vec()));
    assert_eq!(log.last_index(), 11);
}

// ============================================================================
// CONVERGENCE: After chaos, all committed entries match
//
// This is the big one — random-ish sequence of elections, proposals,
// partial deliveries, then full heal. All committed entries must agree.
// ============================================================================

#[test]
fn test_chaos_convergence() {
    let mut c = Cluster::new(5);

    // Phase 1: normal operation
    c.elect_leader(NodeId(0));
    c.propose_and_commit(b"stable-1");
    c.propose_and_commit(b"stable-2");

    // Phase 2: partition {0,1} vs {2,3,4}
    let part_a: HashSet<NodeId> = [NodeId(0), NodeId(1)].into();
    let part_b: HashSet<NodeId> = [NodeId(2), NodeId(3), NodeId(4)].into();

    // Old leader proposes (can't commit)
    let _ = c.nodes[0].propose(b"partitioned-write".to_vec());
    for _ in 0..5 { c.deliver_within(&part_a); }

    // New leader in majority partition — expire deadlines so prevote succeeds
    let past = std::time::Instant::now() - std::time::Duration::from_secs(10);
    for &id in &[NodeId(2), NodeId(3), NodeId(4)] {
        c.nodes[id.0 as usize].election_deadline = past;
    }
    c.nodes[2].tick();
    for _ in 0..20 { c.deliver_within(&part_b); }

    if c.nodes[2].is_leader() {
        let _ = c.nodes[2].propose(b"majority-1".to_vec());
        let _ = c.nodes[2].propose(b"majority-2".to_vec());
        for _ in 0..20 { c.deliver_within(&part_b); }
    }

    // Phase 3: heal — reset election deadlines to prevent re-elections,
    // and set last_tick to the past so heartbeat timers fire on tick().
    let far_future = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let far_past = std::time::Instant::now() - std::time::Duration::from_secs(10);
    for node in &mut c.nodes {
        node.election_deadline = far_future;
        node.last_tick = far_past;
    }
    for _ in 0..50 {
        // Reset last_tick each round so heartbeats keep firing
        for node in &mut c.nodes {
            node.last_tick = far_past;
        }
        c.tick_all();
        c.deliver_all();
    }

    // Verify: for any two nodes, their committed prefixes must agree.
    for i in 0..c.nodes.len() {
        for j in (i + 1)..c.nodes.len() {
            let shared_commit = c.nodes[i].commit_index().min(c.nodes[j].commit_index());

            for idx in 1..=shared_commit {
                let ti = c.nodes[i].log().term_at(idx);
                let tj = c.nodes[j].log().term_at(idx);
                assert_eq!(
                    ti, tj,
                    "SAFETY VIOLATION at index {}: node {} has term {:?}, \
                     node {} has term {:?}",
                    idx, i, ti, j, tj
                );
            }
        }
    }

    // Verify: exactly one leader (or zero, if between elections)
    let leaders: Vec<_> = c.nodes.iter()
        .filter(|n| n.is_leader())
        .map(|n| (n.id(), n.term()))
        .collect();
    let all_states: Vec<_> = c.nodes.iter()
        .map(|n| (n.id(), n.term(), n.is_leader(), n.commit_index()))
        .collect();
    assert!(
        leaders.len() <= 1,
        "multiple leaders after convergence: {:?}\nall node states: {:?}",
        leaders, all_states
    );
}