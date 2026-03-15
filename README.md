# todo-raft

WHAT KIND OF CONSENSUS IS YOUR TYPE?

minimal raft in rust. no runtime. no async. bring your own network/storage.

## features
- leader election (prevote)
- log replication
- snapshots
- readindex

## usage

implement `LogStore`, `StateMachine`, `Transport`. call `tick()`. done.

```rust
let raft = Raft::new(
    NodeId(0),
    vec![NodeId(0), NodeId(1), NodeId(2)],
    Config::default(),
    your_log_store,
    your_state_machine,
    your_transport,
)?;

// in your event loop
raft.tick();
raft.propose(data)?;
```

three traits instead of one because the borrow checker doesn't negotiate.

## building
```
cargo build
```

## notes

`skip_next_fsync()` exists for batching systems where durability is handled externally. if you don't know why you'd skip an fsync, don't call it.

## TODO (haha)
- membership changes

MIT
