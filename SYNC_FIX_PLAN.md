# Sync Fix Plan: iroh + Automerge

**Date:** 2026-11-16
**Scope:** `src/sync/` (mod.rs, automerge_sync.rs, peers.rs, doc.rs, identity.rs) and the sync wiring in `src/main.rs`.
**Baseline:** `cargo check --target wasm32-unknown-unknown` passes. API behavior verified against docs.rs (automerge 0.12.0, iroh 1.4.1).

This plan is the actionable counterpart to the issues found in the sync review. The architecture doc (`SYNC_ARCHITECTURE.md`) has been updated to match; this file tracks the work.

## Status (2026-11-16)

All five phases are implemented and `cargo check --target wasm32-unknown-unknown` passes cleanly:

- **Phase 1 (C1, H1, m2):** `run_automerge_sync` rewritten — the generated message is used for both the send and the termination check (no more deadlock); takes a per-peer `&mut State` (persisted to localStorage via `encode`/`decode`); the UI signal is only set when the doc heads change.
- **Phase 2 (C2, H4):** the engine doc is the single source of truth. Local handlers apply edits to the engine doc (`doc::add_expense`/`remove_expense`) and `on_local_change` refreshes the signal, persists, and notifies peers. The engine doc is persisted after every local change and every remote merge.
- **Phase 3 (C3, C4, M4):** `send_doc_to_peer` deleted. One persistent bi-stream per connection (`peer_sync_task_impl`) — initial catch-up then an event-driven `tokio::select!` loop (block on read; respond; push on local-change notification). No busy-loops, no per-round stream opens.
- **Phase 4 (H2, H3):** `known_peers.<device_id>.node_address` is written into the automerge doc on scan/connect (full `EndpointAddr`), and the peer loop dials peers from the doc's `known_peers` map (falling back to the localStorage registry). Incoming peers keep any previously-known full address. **Each device also writes its own address under `known_peers.<self_id>` on every connection**, so the doc sync carries it to the peer and, transitively, to that peer's peers — the accepter learns the dialer's address without ever seeing its QR. The background loop skips the device's own id to avoid self-dialing. Stored/synced addresses keep only the id + home relay (`EndpointAddr::new(id).with_relay_url(relay)`); direct IPs are dropped since they go stale quickly and are re-discovered via hole-punching at connect time (the full address is kept if no relay is present).
- **Phase 5 (M1, M3, m1, m3, m4):** identity-derived actor applied via `set_actor`; atomic check-then-insert dedup + in-flight dial tracking; `set_status` no longer writes localStorage; "Clear All Data" removes `iroh_share_group_doc` and `iroh_share_sync_states`; unused `getrandom` dependency dropped.

---

## Issue inventory

### Critical

| # | Issue | Location | Fix |
|---|---|---|---|
| C1 | **Sync protocol deadlocks whenever changes need to be exchanged.** The termination check calls `generate_sync_message(&mut state)` and discards the result; the call sets `state.in_flight = true`, so the next loop iteration generates `None` and both sides block forever in `read_exact`. Happens on every real merge (QR scan). | `automerge_sync.rs:64-73` | Rewrite the loop: generate → send → test `is_none()` on the **same** message; break only when `msg.is_none() && !state.in_flight`. |
| C2 | **Local edits never reach the engine doc, and get reverted in the UI.** UI mutates the `doc` signal; `on_local_change` sends `self.doc` (engine copy, stale) and then `doc_signal.set(engine_doc)` clobbers the UI edit. | `mod.rs:136-161`, `main.rs:154-228` | Make the engine doc the single source of truth: local handlers must apply edits to the engine doc (or hand the edited doc to the engine) before pushing. |
| C3 | **`send_doc_to_peer` hangs forever.** It opens a bi-stream and blocks on `read_exact` for a reply, but nothing on the peer side ever calls `conn.accept_bi()` — the stream is never accepted. `on_local_change` hangs on the first peer. | `peers.rs:362-414` | Remove the full-doc push path entirely; route local changes through the same incremental `run_automerge_sync` protocol used everywhere else. |
| C4 | **Background "keep-alive" loops are tight busy-loops.** `loop { run_automerge_sync(...) }` with no wait: each round opens a new stream, re-runs the protocol, sets the UI signal, and repeats immediately — pegging the wasm thread and spamming streams. | `peers.rs:269-287`, `peers.rs:336-354`, `mod.rs:226-255` | One persistent bi-stream per connection with an event-driven loop (block on read; respond when a message arrives; push when local changes occur), plus backoff on error. |

### High

| # | Issue | Location | Fix |
|---|---|---|---|
| H1 | **Sync `State` is recreated per exchange.** Fresh `State::new()` every `run_automerge_sync` call re-sends the full heads/bloom-filter have-list on every round. Automerge docs: persist with `State::encode()`/`decode()` per peer. | `automerge_sync.rs:22` | Store `State` per peer (keyed by device_id) in the engine; reuse across sync sessions. |
| H2 | **Transitive sync broken (a): `known_peers` never written into the automerge doc.** The doc map exists (`doc.rs`) but nothing populates it, so peer addresses never merge transitively. | `doc.rs:21-27`, `mod.rs:166-262` | On scan/connect, write `known_peers.<device_id>.node_address` into the automerge doc (full `EndpointAddr` string). |
| H3 | **Transitive sync broken (b): incoming peers stored with a relay-less address.** `handle_incoming_connection` registers `node_address = device_id` (bare hex key); `parse_node_address` falls back to `EndpointAddr::new(pk)` with no relay/direct addresses → cannot dial over the internet. | `peers.rs:305-307`, `peers.rs:419-440` | Store the full `EndpointAddr` (from the QR payload / connection info) as the node address. |
| H4 | **Remote changes never persisted.** Engine doc is merged by sync but never saved; `save_to_storage` only runs on local edits and saves the UI signal. Remote changes lost on refresh. | `main.rs:230-239`, `mod.rs` | Persist the engine doc to `iroh_share_group_doc` after every merge and every local change. |

### Medium

| # | Issue | Location | Fix |
|---|---|---|---|
| M1 | `actor_id` computed but never applied (`AutoCommit::set_actor` never called); new docs get a random actor. | `identity.rs`, `mod.rs:30`, `doc.rs:21-27` | Apply the identity-derived actor to the doc on creation/load. |
| M2 | Doc mutex held across blocking network I/O (`read_exact`), and in the deadlock case forever. | `automerge_sync.rs`, `mod.rs:205-211` | Don't hold the doc lock across I/O; snapshot the doc for sending, apply received changes under a short lock. |
| M3 | Duplicate-connection races: `scan_and_merge` dials before checking `connections`; `handle_incoming_connection` never dedupes; `PeerSyncLoop` can spawn duplicate dials while one is in flight. | `mod.rs:186-193`, `peers.rs:294-317`, `peers.rs:182-220` | Atomic check-then-insert under the connections lock; dedupe by device_id in `handle_incoming_connection`; track in-flight dials. |
| M4 | Two concurrent sync mechanisms on one connection: background loop (incremental) + `send_doc_to_peer` (full doc). | `peers.rs` | Single sync path per connection (see C3). |

### Minor

| # | Issue | Location | Fix |
|---|---|---|---|
| m1 | `set_status` writes localStorage on every transition (Connecting/Connected/Synced/Disconnected). | `peers.rs:125-132` | Persist only on add/remove, not on status changes. |
| m2 | `run_automerge_sync` sets the UI signal on every round even when nothing changed. | `automerge_sync.rs:80` | Only set the signal when the doc actually changed (compare heads). |
| m3 | Debug "Clear All Data" removes unused keys (`iroh_share_actor_id`, `slopshare_data`) but not `iroh_share_group_doc` / `iroh_share_known_peers`. | `main.rs:806-818` | Remove the real keys. |
| m4 | `getrandom` dependency unused (cargo warning). | `Cargo.toml:10` | Drop it. |

---

## Fix phases

### Phase 1 — Fix the sync protocol (C1, H1, m2)
`src/sync/automerge_sync.rs`

1. Rewrite `run_automerge_sync` loop:
   ```rust
   loop {
       let msg = sync_doc.generate_sync_message(&mut state);
       if let Some(msg) = msg.as_ref() {
           let encoded = msg.encode();
           send_stream.write_all(&(encoded.len() as u32).to_be_bytes()).await?;
           send_stream.write_all(&encoded).await?;
           send_stream.flush().await?;
       }
       if msg.is_none() && !state.in_flight {
           send_stream.write_all(&0u32.to_be_bytes()).await?; // end-of-sync marker
           send_stream.flush().await?;
           break;
       }
       // read length-prefixed message; 0-length => peer is done
       // receive_sync_message(...)
   }
   ```
2. Take a `&mut State` parameter (or a per-peer state map) instead of `State::new()` inside; persist with `encode()`/`decode()` per peer.
3. Only `doc_signal.set(...)` when the doc heads changed.

### Phase 2 — Single source of truth (C2, H4)
`src/sync/mod.rs`, `src/main.rs`

1. Local edit handlers (`add_expense`, `remove_expense`) must apply the edit to the **engine doc** (lock, mutate, unlock), then call `on_local_change`.
2. `on_local_change`: push the engine doc's *new changes* to peers via the incremental protocol (Phase 1), then refresh the signal from the engine doc.
3. Persist the engine doc to `iroh_share_group_doc` after every local change and every remote merge.

### Phase 3 — Single sync path per connection (C3, C4, M4)
`src/sync/peers.rs`, `src/sync/mod.rs`

1. Delete `send_doc_to_peer` and the full-doc push path.
2. Replace the busy-loop with one persistent bi-stream per connection:
   - Open one bi-stream at connect time; both sides block on `read_exact` (event-driven).
   - On local change: generate + send a sync message on that stream.
   - On receive: `receive_sync_message`, apply, respond, refresh signal.
   - On error/close: mark peer disconnected, remove from `connections`, backoff before redial.
3. `scan_and_merge` and `connect_and_maintain` share this one loop (no per-call `open_bi`).

### Phase 4 — Transitive sync (H2, H3)
`src/sync/mod.rs`, `src/sync/peers.rs`

1. On scan/connect, write `known_peers.<device_id>.node_address` (full `EndpointAddr` JSON) into the automerge doc so it merges transitively.
2. `handle_incoming_connection`: store the full `EndpointAddr` (from the QR payload when available; otherwise from `conn`'s peer info) instead of the bare hex key.
3. `PeerSyncLoop` reads peers from the doc's `known_peers` map (falling back to the localStorage registry for backward compat).

### Phase 5 — Races & cleanup (M1, M3, m1, m3, m4)

1. Apply identity-derived actor to the doc (`AutoCommit::set_actor`).
2. Atomic duplicate-connection guard: check-then-insert under the connections lock; dedupe incoming; track in-flight dials.
3. `set_status` stops writing localStorage; persist only on add/remove.
4. Fix "Clear All Data" to remove `iroh_share_group_doc` and `iroh_share_known_peers`.
5. Drop the unused `getrandom` dependency.

---

## Validation

1. `cargo check --target wasm32-unknown-unknown` after each phase.
2. Two-device test: A and B each add expenses, scan each other's QR → both UIs converge, no hang (Phase 1 fixes the deadlock; Phase 2/3 make edits flow both ways).
3. Refresh test: reload a device after a remote merge → merged doc persists (Phase 2).
4. Transitive test: A scans C, B scans C, C goes offline → A and B sync directly (Phase 4).
5. Watch for: no busy-loop stream churn (Phase 3), no UI re-render storms (Phase 1/m2).
