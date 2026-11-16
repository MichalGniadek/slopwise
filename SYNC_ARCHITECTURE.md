# Sync Architecture: iroh + Automerge + Dioxus

**Project:** `iroh-share` (Dioxus, wasm target)
**Status:** Implemented but not yet working end-to-end. The code compiles (`cargo check --target wasm32-unknown-unknown`), but a review found critical bugs in the sync layer: a sync-protocol deadlock, local edits never reaching the engine doc, a hanging push path, and busy-loop background sync. See `SYNC_FIX_PLAN.md` for the issue inventory and fix phases.
**Goal:** A local-first, peer-to-peer sync engine where local edits instantly update the UI and remote sync changes flow back into the same reactive signal.

---

## 1. Goals

- Each device belongs to **exactly one group** at a time (expenses, people, history).
- One button shows a QR code; another scans a QR code.
- Scanning another device's QR **merges both groups into one common shared group**.
- All sync is direct peer-to-peer over iroh QUIC streams. No gossip, no central server.
- On QR scan, both apps exchange their known-peer lists so transitive sync works without the introducer being online.
- Groups are small (<5), so each app connects to all known peers and syncs with all of them.
- Local-first: changes apply instantly locally; sync happens in the background when peers are online.

---

## 2. Core concepts

| Concept | Meaning |
|---|---|
| **Device** | One iroh node (one `Endpoint` / `SecretKey`). Identified solely by its hex `EndpointId`. No per-device user name. Persisted across restarts. |
| **Actor ID** | One automerge `ActorId` per device, reused for the life of the device (keeps doc history compact). |
| **Group** | One automerge `AutoCommit` doc. Contains expenses, people, balances, and the known-peer list. |
| **Known peers** | A map inside the automerge doc: `device_id → { node_address }`. Exchanged on every QR scan so peers learn about each other transitively. |
| **Membership** | A device stores the automerge doc bytes for its current group. Exactly one at a time. |

---

## 3. Data model (automerge doc shape)

```
root: Map {
  device_id: String,         // hex of this device's iroh EndpointId
  known_peers: Map {         // peers we have synced with before
    <device_id>: Map {
      node_address: String,  // iroh NodeAddress string (for dialing)
    }
  },
  expenses: List [ Map { desc, total, payer, included (JSON string), time } ],
}
```

`known_peers` lives in the same automerge doc, so it merges automatically when two devices sync. When A scans C, C's doc gains A's address (and vice versa). When B later scans C, B's doc receives C's `known_peers` — which now includes A — so B can dial A directly even if C is offline.

The existing UI reads `expenses` and computes balances; we keep that layer untouched and add a sync layer underneath.

---

## 4. Identity & the QR code

### Device identity

Each iroh `Endpoint` has a `SecretKey`/`PublicKey` (`EndpointId`). Generate once at first launch, persist to localStorage (wasm) or disk. This is the device's stable address.

### QR code payload

A JSON object, serialized to a string and rendered as a QR code:

```json
{
  "v": 1,
  "app": "iroh-share",
  "node_address": "<iroh NodeAddress as string>"
}
```

- `node_address` encodes the `EndpointId` + relay URL + direct addresses, so another device can dial this endpoint directly.
- Keep the payload minimal (no doc bytes in the QR) to stay within QR capacity limits (~2-3 KB for alphanumeric at reasonable density).

### Display

- Button "Show QR" → render the QR (e.g. a `qrcode` wasm library or an SVG generator) containing the payload above.
- Button "Scan QR" (mobile) → open camera (`mediaDevices.getUserMedia`) or fall back to an image-file input (`<input type="file" accept="image/*">`) on web; decode QR, parse payload, initiate merge.

---

## 5. Transport: direct iroh streams only

One `iroh::Endpoint` per device. Peers find each other by `NodeAddress` (EndpointId + relay URL + direct addresses), exchanged via QR code or via known-peer lists.

- **ALPN:** `iroh-share-sync` (custom protocol for automerge sync).
- Connections are bi-di QUIC streams. iroh handles hole punching + relay fallback automatically.
- Since groups are tiny (<5), each device opens a direct stream to every known peer and runs the automerge sync protocol. No broadcast, no flooding, no topic management.

---

## 6. Sync engine architecture

### 6.1 SyncEngine (single owner)

The `SyncEngine` is the single owner of:
- the iroh `Endpoint`
- the automerge `AutoCommit` doc (wrapped in `Arc<Mutex<...>>`)
- active persistent connections to peers (`HashMap<device_id, Connection>`)
- the `PeerManager` (known-peer registry + status)

It exposes a small, Dioxus-friendly API:

```rust
pub struct SyncEngine {
    endpoint: Endpoint,
    doc: Arc<Mutex<AutoCommit>>,
    peer_manager: PeerManager,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    device_id: SecretKey,
    actor_id: ActorId,
}

impl SyncEngine {
    pub async fn init() -> Result<Self>;           // restore identity + doc, start peer loop + accept loop
    pub fn qr_payload(&self) -> QrPayload;          // for "Show QR"
    pub async fn scan_and_merge(&self, payload: &QrPayload) -> Result<()>; // dial peer, exchange docs, merge
    pub fn on_local_change(&self);                  // triggered when local doc changes; pushes to peers
    pub fn doc_signal(&self) -> Signal<AutoCommit>; // bridge to Dioxus (see section 7)
}
```

### 6.2 PeerSyncLoop (background task)

A background task runs on each device:

1. Read `known_peers` from the local automerge doc (fall back to the localStorage registry for peers added before this was implemented).
2. For each known peer that isn't currently connected:
   - Dial the peer's `node_address` over iroh (`Endpoint::connect(addr, alpn="iroh-share-sync")`).
   - Open a bi-di stream.
   - Run the automerge sync exchange until both sides are caught up.
3. Keep **one persistent bi-di stream per connection** open for real-time bidirectional sync: both sides block on read, respond to incoming sync messages, and push new local changes on the same stream. Do **not** re-run the sync exchange in a tight loop or open a new stream per exchange — that busy-loops the wasm thread and spams streams.
4. Repeat the dial pass on an interval (e.g. every 30s) or when the app regains focus / network comes back.

Failed dials are silently retried on the next loop iteration.

### 6.3 Automerge sync protocol (point-to-point catch-up)

Both the initial QR-scan merge and the background sync loop use the same automerge sync protocol (`AutoCommit::sync()` → `SyncDoc`) over a bi-di iroh stream:

1. Peer A calls `generate_sync_message()` → sends sync msg 1 to peer B.
2. B calls `receive_sync_message(msg1)` → generates reply msg 2; sends it back.
3. A calls `receive_sync_message(msg2)` → generates reply msg 3; sends it back.
4. Repeat until a side has nothing to send: `generate_sync_message()` returns `None` **and** `State::in_flight` is `false` (there is no `SyncDoc::is_done()` in automerge 0.12). Signal end-of-sync with a zero-length message; the peer does the same.

Implementation notes (these are the current bugs, see `SYNC_FIX_PLAN.md`):
- Generate, send, and test `is_none()` on the **same** message. Discarding a generated message leaves `State::in_flight = true`, so the next `generate_sync_message()` returns `None` and both sides block forever in `read_exact` — a deadlock on every real merge.
- Persist the per-peer `State` across sessions with `State::encode()`/`decode()`. Recreating it per exchange re-sends the full heads/bloom-filter have-list every time.
- Do not hold the doc lock across blocking stream I/O.

The protocol exchanges only the changes needed to converge, rather than sending the entire doc on every sync. When changes are applied locally, the Dioxus signal reacts and the UI re-renders; the engine doc is persisted to localStorage after every merge and every local change.

### 6.4 Persistence

`localStorage` keys:
- `iroh_share_device_secret_key` (hex) — device identity (the automerge `ActorId` is derived from this key; there is no separate `iroh_share_actor_id` key)
- `iroh_share_group_doc` (binary, base64-encoded) — automerge doc bytes for the current group (includes `known_peers`)
- `iroh_share_known_peers` (JSON) — known-peer registry for the peer loop (legacy; peers should also live in the doc's `known_peers` map so they merge transitively)

On launch: rebuild the `Endpoint` from the secret key, load the current group's doc bytes into an `AutoCommit`, start the background sync loop against known peers.

Persistence rule: the engine doc is the source of truth and must be written to `iroh_share_group_doc` after **every** local change **and** every remote merge — not only on local UI edits (the current implementation loses remote changes on refresh).

---

## 7. Dioxus UI integration (the reactive bridge)

This is the core of the request: **bidirectional reactivity between the automerge doc and the Dioxus UI.**

### 7.1 Single source of truth

The Dioxus `App` component holds one `use_signal(|| AutoCommit)` — call it `doc_signal`. Every UI read derives from `doc_signal.read()`.

The `SyncEngine` owns the authoritative `Arc<Mutex<AutoCommit>>` (its working copy). The bridge keeps them in sync:

| Direction | Mechanism |
|---|---|
| **Local edit → UI → peers** | User edits → apply the edit to the **engine doc** (not just the signal) → `doc_signal.set(...)` → `engine.on_local_change()` → push the new changes to all connected peers |
| **Remote sync → engine → UI** | Peer sends changes → engine merges into its `Arc<Mutex<AutoCommit>>` → engine calls `doc_signal.set(merged_doc)` → UI re-renders |

⚠️ The current implementation only does the second row: local handlers mutate the signal and `on_local_change` pushes the engine's *stale* doc, then overwrites the signal with it — reverting the user's edit and never sending it to peers. Local edits must be applied to the engine doc first (see `SYNC_FIX_PLAN.md` Phase 2).

### 7.2 Local change flow (UI → network)

1. User adds/removes an expense in the Dioxus form.
2. Handler clones `doc_signal.read()`, mutates the clone, calls `doc_signal.set(clone)`.
3. UI re-renders instantly (local-first, optimistic).
4. Handler calls `engine.on_local_change()` (spawned async).
5. `on_local_change` iterates `connections`, sends current `doc.save()` bytes to each connected peer over its open bi-di stream.
6. Each peer receives, merges, and updates its own UI signal.

### 7.3 Remote change flow (network → UI)

1. Background `PeerSyncLoop` or an incoming connection receives doc bytes from a peer.
2. Engine loads peer bytes into a temp `AutoCommit`, merges into its local `Arc<Mutex<AutoCommit>>`.
3. Engine calls `doc_signal.set(engine_doc.clone())` — this is the critical bridge call.
4. Dioxus detects the signal change and re-renders the affected components (balances, expense list, etc.).
5. `save_to_storage` persists the updated doc to localStorage.

### 7.4 Why `Signal<AutoCommit>` and not individual signals?

Automerge `AutoCommit` is a CRDT document. Individual field signals would require tracking every path and reconciling merges per-field. Storing the whole `AutoCommit` in one Dioxus signal is simpler and correct for small docs (<5 people, modest expense lists). The cost is cloning the doc on each set, which is acceptable at this scale.

---

## 8. Module structure (Rust)

```
src/
  main.rs              # Dioxus App (existing UI) — thin layer, calls sync engine
  qr.rs                # QR encode/decode + payload struct (exists)
  sync/
    mod.rs             # SyncEngine struct: owns Endpoint, doc, connections, peer loop
    identity.rs        # Device secret key, ActorId, persistence to localStorage (exists)
    doc.rs             # Helpers: save/load AutoCommit to bytes, group doc shape (exists)
    automerge_sync.rs  # Automerge SyncDoc exchange over an iroh bi-di stream (exists)
    peers.rs           # Known-peer management + background dial/sync loop (exists)

(There is no `merge.rs`; the incremental `SyncDoc` protocol in `automerge_sync.rs` is the only exchange mechanism.)
```

### SyncEngine API sketch

```rust
pub struct SyncEngine {
    endpoint: Endpoint,
    doc: Arc<Mutex<AutoCommit>>,
    peer_manager: PeerManager,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    device_id: SecretKey,
    actor_id: ActorId,
}

impl SyncEngine {
    pub async fn init() -> Result<Self, Box<dyn std::error::Error + Send + Sync>>;
    pub fn qr_payload(&self) -> QrPayload;
    pub async fn scan_and_merge(&self, payload: &QrPayload, doc_signal: Signal<AutoCommit>) -> Result<()>;
    pub fn on_local_change(&self, doc_signal: Signal<AutoCommit>);
    pub fn doc_signal(&self) -> Signal<AutoCommit>;
    pub async fn known_peers(&self) -> Vec<peers::KnownPeer>;
    pub async fn peer_status_summary(&self) -> String;
}
```

`on_local_change` and `scan_and_merge` take the Dioxus `Signal<AutoCommit>` so the engine can push merged results back into the UI. Alternatively, the engine exposes a callback/closure set at init time.

---

## 9. State machine (per device)

```
IDLE
  ├─ "Show QR" ──> DISPLAYING_QR
  ├─ "Scan QR" ──> SCANNING (camera / file input)
  ├─ scanned payload ──> MERGING (dial peer, exchange docs, automerge merge)
  │                         ├─ success ──> IDLE (new group, known_peers merged, peer loop running)
  │                         └─ failure ──> IDLE (error toast)
  └─ local edit ──> PEER_SYNC (background loop dials known peers, automerge sync)
```

---

## 10. Group merge flow (the scan flow)

This is the core UX. Let device **S** (scanner) scan device **P** (scanned / peer).

1. **P displays QR.** P's QR contains P's `node_address` (serialized `EndpointAddr`).
2. **S scans P's QR.** S parses the payload: gets P's `NodeAddress`.
3. **S dials P directly** over iroh (`Endpoint::connect(node_address, alpn="iroh-share-sync")`).
4. **Both devices run the automerge sync protocol** over a bi-di stream (`AutoCommit::sync()` → `SyncDoc`), exchanging only the changes needed to converge.
5. **Both devices merge** the received changes into their local doc. Result: both devices now have a doc containing both groups' expenses, people, and `known_peers`. Conflicts resolve deterministically via automerge.
6. **Both devices persist** the merged doc to localStorage.
7. **Both devices add each other to `known_peers`** — write `known_peers.<device_id>.node_address` (the full `EndpointAddr` string, with relay URL) into the automerge doc so it merges automatically and transitively. (The current implementation never writes into the doc's `known_peers` map — see `SYNC_FIX_PLAN.md` Phase 4.)
8. **Both devices start background sync** against all known peers (including each other and any peers learned from the other's `known_peers`).
9. **Both devices update their Dioxus `doc_signal`** so the UI reflects the merged group.

**Who initiates?** The scanner (S) always initiates the direct connection. Only one side needs to dial; iroh handles relay/hole-punching.

---

## 11. Transitive sync (A scans C; B scans C)

1. **A scans C:** A and C merge docs. C's `known_peers` now includes A. A's `known_peers` now includes C.
2. **B scans C:** B and C merge docs. B's `known_peers` now includes C **and A** (because C's doc already contained A from step 1). C's `known_peers` now includes B.
3. **A and B sync directly:** B's background sync loop sees A in its `known_peers`, dials A directly over iroh, and runs automerge sync. C does not need to be online.

**Requirement satisfied:** A and B can sync with each other through C's address book, even when C is offline.

**Prerequisite (currently missing):** peer addresses must actually be written into the doc's `known_peers` map with full `EndpointAddr` values (relay URL included). Storing a bare hex key (as `handle_incoming_connection` does today) makes the peer undialable over the internet.

---

## 12. Dependencies

Already in `Cargo.toml`: `dioxus 0.7` (web), `tokio` (sync), `iroh 1.4.1`, `automerge 0.12.0`, `serde_json`, `qrcode`, `base64`, `serde`, `hex`, `js-sys`, `web-sys`, `wasm-bindgen`, `wasm-bindgen-futures`, `zerocopy`, `n0-error`.

No new crates strictly required. `blake3` (group id hashing) is optional and can be skipped.

Notes:
- On wasm, `Endpoint::bind(presets::N0)` works (iroh has wasm support via web-sys/wasm-bindgen).
- QR scanning on wasm: `web-sys` `mediaDevices.getUserMedia` for camera, or a file `<input>`. A pure-Rust QR decoder may not compile to wasm easily; consider calling a JS QR library via `web-sys`/`js-sys` or accepting an image upload + decoding via a wasm QR crate.

---

## 13. Open questions & risks

1. **Automerge merge of unrelated docs.** `AutoCommit::merge` applies all changes from one doc into another. Two independently created docs share no history, so every change from the scanned device becomes a new change on the scanner's actor timeline. Conflicts resolve deterministically but "randomly" (last-writer-wins by automerge's deterministic heuristic). Acceptable for expenses; document the behavior.
2. **QR payload size.** A `NodeAddress` string can be long (relay URLs + addresses). QR codes have capacity limits (~2-3 KB for alphanumeric at reasonable density). Should fit, but keep the payload minimal (no doc bytes in the QR).
3. **One group per device vs. merge history.** After A scans C then B scans C, all three share one group. If A later scans D (a different group), A leaves the A/B/C group and joins D's group — A's device is now in only one group, but A's historical expenses stay in the old group's doc (not carried over). This matches "one group per device" but means a device's history can be left behind when it switches groups. Decide if that's acceptable UX.
4. **Mobile camera on wasm.** Confirm `web_sys` `MediaDevices::get_user_media` works in the target browsers; provide file-upload fallback.
5. **Relay servers.** `presets::N0` uses number0's public relays by default. For a self-hosted/offline LAN scenario, configure custom relays or allow direct-address-only connections.
6. **Connection storms.** With <5 peers, dialing all known peers on every loop iteration is fine. If the list grows, add backoff / concurrency limits. Note: the current background loops also re-run the sync exchange in a tight loop per connection — that must be replaced with one persistent stream per connection (see §6.2).
7. **Signal cloning cost.** Storing the whole `AutoCommit` in one Dioxus signal means cloning the doc on every local change and every remote merge. For small docs this is fine; monitor if docs grow large.

---

## 14. Implementation order

The code compiles; the remaining work is fixing the sync layer. Follow `SYNC_FIX_PLAN.md`:

1. **Phase 1:** Fix the `SyncDoc` exchange (`automerge_sync.rs`) — correct termination, persistent per-peer `State`, no signal churn.
2. **Phase 2:** Make the engine doc the single source of truth — local edits flow into the engine doc; persist after every change/merge.
3. **Phase 3:** One persistent stream per connection; delete the full-doc push path (`send_doc_to_peer`).
4. **Phase 4:** Write `known_peers` into the automerge doc with full `EndpointAddr` values; fix incoming-peer address storage.
5. **Phase 5:** Races, actor ID, storage cleanup.
6. Test end-to-end sync between two devices (QR scan → merge → both UIs update).
7. Test transitive scenario (A scans C, B scans C, A and B sync directly).
