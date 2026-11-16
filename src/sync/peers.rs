use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use automerge::sync::{Message, State, SyncDoc};
use automerge::AutoCommit;
use dioxus::prelude::{Signal, WritableExt};
use iroh::endpoint::{Connection, RecvStream, VarInt};
use iroh::{Endpoint, EndpointAddr};

use crate::sync::automerge_sync::{
    recv_sync_message, run_automerge_sync, send_sync_message, SyncRecv,
};
use serde_json;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;
use web_sys::window;
use zerocopy::IntoByteSlice;

const KNOWN_PEERS_STORAGE_KEY: &str = "iroh_share_known_peers";

/// Status of a known peer connection.
/// This is a runtime property and is NOT persisted to localStorage.
#[derive(Debug, Clone, PartialEq)]
pub enum PeerStatus {
    Disconnected,
    Connecting,
    Connected,
    Synced,
}

/// A known peer with its address and connection status.
#[derive(Debug, Clone)]
pub struct KnownPeer {
    pub device_id: String,
    pub node_address: String,
    pub status: PeerStatus,
}

/// Persisted peer data — only device_id and node_address survive a refresh.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPeer {
    device_id: String,
    node_address: String,
}

/// A live connection to a peer, as tracked in the connections map.
///
/// `notify` wakes the per-connection sync task when a local change happens.
/// The remaining fields exist so that a losing duplicate connection can be
/// closed actively and safely:
/// - `conn` lets us close the connection when a duplicate tie-break replaces
///   it (its task may be parked in a blocking read and would otherwise never
///   notice).
/// - `dialer_id` is the node id of whoever initiated the connection; the
///   duplicate tie-break keeps the connection dialed by the *lower* node id
///   so both sides always reach the same conclusion.
/// - `token` identifies this entry: a task's teardown only removes the map
///   entry while it still points at *its own* connection, so a replaced task
///   can never unregister its replacement.
#[derive(Debug, Clone)]
pub struct PeerConnection {
    pub notify: mpsc::UnboundedSender<()>,
    conn: Connection,
    dialer_id: String,
    token: Arc<()>,
}

/// Manages the set of known peers and their connection statuses.
#[derive(Debug, Clone, Default)]
pub struct PeerManager {
    peers: Arc<Mutex<HashMap<String, KnownPeer>>>,
}

impl PeerManager {
    /// Create a new empty PeerManager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load known peers from localStorage.
    /// All peer statuses are reset to `Disconnected` since the connection
    /// is not actually active after a page refresh.
    pub async fn load_from_storage() -> Self {
        let peers = Self::new();
        if let Some(win) = window() {
            if let Ok(Some(storage)) = win.local_storage() {
                if let Ok(Some(json)) = storage.get_item(KNOWN_PEERS_STORAGE_KEY) {
                    if let Ok(map) = serde_json::from_str::<HashMap<String, PersistedPeer>>(&json) {
                        for (_, persisted) in map {
                            let peer = KnownPeer {
                                device_id: persisted.device_id,
                                node_address: persisted.node_address,
                                status: PeerStatus::Disconnected,
                            };
                            peers
                                .peers
                                .lock()
                                .await
                                .insert(peer.device_id.clone(), peer);
                        }
                    }
                }
            }
        }
        peers
    }

    /// Persist only device_id and node_address to localStorage.
    /// Connection status is a runtime property and is NOT saved.
    async fn save_to_storage(&self) {
        let peers = self.peers.lock().await;
        let persisted: HashMap<String, PersistedPeer> = peers
            .values()
            .map(|p| {
                (
                    p.device_id.clone(),
                    PersistedPeer {
                        device_id: p.device_id.clone(),
                        node_address: p.node_address.clone(),
                    },
                )
            })
            .collect();
        drop(peers);
        let json = serde_json::to_string(&persisted).unwrap_or_default();
        if let Some(win) = window() {
            if let Ok(Some(storage)) = win.local_storage() {
                let _ = storage.set_item(KNOWN_PEERS_STORAGE_KEY, &json);
            }
        }
    }

    /// Add or update a known peer. Persisted to localStorage.
    pub async fn add_peer(&self, device_id: String, node_address: String) {
        let mut peers = self.peers.lock().await;
        peers.insert(
            device_id.clone(),
            KnownPeer {
                device_id,
                node_address,
                status: PeerStatus::Disconnected,
            },
        );
        drop(peers);
        self.save_to_storage().await;
    }

    /// Update a peer's status. Runtime-only; NOT persisted.
    pub async fn set_status(&self, device_id: &str, status: PeerStatus) {
        let mut peers = self.peers.lock().await;
        if let Some(peer) = peers.get_mut(device_id) {
            peer.status = status;
        }
    }

    /// Get all known peers.
    pub async fn get_peers(&self) -> Vec<KnownPeer> {
        let peers = self.peers.lock().await;
        peers.values().cloned().collect()
    }

    /// Get the stored node address for a peer, if known.
    pub async fn get_node_address(&self, device_id: &str) -> Option<String> {
        let peers = self.peers.lock().await;
        peers.get(device_id).map(|p| p.node_address.clone())
    }
}

/// Delay for the given number of milliseconds using a JS timer.
/// This avoids tokio::time::sleep which requires a tokio reactor.
async fn sleep_ms(ms: u32) {
    let promise = js_sys::eval(&format!(
        "new Promise(resolve => setTimeout(resolve, {ms}))"
    ))
    .unwrap()
    .dyn_into::<js_sys::Promise>()
    .unwrap();
    wasm_bindgen_futures::JsFuture::from(promise).await.ok();
}

/// Background sync loop: dials known peers (from the localStorage registry and
/// the doc's `known_peers` map) that aren't already connected, then waits.
/// Each connection runs its own persistent, event-driven sync loop.
pub struct PeerSyncLoop {
    endpoint: Endpoint,
    self_id: String,
    peer_manager: PeerManager,
    connections: Arc<Mutex<HashMap<String, PeerConnection>>>,
    sync_states: Arc<Mutex<HashMap<String, State>>>,
    in_flight_dials: Arc<Mutex<HashSet<String>>>,
    doc: Arc<Mutex<AutoCommit>>,
    doc_signal: Signal<AutoCommit>,
}

impl PeerSyncLoop {
    pub fn new(
        endpoint: Endpoint,
        self_id: String,
        peer_manager: PeerManager,
        connections: Arc<Mutex<HashMap<String, PeerConnection>>>,
        sync_states: Arc<Mutex<HashMap<String, State>>>,
        in_flight_dials: Arc<Mutex<HashSet<String>>>,
        doc: Arc<Mutex<AutoCommit>>,
        doc_signal: Signal<AutoCommit>,
    ) -> Self {
        Self {
            endpoint,
            self_id,
            peer_manager,
            connections,
            sync_states,
            in_flight_dials,
            doc,
            doc_signal,
        }
    }

    /// Run the background sync loop forever.
    /// Dials known peers that aren't connected, then waits before retrying
    /// (failed dials are retried on the next pass).
    pub async fn run(self) {
        // A per-device phase offset derived from our own node id, plus a
        // little jitter. Two devices whose loops started around the same
        // time (e.g. both refreshed together) would otherwise dial in
        // lockstep every 5s, repeatedly hitting the simultaneous-dial race.
        let dial_offset_ms = {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in self.self_id.bytes() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
            (hash % 2_500) as u32
        };
        loop {
            // Combine the localStorage registry with the doc's known_peers so
            // peers learned transitively are dialed too. The doc value wins
            // because it is the synced, authoritative copy.
            let mut combined: HashMap<String, String> = HashMap::new();
            for peer in self.peer_manager.get_peers().await {
                combined.insert(peer.device_id, peer.node_address);
            }
            for (device_id, node_address) in crate::sync::doc::known_peers(&self.doc).await {
                combined.insert(device_id, node_address);
            }

            for (device_id, node_address) in combined {
                // Never dial ourselves.
                if device_id == self.self_id {
                    continue;
                }
                // Skip peers already connected or already being dialed.
                {
                    let conns = self.connections.lock().await;
                    if conns.contains_key(&device_id) {
                        continue;
                    }
                }
                {
                    let mut dials = self.in_flight_dials.lock().await;
                    if dials.contains(&device_id) {
                        continue;
                    }
                    dials.insert(device_id.clone());
                }

                let endpoint = self.endpoint.clone();
                let peer_manager = self.peer_manager.clone();
                let connections = self.connections.clone();
                let sync_states = self.sync_states.clone();
                let in_flight = self.in_flight_dials.clone();
                let doc = self.doc.clone();
                let doc_signal = self.doc_signal.clone();

                let self_id = self.self_id.clone();
                let own_addr = crate::sync::own_addr_string(&endpoint);
                spawn_local(async move {
                    let _ = connect_and_maintain(
                        &endpoint,
                        &self_id,
                        &own_addr,
                        &device_id,
                        &node_address,
                        &peer_manager,
                        &connections,
                        &sync_states,
                        &doc,
                        doc_signal,
                    )
                    .await;
                    in_flight.lock().await.remove(&device_id);
                });
            }

            let jitter_ms = (js_sys::Math::random() * 500.0) as u32;
            sleep_ms(5_000 + dial_offset_ms + jitter_ms).await;
        }
    }
}

/// Dial a known peer and start the persistent per-connection sync loop.
async fn connect_and_maintain(
    endpoint: &Endpoint,
    self_id: &str,
    own_addr: &str,
    device_id: &str,
    node_address: &str,
    peer_manager: &PeerManager,
    connections: &Arc<Mutex<HashMap<String, PeerConnection>>>,
    sync_states: &Arc<Mutex<HashMap<String, State>>>,
    doc: &Arc<Mutex<AutoCommit>>,
    doc_signal: Signal<AutoCommit>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    peer_manager
        .set_status(device_id, PeerStatus::Connecting)
        .await;

    // Guard: don't dial a peer we're already connected to.
    if connections.lock().await.contains_key(device_id) {
        return Ok(());
    }

    let addr: EndpointAddr = parse_node_address(node_address)?;
    let conn = endpoint
        .connect(addr, b"iroh-share-sync")
        .await
        .map_err(|e| e.to_string())?;

    // Use the authoritative remote id as the device id.
    let remote_id = conn.remote_id();
    let device_id = hex::encode(remote_id.into_byte_slice());

    register_peer_and_spawn(
        device_id,
        conn,
        node_address.to_string(),
        self_id,
        own_addr,
        peer_manager,
        connections,
        sync_states,
        doc,
        doc_signal,
        self_id.to_string(),
    )
    .await;

    Ok(())
}

/// Handle an incoming connection from a peer. Runs the same persistent
/// per-connection sync loop as an outgoing connection (accept side).
pub async fn handle_incoming_connection(
    conn: Connection,
    self_id: &str,
    own_addr: &str,
    doc: &Arc<Mutex<AutoCommit>>,
    peer_manager: &PeerManager,
    connections: &Arc<Mutex<HashMap<String, PeerConnection>>>,
    sync_states: &Arc<Mutex<HashMap<String, State>>>,
    doc_signal: Signal<AutoCommit>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let remote_id = conn.remote_id();
    let device_id = hex::encode(remote_id.into_byte_slice());

    // Resolve the peer's address: prefer an address we already know (registry
    // or doc known_peers); otherwise fall back to the bare hex key.
    let known = match peer_manager.get_node_address(&device_id).await {
        Some(addr) => Some(addr),
        None => crate::sync::doc::known_peer(doc, &device_id).await,
    };
    let node_address = known.unwrap_or_else(|| device_id.clone());

    register_peer_and_spawn(
        device_id.clone(),
        conn,
        node_address,
        self_id,
        own_addr,
        peer_manager,
        connections,
        sync_states,
        doc,
        doc_signal,
        device_id,
    )
    .await;

    Ok(())
}

/// Dedupe-register a connection and spawn the persistent per-connection sync
/// task. `dialer_id` is the node id of whoever initiated the connection (our
/// `self_id` for outgoing dials, the remote's id for accepted connections);
/// it selects `open_bi` vs `accept_bi` and drives the duplicate tie-break.
///
/// Duplicates are real: when both sides dial at the same time (typical right
/// after a refresh), each side ends up with two connections to the same peer.
/// The tie-break keeps the connection dialed by the *lower* node id — both
/// sides compare the same pair of ids, so they always keep the same
/// connection. Without this, each side could keep its own dial and close the
/// peer's, killing both connections (and with in-phase redial loops, again
/// and again).
pub async fn register_peer_and_spawn(
    device_id: String,
    conn: Connection,
    node_address: String,
    self_id: &str,
    own_addr: &str,
    peer_manager: &PeerManager,
    connections: &Arc<Mutex<HashMap<String, PeerConnection>>>,
    sync_states: &Arc<Mutex<HashMap<String, State>>>,
    doc: &Arc<Mutex<AutoCommit>>,
    doc_signal: Signal<AutoCommit>,
    dialer_id: String,
) {
    let dialer_side = dialer_id == self_id;

    let token = Arc::new(());
    let (notify_tx, notify_rx) = mpsc::unbounded_channel();
    let mut replaced_conn: Option<Connection> = None;
    {
        let mut conns = connections.lock().await;
        // Snapshot the existing entry (cloned) so no borrow of the guard is
        // held across the mutation below.
        let existing = conns
            .get(&device_id)
            .map(|c| (c.dialer_id.clone(), c.conn.clone()));
        let new_entry = PeerConnection {
            notify: notify_tx,
            conn: conn.clone(),
            dialer_id,
            token: token.clone(),
        };
        let new_dialer_id = new_entry.dialer_id.clone();
        match existing {
            // The existing connection wins the tie-break: reject the new one.
            Some((ref existing_dialer, _)) if *existing_dialer < new_dialer_id => {
                drop(conns);
                let _ = conn.close(VarInt::from_u32(0), b"duplicate connection");
                return;
            }
            // We win: replace the entry and close the old connection actively
            // so its task unblocks (it may be parked in a blocking read) and
            // tears down. Its token-guarded teardown cannot remove this new
            // entry.
            Some((_, old_conn)) => {
                conns.insert(device_id.clone(), new_entry);
                replaced_conn = Some(old_conn);
            }
            None => {
                conns.insert(device_id.clone(), new_entry);
            }
        }
    }
    if let Some(old) = replaced_conn {
        let _ = old.close(VarInt::from_u32(0), b"replaced by duplicate tie-break");
    }

    // Never treat our own connection as a peer or write ourselves into the
    // registry / known_peers.
    if device_id != self_id {
        peer_manager
            .add_peer(device_id.clone(), node_address.clone())
            .await;
        peer_manager
            .set_status(&device_id, PeerStatus::Connected)
            .await;

        // Record our own address under our own id so the peer (and, via the
        // synced doc, any of its peers) can dial us back. This is how a device
        // learns the dialer's address without ever seeing its QR code.
        if serde_json::from_str::<EndpointAddr>(own_addr).is_ok() {
            crate::sync::doc::set_known_peer(doc, self_id, own_addr).await;
        }

        // Only record a real (JSON EndpointAddr) address into the doc's
        // known_peers — a bare hex key is not dialable over the internet.
        if serde_json::from_str::<EndpointAddr>(&node_address).is_ok() {
            crate::sync::doc::set_known_peer(doc, &device_id, &node_address).await;
        }
        crate::sync::doc::save(doc).await;
    } else {
        peer_manager
            .set_status(&device_id, PeerStatus::Connected)
            .await;
    }

    // Reuse the persisted per-peer state, but only the parts automerge
    // deems reusable across connections: `State::encode()` round-trips
    // exactly `shared_heads` and resets everything session-scoped
    // (`in_flight`, `have_responded`, `their_*`). Reusing the raw live state
    // is what let a connection that died mid-exchange leave `in_flight`
    // set: on the next connection `generate_sync_message` returned `None`
    // on both sides and they sat silently on a live connection forever —
    // the "devices stop syncing after a reconnect" wedge.
    let mut state = match sync_states.lock().await.remove(&device_id) {
        Some(saved) => State::decode(&saved.encode()).unwrap_or_default(),
        None => State::new(),
    };
    // The doc can be older than the saved state (e.g. a catch-up merge that
    // was never persisted before an old crash, then a refresh). shared_heads
    // the doc doesn't actually contain would feed the protocol bogus
    // last-sync data — start fresh instead.
    if !state.shared_heads.is_empty() {
        let missing = doc.lock().await.get_missing_deps(&state.shared_heads);
        if !missing.is_empty() {
            state = State::new();
        }
    }

    let doc = doc.clone();
    let peer_manager = peer_manager.clone();
    let connections = connections.clone();
    let sync_states = sync_states.clone();
    spawn_local(async move {
        peer_sync_task_impl(
            device_id,
            conn,
            doc,
            doc_signal,
            connections,
            peer_manager,
            sync_states,
            state,
            notify_rx,
            dialer_side,
            token,
        )
        .await;
    });
}

/// Framed items from the dedicated stream-reader task.
enum FromPeer {
    Message(Message),
    /// The peer sent the zero-length end-of-sync marker; the stream stays open.
    PeerDone,
    /// The stream or connection ended.
    Closed,
}

/// Owns the `RecvStream` and forwards framed messages over a channel.
///
/// iroh's `RecvStream::read_exact` is explicitly *not* cancel-safe: dropping
/// the future mid-read loses the bytes already consumed. A `tokio::select!`
/// over `recv_sync_message` and the local-change notification would do exactly
/// that whenever a local edit arrives while a message is being read,
/// permanently desyncing the length-prefixed framing. Isolating reads in this
/// task means the per-connection loop only ever awaits cancel-safe channel
/// receives.
async fn stream_reader(mut recv_stream: RecvStream, tx: mpsc::UnboundedSender<FromPeer>) {
    loop {
        match recv_sync_message(&mut recv_stream).await {
            Ok(SyncRecv::Message(msg)) => {
                if tx.send(FromPeer::Message(msg)).is_err() {
                    return;
                }
            }
            Ok(SyncRecv::PeerDone) => {
                // The marker does not close the stream; keep reading.
                if tx.send(FromPeer::PeerDone).is_err() {
                    return;
                }
            }
            Ok(SyncRecv::Closed) | Err(_) => {
                let _ = tx.send(FromPeer::Closed);
                return;
            }
        }
    }
}

/// The single per-connection sync path: open one bi-stream, run the initial
/// catch-up exchange on it, then keep it open and react to incoming messages
/// and local-change notifications until the connection dies.
async fn peer_sync_task_impl(
    device_id: String,
    conn: Connection,
    doc: Arc<Mutex<AutoCommit>>,
    mut doc_signal: Signal<AutoCommit>,
    connections: Arc<Mutex<HashMap<String, PeerConnection>>>,
    peer_manager: PeerManager,
    sync_states: Arc<Mutex<HashMap<String, State>>>,
    mut state: State,
    mut notify: mpsc::UnboundedReceiver<()>,
    dialer_side: bool,
    token: Arc<()>,
) {
    let stream = if dialer_side {
        conn.open_bi().await
    } else {
        conn.accept_bi().await
    };
    let (mut send_stream, mut recv_stream) = match stream {
        Ok(s) => s,
        Err(_) => {
            teardown_peer(
                &device_id,
                &connections,
                &peer_manager,
                &sync_states,
                state,
                token,
            )
            .await;
            return;
        }
    };

    // Initial catch-up exchange (reuses the persistent stream).
    match run_automerge_sync(
        &mut send_stream,
        &mut recv_stream,
        &doc,
        &mut state,
        Some(doc_signal),
    )
    .await
    {
        Ok(changed) => {
            // Persist what the exchange merged: without this, a refresh right
            // after connecting would lose everything just received (and the
            // saved doc would fall behind the persisted sync state).
            if changed {
                crate::sync::doc::save(&doc).await;
            }
            peer_manager
                .set_status(&device_id, PeerStatus::Synced)
                .await;
        }
        Err(_) => {}
    }

    // Hand the receive half to the dedicated reader task (see
    // `stream_reader`). Bytes that arrive while we are busy stay buffered in
    // the stream; nothing is lost between the catch-up and the reader.
    let (from_peer_tx, mut from_peer_rx) = mpsc::unbounded_channel();
    spawn_local(stream_reader(recv_stream, from_peer_tx));

    // Event-driven loop: respond to incoming messages and push local changes
    // when notified. No busy-looping, no per-round stream opens, and every
    // await is cancel-safe.
    loop {
        let keep_going = tokio::select! {
            notified = notify.recv() => {
                match notified {
                    // Local change: generate + send whatever the peer still needs.
                    Some(()) => {
                        let msg = {
                            let mut guard = doc.lock().await;
                            let sync_view = guard.sync();
                            sync_view.generate_sync_message(&mut state)
                        };
                        match msg {
                            Some(msg) => send_sync_message(&mut send_stream, msg).await.is_ok(),
                            None => true,
                        }
                    }
                    None => false, // dropped from the connections map (replaced)
                }
            }
            received = from_peer_rx.recv() => {
                match received {
                    Some(FromPeer::Message(msg)) => {
                        let heads_before = doc.lock().await.get_heads();
                        let mut guard = doc.lock().await;
                        let mut sync_view = guard.sync();
                        if sync_view.receive_sync_message(&mut state, msg).is_err() {
                            false
                        } else {
                            let reply = sync_view.generate_sync_message(&mut state);
                            drop(sync_view);
                            let changed = guard.get_heads() != heads_before;
                            drop(guard);
                            if changed {
                                doc_signal.set(doc.lock().await.clone());
                                crate::sync::doc::save(&doc).await;
                            }
                            match reply {
                                Some(reply) => {
                                    send_sync_message(&mut send_stream, reply).await.is_ok()
                                }
                                None => true,
                            }
                        }
                    }
                    Some(FromPeer::PeerDone) => true, // peer finished a sync phase; stream still open
                    Some(FromPeer::Closed) => false,
                    None => false, // reader task ended
                }
            }
        };
        if !keep_going {
            break;
        }
    }

    teardown_peer(
        &device_id,
        &connections,
        &peer_manager,
        &sync_states,
        state,
        token,
    )
    .await;
}

/// Common teardown for a dead connection: persist the sync state, drop the
/// connection entry, and mark the peer disconnected. Only our *own* entry is
/// removed: if a newer connection replaced us (duplicate tie-break), the map
/// holds theirs now.
async fn teardown_peer(
    device_id: &str,
    connections: &Arc<Mutex<HashMap<String, PeerConnection>>>,
    peer_manager: &PeerManager,
    sync_states: &Arc<Mutex<HashMap<String, State>>>,
    state: State,
    token: Arc<()>,
) {
    {
        let mut states = sync_states.lock().await;
        states.insert(device_id.to_string(), state);
    }
    crate::sync::persist_sync_states(sync_states).await;
    {
        let mut conns = connections.lock().await;
        let is_ours = conns
            .get(device_id)
            .is_some_and(|c| Arc::ptr_eq(&c.token, &token));
        if is_ours {
            conns.remove(device_id);
            drop(conns);
            peer_manager
                .set_status(device_id, PeerStatus::Disconnected)
                .await;
        }
    }
}

/// Parse a node address string into an EndpointAddr.
/// Tries JSON deserialization first (full EndpointAddr with relay URLs),
/// then falls back to hex public key parsing.
fn parse_node_address(
    node_address: &str,
) -> Result<EndpointAddr, Box<dyn std::error::Error + Send + Sync>> {
    // Try JSON deserialization first (serialized EndpointAddr from QR payload)
    if let Ok(addr) = serde_json::from_str::<EndpointAddr>(node_address) {
        return Ok(addr);
    }

    // Fall back to hex public key parsing
    let hex_str = node_address.trim().trim_start_matches("iroh://");
    let hex_str = hex_str.split('@').next().unwrap_or(hex_str);
    let hex_str = hex_str.split('?').next().unwrap_or(hex_str);

    let bytes = hex::decode(hex_str).map_err(|_| "invalid hex in node address")?;
    if bytes.len() != 32 {
        return Err(format!("expected 32-byte public key, got {}", bytes.len()).into());
    }

    let array: [u8; 32] = bytes.try_into().map_err(|_| "invalid key length")?;
    let public_key = iroh::PublicKey::from_bytes(&array).map_err(|_| "invalid public key bytes")?;
    Ok(EndpointAddr::new(public_key))
}
