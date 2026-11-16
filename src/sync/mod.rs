pub mod automerge_sync;
pub mod doc;
pub mod identity;
pub mod peers;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use automerge::sync::State;
use automerge::{AutoCommit};
use base64::Engine as _;
use dioxus::prelude::{ReadableExt, Signal, WritableExt};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use serde::de;
use serde_json;
use tokio::sync::Mutex;
use wasm_bindgen_futures::spawn_local;

use crate::qr::QrPayload;
use crate::sync::identity::load_or_create_identity;
use crate::sync::peers::{
    handle_incoming_connection, register_peer_and_spawn, PeerConnection, PeerManager, PeerSyncLoop,
};
use zerocopy::IntoByteSlice;

const SYNC_STATES_STORAGE_KEY: &str = "iroh_share_sync_states";

/// The sync engine: owns the iroh Endpoint, the automerge doc, and active
/// persistent connections to peers.
pub struct SyncEngine {
    pub endpoint: Endpoint,
    pub doc: Arc<Mutex<AutoCommit>>,
    pub peer_manager: PeerManager,
    pub device_id: SecretKey,
    /// Active connections + local-change notification channels, keyed by device_id.
    pub connections: Arc<Mutex<HashMap<String, PeerConnection>>>,
    /// Per-peer automerge sync state (reused across connections, persisted to
    /// localStorage).
    pub sync_states: Arc<Mutex<HashMap<String, State>>>,
    /// Peers currently being dialed (dedup for the background peer loop).
    pub in_flight_dials: Arc<Mutex<HashSet<String>>>,
}

impl SyncEngine {
    /// Initialize the sync engine: restore/create identity, create endpoint,
    /// load or create the group doc, and start the background peer sync loop.
    ///
    /// `doc_signal` is the Dioxus signal that the engine pushes merged docs to
    /// when remote peers sync changes, keeping the UI in sync.
    pub async fn init(
        doc_signal: Signal<AutoCommit>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (secret_key, actor_id) =
            load_or_create_identity().map_err(|e| format!("identity error: {e:?}"))?;

        // Create the iroh endpoint.
        let endpoint = Endpoint::builder(iroh::endpoint::presets::N0)
            .alpns(vec![b"iroh-share-sync".to_vec()])
            .secret_key(secret_key.clone())
            .bind()
            .await
            .map_err(|e| format!("endpoint bind error: {e:?}"))?;

        // Load or create the group doc and apply the identity-derived actor.
        let mut loaded_doc = doc::load_or_new();
        loaded_doc.set_actor(actor_id.clone());
        let doc: Arc<Mutex<AutoCommit>> = Arc::new(Mutex::new(loaded_doc));

        // Merge any UI-signal edits made before the engine finished starting
        // into the engine doc (the engine doc is the single source of truth).
        {
            let mut doc_guard = doc.lock().await;
            let mut signal_doc = doc_signal.read().clone();
            let _ = doc_guard.merge(&mut signal_doc);
        }

        let peer_manager = PeerManager::load_from_storage().await;

        let self_id = hex::encode(secret_key.public().as_bytes());
        let own_addr = own_addr_string(&endpoint);
        let engine = Self {
            endpoint,
            doc,
            peer_manager,
            device_id: secret_key,
            connections: Arc::new(Mutex::new(HashMap::new())),
            sync_states: Arc::new(Mutex::new(load_sync_states_from_storage())),
            in_flight_dials: Arc::new(Mutex::new(HashSet::new())),
        };

        // Start the background peer sync loop (maintains connections, re-dials periodically).
        let peer_loop = PeerSyncLoop::new(
            engine.endpoint.clone(),
            self_id.clone(),
            engine.peer_manager.clone(),
            engine.connections.clone(),
            engine.sync_states.clone(),
            engine.in_flight_dials.clone(),
            engine.doc.clone(),
            doc_signal.clone(),
        );
        spawn_local(async move {
            peer_loop.run().await;
        });

        // Start accepting incoming connections.
        let accept_endpoint = engine.endpoint.clone();
        let accept_peer_manager = engine.peer_manager.clone();
        let accept_doc = engine.doc.clone();
        let accept_connections = engine.connections.clone();
        let accept_sync_states = engine.sync_states.clone();
        let accept_self_id = self_id.clone();
        let accept_own_addr = own_addr.clone();
        let accept_doc_signal = doc_signal.clone();
        spawn_local(async move {
            loop {
                match accept_endpoint.accept().await {
                    Some(conn_future) => match conn_future.await {
                        Ok(conn) => {
                            let _ = handle_incoming_connection(
                                conn,
                                &accept_self_id,
                                &accept_own_addr,
                                &accept_doc,
                                &accept_peer_manager,
                                &accept_connections,
                                &accept_sync_states,
                                accept_doc_signal.clone(),
                            )
                            .await;
                        }
                        Err(_) => continue,
                    },
                    None => break,
                }
            }
        });

        Ok(engine)
    }

    /// Get the QR payload for this device (node_address from endpoint addr).
    /// Serializes the full EndpointAddr (public key + relay URLs + direct addresses)
    /// so the scanning peer can dial us directly.
    pub fn qr_payload(&self) -> crate::qr::QrPayload {
        let node_address = own_addr_string(&self.endpoint);
        crate::qr::QrPayload {
            v: 1,
            app: "iroh-share".to_string(),
            node_address,
        }
    }

    /// Called when the local automerge doc changes: refresh the UI signal from
    /// the engine doc (single source of truth), persist it, and push the new
    /// changes to all connected peers via their per-connection sync loops.
    pub async fn on_local_change(&self, mut doc_signal: Signal<AutoCommit>) {
        // Refresh the UI from the engine doc.
        {
            let doc_guard = self.doc.lock().await;
            doc_signal.set(doc_guard.clone());
        }

        // Persist the engine doc.
        crate::sync::doc::save(&self.doc).await;

        // Wake each per-connection sync loop so it generates + sends the change.
        let notifiers: Vec<_> = {
            let conns = self.connections.lock().await;
            conns.values().map(|pc| pc.notify.clone()).collect()
        };
        for tx in notifiers {
            let _ = tx.send(());
        }
    }

    /// Dial a peer from a QR payload and start persistent sync with it. The
    /// initial doc exchange and every subsequent change flow over one bi-stream.
    pub async fn scan_and_merge(
        &self,
        payload: &QrPayload,
        doc_signal: Signal<AutoCommit>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Deserialize the full EndpointAddr from the QR payload.
        let addr: EndpointAddr =
            serde_json::from_str(&payload.node_address).map_err(|e| e.to_string())?;

        // Guard: don't dial a peer we're already connected to.
        let device_id = hex::encode(addr.id.into_byte_slice());
        if self.connections.lock().await.contains_key(&device_id) {
            return Ok(());
        }

        // Connect to the peer.
        let conn = self
            .endpoint
            .connect(addr, b"iroh-share-sync")
            .await
            .map_err(|e| e.to_string())?;

        // Use conn.remote_id() as the authoritative device ID.
        let remote_id = conn.remote_id();
        let device_id = hex::encode(remote_id.into_byte_slice());

        let self_id = hex::encode(self.device_id.public().as_bytes());
        let own_addr = own_addr_string(&self.endpoint);

        register_peer_and_spawn(
            device_id,
            conn,
            payload.node_address.clone(),
            &self_id,
            &own_addr,
            &self.peer_manager,
            &self.connections,
            &self.sync_states,
            &self.doc,
            doc_signal,
            self_id.clone(),
        )
        .await;

        Ok(())
    }
}

/// Serialize this endpoint's address for storage/sync, keeping only the stable
/// rendezvous info (id + home relay). Direct IP addresses change often and are
/// re-discovered via hole-punching at connect time, so they're omitted. If no
/// relay is available, the full address is kept (direct addresses are then the
/// only way to dial).
pub fn own_addr_string(endpoint: &Endpoint) -> String {
    let addr = endpoint.addr();
    let relay = addr.relay_urls().next().cloned();
    let trimmed = match relay {
        Some(relay) => EndpointAddr::new(addr.id.clone()).with_relay_url(relay),
        None => addr,
    };
    serde_json::to_string(&trimmed).unwrap_or_default()
}

fn load_from_local_storage<T: de::DeserializeOwned>(key: &str) -> Option<T> {
    if let Some(win) = web_sys::window() {
        if let Ok(Some(storage)) = win.local_storage() {
            if let Ok(Some(json)) = storage.get_item(key) {
                if let Ok(value) = serde_json::from_str(&json) {
                    return Some(value);
                }
            }
        }
    }

    None
}

/// Load persisted per-peer sync states from localStorage.
fn load_sync_states_from_storage() -> HashMap<String, State> {
    load_from_local_storage::<HashMap<String, String>>(SYNC_STATES_STORAGE_KEY)
        .map(|map| {
            map.into_iter()
                .filter_map(|(k, v)| {
                    let bytes = base64::engine::general_purpose::STANDARD.decode(&v).ok()?;
                    State::decode(&bytes).ok().map(|s| (k, s))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Persist the per-peer sync states to localStorage.
pub async fn persist_sync_states(states: &Arc<Mutex<HashMap<String, State>>>) {
    let guard = states.lock().await;
    let map: HashMap<String, String> = guard
        .iter()
        .map(|(k, s)| {
            (
                k.clone(),
                base64::engine::general_purpose::STANDARD.encode(s.encode()),
            )
        })
        .collect();
    drop(guard);
    let json = serde_json::to_string(&map).unwrap_or_default();
    if let Some(win) = web_sys::window() {
        if let Ok(Some(storage)) = win.local_storage() {
            let _ = storage.set_item(SYNC_STATES_STORAGE_KEY, &json);
        }
    }
}
