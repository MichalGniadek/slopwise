use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjId, ObjType, ReadDoc, Value};
use base64::{Engine as _, engine::general_purpose};
use std::sync::Arc;
use tokio::sync::Mutex;

const KNOWN_PEERS_KEY: &str = "known_peers";
const GROUP_DOC_STORAGE_KEY: &str = "iroh_share_group_doc";

/// Load an AutoCommit doc from a base64-encoded string.
pub fn from_base64(b64: &str) -> Option<AutoCommit> {
    let bytes = general_purpose::STANDARD.decode(b64).ok()?;
    AutoCommit::load(&bytes).ok()
}

pub fn load_or_new() -> AutoCommit {
    // The doc is stored as a raw base64 string (see `save`), so read it back
    // directly rather than through `load_from_local_storage`, which parses the
    // value as JSON. A bare base64 string is not valid JSON, so that path would
    // fail and silently reset the group to a fresh empty doc on every refresh.
    let stored = web_sys::window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(GROUP_DOC_STORAGE_KEY).ok().flatten());
    stored.and_then(|value| from_base64(&value)).unwrap_or_else(new)
}

/// Initialize a new group doc.
///
/// Deliberately does NOT pre-create the `expenses` list or `known_peers` map.
/// If every device eagerly created these objects, each device would have its
/// own conflicting value for the same key, and automerge's conflict resolution
/// would hide one device's data after a sync (e.g. a fresh device's empty
/// `expenses` list winning over a device that already had history). They are
/// created lazily by `add_expense`/`set_known_peer` instead.
pub fn new() -> AutoCommit {
    AutoCommit::new()
}

/// Persist the engine doc to localStorage. The engine doc is the single source
/// of truth, so this must run after every local change and every remote merge.
pub async fn save(doc: &Arc<Mutex<AutoCommit>>) {
    let bytes = doc.lock().await.save();
    let b64 = general_purpose::STANDARD.encode(&bytes);
    if let Some(win) = web_sys::window() {
        if let Ok(Some(storage)) = win.local_storage() {
            let _ = storage.set_item(GROUP_DOC_STORAGE_KEY, &b64);
        }
    }
}

/// Write `known_peers.<device_id>.node_address` into the doc so peer addresses
/// merge transitively. No-op if the value is unchanged.
pub async fn set_known_peer(doc: &Arc<Mutex<AutoCommit>>, device_id: &str, node_address: &str) {
    let mut guard = doc.lock().await;
    let root = ObjId::Root;
    let known_peers = match guard.get(&root, KNOWN_PEERS_KEY) {
        Ok(Some((Value::Object(ObjType::Map), id))) => id,
        _ => match guard.put_object(&root, KNOWN_PEERS_KEY, ObjType::Map) {
            Ok(id) => id,
            Err(_) => return,
        },
    };
    let unchanged = match guard.get(&known_peers, device_id).ok().flatten() {
        Some((value, _)) => value.as_str() == Some(node_address),
        None => false,
    };
    if !unchanged {
        guard.put(&known_peers, device_id, node_address).ok();
        let _ = guard.commit();
    }
}

/// Peers from the doc's `known_peers` map: `(device_id, node_address)`.
pub async fn known_peers(doc: &Arc<Mutex<AutoCommit>>) -> Vec<(String, String)> {
    let guard = doc.lock().await;
    let mut out = Vec::new();
    if let Ok(Some((Value::Object(ObjType::Map), id))) = guard.get(&ObjId::Root, KNOWN_PEERS_KEY) {
        for key in guard.keys(&id) {
            if let Ok(Some((value, _))) = guard.get(&id, key.as_str()) {
                if let Some(addr) = value.as_str() {
                    out.push((key, addr.to_string()));
                }
            }
        }
    }
    out
}

/// The stored node address for a peer from the doc's `known_peers` map.
pub async fn known_peer(doc: &Arc<Mutex<AutoCommit>>, device_id: &str) -> Option<String> {
    let guard = doc.lock().await;
    match guard.get(&ObjId::Root, KNOWN_PEERS_KEY).ok().flatten() {
        Some((Value::Object(ObjType::Map), id)) => guard
            .get(&id, device_id)
            .ok()
            .flatten()
            .and_then(|(value, _)| value.as_str().map(|s| s.to_string())),
        _ => None,
    }
}

/// Apply the app's expense-add to `doc`. Returns `true` on success.
///
/// Each expense is stored as its own root key (`exp-<unique id>`), so
/// concurrent adds from different devices merge instead of conflicting. A
/// shared `expenses` container would be re-created independently by every
/// device, producing conflicting objects under the same key; keying by a
/// unique id at the root avoids that entirely.
pub fn add_expense(
    doc: &mut AutoCommit,
    id: String,
    desc: String,
    total: f64,
    payer: String,
    included_json: String,
    now: i64,
) -> bool {
    let root = ObjId::Root;
    let expense_map = match doc.put_object(&root, &id, ObjType::Map) {
        Ok(map_id) => map_id,
        Err(_) => return false,
    };
    doc.put(&expense_map, "desc", desc).ok();
    doc.put(&expense_map, "total", total).ok();
    doc.put(&expense_map, "payer", payer).ok();
    doc.put(&expense_map, "included", included_json).ok();
    doc.put(&expense_map, "time", now).ok();
    let _ = doc.commit();
    true
}

/// Apply the app's expense-remove to `doc`.
pub fn remove_expense(doc: &mut AutoCommit, id: &str) {
    doc.delete(&ObjId::Root, id).ok();
    let _ = doc.commit();
}
