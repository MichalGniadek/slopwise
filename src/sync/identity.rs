use automerge::ActorId;
use iroh::SecretKey;
use wasm_bindgen::JsValue;
use web_sys::Storage;
const DEVICE_SECRET_KEY_KEY: &str = "iroh_share_device_secret_key";

/// Load or create the device identity.
///
/// On first launch, generates a new `SecretKey` and `ActorId` and persists them
/// to localStorage. On subsequent launches, restores them from localStorage.
pub fn load_or_create_identity() -> Result<(SecretKey, automerge::ActorId), JsValue> {
    let storage = local_storage()?;
    let secret_key = if let Ok(Some(hex_str)) = storage.get_item(DEVICE_SECRET_KEY_KEY) {
        let _bytes = hex::decode(&hex_str).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let array: [u8; 32] = _bytes
            .try_into()
            .map_err(|_| JsValue::from_str("invalid key length"))?;
        SecretKey::from_bytes(&array)
    } else {
        let sk = SecretKey::generate();
        let hex_str = hex::encode(sk.to_bytes());
        storage.set_item(DEVICE_SECRET_KEY_KEY, &hex_str)?;
        sk
    };
    let actor_id = ActorId::from(secret_key.to_bytes());

    Ok((secret_key, actor_id))
}

fn local_storage() -> Result<Storage, JsValue> {
    let win = web_sys::window().ok_or_else(|| JsValue::from_str("no window"))?;
    let result = win.local_storage();
    match result {
        Ok(Some(s)) => Ok(s),
        Ok(None) => Err(JsValue::from_str("localStorage unavailable")),
        Err(e) => Err(e),
    }
}
