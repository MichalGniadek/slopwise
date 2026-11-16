use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{HtmlVideoElement, MediaStreamConstraints};

/// QR code payload exchanged between devices.
///
/// Matches the payload shape from SYNC_ENGINE_PLAN.md section 5:
/// `{ "v": 1, "app": "slopwise", "node_address": "..." }`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QrPayload {
    pub v: u8,
    pub app: String,
    pub node_address: String,
}

impl QrPayload {
    /// Dummy data for UX demonstration.
    pub fn dummy() -> Self {
        QrPayload {
            v: 1,
            app: "slopwise".to_string(),
            node_address:
                "iroh://02abc123def4567890@relay.iroh.network?relay=https://relay.iroh.network"
                    .to_string(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

/// Generate an SVG string representing the QR code for the given payload.
pub fn generate_qr_svg(payload: &QrPayload, size: u32) -> String {
    let data = payload.to_json();
    let code = qrcode::QrCode::new(data.as_bytes()).expect("QR code generation failed");
    let width = code.width();

    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" viewBox="0 0 {} {}" role="img" aria-label="QR code">"##,
        size, size, width, width
    ));
    svg.push_str(r##"<rect width="100%" height="100%" fill="#ffffff"/>"##);

    for y in 0..width {
        for x in 0..width {
            if code[(x, y)] == qrcode::Color::Dark {
                svg.push_str(&format!(
                    r##"<rect x="{}" y="{}" width="1" height="1" fill="#000000"/>"##,
                    x, y
                ));
            }
        }
    }

    svg.push_str("</svg>");
    svg
}

/// Request camera access and attach the stream to the video element.
pub async fn start_camera(video: &HtmlVideoElement) -> Result<(), JsValue> {
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("brak okna"))?;
    let navigator = window.navigator();
    let media_devices = match navigator.media_devices() {
        Ok(md) => md,
        Err(_) => {
            return Err(JsValue::from_str(
                "API kamery niedostępne (wymagane HTTPS/localhost)",
            ));
        }
    };
    if media_devices.is_null() || media_devices.is_undefined() {
        return Err(JsValue::from_str(
            "API kamery niedostępne (wymagane HTTPS/localhost)",
        ));
    }

    let constraints = MediaStreamConstraints::new();
    constraints.set_video(&JsValue::from(true));

    let stream =
        JsFuture::from(media_devices.get_user_media_with_constraints(&constraints)?).await?;
    let stream: web_sys::MediaStream = stream.dyn_into()?;
    video.set_src_object(Some(&stream));
    let _ = video.play();

    Ok(())
}

/// Stop all camera tracks and detach the stream from the video element.
pub fn stop_camera(video: &HtmlVideoElement) {
    if let Some(src) = video.src_object() {
        if let Ok(stream) = src.dyn_into::<web_sys::MediaStream>() {
            let tracks = stream.get_tracks();
            for i in 0..tracks.length() {
                let track = tracks.get(i);
                if let Ok(track) = track.dyn_into::<web_sys::MediaStreamTrack>() {
                    let _ = track.stop();
                }
            }
        }
    }
    video.set_src_object(None);
}
