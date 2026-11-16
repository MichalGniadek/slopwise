use automerge::{AutoCommit, ObjId, ObjType, ReadDoc, Value};
use base64::Engine as _;
use dioxus::prelude::*;
use js_sys::Date;
use serde_json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;
use web_sys::{HtmlVideoElement, window};
mod qr;
mod sync;
use qr::{QrPayload, generate_qr_svg, start_camera, stop_camera};
use sync::SyncEngine;

fn format_time(millis: i64) -> String {
    if millis == 0 {
        return String::new();
    }
    let date = Date::new(&(millis as f64).into());
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}",
        date.get_full_year(),
        date.get_month() + 1,
        date.get_date(),
        date.get_hours(),
        date.get_minutes()
    )
}

/// Format a money amount the Polish way: comma decimal separator followed by
/// the "zł" unit, e.g. `12,34 zł`.
fn fmt_pln(amount: f64) -> String {
    format!("{:.2}", amount).replace('.', ",") + " zł"
}

/// Generate a unique expense id. Combines the creation timestamp with a random
/// component so ids are unique both within a device and across devices, which
/// is what lets concurrent adds from different devices merge in the CRDT map.
fn new_expense_id(now: i64) -> String {
    let rand = js_sys::Math::random();
    format!("exp-{}-{}", now, (rand * 1e12) as u64)
}

/// Collect every person who appears in any expense (as payer or included),
/// sorted alphabetically. This is the pool of people offered by the person
/// selector when adding a new expense.
fn historical_people_from_doc(doc: &AutoCommit) -> Vec<String> {
    let mut people: HashSet<String> = HashSet::new();
    let root = ObjId::Root;
    for key in doc.keys(&root) {
        if !key.starts_with("exp-") {
            continue;
        }
        if let Ok(Some((Value::Object(ObjType::Map), expense_id))) = doc.get(&root, key.as_str()) {
            if let Some(payer) = doc
                .get(&expense_id, "payer")
                .ok()
                .flatten()
                .and_then(|(v, _)| v.as_str().map(|s| s.to_string()))
            {
                people.insert(payer);
            }
            if let Some(inc_json) = doc
                .get(&expense_id, "included")
                .ok()
                .flatten()
                .and_then(|(v, _)| v.as_str().map(|s| s.to_string()))
            {
                if let Ok(inc) = serde_json::from_str::<Vec<String>>(&inc_json) {
                    for name in inc {
                        people.insert(name);
                    }
                }
            }
        }
    }
    let mut v: Vec<String> = people.into_iter().collect();
    v.sort();
    v
}

/// Reset the person selector to the defaults for a new expense: everyone from
/// history is included and the first person is selected as payer.
fn reset_person_defaults(
    doc: &AutoCommit,
    mut included: Signal<Vec<String>>,
    mut selected_payer: Signal<String>,
    mut last_seen_history: Signal<Vec<String>>,
) {
    let people = historical_people_from_doc(doc);
    included.set(people.clone());
    selected_payer.set(people.first().cloned().unwrap_or_default());
    last_seen_history.set(people);
}

fn main() {
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let mut new_desc = use_signal(|| String::new());
    let mut new_amount = use_signal(|| String::new());
    let mut included = use_signal(|| Vec::<String>::new());
    let mut new_person = use_signal(|| String::new());
    let mut selected_payer = use_signal(|| String::new());
    let mut dark_mode = use_signal(|| false);
    let mut show_qr = use_signal(|| false);
    let mut scanning = use_signal(|| false);
    let mut scan_result = use_signal(|| Option::<String>::None);
    let mut scan_error = use_signal(|| Option::<String>::None);
    let mut show_debug = use_signal(|| false);
    let mut video_ref = use_signal(|| None::<HtmlVideoElement>);
    let mut sync_status = use_signal(|| String::new());
    let mut known_peers_count = use_signal(|| 0usize);
    let mut peer_connected_count = use_signal(|| 0usize);
    let mut known_peers_list = use_signal(|| Vec::<(String, String)>::new());
    let mut doc = use_signal(|| AutoCommit::new());
    let mut sync_engine = use_signal(|| None::<Arc<SyncEngine>>);

    // All people that ever appear in an expense — the pool shown in the person
    // selector. Recomputes whenever the doc changes (local add or sync merge).
    let historical_people = use_memo(move || historical_people_from_doc(&doc.read()));

    // People already present in history the last time the selector synced with
    // the doc. Used to detect brand-new people so they can be included by
    // default (checked) without re-checking people the user explicitly unchecked.
    let mut last_seen_history = use_signal(|| Vec::<String>::new());

    // Newly appearing people in history are included by default (checked), and
    // the payer defaults to the first checked person whenever none is set.
    //
    // IMPORTANT: this effect only ever subscribes to the history memo, and it
    // reads the form signals with peek() and only writes when a value actually
    // changes. Writing to a signal it subscribes to (or writing unconditionally)
    // would make the effect re-run itself forever and freeze the app.
    use_effect(move || {
        let people = historical_people.read().clone();
        let last = last_seen_history.peek().clone();
        let mut inc = included.peek().clone();
        for p in &people {
            if !last.contains(p) && !inc.contains(p) {
                inc.push(p.clone());
            }
        }
        if inc != included.peek().clone() {
            included.set(inc.clone());
        }
        if selected_payer.peek().is_empty() {
            // When the form starts, every history person is checked, so this
            // is just the first person in the list.
            if let Some(first) = inc.first() {
                selected_payer.set(first.clone());
            }
        }
        if last != people {
            last_seen_history.set(people);
        }
    });

    use_effect(move || {
        let system_dark = web_sys::window()
            .and_then(|w| w.match_media("(prefers-color-scheme: dark)").ok().flatten())
            .map(|m| m.matches())
            .unwrap_or(false);
        dark_mode.set(system_dark);

        if let Some(win) = web_sys::window() {
            if let Some(doc) = win.document() {
                if let Some(body) = doc.body() {
                    if system_dark {
                        let _ = body.class_list().add_1("dark");
                    }
                }
            }
        }
    });

    // Initialize the sync engine (iroh endpoint + automerge doc + peer loop).
    use_effect(move || {
        spawn_local(async move {
            let engine = match SyncEngine::init(doc).await {
                Ok(engine) => engine,
                Err(e) => {
                    sync_status.set(format!("Błąd silnika synchronizacji: {e:?}"));
                    return;
                }
            };

            // Initialize the Dioxus doc signal from the engine's loaded doc.
            {
                let doc_guard = engine.doc.lock().await;
                doc.set(doc_guard.clone());
            }

            let engine = Arc::new(engine);
            sync_engine.set(Some(engine.clone()));
            sync_status.set("Silnik synchronizacji zainicjowany".to_string());

            // Start polling peer counts for the debug view.
            spawn_local(async move {
                loop {
                    let peers = engine.peer_manager.get_peers().await;
                    known_peers_count.set(peers.len());
                    let connected = peers
                        .iter()
                        .filter(|p| {
                            matches!(
                                p.status,
                                sync::peers::PeerStatus::Connected
                                    | sync::peers::PeerStatus::Synced
                            )
                        })
                        .count();
                    peer_connected_count.set(connected);
                    let peer_list: Vec<(String, String)> = peers
                        .iter()
                        .map(|p| (p.device_id.clone(), format!("{:?}", p.status)))
                        .collect();
                    known_peers_list.set(peer_list);
                    let promise = js_sys::eval("new Promise(resolve => setTimeout(resolve, 5000))")
                        .unwrap()
                        .dyn_into::<js_sys::Promise>()
                        .unwrap();
                    wasm_bindgen_futures::JsFuture::from(promise).await.ok();
                }
            });
        });
    });

    use_effect(move || {
        if let Some(win) = web_sys::window() {
            if let Some(doc) = win.document() {
                if let Some(video_el) = doc.query_selector(".scanner-video").ok().flatten() {
                    if let Ok(video) = video_el.dyn_into::<HtmlVideoElement>() {
                        video_ref.set(Some(video));
                    }
                }
            }
        }
    });

    let add_expense = move |_| {
        // Accept the comma decimal separator (Polish locale) as well as the dot.
        let total: f64 = new_amount.read().replace(',', ".").parse().unwrap_or(0.0);
        let payer = selected_payer.read().clone();
        let inc: Vec<String> = included.read().clone();

        if total <= 0.0 || payer.is_empty() || inc.is_empty() {
            return;
        }
        if !inc.contains(&payer) {
            return;
        }

        let inc_json = serde_json::to_string(&inc).unwrap_or_default();
        let desc = new_desc.read().clone();
        let now = js_sys::Date::new_0().get_time() as i64;
        let id = new_expense_id(now);

        // Apply the edit to the engine doc (the single source of truth), then
        // let the engine update the UI, persist, and push to peers.
        let engine = sync_engine.read().clone();
        if let Some(engine) = engine {
            let doc_sig = doc;
            spawn_local(async move {
                {
                    let mut guard = engine.doc.lock().await;
                    if !sync::doc::add_expense(&mut guard, id, desc, total, payer, inc_json, now) {
                        return;
                    }
                }
                // Refresh the UI signal, then start the next expense with the
                // default person selection (everyone included, first pays).
                engine.on_local_change(doc_sig).await;
                reset_person_defaults(&doc_sig.read(), included, selected_payer, last_seen_history);
            });
        } else {
            // Engine not ready yet: fall back to editing the UI signal directly.
            // The engine merges the signal into its doc when it finishes starting.
            let mut d2 = doc.read().clone();
            sync::doc::add_expense(&mut d2, id, desc, total, payer, inc_json, now);
            doc.set(d2);
            save_to_storage(&doc);
            reset_person_defaults(&doc.read(), included, selected_payer, last_seen_history);
        }

        new_desc.set(String::new());
        new_amount.set(String::new());
    };

    let mut remove_expense = move |id: String| {
        let engine = sync_engine.read().clone();
        if let Some(engine) = engine {
            let doc_sig = doc;
            spawn_local(async move {
                {
                    let mut guard = engine.doc.lock().await;
                    sync::doc::remove_expense(&mut guard, &id);
                }
                engine.on_local_change(doc_sig).await;
            });
        } else {
            let mut d2 = doc.read().clone();
            sync::doc::remove_expense(&mut d2, &id);
            doc.set(d2);
            save_to_storage(&doc);
        }
    };

    fn save_to_storage(doc: &Signal<AutoCommit>) {
        let mut d = doc.read().clone();
        let bytes = d.save();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        if let Some(win) = window() {
            if let Ok(Some(storage)) = win.local_storage() {
                let _ = storage.set_item("iroh_share_group_doc", &b64);
            }
        }
    }

    let expenses: Vec<(String, String, f64, String, Vec<String>, String)> = {
        let d = doc.read();
        let root = ObjId::Root;
        // (id, desc, total, payer, inc, date, time) — time kept only for sorting.
        let mut out: Vec<(String, String, f64, String, Vec<String>, String, i64)> = Vec::new();

        // Each expense is a root key prefixed with "exp-".
        for key in d.keys(&root) {
            if !key.starts_with("exp-") {
                continue;
            }
            if let Ok(Some((Value::Object(ObjType::Map), expense_id))) =
                d.get(&root, key.as_str())
            {
                let desc = d
                    .get(&expense_id, "desc")
                    .ok()
                    .flatten()
                    .and_then(|(v, _)| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_default();
                let total = d
                    .get(&expense_id, "total")
                    .ok()
                    .flatten()
                    .and_then(|(v, _)| v.as_f64())
                    .unwrap_or(0.0);
                let payer = d
                    .get(&expense_id, "payer")
                    .ok()
                    .flatten()
                    .and_then(|(v, _)| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_default();
                let inc_json = d
                    .get(&expense_id, "included")
                    .ok()
                    .flatten()
                    .and_then(|(v, _)| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_default();
                let inc: Vec<String> = serde_json::from_str(&inc_json).unwrap_or_default();
                let time = d
                    .get(&expense_id, "time")
                    .ok()
                    .flatten()
                    .and_then(|(v, _)| v.as_i64())
                    .unwrap_or(0);
                let date = format_time(time);
                out.push((key, desc, total, payer, inc, date, time));
            }
        }
        out.sort_by(|a, b| b.6.cmp(&a.6));
        out.into_iter()
            .map(|(id, desc, total, payer, inc, date, _)| (id, desc, total, payer, inc, date))
            .collect()
    };

    let mut net: HashMap<String, f64> = HashMap::new();
    for (_, _, total, payer, inc, _) in &expenses {
        let n = inc.len() as f64;
        if n == 0.0 {
            continue;
        }
        let share = total / n;
        *net.entry(payer.clone()).or_insert(0.0) += total;
        for name in inc {
            *net.entry(name.clone()).or_insert(0.0) -= share;
        }
    }

    let mut creditors: Vec<(String, f64)> = net
        .iter()
        .filter(|&(_, &b)| b > 0.01)
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    let mut debtors: Vec<(String, f64)> = net
        .iter()
        .filter(|&(_, &b)| b < -0.01)
        .map(|(k, &v)| (k.clone(), -v))
        .collect();

    creditors.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    debtors.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let mut simplified: Vec<(String, String, f64)> = Vec::new();
    let mut ci = 0usize;
    let mut di = 0usize;
    while ci < creditors.len() && di < debtors.len() {
        let amount = creditors[ci].1.min(debtors[di].1);
        if amount > 0.01 {
            simplified.push((debtors[di].0.clone(), creditors[ci].0.clone(), amount));
        }
        creditors[ci].1 -= amount;
        debtors[di].1 -= amount;
        if creditors[ci].1 <= 0.01 {
            ci += 1;
        }
        if debtors[di].1 <= 0.01 {
            di += 1;
        }
    }

    let mut net_vec = net
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect::<Vec<(String, f64)>>();
    net_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let balance_lines: Vec<(String, String, String)> = net_vec
        .iter()
        .map(|(name, bal)| {
            let cls = if *bal > 0.01 {
                "credit".to_string()
            } else if *bal < -0.01 {
                "debit".to_string()
            } else {
                "settled".to_string()
            };
            let val = if *bal > 0.0 {
                format!("+{}", fmt_pln(*bal))
            } else if *bal < 0.0 {
                format!("-{}", fmt_pln(bal.abs()))
            } else {
                "0,00 zł".to_string()
            };
            (name.clone(), cls, val)
        })
        .collect();

    let expense_elements: Vec<Element> = expenses
        .iter()
        .map(|(id, desc, total, payer, inc, date)| {
            let id = id.clone();
            let desc = desc.clone();
            let total = *total;
            let payer = payer.clone();
            let inc_str = inc.join(", ");
            let date = date.clone();
            rsx! {
                tr { class: "expense-row",
                    td { class: "col-title", "data-label": "Tytuł", "{desc}" }
                    td { class: "col-amount", "data-label": "Kwota", "{fmt_pln(total)}" }
                    td { class: "col-payer", "data-label": "Płacący", "{payer}" }
                    td { class: "col-included", "data-label": "Uczestnicy", "{inc_str}" }
                    td { class: "col-date", "data-label": "Data", "{date}" }
                    td { class: "col-actions", "data-label": "",
                        button {
                            class: "btn-remove",
                            onclick: move |_| remove_expense(id.clone()),
                            "×"
                        }
                    }
                }
            }
        })
        .collect();

    // Every person shown in the selector: everyone from history plus anyone
    // added for this expense, sorted alphabetically.
    let selector_people: Vec<String> = {
        let mut set: HashSet<String> = historical_people.read().iter().cloned().collect();
        for name in included.read().iter() {
            set.insert(name.clone());
        }
        let mut v: Vec<String> = set.into_iter().collect();
        v.sort();
        v
    };

    let person_selector_rows: Vec<Element> = selector_people
        .iter()
        .map(|name| {
            let name = name.clone();
            let checkbox_name = name.clone();
            let radio_name = name.clone();
            let is_checked = included.read().contains(&name);
            let is_payer = selected_payer.read().clone() == name;
            rsx! {
                div { class: "person-row",
                    input {
                        r#type: "checkbox",
                        checked: is_checked,
                        onchange: move |_| {
                            let mut inc = included.read().clone();
                            if inc.contains(&checkbox_name) {
                                let was_payer = selected_payer.read().clone() == checkbox_name;
                                inc.retain(|n| n != &checkbox_name);
                                included.set(inc);
                                // Keep the payer consistent: fall back to the
                                // first checked person.
                                if was_payer {
                                    selected_payer.set(
                                        included.read().first().cloned().unwrap_or_default(),
                                    );
                                }
                            } else {
                                inc.push(checkbox_name.clone());
                                if selected_payer.read().is_empty() {
                                    selected_payer.set(checkbox_name.clone());
                                }
                                included.set(inc);
                            }
                        },
                    }
                    input {
                        r#type: "radio",
                        name: "payer",
                        checked: is_payer,
                        disabled: !is_checked,
                        onchange: move |_| selected_payer.set(radio_name.clone()),
                    }
                    span { class: "name", "{name}" }
                }
            }
        })
        .collect();

    /* --- QR code UX handlers --- */

    // Stop the camera stream and close the scan flow.
    // Stops camera first, then signals the flow to close (which removes the video element from DOM).
    let mut stop_camera_flow = move || {
        scan_result.set(None);
        scan_error.set(None);

        // Stop camera stream first, before DOM removes the video element
        let video = video_ref.read().clone();
        video_ref.set(None);

        if let Some(video) = video {
            stop_camera(&video);
        }

        scanning.set(false);
    };

    let mut show_qr_handler = move || {
        show_qr.set(true);
        stop_camera_flow();
    };

    let mut close_qr_handler = move || {
        show_qr.set(false);
    };

    let mut close_scanner_handler = move || {
        stop_camera_flow();
    };

    let mut start_scan_flow = move || {
        scanning.set(true);
        show_qr.set(false);
        scan_result.set(None);
        scan_error.set(None);

        wasm_bindgen_futures::spawn_local(async move {
            // Small delay so the DOM renders the <video> element before we look it up
            let delay = js_sys::eval("new Promise(resolve => setTimeout(resolve, 100))")
                .unwrap()
                .dyn_into::<js_sys::Promise>()
                .unwrap();
            wasm_bindgen_futures::JsFuture::from(delay).await.ok();

            let video = {
                let existing = video_ref.read().clone();
                match existing {
                    Some(v) => v,
                    None => {
                        match web_sys::window()
                            .and_then(|w| w.document())
                            .and_then(|d| d.query_selector(".scanner-video").ok().flatten())
                        {
                            Some(el) => {
                                if let Ok(v) = el.dyn_into::<HtmlVideoElement>() {
                                    video_ref.set(Some(v.clone()));
                                    v
                                } else {
                                    scan_error.set(Some("Nie znaleziono elementu wideo".to_string()));
                                    return;
                                }
                            }
                            None => {
                                scan_error.set(Some("Element wideo nie jest gotowy".to_string()));
                                return;
                            }
                        }
                    }
                }
            };

            if let Err(e) = start_camera(&video).await {
                scan_error.set(Some(format!("Błąd dostępu do kamery: {e:?}")));
                return;
            }

            loop {
                if !*scanning.read() {
                    break;
                }
                let document = match web_sys::window().and_then(|w| w.document()) {
                    Some(d) => d,
                    None => break,
                };
                let canvas = match document.create_element("canvas") {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let canvas: web_sys::HtmlCanvasElement =
                    match canvas.dyn_into::<web_sys::HtmlCanvasElement>() {
                        Ok(c) => c,
                        Err(_) => break,
                    };
                let result = qr_scan_frame(&video, &canvas);
                match result {
                    Ok(data) => {
                        scan_result.set(Some(data.clone()));
                        stop_camera(&video);
                        video_ref.set(None);
                        scanning.set(false);

                        // Attempt to connect to the scanned peer and merge docs.
                        let engine = sync_engine.read().clone();
                        if let Some(engine) = engine {
                            if let Some(payload) = QrPayload::from_json(&data) {
                                spawn_local(async move {
                                    if let Err(e) = engine.scan_and_merge(&payload, doc).await
                                    {
                                        scan_error.set(Some(format!("Błąd synchronizacji: {e}")));
                                    }
                                });
                            }
                        }

                        break;
                    }
                    Err(_) => {}
                }
                let promise = js_sys::eval("new Promise(resolve => setTimeout(resolve, 1000))")
                    .unwrap()
                    .dyn_into::<js_sys::Promise>()
                    .unwrap();
                wasm_bindgen_futures::JsFuture::from(promise).await.ok();
            }
        });
    };

    let mut scan_qr_handler = move || {
        start_scan_flow();
    };

    let mut stop_scan_flow = move || {
        stop_camera_flow();
    };

    let mut close_debug_handler = move || {
        show_debug.set(false);
    };

    /* --- QR scanning helpers --- */

    fn qr_scan_frame(
        video: &HtmlVideoElement,
        canvas: &web_sys::HtmlCanvasElement,
    ) -> Result<String, String> {
        let ctx = canvas
            .get_context("2d")
            .ok()
            .flatten()
            .and_then(|c| {
                JsValue::from(c)
                    .dyn_into::<web_sys::CanvasRenderingContext2d>()
                    .ok()
            })
            .ok_or_else(|| "Nie można uzyskać kontekstu canvas".to_string())?;

        let width = video.video_width() as u32;
        let height = video.video_height() as u32;

        if width == 0 || height == 0 {
            return Err("Wideo nie ma jeszcze rozmiarów. Spróbuj ponownie.".to_string());
        }

        canvas.set_width(width);
        canvas.set_height(height);

        let _ = ctx.draw_image_with_html_video_element(video, 0.0, 0.0);

        scan_canvas_qr(canvas)
    }

    fn scan_canvas_qr(canvas: &web_sys::HtmlCanvasElement) -> Result<String, String> {
        let ctx = canvas
            .get_context("2d")
            .ok()
            .flatten()
            .and_then(|c| {
                JsValue::from(c)
                    .dyn_into::<web_sys::CanvasRenderingContext2d>()
                    .ok()
            })
            .ok_or_else(|| "Nie można uzyskać kontekstu canvas".to_string())?;

        let width = canvas.width();
        let height = canvas.height();

        if width == 0 || height == 0 {
            return Err("Canvas nie ma rozmiarów".to_string());
        }

        let get_image_data_fn = js_sys::Reflect::get(&ctx, &"getImageData".into())
            .map_err(|_| "getImageData niedostępny".to_string())?
            .dyn_into::<js_sys::Function>()
            .map_err(|_| "getImageData nie jest funkcją".to_string())?;
        let image_data = get_image_data_fn
            .call4(
                &JsValue::from(ctx.clone()),
                &JsValue::from(0.0),
                &JsValue::from(0.0),
                &JsValue::from(width as f64),
                &JsValue::from(height as f64),
            )
            .map_err(|_| "Nie udało się wywołać getImageData".to_string())?;
        let data = js_sys::Reflect::get(&image_data, &"data".into())
            .map_err(|_| "Nie można pobrać danych obrazu".to_string())?;

        let window = web_sys::window().ok_or_else(|| "Brak okna".to_string())?;
        let jsqr_val = js_sys::Reflect::get(&window, &"jsQR".into())
            .map_err(|_| "Biblioteka jsQR nie została załadowana (sprawdź dostęp do CDN)".to_string())?;
        if jsqr_val.is_null() || jsqr_val.is_undefined() {
            return Err("Biblioteka jsQR nie została załadowana (sprawdź dostęp do CDN)".to_string());
        }
        let jsqr = jsqr_val
            .dyn_into::<js_sys::Function>()
            .map_err(|_| "jsQR nie jest funkcją".to_string())?;

        let result = jsqr
            .call3(
                &JsValue::NULL,
                &data,
                &wasm_bindgen::JsValue::from(width as u32),
                &wasm_bindgen::JsValue::from(height as u32),
            )
            .map_err(|_| "Nie udało się wywołać jsQR".to_string())?;

        let code_data = js_sys::Reflect::get(&result, &"data".into())
            .map_err(|_| "Nie można odczytać wyniku jsQR".to_string())?;
        if code_data.is_null() || code_data.is_undefined() {
            return Err("Nie znaleziono kodu QR w kadrze".to_string());
        }

        code_data
            .as_string()
            .ok_or_else(|| "Nieprawidłowe dane kodu QR".to_string())
    }



    rsx! {
        div { class: "app",
            style { {include_str!("styles.css")} }
            script { src: "https://cdn.jsdelivr.net/npm/jsqr@1.4.0/dist/jsQR.js" }
            h1 { "SlopWise" }
            div { class: "theme-controls",
                button {
                    class: "theme-toggle",
                    onclick: move |_| {
                        let val = !*dark_mode.read();
                        dark_mode.set(val);
                        if let Some(win) = web_sys::window() {
                            if let Some(doc) = win.document() {
                                if let Some(body) = doc.body() {
                                    if val {
                                        let _ = body.class_list().add_1("dark");
                                    } else {
                                        let _ = body.class_list().remove_1("dark");
                                    }
                                }
                            }
                        }
                    },
                    "🌓"
                }
                button {
                    class: "theme-toggle",
                    onclick: move |_| {
                        show_debug.set(true);
                        show_qr.set(false);
                        stop_camera_flow();
                    },
                    "🐛 Debug"
                }
            }
            p { "Jedna grupa na urządzenie. Każdy może dodawać wydatki. Zostajesz zapamiętany(a)." }

            div { class: "card",
                h3 { "Synchronizacja grupy" }
                p { class: "muted", "Zeskanuj kod QR innego urządzenia, aby połączyć grupy, albo pokaż swój kod QR do zeskanowania." }
                div { class: "qr-actions",
                    button {
                        class: "btn",
                        onclick: move |_| {
                            if *scanning.read() {
                                stop_scan_flow();
                            }
                            show_qr_handler();
                        },
                        "📷 Pokaż QR"
                    }
                    button {
                        class: "btn",
                        onclick: move |_| {
                            if *show_qr.read() {
                                close_qr_handler();
                            }
                            scan_qr_handler();
                        },
                        "📱 Skanuj QR (kamera)"
                    }
                }
            }

            {if *show_qr.read() {
                let payload = sync_engine.read().as_ref().map(|e| e.qr_payload()).unwrap_or_else(QrPayload::dummy);
                let svg = generate_qr_svg(&payload, 300);
                rsx! {
                    div { class: "card qr-display",
                        h3 { "Twój kod QR" }
                        div { class: "qr-svg-container",
                            div { dangerous_inner_html: "{svg}" }
                        }
                        p { class: "qr-payload", "{payload.to_json()}" }
                        button { class: "btn", onclick: move |_| close_qr_handler(), "Zamknij" }
                    }
                }
            } else {
                rsx! {}
            }}

            {if *scanning.read() {
                rsx! {
                    div { class: "card scanner-card",
                        h3 { "Skanuj kod QR" }
                        p { "Skieruj kamerę na kod QR." }
                        div { class: "scanner-preview",
                            video {
                                class: "scanner-video",
                                autoplay: true,
                                muted: true,
                                playsinline: true,
                                width: "400",
                                height: "300",
                            }
                            canvas { class: "scanner-canvas", style: "display: none;" }
                        }
                        div { class: "scanner-controls",
                            button { class: "btn", onclick: move |_| stop_scan_flow(), "Zamknij" }
                        }
                        {if let Some(result) = scan_result.read().clone() {
                            rsx! {
                                div { class: "scan-result",
                                    h4 { "Zeskanowane dane:" }
                                    p { class: "scan-data", "{result}" }
                                    button { class: "btn", onclick: move |_| close_scanner_handler(), "Gotowe" }
                                }
                            }
                        } else {
                            rsx! {}
                        }}
                        {if let Some(err) = scan_error.read().clone() {
                            rsx! {
                                div { class: "scan-error",
                                    "{err}"
                                    button { class: "btn", onclick: move |_| { scan_error.set(None); }, "Zamknij" }
                                }
                            }
                        } else {
                            rsx! {}
                        }}
                    }
                }
            } else {
                rsx! {}
            }}

            {if *show_debug.read() {
                let engine = sync_engine.read().clone();
                // The device id is the endpoint's public key hex, which is what
                // other devices show in their Known Peers list.
                let device_id = engine.as_ref().map(|e| hex::encode(e.device_id.public().as_bytes())).unwrap_or_else(|| "nie zainicjowano".to_string());

                rsx! {
                    div { class: "card debug-card",
                        h3 { "Informacje debugowania" }
                        div { class: "debug-actions",
                            button { class: "btn", onclick: move |_| close_debug_handler(), "Zamknij" }
                            button { class: "btn", onclick: move |_| {
                                if let Some(win) = web_sys::window() {
                                    if let Ok(Some(storage)) = win.local_storage() {
                                        let _ = storage.remove_item("iroh_share_device_secret_key");
                                        let _ = storage.remove_item("iroh_share_actor_id");
                                        let _ = storage.remove_item("iroh_share_known_peers");
                                        let _ = storage.remove_item("iroh_share_group_doc");
                                        let _ = storage.remove_item("iroh_share_sync_states");
                                        let _ = storage.remove_item("slopwise_data");
                                    }
                                }
                                if let Some(win) = web_sys::window() {
                                    let _ = win.location().reload();
                                }
                            }, "Wyczyść wszystkie dane i przeładuj" }
                        }
                        div { class: "debug-info",
                            p { "Identyfikator urządzenia: {device_id}" }
                            p { "Uprawnienie do kamery: nieznane" }
                            h4 { "Znane urządzenia" }
                            if known_peers_list.read().is_empty() {
                                p { "Brak" }
                            } else {
                                for (peer_id, status) in known_peers_list.read().iter() {
                                    p { "  {peer_id}: {status}" }
                                }
                            }
                        }
                    }
                }
            } else {
                rsx! {}
            }}

            div { class: "card",
                h3 { "Salda" }
                if simplified.is_empty() {
                    div { class: "empty-state", "Wszyscy są rozliczeni." }
                } else {
                    for (from, to, amt) in &simplified {
                        div { class: "simplified-item",
                            span { "{from} płaci {to}" }
                            span { "{fmt_pln(*amt)}" }
                        }
                    }
                }
                h4 { "Salda netto" }
                if balance_lines.is_empty() {
                    div { class: "empty-state", "Brak wydatków." }
                } else {
                    for (name, cls, val) in &balance_lines {
                        div { class: "balance-item",
                            span { "{name}" }
                            span { class: cls, "{val}" }
                        }
                    }
                }
            }

            div { class: "card add-expense-card",
                h3 { "Dodaj wydatek" }
                input {
                    placeholder: "Tytuł (opcjonalnie)",
                    value: "{new_desc}",
                    oninput: move |evt| new_desc.set(evt.value()),
                }
                input {
                    r#type: "number",
                    placeholder: "Całkowita kwota",
                    value: "{new_amount}",
                    oninput: move |evt| new_amount.set(evt.value()),
                }
                h4 { "Osoby (po równo)" }

                div { class: "people-section",
                    div { class: "people-list",
                        {person_selector_rows.into_iter().flatten()}
                        div { class: "people-add-row",
                            input {
                                r#type: "text",
                                placeholder: "Nazwa osoby",
                                value: "{new_person}",
                                oninput: move |evt| new_person.set(evt.value()),
                                onkeydown: move |evt: KeyboardEvent| {
                                    if evt.key() == Key::Enter {
                                        let mut inc = included.read().clone();
                                        let name = new_person.read().clone();
                                        if name.is_empty() {
                                            return;
                                        }
                                        if inc.contains(&name) {
                                            new_person.set(String::new());
                                            return;
                                        }
                                        inc.push(name.clone());
                                        included.set(inc);
                                        new_person.set(String::new());
                                        if selected_payer.read().is_empty() {
                                            selected_payer.set(name);
                                        }
                                    }
                                },
                            }
                            button {
                                onclick: move |_| {
                                    let mut inc = included.read().clone();
                                    let name = new_person.read().clone();
                                    if name.is_empty() {
                                        return;
                                    }
                                    if inc.contains(&name) {
                                        new_person.set(String::new());
                                        return;
                                    }
                                    inc.push(name.clone());
                                    included.set(inc);
                                    new_person.set(String::new());
                                    if selected_payer.read().is_empty() {
                                        selected_payer.set(name);
                                    }
                                },
                                "+"
                            }
                        }
                    }
                }
                button { class: "add-expense-btn", onclick: add_expense, "Dodaj wydatek" }
            }

            div { class: "card",
                h3 { "Wydatki" }
                if expenses.is_empty() {
                    div { class: "empty-state", "Brak wydatków." }
                } else {
                    table { class: "expenses-table",
                        thead {
                            tr {
                                th { class: "col-title", "Tytuł" }
                                th { class: "col-amount", "Kwota" }
                                th { class: "col-payer", "Płacący" }
                                th { class: "col-included", "Uczestnicy" }
                                th { class: "col-date", "Data" }
                                th { class: "col-actions", "" }
                            }
                        }
                        tbody {
                            {expense_elements.into_iter().flatten()}
                        }
                    }
                }
            }
        }
    }
}
