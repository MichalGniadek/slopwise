use automerge::sync::{Message, State, SyncDoc};
use automerge::AutoCommit;
use dioxus::prelude::{Signal, WritableExt};
use iroh::endpoint::{RecvStream, SendStream};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// What `recv_sync_message` produced.
#[derive(Debug)]
pub enum SyncRecv {
    /// A sync message to apply.
    Message(Message),
    /// The peer signaled end-of-sync with a zero-length marker; the stream is
    /// still open.
    PeerDone,
    /// The stream was closed.
    Closed,
}

/// Upper bound for a single framed sync message.
///
/// A corrupted or malicious length prefix must not turn into a multi-GB
/// `vec![0; n]` allocation: in wasm that aborts the task without any teardown
/// running, leaving a dead entry in the connections map that blocks redial.
const MAX_SYNC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Send a length-prefixed sync message on a bi-stream.
pub async fn send_sync_message(
    send_stream: &mut SendStream,
    msg: Message,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let encoded = msg.encode();
    let len = encoded.len() as u32;
    send_stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| e.to_string())?;
    send_stream
        .write_all(&encoded)
        .await
        .map_err(|e| e.to_string())?;
    send_stream.flush().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Read a length-prefixed sync message from a bi-stream.
///
/// Returns `SyncRecv::Closed` when the stream is closed (EOF) and
/// `SyncRecv::PeerDone` when the peer sends a zero-length end-of-sync marker.
pub async fn recv_sync_message(
    recv_stream: &mut RecvStream,
) -> Result<SyncRecv, Box<dyn std::error::Error + Send + Sync>> {
    let mut len_buf = [0u8; 4];
    if recv_stream.read_exact(&mut len_buf).await.is_err() {
        return Ok(SyncRecv::Closed);
    }
    let peer_len = u32::from_be_bytes(len_buf) as usize;
    if peer_len == 0 {
        return Ok(SyncRecv::PeerDone);
    }
    if peer_len > MAX_SYNC_MESSAGE_BYTES {
        return Err(format!("sync message too large: {peer_len} bytes").into());
    }
    let mut peer_bytes = vec![0u8; peer_len];
    recv_stream
        .read_exact(&mut peer_bytes)
        .await
        .map_err(|e| e.to_string())?;
    let msg = Message::decode(&peer_bytes).map_err(|e| e.to_string())?;
    Ok(SyncRecv::Message(msg))
}

/// Run the incremental automerge sync protocol over an already-open bi-stream
/// until both sides are caught up.
///
/// Returns `true` if the document changed as a result of the exchange. The UI
/// signal is refreshed only when the document actually changed.
///
/// The protocol alternates: generate a sync message → send it → receive a reply
/// → apply it → generate the next message → ... until both sides have no more
/// changes to send (`generate_sync_message` returns `None` and `in_flight` is
/// false). The generated message is used for both the send and the termination
/// check — never discard it, since discarding leaves `state.in_flight` set and
/// both sides block forever in `read_exact`.
///
/// `state` is passed in (rather than created fresh) so the per-peer sync state
/// can be reused and persisted across sessions. The doc lock is only held
/// briefly per operation, never across blocking stream I/O.
pub async fn run_automerge_sync(
    send_stream: &mut SendStream,
    recv_stream: &mut RecvStream,
    doc: &Arc<Mutex<AutoCommit>>,
    state: &mut State,
    doc_signal: Option<Signal<AutoCommit>>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let heads_before = doc.lock().await.get_heads();

    loop {
        // Generate a message to send, if we have one.
        let msg = {
            let mut guard = doc.lock().await;
            let sync_view = guard.sync();
            sync_view.generate_sync_message(state)
        };

        // Nothing to send and nothing in flight: signal end-of-sync to the
        // peer, but keep reading until the peer signals done too. Breaking
        // here would drop any changes the peer still has in flight for us.
        let we_are_done = msg.is_none() && !state.in_flight;

        if let Some(msg) = msg {
            send_sync_message(send_stream, msg).await?;
        }

        if we_are_done {
            send_stream
                .write_all(&0u32.to_be_bytes())
                .await
                .map_err(|e| e.to_string())?;
            send_stream.flush().await.map_err(|e| e.to_string())?;
        }

        // Wait for the peer's reply; a zero-length marker or a closed stream
        // ends the exchange.
        match recv_sync_message(recv_stream).await? {
            SyncRecv::Message(msg) => {
                let mut guard = doc.lock().await;
                let mut sync_view = guard.sync();
                sync_view
                    .receive_sync_message(state, msg)
                    .map_err(|e| e.to_string())?;
            }
            SyncRecv::PeerDone | SyncRecv::Closed => break,
        }
    }

    let changed = doc.lock().await.get_heads() != heads_before;
    if changed {
        if let Some(mut signal) = doc_signal {
            signal.set(doc.lock().await.clone());
        }
    }

    Ok(changed)
}
