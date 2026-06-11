//! BLE GATT transport using `bluest`.
//!
//! A Rynk device is identified by its GATT service UUID ([`RYNK_SERVICE_UUID`]),
//! never by its (customizable) BLE name — the BLE counterpart to the serial
//! transport's immutable serial-number marker.
//!
//! [`discover`] lists Rynk keyboards as [`BleDevice`]s by asking the OS which
//! already-connected devices expose the Rynk service — no advertising scan and no
//! GATT attach; the service UUID alone identifies them (a keyboard being
//! configured is one you are already using, whose services the OS has discovered).
//! The app picks one and calls [`BleDevice::connect`], which attaches and
//! completes the handshake ([`Client::connect`]) — the authoritative confirmation.

use std::time::Duration;

use bluest::{Adapter, Characteristic, Device, DeviceId, Uuid};
use futures::StreamExt;
use rmk_types::protocol::rynk::RYNK_BLE_CHUNK_SIZE;
use rynk::io::{Read, Write};
use rynk::{Client, ConnectError, TransportError};
use tokio::sync::{mpsc, oneshot};

const RYNK_SERVICE_UUID: Uuid = Uuid::from_u128(rmk_types::protocol::rynk::RYNK_SERVICE_UUID);
const RYNK_INPUT_CHAR_UUID: Uuid = Uuid::from_u128(rmk_types::protocol::rynk::RYNK_INPUT_CHAR_UUID);
const RYNK_OUTPUT_CHAR_UUID: Uuid = Uuid::from_u128(rmk_types::protocol::rynk::RYNK_OUTPUT_CHAR_UUID);

/// Protocol handshake timeout after the BLE link is attached.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// Bounds one `connect_device` attempt: a BLE connect has no inherent timeout, so
/// a radio-silent entry in the OS connected-device list (or an advertiser that
/// stops responding mid-connect) would otherwise pend forever.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds GATT service/characteristic discovery, which can also stall if the link
/// drops mid-discovery.
const DISCOVER_TIMEOUT: Duration = Duration::from_secs(5);

/// ATT-minimum MTU payload.
const BLE_SAFE_WRITE: usize = 20;

/// Notify bridge channel depth.
const BRIDGE_CHANNEL_CAPACITY: usize = 32;

/// GATT-level I/O failure surfaced through the embedded-io error seam.
#[derive(Debug)]
pub struct BleIoError(String);

impl core::fmt::Display for BleIoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

impl core::error::Error for BleIoError {}

impl rynk::io::Error for BleIoError {
    fn kind(&self) -> rynk::io::ErrorKind {
        rynk::io::ErrorKind::Other
    }
}

/// Byte-stream view over the bridge's notification chunks: doles a chunk out
/// across as many `read` calls as the caller's buffer needs.
struct ChunkReader {
    chunks: mpsc::Receiver<Vec<u8>>,
    pending: Vec<u8>,
    pos: usize,
}

impl rynk::io::ErrorType for ChunkReader {
    type Error = BleIoError;
}

impl Read for ChunkReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        // Skip empty notification chunks: returning `Ok(0)` mid-stream would
        // read as EOF (link gone) to the client.
        while self.pos >= self.pending.len() {
            match self.chunks.recv().await {
                Some(chunk) => {
                    self.pending = chunk;
                    self.pos = 0;
                }
                None => return Ok(0), // bridge gone → EOF → Disconnected
            }
        }
        let n = buf.len().min(self.pending.len() - self.pos);
        buf[..n].copy_from_slice(&self.pending[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Attached Rynk GATT link.
pub struct BleTransport {
    output_char: Characteristic,
    write_chunk: usize,
    reader: ChunkReader,
    /// Notification bridge task.
    bridge: tokio::task::JoinHandle<()>,
    /// The connected device's name, if it advertised one.
    name: Option<String>,
    // Keep the OS connection alive.
    _adapter: Adapter,
    _device: Device,
}

impl BleTransport {
    /// The connected keyboard's BLE name, if any.
    pub fn device_name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl rynk::io::ErrorType for BleTransport {
    type Error = BleIoError;
}

impl Read for BleTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.reader.read(buf).await
    }
}

impl Write for BleTransport {
    /// One GATT write per call, capped to the characteristic capacity; the
    /// client's `write_all` loops over the rest. Acknowledged write: a
    /// silently dropped chunk would desync the firmware's stream reassembler,
    /// which has no mid-frame resync.
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        let n = buf.len().min(self.write_chunk);
        self.output_char
            .write(&buf[..n])
            .await
            .map_err(|e| BleIoError(format!("gatt write: {e}")))?;
        Ok(n)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Drop for BleTransport {
    fn drop(&mut self) {
        self.bridge.abort();
    }
}

/// A Rynk keyboard found by [`discover`], for building a device picker. Holds the
/// bluest handles to attach on demand — `Adapter` and `Device` are cheap
/// cloneable handles, not a live GATT session. Version and capabilities are read
/// by [`connect`](Self::connect), which is the first GATT attach.
pub struct BleDevice {
    /// The keyboard's BLE name, if it advertised one.
    pub name: Option<String>,
    adapter: Adapter,
    device: Device,
}

impl BleDevice {
    /// A stable identifier for this device, usable as a picker key — the BLE
    /// `name` may be absent or shared between keyboards.
    pub fn id(&self) -> DeviceId {
        self.device.id()
    }

    /// Attach and complete the Rynk handshake. A device gone since discovery
    /// surfaces as a normal [`ConnectError`].
    pub async fn connect(&self) -> Result<Client<BleTransport>, ConnectError> {
        connect_transport(attach(&self.adapter, self.device.clone()).await?).await
    }
}

/// List the Rynk keyboards the OS is already connected to — those exposing the
/// Rynk GATT service. No advertising scan and no GATT attach: the service UUID
/// identifies them, like the serial transport's serial-number marker. The picker
/// flow: `discover` → choose a [`BleDevice`] → [`BleDevice::connect`] (which
/// attaches and handshakes).
///
/// Requires Bluetooth permission. A denied or powered-off adapter is not
/// reported distinctly: `wait_available` blocks until the adapter becomes
/// available, so a denied caller observes a hang rather than a specific error.
pub async fn discover() -> Result<Vec<BleDevice>, TransportError> {
    let adapter = Adapter::default()
        .await
        .ok_or_else(|| TransportError::DeviceNotFound("no BLE adapter".into()))?;
    adapter
        .wait_available()
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;

    // Already-connected devices the OS reports as exposing the Rynk service. The
    // 128-bit UUID identifies them without a GATT attach — `connect` attaches.
    let connected = adapter
        .connected_devices_with_services(&[RYNK_SERVICE_UUID])
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(connected
        .into_iter()
        .map(|device| BleDevice {
            name: device.name().ok(),
            adapter: adapter.clone(),
            device,
        })
        .collect())
}

// Intentionally duplicated in `rynk-serial` rather than shared: `rynk`
// is deliberately runtime-free (no `tokio`, builds for `wasm32`), so the
// timeout wrapper can't live there. Each transport crate owns its own runtime.
async fn connect_transport(transport: BleTransport) -> Result<Client<BleTransport>, ConnectError> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, Client::connect(transport))
        .await
        .map_err(|_| ConnectError::Transport(TransportError::DeviceNotFound("handshake timed out".into())))?
}

/// Attach, discover characteristics, and subscribe to notifications.
async fn attach(adapter: &Adapter, device: Device) -> Result<BleTransport, TransportError> {
    // GATT can briefly fail after reconnect, so retry a few times. A *timeout*
    // (rather than a fast error) means the device is unreachable — a BLE connect
    // has no inherent timeout — so abandon the candidate instead of burning the
    // whole retry budget waiting on it.
    let mut last_err = TransportError::Disconnected;
    for attempt in 0..6 {
        if attempt > 0 {
            log::debug!("rynk ble: attach retry {attempt}/5 after {last_err}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        match tokio::time::timeout(CONNECT_TIMEOUT, adapter.connect_device(&device)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                last_err = TransportError::Io(format!("connect_device: {e}"));
                continue;
            }
            Err(_) => return Err(TransportError::Io("connect_device timed out".into())),
        }
        let (input, output) = match tokio::time::timeout(DISCOVER_TIMEOUT, discover_chars(&device)).await {
            Ok(Ok(pair)) => pair,
            // Definitive: discovery completed and the Rynk service/characteristic
            // is absent — not a Rynk device, retrying won't change that.
            Ok(Err(e @ TransportError::DeviceNotFound(_))) => return Err(e),
            Ok(Err(e)) => {
                last_err = e;
                continue;
            }
            Err(_) => return Err(TransportError::Io("service discovery timed out".into())),
        };

        // Clamp to the firmware's characteristic capacity.
        let write_chunk = output
            .max_write_len()
            .unwrap_or(BLE_SAFE_WRITE)
            .clamp(BLE_SAFE_WRITE, RYNK_BLE_CHUNK_SIZE);

        // The bridge owns the characteristic because `notify()` borrows it.
        let (chunk_tx, chunk_rx) = mpsc::channel(BRIDGE_CHANNEL_CAPACITY);
        let (sub_tx, sub_rx) = oneshot::channel();
        let bridge = tokio::spawn(notify_bridge(input, chunk_tx, sub_tx));
        if let Err(e) = sub_rx.await.unwrap_or(Err(TransportError::Disconnected)) {
            bridge.abort();
            last_err = e;
            continue;
        }

        return Ok(BleTransport {
            output_char: output,
            write_chunk,
            reader: ChunkReader {
                chunks: chunk_rx,
                pending: Vec::new(),
                pos: 0,
            },
            bridge,
            name: device.name().ok(),
            _adapter: adapter.clone(),
            _device: device.clone(),
        });
    }
    Err(last_err)
}

/// Discover the Rynk service and its input/output characteristics.
async fn discover_chars(device: &Device) -> Result<(Characteristic, Characteristic), TransportError> {
    let service = device
        .discover_services_with_uuid(RYNK_SERVICE_UUID)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| TransportError::DeviceNotFound("Rynk GATT service not found".into()))?;

    let mut input_char = None;
    let mut output_char = None;
    for c in service
        .discover_characteristics()
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?
    {
        match c.uuid() {
            u if u == RYNK_INPUT_CHAR_UUID => input_char = Some(c),
            u if u == RYNK_OUTPUT_CHAR_UUID => output_char = Some(c),
            _ => {}
        }
    }
    let input = input_char.ok_or_else(|| TransportError::DeviceNotFound("input characteristic missing".into()))?;
    let output = output_char.ok_or_else(|| TransportError::DeviceNotFound("output characteristic missing".into()))?;
    Ok((input, output))
}

/// Subscribe to GATT notifications, ack via `sub_tx`, then forward.
async fn notify_bridge(
    input: Characteristic,
    chunks: mpsc::Sender<Vec<u8>>,
    sub_tx: oneshot::Sender<Result<(), TransportError>>,
) {
    let notifications = match input.notify().await {
        Ok(n) => {
            let _ = sub_tx.send(Ok(()));
            n
        }
        Err(e) => {
            let _ = sub_tx.send(Err(TransportError::Io(e.to_string())));
            return;
        }
    };

    forward_notifications(notifications, chunks).await;
}

/// Forward notification chunks into the transport channel until the stream
/// ends/errors or the transport (the receiver) is dropped.
async fn forward_notifications<E: core::fmt::Debug>(
    mut notifications: impl futures::Stream<Item = Result<Vec<u8>, E>> + Unpin,
    chunks: mpsc::Sender<Vec<u8>>,
) {
    while let Some(item) = notifications.next().await {
        let chunk = match item {
            Ok(c) => c,
            Err(e) => {
                log::debug!("rynk ble: notification stream error: {e:?}");
                break;
            }
        };
        if chunks.send(chunk).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn forwards_chunks_until_stream_ends() {
        let (tx, mut rx) = mpsc::channel(4);
        let items: Vec<Result<Vec<u8>, ()>> = vec![Ok(vec![1, 2]), Ok(vec![3])];
        forward_notifications(stream::iter(items), tx).await;
        assert_eq!(rx.recv().await, Some(vec![1, 2]));
        assert_eq!(rx.recv().await, Some(vec![3]));
        // The sender is gone, so the transport's recv reads Disconnected.
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn stops_at_first_stream_error() {
        let (tx, mut rx) = mpsc::channel(4);
        let items: Vec<Result<Vec<u8>, ()>> = vec![Ok(vec![1]), Err(()), Ok(vec![2])];
        forward_notifications(stream::iter(items), tx).await;
        assert_eq!(rx.recv().await, Some(vec![1]));
        assert_eq!(rx.recv().await, None, "chunks after the error must not be forwarded");
    }

    #[tokio::test]
    async fn stops_when_transport_is_dropped() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        // Endless stream: returns (instead of looping) only because the receiver
        // is gone. The timeout turns a regression (looping forever) into a test
        // failure instead of a hung run; the passing case completes immediately.
        tokio::time::timeout(
            Duration::from_secs(1),
            forward_notifications(stream::repeat_with(|| Ok::<_, ()>(vec![0u8])), tx),
        )
        .await
        .expect("forward_notifications must return when the receiver is dropped");
    }

    fn chunk_reader(capacity: usize) -> (mpsc::Sender<Vec<u8>>, ChunkReader) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            tx,
            ChunkReader {
                chunks: rx,
                pending: Vec::new(),
                pos: 0,
            },
        )
    }

    #[tokio::test]
    async fn chunk_reader_doles_chunk_across_reads() {
        let (tx, mut r) = chunk_reader(2);
        tx.send(vec![1, 2, 3, 4, 5]).await.unwrap();
        drop(tx);

        let mut buf = [0u8; 2];
        assert_eq!(r.read(&mut buf).await.unwrap(), 2);
        assert_eq!(buf, [1, 2]);
        assert_eq!(r.read(&mut buf).await.unwrap(), 2);
        assert_eq!(buf, [3, 4]);
        assert_eq!(r.read(&mut buf).await.unwrap(), 1);
        assert_eq!(buf[0], 5);
        // Channel closed and drained → EOF.
        assert_eq!(r.read(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn chunk_reader_skips_empty_chunks() {
        let (tx, mut r) = chunk_reader(2);
        tx.send(Vec::new()).await.unwrap();
        tx.send(vec![7]).await.unwrap();
        drop(tx);

        let mut buf = [0u8; 4];
        assert_eq!(r.read(&mut buf).await.unwrap(), 1, "empty chunk must not read as EOF");
        assert_eq!(buf[0], 7);
    }

    #[tokio::test]
    async fn chunk_reader_read_is_cancel_safe() {
        use std::future::Future;
        use std::task::{Context, Poll};

        let (tx, mut r) = chunk_reader(2);
        let mut buf = [0u8; 4];
        // A read with nothing buffered must park on the channel (never return
        // Ok(0), which the client reads as EOF). Poll it once to register the
        // wait, then drop it — the contract requires a cancelled read to lose no
        // later-delivered bytes.
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        {
            let read = r.read(&mut buf);
            futures::pin_mut!(read);
            assert!(
                matches!(read.as_mut().poll(&mut cx), Poll::Pending),
                "read must park on an empty channel, not resolve"
            );
        }
        // Bytes sent after the cancelled read are still delivered intact.
        tx.send(vec![1, 2, 3]).await.unwrap();
        drop(tx);
        assert_eq!(r.read(&mut buf).await.unwrap(), 3);
        assert_eq!(&buf[..3], &[1, 2, 3]);
    }
}
