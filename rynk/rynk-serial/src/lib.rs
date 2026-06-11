//! USB CDC-ACM serial transport using `tokio-serial`.
//!
//! [`discover`] returns one [`SerialDevice`] per Rynk keyboard, recognized by the
//! [`RYNK_SERIAL_MAGIC`] marker in its USB serial number — an immutable tag the
//! `rynk` firmware prepends regardless of the user-configured VID/serial. The OS
//! caches the serial string at enumeration, so discovery reads it on
//! Windows/macOS/Linux *without opening the port*; the app then picks a device
//! and calls [`SerialDevice::connect`], which opens it and completes the Rynk
//! handshake ([`Client::connect`]) — the authoritative confirmation.
//!
//! Discovery deliberately never opens a port: opening a CDC port toggles DTR
//! (resetting some MCUs), so only the chosen device is opened, exactly once. The
//! marker is to BLE's service UUID what identifies a device before connecting.

use std::time::Duration;

use embedded_io_adapters::tokio_1::FromTokio;
use rmk_types::protocol::rynk::RYNK_SERIAL_MAGIC;
use rynk::io::{Read, Write};
use rynk::{Client, ConnectError, TransportError};
use tokio_serial::{ClearBuffer, SerialPort as _, SerialPortBuilderExt, SerialPortType, SerialStream};

/// Required by serial APIs; ignored by USB CDC-ACM devices.
const CDC_BAUD_RATE: u32 = 115_200;

/// Per-port handshake timeout used by serial discovery/connect helpers.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// Open CDC-ACM serial port.
pub struct SerialTransport {
    stream: FromTokio<SerialStream>,
    path: String,
}

impl SerialTransport {
    /// Get all valid Rynk USB CDC port candidates.
    fn rynk_serial_ports() -> Result<Vec<String>, TransportError> {
        let ports = tokio_serial::available_ports().map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(ports
            .into_iter()
            .filter(|p| match &p.port_type {
                SerialPortType::UsbPort(info) => info
                    .serial_number
                    .as_deref()
                    .is_some_and(|s| s.to_ascii_lowercase().contains(RYNK_SERIAL_MAGIC)),
                _ => false,
            })
            .map(|p| p.port_name)
            .collect())
    }

    /// Open a specific serial port path.
    async fn open(path: &str) -> Result<Self, TransportError> {
        let stream = tokio_serial::new(path, CDC_BAUD_RATE)
            .open_native_async()
            .map_err(|e| TransportError::Io(format!("open {path}: {e}")))?;
        // Best-effort cleanup of stale bytes from an old session.
        let _ = stream.clear(ClearBuffer::Input);
        Ok(Self {
            stream: FromTokio::new(stream),
            path: path.to_string(),
        })
    }

    /// The port path this transport is connected to.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl rynk::io::ErrorType for SerialTransport {
    type Error = std::io::Error;
}

impl Read for SerialTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.stream.read(buf).await
    }
}

impl Write for SerialTransport {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.stream.write(buf).await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A Rynk keyboard found by [`discover`], for building a device picker. Carries
/// only the port path — version and capabilities are read by
/// [`connect`](Self::connect), which is the first time the port is opened.
pub struct SerialDevice {
    pub path: String,
}

impl SerialDevice {
    /// Open the port and complete the Rynk handshake. A device unplugged since
    /// discovery surfaces as a normal [`ConnectError`].
    pub async fn connect(&self) -> Result<Client<SerialTransport>, ConnectError> {
        connect_transport(SerialTransport::open(&self.path).await?).await
    }
}

/// Enumerate marked serial ports off the reactor — `available_ports` walks OS
/// device registries (IOKit/SetupAPI/sysfs) and can block for tens of ms.
async fn candidates_blocking() -> Result<Vec<String>, TransportError> {
    tokio::task::spawn_blocking(SerialTransport::rynk_serial_ports)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?
}

/// List the marked USB CDC ports — one [`SerialDevice`] per Rynk keyboard,
/// recognized by [`RYNK_SERIAL_MAGIC`] without opening any port. The picker
/// flow: `discover` → choose a [`SerialDevice`] → [`SerialDevice::connect`].
pub async fn discover() -> Result<Vec<SerialDevice>, TransportError> {
    Ok(candidates_blocking()
        .await?
        .into_iter()
        .map(|path| SerialDevice { path })
        .collect())
}

// Duplicated in `rynk-ble`, not shared: `rynk` is runtime-free (builds for
// `wasm32`), so the tokio timeout wrapper can't live there.
async fn connect_transport(transport: SerialTransport) -> Result<Client<SerialTransport>, ConnectError> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, Client::connect(transport))
        .await
        .map_err(|_| ConnectError::Transport(TransportError::DeviceNotFound("handshake timed out".into())))?
}

// PTY-backed tests: `SerialStream::pair()` is a real serial byte stream with no
// hardware, so transport, timeout, and probe all run against a scripted peer.
// Unix only, like the pair.
#[cfg(all(test, unix))]
mod tests {
    use std::os::fd::AsRawFd;

    use rmk_types::protocol::rynk::{
        Cmd, DeviceCapabilities, ProtocolVersion, RYNK_HEADER_SIZE, RynkError, RynkHeader, RynkMessage,
    };
    use serde::Serialize;
    use tokio::io::AsyncReadExt as _;

    use super::*;

    /// A raw-mode PTY pair. `pair()` leaves the pty's line discipline as-is,
    /// so without `cfmakeraw` reads would be line-buffered and echoed.
    fn pty_pair() -> (SerialStream, SerialStream) {
        let (master, slave) = SerialStream::pair().unwrap();
        for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
            unsafe {
                let mut t: libc::termios = std::mem::zeroed();
                assert_eq!(libc::tcgetattr(fd, &mut t), 0);
                libc::cfmakeraw(&mut t);
                assert_eq!(libc::tcsetattr(fd, libc::TCSANOW, &t), 0);
            }
        }
        (master, slave)
    }

    fn transport(stream: SerialStream) -> SerialTransport {
        SerialTransport {
            stream: FromTokio::new(stream),
            path: "<pty>".into(),
        }
    }

    /// Header + postcard payload, framed as the firmware sends it.
    fn frame<T: Serialize>(cmd: Cmd, seq: u8, value: &T) -> Vec<u8> {
        let mut buf = vec![0u8; 1024];
        let len = RynkMessage::build(&mut buf, cmd, seq, value).unwrap().frame_len();
        buf.truncate(len);
        buf
    }

    fn caps() -> DeviceCapabilities {
        DeviceCapabilities {
            num_layers: 4,
            num_rows: 6,
            num_cols: 14,
            num_encoders: 0,
            max_combos: 8,
            max_combo_keys: 4,
            max_macros: 8,
            macro_space_size: 1024,
            max_morse: 4,
            max_patterns_per_key: 4,
            max_forks: 4,
            storage_enabled: true,
            lighting_enabled: false,
            is_split: false,
            num_split_peripherals: 0,
            ble_enabled: false,
            num_ble_profiles: 0,
            max_payload_size: 256,
            max_bulk_keys: 0,
            macro_chunk_size: 64,
            bulk_transfer_supported: false,
        }
    }

    /// Read one request frame off the peer end; returns its cmd + seq.
    async fn read_request(peer: &mut SerialStream) -> (Cmd, u8) {
        let mut bytes = [0u8; RYNK_HEADER_SIZE];
        peer.read_exact(&mut bytes).await.unwrap();
        let header = RynkHeader::parse(&bytes);
        let mut payload = vec![0u8; header.payload_len as usize];
        if !payload.is_empty() {
            peer.read_exact(&mut payload).await.unwrap();
        }
        (header.cmd, header.seq)
    }

    /// Script a Rynk firmware on `peer`: answer the GetVersion/GetCapabilities
    /// handshake with `version`, then keep the line open until dropped.
    fn scripted_firmware(mut peer: SerialStream, version: ProtocolVersion) -> tokio::task::JoinHandle<SerialStream> {
        tokio::spawn(async move {
            let (cmd, seq) = read_request(&mut peer).await;
            assert_eq!(cmd, Cmd::GetVersion);
            tokio::io::AsyncWriteExt::write_all(&mut peer, &frame(cmd, seq, &Ok::<_, RynkError>(version)))
                .await
                .unwrap();
            // A mismatched major never gets the capabilities request.
            if version.major == ProtocolVersion::CURRENT.major {
                let (cmd, seq) = read_request(&mut peer).await;
                assert_eq!(cmd, Cmd::GetCapabilities);
                tokio::io::AsyncWriteExt::write_all(&mut peer, &frame(cmd, seq, &Ok::<_, RynkError>(caps())))
                    .await
                    .unwrap();
            }
            peer
        })
    }

    #[tokio::test]
    async fn transport_round_trips_bytes() {
        let (mut peer, ours) = pty_pair();
        let mut t = transport(ours);

        t.write_all(&[1, 2, 3]).await.unwrap();
        let mut buf = [0u8; 3];
        peer.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, [1, 2, 3]);

        tokio::io::AsyncWriteExt::write_all(&mut peer, &[9, 8]).await.unwrap();
        let mut got = [0u8; 2];
        t.read_exact(&mut got).await.unwrap();
        assert_eq!(got, [9, 8]);
    }

    #[test]
    fn rynk_serial_ports_enumerates() {
        // Returns marked ports the host has (maybe none in CI); must not error.
        SerialTransport::rynk_serial_ports().expect("enumeration must not error");
    }

    #[tokio::test]
    async fn connect_handshakes_against_scripted_peer() {
        let (peer, ours) = pty_pair();
        let device = scripted_firmware(peer, ProtocolVersion::CURRENT);

        let client = connect_transport(transport(ours)).await.unwrap();
        assert_eq!(client.protocol_version(), ProtocolVersion::CURRENT);
        assert_eq!(client.capabilities().num_cols, 14);
        device.await.unwrap();
    }

    #[tokio::test]
    async fn connect_times_out_on_silent_peer() {
        // The peer end stays open but never answers; runs ~HANDSHAKE_TIMEOUT.
        let (_peer, ours) = pty_pair();
        let err = connect_transport(transport(ours)).await.err().expect("must time out");
        assert!(
            matches!(&err, ConnectError::Transport(TransportError::DeviceNotFound(m)) if m.contains("timed out")),
            "expected handshake timeout, got {err:?}"
        );
    }

    /// A silent port is dropped from discovery while a responsive one is listed —
    /// the everyday case: a keyboard's silent logger CDC port alongside its
    /// responsive Rynk port (the logger interface can carry the marker too, since
    /// it shares the serial). Linux-only: macOS cannot open a pty through the
    /// serialport builder (the baud ioctl returns ENOTTY), and the probe opens
    /// ports by path.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn probe_lists_responsive_port_and_drops_silent_one() {
        let (_silent_peer, silent) = pty_pair();
        let (good_peer, good) = pty_pair();
        let silent_path = silent.name().expect("pty has a path");
        let good_path = good.name().expect("pty has a path");
        // Keep `silent`/`good` alive: the probe opens a second fd on each
        // path, and macOS refuses to re-open a fully closed pty slave.
        let device = scripted_firmware(good_peer, ProtocolVersion::CURRENT);

        assert!(
            probe_device(silent_path).await.is_none(),
            "a silent port must not be listed as connectable"
        );
        let dev = probe_device(good_path).await.expect("responsive port is connectable");
        assert_eq!(dev.version, ProtocolVersion::CURRENT);
        device.await.unwrap();
    }

    /// Connectable-only discovery drops a real-but-incompatible device: a
    /// wrong-major firmware answers the version probe but must not be listed.
    /// Linux-only, same pty limitation as above.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn probe_drops_incompatible_version() {
        let (old_peer, old) = pty_pair();
        let old_path = old.name().expect("pty has a path");
        let newer_major = ProtocolVersion {
            major: ProtocolVersion::CURRENT.major + 1,
            minor: 0,
        };
        let device = scripted_firmware(old_peer, newer_major);

        // Keep `old` alive: the probe opens a second fd on the same pty path.
        assert!(
            probe_device(old_path).await.is_none(),
            "a wrong-major device must be excluded from discovery"
        );
        device.await.unwrap();
    }
}
