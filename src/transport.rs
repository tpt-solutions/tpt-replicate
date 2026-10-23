//! Transports: the byte-frame links the engine runs over.
//!
//! The engine needs three things from a link: send a frame, poll for a received frame without
//! blocking, and report whether the link is up. Everything else is up to the implementation.

use alloc::collections::VecDeque;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;

use crate::error::TransportError;

/// A non-blocking, frame-oriented link to one peer.
pub trait Transport {
    /// Sends one frame. Returns [`TransportError::Disconnected`] if the link is down.
    fn send(&mut self, frame: &[u8]) -> Result<(), TransportError>;
    /// Returns the next received frame, or `None` if there is none right now.
    fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError>;
    /// Whether the link is currently up.
    fn is_connected(&self) -> bool;
    /// Whether the link is expected to go quiet for long stretches, such as a courier carrying
    /// files between sites. The engine gives such links a longer liveness timeout, so a quiet
    /// period does not look like a dead peer.
    fn is_delay_tolerant(&self) -> bool {
        false
    }
}

/// Fault injection for [`MemoryTransport`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Faults {
    /// Chance in thousandths that a frame is silently dropped.
    pub loss_permille: u32,
    /// Chance in thousandths that a frame is delivered twice.
    pub duplicate_permille: u32,
    /// Whether frames can overtake earlier ones.
    pub reorder: bool,
}

#[derive(Debug)]
struct Wire {
    up: bool,
    faults: Faults,
    rng: u64,
    /// `inbox[side]` holds frames waiting to be read by that side.
    inbox: [VecDeque<Vec<u8>>; 2],
}

impl Wire {
    fn next_random(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn roll(&mut self, permille: u32) -> bool {
        permille != 0 && self.next_random() % 1000 < u64::from(permille)
    }
}

/// In-memory link pair with optional loss, duplication and reordering. Deterministic for a given
/// seed. Clones share the same link, so one clone can partition the other end.
#[derive(Clone, Debug)]
pub struct MemoryTransport {
    wire: Rc<RefCell<Wire>>,
    side: usize,
}

impl MemoryTransport {
    /// Creates both ends of a link.
    pub fn pair(seed: u64, faults: Faults) -> (Self, Self) {
        let wire = Rc::new(RefCell::new(Wire {
            up: true,
            faults,
            rng: seed | 1,
            inbox: [VecDeque::new(), VecDeque::new()],
        }));
        (
            Self {
                wire: wire.clone(),
                side: 0,
            },
            Self { wire, side: 1 },
        )
    }

    /// Brings the link up or down. Taking it down discards frames still in flight, as a real
    /// partition would.
    pub fn set_up(&self, up: bool) {
        let mut wire = self.wire.borrow_mut();
        wire.up = up;
        if !up {
            wire.inbox[0].clear();
            wire.inbox[1].clear();
        }
    }
}

impl Transport for MemoryTransport {
    fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        let mut wire = self.wire.borrow_mut();
        if !wire.up {
            return Err(TransportError::Disconnected);
        }
        let loss = wire.faults.loss_permille;
        if wire.roll(loss) {
            return Ok(());
        }
        let duplicate = wire.faults.duplicate_permille;
        let copies = if wire.roll(duplicate) { 2 } else { 1 };
        let destination = 1 - self.side;
        for _ in 0..copies {
            let len = wire.inbox[destination].len();
            let index = if wire.faults.reorder && len > 0 {
                (wire.next_random() % (len as u64 + 1)) as usize
            } else {
                len
            };
            wire.inbox[destination].insert(index, frame.to_vec());
        }
        Ok(())
    }

    fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        let mut wire = self.wire.borrow_mut();
        if !wire.up {
            return Ok(None);
        }
        Ok(wire.inbox[self.side].pop_front())
    }

    fn is_connected(&self) -> bool {
        self.wire.borrow().up
    }
}

/// Adapter from a message-oriented WebRTC data channel to [`Transport`].
#[cfg(feature = "webrtc")]
#[derive(Debug)]
pub struct WebRtcTransport<D: crate::stubs::DataChannel> {
    channel: D,
}

#[cfg(feature = "webrtc")]
impl<D: crate::stubs::DataChannel> WebRtcTransport<D> {
    /// Wraps an open or opening data channel.
    pub fn new(channel: D) -> Self {
        Self { channel }
    }
}

#[cfg(feature = "webrtc")]
impl<D: crate::stubs::DataChannel> Transport for WebRtcTransport<D> {
    fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        self.channel.send(frame)
    }

    fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        Ok(self.channel.try_recv())
    }

    fn is_connected(&self) -> bool {
        self.channel.is_open()
    }
}

#[cfg(feature = "std")]
mod std_transports {
    use alloc::format;
    use alloc::string::ToString;
    use alloc::vec::Vec;
    use std::fs;
    use std::io::{self, Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::Transport;
    use crate::codec::MAX_FRAME;
    use crate::error::TransportError;

    /// Largest number of bytes queued for sending on a TCP link before `send` reports
    /// [`TransportError::Backpressure`].
    pub const MAX_OUTBOUND_BYTES: usize = 4 << 20;

    /// Largest number of unread bytes buffered from a TCP link. Reading stops at this bound, so
    /// the peer is throttled by the socket instead of growing this buffer.
    const MAX_RX_BUFFER: usize = MAX_FRAME + 4 + 4096;

    /// How long [`TcpTransport::connect`] waits for each address.
    pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    fn io_error(error: io::Error) -> TransportError {
        TransportError::Io(error.to_string())
    }

    /// Lists `.frame` files in `dir` as `(sequence, path)`, sorted by sequence.
    fn list_frames(dir: &Path) -> Result<Vec<(u64, PathBuf)>, TransportError> {
        let mut frames = Vec::new();
        for entry in fs::read_dir(dir).map_err(io_error)? {
            let path = entry.map_err(io_error)?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("frame") {
                continue;
            }
            let seq = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok());
            if let Some(seq) = seq {
                frames.push((seq, path));
            }
        }
        frames.sort_by_key(|(seq, _)| *seq);
        Ok(frames)
    }

    /// Delay-tolerant link through a directory pair. Each frame is one file. Sends are written to
    /// `outbox` atomically (temp file, synced, then rename). Receives read the oldest file in
    /// `inbox` and delete it. Frames wait on disk until a courier moves them. The other side's
    /// `outbox` is this side's `inbox`.
    ///
    /// The sequence number is taken from the outbox on each send, skipping names that already
    /// exist. A restart therefore cannot overwrite a frame the courier has not collected yet.
    #[derive(Debug)]
    pub struct FileTransport {
        outbox: PathBuf,
        inbox: PathBuf,
        next_seq: u64,
    }

    impl FileTransport {
        /// Opens the link, creating both directories if needed.
        pub fn new(
            outbox: impl Into<PathBuf>,
            inbox: impl Into<PathBuf>,
        ) -> Result<Self, TransportError> {
            let outbox = outbox.into();
            let inbox = inbox.into();
            fs::create_dir_all(&outbox).map_err(io_error)?;
            fs::create_dir_all(&inbox).map_err(io_error)?;
            let next_seq = list_frames(&outbox)?.last().map_or(0, |(seq, _)| seq + 1);
            Ok(Self {
                outbox,
                inbox,
                next_seq,
            })
        }
    }

    impl Transport for FileTransport {
        fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            if frame.len() > MAX_FRAME {
                return Err(TransportError::FrameTooLarge);
            }
            let mut dst = self.outbox.join(format!("{:020}.frame", self.next_seq));
            while dst.exists() {
                self.next_seq += 1;
                dst = self.outbox.join(format!("{:020}.frame", self.next_seq));
            }
            let tmp = self.outbox.join(format!("{:020}.tmp", self.next_seq));
            let mut file = fs::File::create(&tmp).map_err(io_error)?;
            file.write_all(frame).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            drop(file);
            fs::rename(&tmp, &dst).map_err(io_error)?;
            self.next_seq += 1;
            Ok(())
        }

        fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            for (_, path) in list_frames(&self.inbox)? {
                // A file larger than any legal frame is junk. Remove it and keep looking.
                let len = fs::metadata(&path).map_err(io_error)?.len();
                if len > MAX_FRAME as u64 {
                    fs::remove_file(&path).map_err(io_error)?;
                    continue;
                }
                let bytes = fs::read(&path).map_err(io_error)?;
                fs::remove_file(&path).map_err(io_error)?;
                return Ok(Some(bytes));
            }
            Ok(None)
        }

        fn is_connected(&self) -> bool {
            self.inbox.is_dir() && self.outbox.is_dir()
        }

        fn is_delay_tolerant(&self) -> bool {
            true
        }
    }

    /// Length-prefixed frames over a TCP stream. The socket is non-blocking and never spins.
    ///
    /// `send` queues the frame and writes as much as the socket accepts. Whatever is left is
    /// written by later calls to `send` or `poll_recv`, so `poll` is what drives the link forward.
    #[derive(Debug)]
    pub struct TcpTransport {
        stream: TcpStream,
        rx: Vec<u8>,
        tx: Vec<u8>,
        closed: bool,
    }

    impl TcpTransport {
        /// Wraps an established stream.
        pub fn from_stream(stream: TcpStream) -> Result<Self, TransportError> {
            stream.set_nonblocking(true).map_err(io_error)?;
            stream.set_nodelay(true).map_err(io_error)?;
            Ok(Self {
                stream,
                rx: Vec::new(),
                tx: Vec::new(),
                closed: false,
            })
        }

        /// Connects to the first address in `addr` that accepts within [`CONNECT_TIMEOUT`].
        pub fn connect(addr: impl ToSocketAddrs) -> Result<Self, TransportError> {
            let mut last = None;
            for candidate in addr.to_socket_addrs().map_err(io_error)? {
                match TcpStream::connect_timeout(&candidate, CONNECT_TIMEOUT) {
                    Ok(stream) => return Self::from_stream(stream),
                    Err(error) => last = Some(error),
                }
            }
            Err(match last {
                Some(error) => io_error(error),
                None => TransportError::Io("no address to connect to".to_string()),
            })
        }

        /// Writes queued bytes until the socket would block.
        fn flush(&mut self) -> Result<(), TransportError> {
            while !self.tx.is_empty() {
                match self.stream.write(&self.tx) {
                    Ok(0) => return Err(self.hang_up()),
                    Ok(n) => {
                        self.tx.drain(..n);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => return Err(self.hang_up()),
                }
            }
            Ok(())
        }

        fn hang_up(&mut self) -> TransportError {
            self.closed = true;
            TransportError::Disconnected
        }
    }

    impl Transport for TcpTransport {
        fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            if self.closed {
                return Err(TransportError::Disconnected);
            }
            if frame.len() > MAX_FRAME {
                return Err(TransportError::FrameTooLarge);
            }
            if self.tx.len() + 4 + frame.len() > MAX_OUTBOUND_BYTES {
                // Try to make room first. The caller keeps the frame and can retry later.
                self.flush()?;
                if self.tx.len() + 4 + frame.len() > MAX_OUTBOUND_BYTES {
                    return Err(TransportError::Backpressure);
                }
            }
            self.tx
                .extend_from_slice(&(frame.len() as u32).to_be_bytes());
            self.tx.extend_from_slice(frame);
            self.flush()
        }

        fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            self.flush()?;
            let mut buf = [0u8; 4096];
            while !self.closed && self.rx.len() < MAX_RX_BUFFER {
                match self.stream.read(&mut buf) {
                    Ok(0) => self.closed = true,
                    Ok(n) => self.rx.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => self.closed = true,
                }
            }
            if self.rx.len() >= 4 {
                let mut header = [0u8; 4];
                header.copy_from_slice(&self.rx[..4]);
                let len = u32::from_be_bytes(header) as usize;
                if len > MAX_FRAME {
                    self.closed = true;
                    return Err(TransportError::FrameTooLarge);
                }
                if self.rx.len() >= 4 + len {
                    let frame = self.rx[4..4 + len].to_vec();
                    self.rx.drain(..4 + len);
                    return Ok(Some(frame));
                }
            }
            if self.closed {
                Err(TransportError::Disconnected)
            } else {
                Ok(None)
            }
        }

        fn is_connected(&self) -> bool {
            !self.closed
        }
    }
}

#[cfg(feature = "std")]
pub use std_transports::{FileTransport, TcpTransport, CONNECT_TIMEOUT, MAX_OUTBOUND_BYTES};

#[cfg(feature = "std")]
mod reconnect;

#[cfg(feature = "std")]
pub use reconnect::{ReconnectingTcp, TcpAcceptor};
