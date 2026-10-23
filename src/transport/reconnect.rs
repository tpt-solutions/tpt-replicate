//! TCP links that repair themselves, and a listener that accepts them.
//!
//! The engine does not own connections. The application decides which peer each accepted
//! connection belongs to, and passes it to `add_peer`. A [`ReconnectingTcp`] can be given to
//! `add_peer` on the dialing side. It dials on its own, with exponential backoff, whenever the
//! link is down.

use alloc::vec::Vec;
use std::cell::{Cell, RefCell};
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::string::{String, ToString};
use std::time::{Duration, Instant};

use super::{TcpTransport, Transport};
use crate::error::TransportError;

/// First delay after a failed or lost connection.
const BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Longest delay between attempts.
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Connect timeout for each attempt. Shorter than the initial connect timeout, so a dead address
/// does not hold up `poll` for long.
const RECONNECT_TIMEOUT: Duration = Duration::from_millis(500);

fn io_error(error: std::io::Error) -> TransportError {
    TransportError::Io(error.to_string())
}

/// Accepts incoming TCP connections without blocking.
#[derive(Debug)]
pub struct TcpAcceptor {
    listener: TcpListener,
}

impl TcpAcceptor {
    /// Starts listening on `addr`. Use port 0 to let the OS pick one, then read it back with
    /// [`TcpAcceptor::local_addr`].
    pub fn bind(addr: impl ToSocketAddrs) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr).map_err(io_error)?;
        listener.set_nonblocking(true).map_err(io_error)?;
        Ok(Self { listener })
    }

    /// The address actually bound.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.listener.local_addr().map_err(io_error)
    }

    /// Returns the next waiting connection with its remote address, or `None` if none is waiting.
    /// Call it from the same loop that polls the replica.
    pub fn accept(&mut self) -> Result<Option<(SocketAddr, TcpTransport)>, TransportError> {
        match self.listener.accept() {
            Ok((stream, remote)) => Ok(Some((remote, TcpTransport::from_stream(stream)?))),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(io_error(error)),
        }
    }
}

/// Dials `addr` and repairs the link when it drops.
///
/// While the link is down, each attempt waits for an exponential backoff, from 100 ms up to 30 s.
/// The backoff resets once a connection succeeds. Each attempt blocks for at most 500 ms.
///
/// [`Transport::is_connected`] makes an attempt when one is due, so the engine's own polling drives
/// reconnection. Backoff uses the system clock, not the replica's injected clock, so it works the
/// same under simulated time.
#[derive(Debug)]
pub struct ReconnectingTcp {
    addr: String,
    link: RefCell<Option<TcpTransport>>,
    backoff: Cell<Duration>,
    next_attempt: Cell<Instant>,
}

impl ReconnectingTcp {
    /// Creates a link to `addr`. The first connection is attempted on the next poll.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            link: RefCell::new(None),
            backoff: Cell::new(BACKOFF_MIN),
            next_attempt: Cell::new(Instant::now()),
        }
    }

    /// Connects if no link is up and the backoff has passed. Returns whether a link is up.
    fn ensure(&self) -> bool {
        if self.link.borrow().is_some() {
            return true;
        }
        if Instant::now() < self.next_attempt.get() {
            return false;
        }
        match connect_quick(&self.addr) {
            Ok(link) => {
                *self.link.borrow_mut() = Some(link);
                self.backoff.set(BACKOFF_MIN);
                true
            }
            Err(_) => {
                self.lose();
                false
            }
        }
    }

    /// Drops the link and schedules the next attempt.
    fn lose(&self) {
        *self.link.borrow_mut() = None;
        let delay = self.backoff.get();
        self.next_attempt.set(Instant::now() + delay);
        self.backoff.set((delay * 2).min(BACKOFF_MAX));
    }
}

fn connect_quick(addr: &str) -> Result<TcpTransport, TransportError> {
    let mut last = TransportError::Io("no address to connect to".to_string());
    for candidate in addr.to_socket_addrs().map_err(io_error)? {
        match TcpStream::connect_timeout(&candidate, RECONNECT_TIMEOUT) {
            Ok(stream) => return TcpTransport::from_stream(stream),
            Err(error) => last = io_error(error),
        }
    }
    Err(last)
}

impl Transport for ReconnectingTcp {
    fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        if !self.ensure() {
            return Err(TransportError::Disconnected);
        }
        let result = match self.link.get_mut().as_mut() {
            Some(link) => link.send(frame),
            None => return Err(TransportError::Disconnected),
        };
        if matches!(
            result,
            Err(TransportError::Disconnected | TransportError::Io(_))
        ) {
            self.lose();
        }
        result
    }

    fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        if !self.ensure() {
            return Ok(None);
        }
        let result = match self.link.get_mut().as_mut() {
            Some(link) => link.poll_recv(),
            None => return Ok(None),
        };
        if result.is_err() {
            self.lose();
        }
        result
    }

    /// Reports whether a link is up. Makes a reconnection attempt if one is due.
    fn is_connected(&self) -> bool {
        self.ensure()
    }
}
