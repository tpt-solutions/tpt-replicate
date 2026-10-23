//! A [`Transport`] over a `tpt-webrtc` data channel, behind the `webrtc-app` feature.
//!
//! The engine is synchronous, and the WebRTC stack is async. [`WebRtcLink`] bridges the two. It owns
//! a single-threaded tokio runtime and a `PeerConnection`. Each engine poll pumps the peer briefly,
//! which moves packets and flushes queued sends. A frame goes out as one binary message on the
//! data channel.
//!
//! Connection setup (SDP, ICE, DTLS) is async and is the application's job, as in the
//! `data_channel_pingpong` example of tpt-webrtc. [`WebRtcLink::from_connected`] takes over an
//! established peer. [`WebRtcLink::loopback_pair`] negotiates two peers in one process, for tests
//! and demos.
//!
//! Call these methods from a thread that is not inside an async runtime. `block_on` panics there.

use alloc::vec::Vec;
use core::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tpt_webrtc_app::{
    DataChannelConfig, DataChannelMessage, PeerConnection, PeerConnectionConfig,
    PeerConnectionState,
};

use crate::error::TransportError;
use crate::transport::Transport;

/// How long each pump of the peer runs. Short, so the engine's poll stays responsive.
const PUMP: Duration = Duration::from_millis(1);

/// The data channel's label. Both ends must create the channel with the same label.
const LABEL: &str = "tpt-replicate";

/// A [`Transport`] over one WebRTC data channel.
pub struct WebRtcLink {
    runtime: Runtime,
    peer: PeerConnection,
    channel: u16,
}

impl core::fmt::Debug for WebRtcLink {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WebRtcLink")
            .field("channel", &self.channel)
            .field("state", &self.peer.peer_connection_state())
            .finish()
    }
}

fn io_error(error: impl core::fmt::Debug) -> TransportError {
    TransportError::Io(alloc::format!("{error:?}"))
}

fn runtime() -> Result<Runtime, TransportError> {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io_error)
}

impl WebRtcLink {
    /// Wraps a peer whose connection is already established. `channel` is the id returned by
    /// `create_data_channel`.
    pub fn from_connected(peer: PeerConnection, channel: u16) -> Result<Self, TransportError> {
        Ok(Self {
            runtime: runtime()?,
            peer,
            channel,
        })
    }

    /// Negotiates two peers in this process, over loopback, and returns a link for each end. The
    /// first link is the offerer. Each peer is created with `config`, with local addresses limited
    /// to 127.0.0.1 unless `config` says otherwise.
    pub fn loopback_pair(
        config: PeerConnectionConfig,
    ) -> Result<(WebRtcLink, WebRtcLink), TransportError> {
        let rt = runtime()?;
        let mut a = PeerConnection::new(config.clone()).map_err(io_error)?;
        let mut b = PeerConnection::new(config).map_err(io_error)?;
        let ch_a = a
            .create_data_channel(LABEL, DataChannelConfig::default())
            .map_err(io_error)?;
        let ch_b = b
            .create_data_channel(LABEL, DataChannelConfig::default())
            .map_err(io_error)?;

        rt.block_on(async {
            let offer = a.create_offer().await.map_err(io_error)?;
            a.set_local_description(&offer).map_err(io_error)?;
            b.set_remote_description(&offer).map_err(io_error)?;
            let answer = b.create_answer(&offer).await.map_err(io_error)?;
            b.set_local_description(&answer).map_err(io_error)?;
            a.set_remote_description(&answer).map_err(io_error)?;
            let (ra, rb) = tokio::join!(
                a.connect(Duration::from_secs(30)),
                b.connect(Duration::from_secs(30))
            );
            ra.map_err(io_error)?;
            rb.map_err(io_error)?;
            Ok::<_, TransportError>(())
        })?;

        let link_a = WebRtcLink {
            runtime: rt,
            peer: a,
            channel: ch_a,
        };
        let link_b = WebRtcLink {
            runtime: runtime()?,
            peer: b,
            channel: ch_b,
        };
        Ok((link_a, link_b))
    }

    /// Runs the peer briefly, so queued packets go out and arrived ones are read in.
    fn pump(&mut self) {
        let peer = &mut self.peer;
        self.runtime.block_on(peer.poll(PUMP));
    }
}

impl Transport for WebRtcLink {
    fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        if !self.is_connected() {
            return Err(TransportError::Disconnected);
        }
        self.peer
            .send_data_channel(self.channel, &DataChannelMessage::Binary(frame.to_vec()))
            .map_err(io_error)?;
        self.pump();
        Ok(())
    }

    fn poll_recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        self.pump();
        Ok(match self.peer.recv_data_channel(self.channel) {
            Some(DataChannelMessage::Binary(bytes)) => Some(bytes),
            Some(DataChannelMessage::Text(text)) => Some(text.into_bytes()),
            None => None,
        })
    }

    fn is_connected(&self) -> bool {
        self.peer.peer_connection_state() == PeerConnectionState::Connected
    }
}
