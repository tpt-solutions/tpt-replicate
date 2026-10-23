//! Replicas syncing over a real tpt-webrtc data channel (loopback UDP), behind `webrtc-app`.

#![cfg(feature = "webrtc-app")]

mod common;

use std::net::IpAddr;
use std::thread::sleep;
use std::time::Duration;

use common::*;
use tpt_replicate::clock::ManualClock;
use tpt_replicate::stubs::State;
use tpt_replicate::webrtc_app::WebRtcLink;
use tpt_webrtc_app::PeerConnectionConfig;

fn loopback() -> PeerConnectionConfig {
    PeerConnectionConfig {
        local_addresses: vec![IpAddr::from([127, 0, 0, 1])],
        ..PeerConnectionConfig::default()
    }
}

#[test]
fn replicas_sync_over_a_webrtc_data_channel() {
    let (link_a, link_b) = WebRtcLink::loopback_pair(loopback()).expect("negotiate");
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    a.add_peer(b.actor(), Box::new(link_a));
    b.add_peer(a.actor(), Box::new(link_b));

    a.local_op(set("k", "over webrtc")).unwrap();
    for _ in 0..3_000 {
        clock.advance(10);
        a.poll();
        b.poll();
        if b.state().get(b"k").is_some() {
            break;
        }
        sleep(Duration::from_millis(2));
    }
    assert_eq!(b.state().get(b"k"), Some(&b"over webrtc"[..]));
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}
