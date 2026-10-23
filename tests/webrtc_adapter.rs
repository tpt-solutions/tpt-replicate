//! The WebRTC adapter, against an in-process loopback data channel.

#![cfg(feature = "webrtc")]

mod common;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use common::*;
use tpt_replicate::clock::ManualClock;
use tpt_replicate::error::TransportError;
use tpt_replicate::stubs::{DataChannel, State};
use tpt_replicate::transport::WebRtcTransport;

type Queue = Rc<RefCell<VecDeque<Vec<u8>>>>;

/// One end of an in-process data channel pair.
struct Loopback {
    tx: Queue,
    rx: Queue,
}

impl DataChannel for Loopback {
    fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.tx.borrow_mut().push_back(data.to_vec());
        Ok(())
    }

    fn try_recv(&mut self) -> Option<Vec<u8>> {
        self.rx.borrow_mut().pop_front()
    }

    fn is_open(&self) -> bool {
        true
    }
}

#[test]
fn replicas_sync_over_data_channel_adapter() {
    let a_to_b: Queue = Rc::default();
    let b_to_a: Queue = Rc::default();
    let a_end = Loopback {
        tx: a_to_b.clone(),
        rx: b_to_a.clone(),
    };
    let b_end = Loopback {
        tx: b_to_a,
        rx: a_to_b,
    };

    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    a.add_peer(b.actor(), Box::new(WebRtcTransport::new(a_end)));
    b.add_peer(a.actor(), Box::new(WebRtcTransport::new(b_end)));

    for i in 0..5 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    settle(&mut [&mut a, &mut b], 6);
    assert_eq!(b.log().len(), 5);
    assert_eq!(a.state().state_hash(), b.state().state_hash());
}
