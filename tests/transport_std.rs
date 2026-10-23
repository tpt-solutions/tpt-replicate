//! Std-only transports: directory-backed delay-tolerant links and TCP.

#![cfg(feature = "std")]

mod common;

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use common::*;
use tpt_replicate::clock::ManualClock;
use tpt_replicate::stubs::State;
use tpt_replicate::transport::{FileTransport, TcpTransport};
use tpt_replicate::PeerState;

#[test]
fn file_transport_syncs_through_directories() {
    let root: PathBuf =
        std::env::temp_dir().join(format!("tpt-replicate-file-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let a_to_b = root.join("a2b");
    let b_to_a = root.join("b2a");

    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    a.add_peer(
        b.actor(),
        Box::new(FileTransport::new(&a_to_b, &b_to_a).unwrap()),
    );
    b.add_peer(
        a.actor(),
        Box::new(FileTransport::new(&b_to_a, &a_to_b).unwrap()),
    );

    for i in 0..5 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    settle(&mut [&mut a, &mut b], 8);
    assert_eq!(b.log().len(), 5);
    assert_eq!(a.state().state_hash(), b.state().state_hash());

    // Frames are consumed on read, so nothing is left behind after convergence.
    let leftover =
        std::fs::read_dir(&a_to_b).unwrap().count() + std::fs::read_dir(&b_to_a).unwrap().count();
    assert_eq!(leftover, 0);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tcp_transport_syncs_over_loopback() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).unwrap();
    let (server, _) = listener.accept().unwrap();

    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    a.add_peer(
        b.actor(),
        Box::new(TcpTransport::from_stream(client).unwrap()),
    );
    b.add_peer(
        a.actor(),
        Box::new(TcpTransport::from_stream(server).unwrap()),
    );

    for i in 0..5 {
        a.local_op(set(&format!("k{i}"), "v")).unwrap();
    }
    for _ in 0..2_000 {
        a.poll();
        b.poll();
        if b.log().len() == 5 && a.peer_state(b.actor()) == Some(PeerState::UpToDate) {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(b.log().len(), 5);
    assert_eq!(a.state().state_hash(), b.state().state_hash());
    assert_eq!(b.peer_state(a.actor()), Some(PeerState::UpToDate));
}
