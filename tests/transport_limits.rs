//! Regression tests for the std transports: bounded TCP sends, connect timeouts, oversized files in
//! a delay-tolerant inbox, and liveness on links that go quiet for a long time.

#![cfg(feature = "std")]

mod common;

use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::*;
use tpt_replicate::clock::ManualClock;
use tpt_replicate::codec::MAX_FRAME;
use tpt_replicate::error::TransportError;
use tpt_replicate::transport::{FileTransport, TcpTransport, Transport};
use tpt_replicate::{Event, ReplicaConfig};

fn scratch(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("tpt-replicate-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    root
}

#[test]
fn tcp_send_reports_backpressure_instead_of_spinning() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).unwrap();
    // Keep the server end open but never read from it, so the client's buffers fill up.
    let (_server, _) = listener.accept().unwrap();
    let mut link = TcpTransport::from_stream(client).unwrap();

    let frame = vec![7u8; 1 << 20];
    let start = Instant::now();
    let mut sent_before_full = 0usize;
    let outcome = loop {
        match link.send(&frame) {
            Ok(()) => sent_before_full += 1,
            Err(error) => break error,
        }
        assert!(sent_before_full < 256, "buffer never reported as full");
    };
    // Before the fix, send blocked in a yield loop until the socket drained, which never happens
    // here. Now it returns promptly and the link stays up.
    assert_eq!(outcome, TransportError::Backpressure);
    assert!(start.elapsed() < Duration::from_secs(10));
    assert!(link.is_connected());
}

#[test]
fn tcp_connect_fails_quickly_when_nothing_listens() {
    // Port 1 is almost never open. The point is that connect returns instead of hanging.
    let start = Instant::now();
    assert!(TcpTransport::connect("127.0.0.1:1").is_err());
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[test]
fn file_transport_discards_oversized_files_and_reads_on() {
    let root = scratch("oversized");
    let outbox = root.join("out");
    let inbox = root.join("in");
    let mut link = FileTransport::new(&outbox, &inbox).unwrap();

    // A file bigger than any legal frame. It is removed, and the valid frame after it is read.
    let junk = inbox.join(format!("{:020}.frame", 0));
    fs::File::create(&junk)
        .unwrap()
        .set_len(MAX_FRAME as u64 + 1)
        .unwrap();
    fs::write(inbox.join(format!("{:020}.frame", 1)), b"ok").unwrap();

    assert_eq!(link.poll_recv().unwrap(), Some(b"ok".to_vec()));
    assert!(!junk.exists());
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn file_transport_rejects_frames_over_the_limit_on_send() {
    let root = scratch("send-limit");
    let mut link = FileTransport::new(root.join("out"), root.join("in")).unwrap();
    let too_big = vec![0u8; MAX_FRAME + 1];
    assert_eq!(link.send(&too_big), Err(TransportError::FrameTooLarge));
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn delay_tolerant_link_survives_a_quiet_day_that_would_kill_a_live_one() {
    let root = scratch("dtn-quiet");
    let a_to_b = root.join("a2b");
    let b_to_a = root.join("b2a");
    let clock = ManualClock::new(1_000);
    let config = ReplicaConfig {
        // Two hours of silence is far beyond this timeout. A courier link is allowed to be quiet
        // for that long. A live link is not.
        peer_timeout_ms: 60_000,
        ..ReplicaConfig::default()
    };
    let mut a = replica(1, &clock).with_config(config.clone());
    let mut b = replica(2, &clock).with_config(config);
    a.add_peer(
        b.actor(),
        Box::new(FileTransport::new(&a_to_b, &b_to_a).unwrap()),
    );
    b.add_peer(
        a.actor(),
        Box::new(FileTransport::new(&b_to_a, &a_to_b).unwrap()),
    );
    a.poll();
    b.poll();
    a.drain_events();

    clock.advance(2 * 60 * 60 * 1_000);
    a.poll();
    assert!(!a
        .drain_events()
        .contains(&Event::PeerDisconnected(b.actor())));
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn reconnecting_link_repairs_itself_after_the_far_end_goes_away() {
    use std::thread::sleep;
    use tpt_replicate::transport::{ReconnectingTcp, TcpAcceptor};

    let mut acceptor = TcpAcceptor::bind("127.0.0.1:0").unwrap();
    let addr = acceptor.local_addr().unwrap().to_string();
    // A fixed clock keeps the test about the socket, not about timers.
    let clock = ManualClock::new(1_000);
    let mut a = replica(1, &clock);
    let mut b = replica(2, &clock);
    a.add_peer(b.actor(), Box::new(ReconnectingTcp::new(addr)));
    a.local_op(set("k", "v")).unwrap();

    let first = (0..400)
        .find_map(|_| {
            a.poll();
            let accepted = acceptor.accept().unwrap();
            sleep(Duration::from_millis(5));
            accepted.map(|(_, link)| link)
        })
        .expect("dialing side connects to the listener");
    b.add_peer(a.actor(), Box::new(first));
    for _ in 0..400 {
        a.poll();
        b.poll();
        if b.log().len() == 1 {
            break;
        }
        sleep(Duration::from_millis(5));
    }
    assert_eq!(b.log().len(), 1);

    // The far end drops its connection. The dialing side notices, backs off and dials again.
    b.remove_peer(a.actor());
    a.local_op(set("k2", "v2")).unwrap();
    let second = (0..800)
        .find_map(|_| {
            a.poll();
            let accepted = acceptor.accept().unwrap();
            sleep(Duration::from_millis(10));
            accepted.map(|(_, link)| link)
        })
        .expect("dialing side reconnects after backing off");
    b.add_peer(a.actor(), Box::new(second));
    for _ in 0..400 {
        a.poll();
        b.poll();
        if b.log().len() == 2 {
            break;
        }
        sleep(Duration::from_millis(5));
    }
    assert_eq!(b.log().len(), 2);
    assert_eq!(b.state().get(b"k2"), Some(&b"v2"[..]));
}
