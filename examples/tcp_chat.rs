//! A two-person chat over TCP. Each line you type is written to the replica's log, and the other
//! side prints it once it arrives. Lines typed while the link is down are sent when it returns.
//!
//! One side listens and the other dials:
//!
//! ```text
//! cargo run --example tcp_chat --features std -- serve 127.0.0.1:7000 1 2
//! cargo run --example tcp_chat --features std -- connect 127.0.0.1:7000 2 1
//! ```
//!
//! The arguments are the listen or dial address, your actor id, and the other side's actor id.
//! Start the listener first. The dialer retries with backoff until it connects.

use std::collections::BTreeSet;
use std::io::{self, BufRead};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use tpt_replicate::clock::SystemClock;
use tpt_replicate::prelude::*;
use tpt_replicate::transport::{ReconnectingTcp, TcpAcceptor};

/// Stand-in key for the demo. Both sides must agree on it. Use real keys in production.
const DEMO_SECRET: u64 = 0xC4A7;

const USAGE: &str = "usage: tcp_chat serve <listen-addr> <my-id> <peer-id>\n       \
                     tcp_chat connect <peer-addr> <my-id> <peer-id>";

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err(USAGE.to_string());
    }
    let me = ActorId(args[3].parse().map_err(|_| USAGE.to_string())?);
    let them = ActorId(args[4].parse().map_err(|_| USAGE.to_string())?);

    let mut chat = Replica::builder(me, LwwMap::new())
        .shared_secret(DEMO_SECRET)
        .clock(Box::new(SystemClock))
        .build()
        .map_err(|error| format!("{error:?}"))?;

    let mut acceptor = match args[1].as_str() {
        "serve" => Some(TcpAcceptor::bind(args[2].as_str()).map_err(|e| format!("{e:?}"))?),
        "connect" => {
            chat.add_peer(them, Box::new(ReconnectingTcp::new(args[2].clone())));
            None
        }
        _ => return Err(USAGE.to_string()),
    };

    // Reading stdin on its own thread keeps the poll loop from blocking.
    let (lines_tx, lines_rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        for line in io::stdin().lock().lines().map_while(Result::ok) {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut sent = 0u64;
    let mut printed = BTreeSet::new();
    loop {
        if let Some(listener) = acceptor.as_mut() {
            if let Some((remote, link)) = listener.accept().map_err(|e| format!("{e:?}"))? {
                eprintln!("[accepted connection from {remote}]");
                // A new connection replaces the old link to the same peer.
                chat.add_peer(them, Box::new(link));
            }
        }
        while let Ok(line) = lines_rx.try_recv() {
            sent += 1;
            let op = LwwOp::Set {
                key: format!("{}/{sent:08}", me.0).into_bytes(),
                value: line.into_bytes(),
            };
            chat.local_op(op.to_node()).map_err(|e| format!("{e:?}"))?;
        }

        chat.poll();
        for event in chat.drain_events() {
            match event {
                Event::PeerConnected(_) => eprintln!("[linked]"),
                Event::PeerDisconnected(_) => eprintln!("[link lost, retrying]"),
                _ => {}
            }
        }
        for (key, value) in chat.state().iter() {
            if printed.insert(key.to_vec()) {
                let author = String::from_utf8_lossy(key);
                println!("{author}: {}", String::from_utf8_lossy(value));
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
}
