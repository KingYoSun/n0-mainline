//! Bounds and cancellation of lookups, observed at the nodes a lookup asks.
//!
//! The nodes are plain UDP sockets that answer `get_peers` the way each test
//! needs, so the counts do not depend on how a real network converges.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

use crate::actor::ActorMessage;
use crate::common::{
    AnnouncePeerRequestArguments, GetPeersResponseArguments, Message, MessageType,
    NoValuesResponseArguments, PutRequestSpecific, RequestSpecific, RequestTypeSpecific,
    ResponseSpecific,
};
use crate::{Dht, Id, Node, Testnet};

/// The peers and the closer nodes a fake node answers for an info hash.
type Answer = Arc<dyn Fn(Id) -> (Vec<SocketAddrV4>, Vec<Node>) + Send + Sync>;

/// A UDP socket that counts the `get_peers` requests it receives.
struct FakeNode {
    addr: SocketAddrV4,
    requests: Arc<AtomicUsize>,
}

impl FakeNode {
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

async fn bind() -> (Arc<UdpSocket>, SocketAddrV4) {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let SocketAddr::V4(addr) = socket.local_addr().unwrap() else {
        unreachable!("bound to an IPv4 address")
    };
    (Arc::new(socket), addr)
}

/// Serves `get_peers` on `socket`, answering after `delay`, or never without an `answer`.
fn serve(
    socket: Arc<UdpSocket>,
    addr: SocketAddrV4,
    id: Id,
    delay: Duration,
    answer: Option<Answer>,
) -> FakeNode {
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    tokio::spawn(async move {
        let mut buf = [0; 2048];
        loop {
            // Windows reports an ICMP port unreachable as a receive error.
            let Ok((len, SocketAddr::V4(from))) = socket.recv_from(&mut buf).await else {
                continue;
            };
            let Ok(message) = Message::from_bytes(&buf[..len]) else {
                continue;
            };
            let MessageType::Request(RequestSpecific {
                request_type: RequestTypeSpecific::GetPeers(args),
                ..
            }) = message.message_type
            else {
                continue;
            };
            counted.fetch_add(1, Ordering::SeqCst);
            let Some(answer) = &answer else {
                continue;
            };
            let (values, nodes) = answer(args.info_hash);
            let token: Box<[u8]> = Box::new([0; 4]);
            let nodes = Some(nodes.into_boxed_slice());
            let response = if values.is_empty() {
                ResponseSpecific::NoValues(NoValuesResponseArguments {
                    responder_id: id,
                    token,
                    nodes,
                })
            } else {
                ResponseSpecific::GetPeers(GetPeersResponseArguments {
                    responder_id: id,
                    token,
                    values,
                    nodes,
                })
            };
            let bytes = Message {
                transaction_id: message.transaction_id,
                version: None,
                requester_ip: Some(from),
                message_type: MessageType::Response(response),
                read_only: false,
            }
            .to_bytes()
            .unwrap();
            let socket = socket.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = socket.send_to(&bytes, from).await;
            });
        }
    });
    FakeNode { addr, requests }
}

/// Nodes that each know only the next one, which is closer to `target`, so a
/// lookup takes one hop after another. With `with_peers` each answers a peer.
async fn chain(target: Id, hops: usize, delay: Duration, with_peers: bool) -> Vec<FakeNode> {
    let mut sockets = Vec::new();
    for _ in 0..hops {
        sockets.push(bind().await);
    }
    let addrs: Vec<SocketAddrV4> = sockets.iter().map(|(_, addr)| *addr).collect();
    sockets
        .into_iter()
        .enumerate()
        .map(|(hop, (socket, addr))| {
            let next = addrs
                .get(hop + 1)
                .map(|next| Node::new(closer(target, hop + 1), *next));
            let peer = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1000 + hop as u16);
            let answer: Answer = Arc::new(move |_| {
                let peers = if with_peers { vec![peer] } else { vec![] };
                (peers, next.clone().into_iter().collect())
            });
            serve(socket, addr, closer(target, hop), delay, Some(answer))
        })
        .collect()
}

/// An id that differs from `target` in `bit` only: a higher bit is closer.
fn closer(target: Id, bit: usize) -> Id {
    let mut bytes = *target.as_bytes();
    bytes[bit / 8] ^= 0x80 >> (bit % 8);
    Id::from(bytes)
}

fn requests(nodes: &[FakeNode]) -> usize {
    nodes.iter().map(FakeNode::requests).sum()
}

fn client(bootstrap: SocketAddrV4) -> Dht {
    Dht::builder()
        .bootstrap(&[bootstrap])
        .port(0)
        .build()
        .unwrap()
}

async fn wait_for(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the condition in time");
}

/// Dropping a receiver leaves the lookup to the others; the last one ends it.
#[tokio::test]
async fn a_lookup_ends_with_its_last_receiver() {
    let target = Id::random();
    let nodes = chain(target, 20, Duration::from_millis(100), true).await;
    let dht = client(nodes[0].addr);
    let mut first = dht.get_peers(target).await.unwrap();
    let mut second = dht.get_peers(target).await.unwrap();
    assert!(first.next().await.is_some());
    assert!(second.next().await.is_some());

    drop(first);
    for _ in 0..2 {
        let peers = tokio::time::timeout(Duration::from_secs(1), second.next()).await;
        assert!(
            matches!(peers, Ok(Some(_))),
            "the lookup stopped with a receiver left"
        );
    }

    drop(second);
    dht.info().await.unwrap();
    let asked = requests(&nodes);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        requests(&nodes),
        asked,
        "the lookup went on without receivers"
    );
}

/// A receiver that is not read holds a bounded number of answers.
#[tokio::test]
async fn an_unread_receiver_holds_a_bounded_number_of_answers() {
    let target = Id::random();
    let nodes = chain(target, 20, Duration::ZERO, true).await;
    let dht = client(nodes[0].addr);
    let mut peers = dht.get_peers(target).await.unwrap();
    wait_for(|| nodes.iter().all(|node| node.requests() > 0)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    dht.info().await.unwrap();

    let mut held = 0;
    while let Some(Some(_)) = peers.next().now_or_never() {
        held += 1;
    }
    assert!((1..=16).contains(&held), "held {held} answers");
}

/// A lookup beyond the limit is refused without asking any node.
#[tokio::test]
async fn lookups_beyond_the_limit_are_refused() {
    let (socket, addr) = bind().await;
    let silent = serve(socket, addr, Id::random(), Duration::ZERO, None);
    let dht = client(addr);
    let mut lookups = Vec::new();
    for _ in 0..=256 {
        lookups.push(dht.get_peers(Id::random()).await.unwrap());
    }
    dht.info().await.unwrap();

    let mut refused = lookups.pop().unwrap();
    assert_eq!(
        refused.next().now_or_never(),
        Some(None),
        "the lookup over the limit was accepted"
    );
    wait_for(|| silent.requests() >= 256).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(silent.requests(), 256);
}

/// A receiver beyond the limit of one lookup is refused; the others keep waiting.
#[tokio::test]
async fn receivers_beyond_the_limit_are_refused() {
    let (socket, addr) = bind().await;
    let _silent = serve(socket, addr, Id::random(), Duration::ZERO, None);
    let dht = client(addr);
    let target = Id::random();
    let mut receivers = Vec::new();
    for _ in 0..=64 {
        receivers.push(dht.get_peers(target).await.unwrap());
    }
    dht.info().await.unwrap();

    assert_eq!(
        receivers[64].next().now_or_never(),
        Some(None),
        "the receiver over the limit was accepted"
    );
    assert_eq!(receivers[63].next().now_or_never(), None);
}

/// An announcement waiting for a lookup of its hash outlives the lookup's receiver.
#[tokio::test]
async fn an_announcement_outlives_a_dropped_lookup_of_its_hash() {
    let testnet = Testnet::new(10).await.unwrap();
    let a = Dht::builder()
        .bootstrap(&testnet.bootstrap)
        .build()
        .unwrap();
    let info_hash = Id::random();
    let lookup = a.get_peers(info_hash).await.unwrap();
    let (tx, rx) = oneshot::channel();
    let announce = PutRequestSpecific::AnnouncePeer(AnnouncePeerRequestArguments {
        info_hash,
        port: 45555,
        implied_port: None,
    });
    a.0.send(ActorMessage::Put(announce, tx, None))
        .await
        .unwrap();
    a.info().await.unwrap();
    drop(lookup);

    let announced = tokio::time::timeout(Duration::from_secs(10), rx).await;
    assert!(matches!(announced, Ok(Ok(Ok(_)))), "{announced:?}");

    let b = Dht::builder()
        .bootstrap(&testnet.bootstrap)
        .build()
        .unwrap();
    let peers = b
        .get_peers(info_hash)
        .await
        .unwrap()
        .next()
        .await
        .expect("no peers");
    assert_eq!(peers.first().unwrap().port(), 45555);
}

/// Lookups never have more than the limit of requests awaiting answers.
#[tokio::test]
async fn requests_awaiting_answers_stay_within_the_limit() {
    let mut silent = Vec::new();
    for _ in 0..20 {
        let (socket, addr) = bind().await;
        silent.push(serve(socket, addr, Id::random(), Duration::ZERO, None));
    }
    let far: Vec<SocketAddrV4> = silent.iter().map(|node| node.addr).collect();
    // The first node tells every lookup about the same 20 nodes, which never answer.
    let answer: Answer = Arc::new(move |info_hash| {
        let nodes = far
            .iter()
            .enumerate()
            .map(|(bit, addr)| Node::new(closer(info_hash, bit), *addr))
            .collect();
        (vec![], nodes)
    });
    let (socket, addr) = bind().await;
    let first = serve(socket, addr, Id::random(), Duration::ZERO, Some(answer));
    let dht = client(addr);
    let mut lookups = Vec::new();
    for _ in 0..64 {
        lookups.push(dht.get_peers(Id::random()).await.unwrap());
    }
    wait_for(|| first.requests() >= 64).await;
    // Well before any request times out.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let waiting = requests(&silent);
    assert!(
        waiting <= 1024,
        "{waiting} requests awaited answers at once"
    );
    // The lookups had more to send, which goes out as earlier requests time out.
    wait_for(|| requests(&silent) == 64 * 20).await;
}

/// An answer that comes after its request timed out still counts while the
/// lookup goes on.
#[tokio::test]
async fn a_late_answer_counts_while_the_lookup_goes_on() {
    let target = Id::random();
    let mut sockets = Vec::new();
    for _ in 0..6 {
        sockets.push(bind().await);
    }
    let (holder_socket, holder_addr) = bind().await;
    let holder = Node::new(closer(target, 10), holder_addr);
    let addrs: Vec<SocketAddrV4> = sockets.iter().map(|(_, addr)| *addr).collect();
    // A line of nodes that keeps the lookup going for about two seconds; the
    // first one also tells about the holder.
    let mut _line = Vec::new();
    for (hop, (socket, addr)) in sockets.into_iter().enumerate() {
        let mut nodes: Vec<Node> = addrs
            .get(hop + 1)
            .map(|next| Node::new(closer(target, hop + 1), *next))
            .into_iter()
            .collect();
        if hop == 0 {
            nodes.push(holder.clone());
        }
        let answer: Answer = Arc::new(move |_| (vec![], nodes.clone()));
        _line.push(serve(
            socket,
            addr,
            closer(target, hop),
            Duration::from_millis(300),
            Some(answer),
        ));
    }
    // The holder answers after the client's request timeout of 500 ms.
    let peer = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4242);
    let answer: Answer = Arc::new(move |_| (vec![peer], vec![]));
    let _holder = serve(
        holder_socket,
        holder_addr,
        *holder.id(),
        Duration::from_millis(700),
        Some(answer),
    );

    let dht = client(addrs[0]);
    let mut peers = dht.get_peers(target).await.unwrap();
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(batch) = peers.next().await {
            if batch.contains(&peer) {
                return true;
            }
        }
        false
    })
    .await
    .expect("the lookup ended in time");
    assert!(found, "the holder's late answer was dropped");
}
