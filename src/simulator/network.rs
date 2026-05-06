use std::collections::VecDeque;
use std::mem;
use std::time::Duration;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::algorithms::rateless_bloom::{RatelessBF, StoppingStrategy};
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::replica::Element;
use crate::simulator::topology::Topology;

/// Wire-size contract for anything sent through `Network`.
/// Split into `state_bytes` (payload: actual set elements being transferred)
/// and `metadata_bytes` (sketches, filters, coefficients, headers).
///
/// `state_bytes` is consulted by `Network::send` at send time;
/// `metadata_bytes` is consulted by `Network::bill_metadata_post_recv`
/// after `recv_phase` returns. The split is what lets RIBLT and rateless
/// Bloom messages bill their post-decode size honestly.
pub trait WireSized {
    fn state_bytes(&self) -> u64;
    fn metadata_bytes(&self) -> u64;
}

#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    pub bytes_state: u64,
    pub bytes_metadata: u64,
    pub per_node_state: Vec<u64>,
    pub per_node_metadata: Vec<u64>,
}

/// A simulated point-to-point network.
///
/// Each node has a per-node inbox.  Messages are delivered by calling
/// `send`, which checks if `from` and `to` are topology neighbours and
/// places `msg` in `to`'s inbox.
pub struct Network<Msg: WireSized> {
    inboxes: Vec<VecDeque<(usize, Msg)>>,
    neighbors: Vec<Vec<usize>>,
    bytes_state: u64,
    bytes_metadata: u64,
    per_node_state: Vec<u64>,
    per_node_metadata: Vec<u64>,
}

impl<Msg: WireSized> Network<Msg> {
    pub fn from_topology(topology: &Topology) -> Self {
        let n = topology.node_count();
        let neighbors = (0..n).map(|id| topology.neighbors(id).to_vec()).collect();
        Self {
            inboxes: (0..n).map(|_| VecDeque::new()).collect(),
            neighbors,
            bytes_state: 0,
            bytes_metadata: 0,
            per_node_state: vec![0; n],
            per_node_metadata: vec![0; n],
        }
    }

    /// Deliver `msg` from `from` to `to`. Bills `state_bytes` to the
    /// sender. Metadata is billed separately, post-recv, by
    /// `bill_metadata_post_recv` — sketches and filters do not have a
    /// final wire cost until after the receiver has interacted with them.
    pub fn send(&mut self, from: usize, to: usize, msg: Msg) {
        assert!(
            self.neighbors[from].contains(&to),
            "node {from} attempted to send to non-neighbour {to}"
        );
        let s = msg.state_bytes();
        self.bytes_state += s;
        self.per_node_state[from] += s;
        self.inboxes[to].push_back((from, msg));
    }

    pub fn drain_inbox(&mut self, node: usize) -> Vec<(usize, Msg)> {
        self.inboxes[node].drain(..).collect()
    }

    /// Bill metadata bytes for every message in `inbox`, attributed to
    /// the original sender. Called by the engine after `recv_phase`
    /// returns, so any decode-time growth (RIBLT consumed symbols,
    /// rateless Bloom extension) is captured. Protocols cannot influence
    /// this billing path — it reads `metadata_bytes()` off the message
    /// directly.
    pub fn bill_metadata_post_recv(&mut self, inbox: &[(usize, Msg)]) {
        for (from, msg) in inbox {
            let m = msg.metadata_bytes();
            self.bytes_metadata += m;
            self.per_node_metadata[*from] += m;
        }
    }

    pub fn stats(&self) -> NetworkStats {
        NetworkStats {
            bytes_state: self.bytes_state,
            bytes_metadata: self.bytes_metadata,
            per_node_state: self.per_node_state.clone(),
            per_node_metadata: self.per_node_metadata.clone(),
        }
    }

    pub fn reset(&mut self) {
        for inbox in &mut self.inboxes {
            inbox.clear();
        }
        self.bytes_state = 0;
        self.bytes_metadata = 0;
        self.per_node_state.iter_mut().for_each(|v| *v = 0);
        self.per_node_metadata.iter_mut().for_each(|v| *v = 0);
    }
}

// ---------------------------------------------------------------------------
// Sealed message wrappers.
//
// Each wrapper owns a sketch / filter and exposes only the operations the
// receiver legitimately needs. Constructors are module-private (no `pub`),
// so callers outside `network.rs` cannot build one — and therefore cannot
// overwrite a `&mut RibltMsg` they receive in `recv_phase` with a freshly
// constructed wrapper that would zero out the billed wire cost.
//
// `#![forbid(unsafe_code)]` at the crate root closes the `ptr::write`
// escape hatch.
// ---------------------------------------------------------------------------

pub struct RibltMsg {
    inner: RatelessIBLT<u64>,
}

impl RibltMsg {
    fn new(inner: RatelessIBLT<u64>) -> Self {
        Self { inner }
    }

    /// Decode this sketch against the receiver's `local` sketch. Both
    /// sides are extended in lockstep until decode succeeds; decoded
    /// differences accumulate on `self`. Returns the number of consumed
    /// coded symbols (== `consumed_symbols()` after the call).
    pub fn decode_against(&mut self, local: &mut RatelessIBLT<u64>) -> usize {
        self.inner.find_all_differences(local)
    }

    pub fn consumed_symbols(&self) -> usize {
        self.inner.consumed_symbols()
    }

    /// Symbols on the message side (sender) but not on `local`.
    pub fn local_only(&self) -> Vec<u64> {
        self.inner.get_local_only_symbols()
    }

    /// Symbols on `local` (receiver) but not on the message side (sender).
    pub fn remote_only(&self) -> Vec<u64> {
        self.inner.get_remote_only_symbols()
    }

    pub fn t_enc(&self) -> Duration {
        self.inner.t_enc()
    }

    pub fn t_dec(&self) -> Duration {
        self.inner.t_dec()
    }
}

pub struct BloomMsg {
    inner: BloomFilter<u64>,
}

impl BloomMsg {
    fn new(inner: BloomFilter<u64>) -> Self {
        Self { inner }
    }

    pub fn contains(&mut self, digest: &u64) -> bool {
        self.inner.timed_contains(digest)
    }

    pub fn byte_len(&self) -> usize {
        self.inner.byte_len()
    }

    pub fn t_enc(&self) -> Duration {
        self.inner.t_enc()
    }

    pub fn t_dec(&self) -> Duration {
        self.inner.t_dec()
    }
}

pub struct RatelessBloomMsg {
    inner: RatelessBF<u64>,
}

impl RatelessBloomMsg {
    fn new(inner: RatelessBF<u64>) -> Self {
        Self { inner }
    }

    pub fn bits_per_filter(&self) -> usize {
        self.inner.bits_per_filter()
    }

    pub fn source_size(&self) -> usize {
        self.inner.source_size()
    }

    pub fn extend_until<S: StoppingStrategy<u64>>(
        &mut self,
        strategy: S,
    ) -> (Vec<u64>, Vec<u64>) {
        self.inner.extend_until(strategy)
    }

    pub fn size_of(&self) -> usize {
        self.inner.size_of()
    }

    pub fn t_enc(&self) -> Duration {
        self.inner.t_enc()
    }

    pub fn t_dec(&self) -> Duration {
        self.inner.t_dec()
    }
}

// ---------------------------------------------------------------------------
// ProtocolMsg — opaque envelope.
//
// The inner enum is private; protocols cannot pattern-match on it and
// cannot construct a new `ProtocolMsg` (no public constructor). Access
// goes through the methods below, which return the sealed wrappers
// above. There is no way to swap variants from outside this module.
// ---------------------------------------------------------------------------

pub struct ProtocolMsg {
    inner: ProtocolMsgInner,
}

enum ProtocolMsgInner {
    Elements(Vec<Element>),
    Riblt(RibltMsg),
    Bloom(BloomMsg),
    RatelessBloom(RatelessBloomMsg),
    BloomRiblt {
        bf: BloomMsg,
        riblt: RibltMsg,
    },
    RatelessBloomRiblt {
        bf: RatelessBloomMsg,
        riblt: RibltMsg,
    },
}

impl ProtocolMsg {
    /// Take ownership of the elements payload, leaving an empty Vec
    /// behind. Returns `None` if the message is not an Elements variant.
    pub fn take_elements(&mut self) -> Option<Vec<Element>> {
        if let ProtocolMsgInner::Elements(v) = &mut self.inner {
            Some(mem::take(v))
        } else {
            None
        }
    }

    pub fn as_riblt(&mut self) -> Option<&mut RibltMsg> {
        if let ProtocolMsgInner::Riblt(r) = &mut self.inner {
            Some(r)
        } else {
            None
        }
    }

    pub fn as_bloom(&mut self) -> Option<&mut BloomMsg> {
        if let ProtocolMsgInner::Bloom(b) = &mut self.inner {
            Some(b)
        } else {
            None
        }
    }

    pub fn as_rateless_bloom(&mut self) -> Option<&mut RatelessBloomMsg> {
        if let ProtocolMsgInner::RatelessBloom(r) = &mut self.inner {
            Some(r)
        } else {
            None
        }
    }

    pub fn as_bloom_riblt(&mut self) -> Option<(&mut BloomMsg, &mut RibltMsg)> {
        if let ProtocolMsgInner::BloomRiblt { bf, riblt } = &mut self.inner {
            Some((bf, riblt))
        } else {
            None
        }
    }

    pub fn as_rateless_bloom_riblt(
        &mut self,
    ) -> Option<(&mut RatelessBloomMsg, &mut RibltMsg)> {
        if let ProtocolMsgInner::RatelessBloomRiblt { bf, riblt } = &mut self.inner {
            Some((bf, riblt))
        } else {
            None
        }
    }
}

impl WireSized for ProtocolMsg {
    fn state_bytes(&self) -> u64 {
        match &self.inner {
            ProtocolMsgInner::Elements(els) => els.iter().map(|e| e.wire_size() as u64).sum(),
            _ => 0,
        }
    }

    fn metadata_bytes(&self) -> u64 {
        match &self.inner {
            ProtocolMsgInner::Elements(_) => 0,
            ProtocolMsgInner::Riblt(r) => riblt_wire_bytes(r),
            ProtocolMsgInner::Bloom(b) => bloom_wire_bytes(b),
            ProtocolMsgInner::RatelessBloom(r) => rateless_bloom_wire_bytes(r),
            ProtocolMsgInner::BloomRiblt { bf, riblt } => {
                bloom_wire_bytes(bf) + riblt_wire_bytes(riblt)
            }
            ProtocolMsgInner::RatelessBloomRiblt { bf, riblt } => {
                rateless_bloom_wire_bytes(bf) + riblt_wire_bytes(riblt)
            }
        }
    }
}

fn riblt_wire_bytes(r: &RibltMsg) -> u64 {
    (r.consumed_symbols() * mem::size_of::<u64>()) as u64
}

fn bloom_wire_bytes(b: &BloomMsg) -> u64 {
    (b.byte_len() + mem::size_of::<usize>() + mem::size_of::<u64>()) as u64
}

fn rateless_bloom_wire_bytes(r: &RatelessBloomMsg) -> u64 {
    r.size_of() as u64
}

// ---------------------------------------------------------------------------
// Outbox — the only path protocols have to emit messages. Each factory
// method takes the raw sketches/filters and packages them into a
// ProtocolMsg internally; the protocol never holds a constructible
// reference to the envelope.
// ---------------------------------------------------------------------------

pub struct Outbox<'a> {
    network: &'a mut Network<ProtocolMsg>,
    from: usize,
}

impl<'a> Outbox<'a> {
    /// Bind this outbox to a specific sender. The engine constructs one
    /// per replica per round; protocols receive it already bound and
    /// cannot supply a different `from`.
    pub fn for_replica(network: &'a mut Network<ProtocolMsg>, from: usize) -> Self {
        Self { network, from }
    }

    pub fn send_elements(&mut self, to: usize, elements: Vec<Element>) {
        self.network.send(
            self.from,
            to,
            ProtocolMsg {
                inner: ProtocolMsgInner::Elements(elements),
            },
        );
    }

    pub fn send_riblt(&mut self, to: usize, riblt: RatelessIBLT<u64>) {
        self.network.send(
            self.from,
            to,
            ProtocolMsg {
                inner: ProtocolMsgInner::Riblt(RibltMsg::new(riblt)),
            },
        );
    }

    pub fn send_bloom(&mut self, to: usize, bloom: BloomFilter<u64>) {
        self.network.send(
            self.from,
            to,
            ProtocolMsg {
                inner: ProtocolMsgInner::Bloom(BloomMsg::new(bloom)),
            },
        );
    }

    pub fn send_rateless_bloom(&mut self, to: usize, bf: RatelessBF<u64>) {
        self.network.send(
            self.from,
            to,
            ProtocolMsg {
                inner: ProtocolMsgInner::RatelessBloom(RatelessBloomMsg::new(bf)),
            },
        );
    }

    pub fn send_bloom_riblt(
        &mut self,
        to: usize,
        bloom: BloomFilter<u64>,
        riblt: RatelessIBLT<u64>,
    ) {
        self.network.send(
            self.from,
            to,
            ProtocolMsg {
                inner: ProtocolMsgInner::BloomRiblt {
                    bf: BloomMsg::new(bloom),
                    riblt: RibltMsg::new(riblt),
                },
            },
        );
    }

    pub fn send_rateless_bloom_riblt(
        &mut self,
        to: usize,
        bf: RatelessBF<u64>,
        riblt: RatelessIBLT<u64>,
    ) {
        self.network.send(
            self.from,
            to,
            ProtocolMsg {
                inner: ProtocolMsgInner::RatelessBloomRiblt {
                    bf: RatelessBloomMsg::new(bf),
                    riblt: RibltMsg::new(riblt),
                },
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::topology::Topology;

    struct Dummy {
        s: u64,
        m: u64,
    }
    impl WireSized for Dummy {
        fn state_bytes(&self) -> u64 {
            self.s
        }
        fn metadata_bytes(&self) -> u64 {
            self.m
        }
    }

    #[test]
    fn send_counts_state_bytes_only() {
        let topo = Topology::star(3);
        let mut net: Network<Dummy> = Network::from_topology(&topo);

        net.send(0, 1, Dummy { s: 100, m: 7 });
        net.send(0, 2, Dummy { s: 50, m: 3 });

        let st = net.stats();
        assert_eq!(st.bytes_state, 150);
        // Metadata is billed separately, post-recv.
        assert_eq!(st.bytes_metadata, 0);
        assert_eq!(st.per_node_state[0], 150);
    }

    #[test]
    fn bill_metadata_post_recv_attributes_to_sender() {
        let topo = Topology::star(3);
        let mut net: Network<Dummy> = Network::from_topology(&topo);

        net.send(0, 1, Dummy { s: 0, m: 7 });
        net.send(0, 2, Dummy { s: 0, m: 3 });

        let inbox1 = net.drain_inbox(1);
        let inbox2 = net.drain_inbox(2);
        net.bill_metadata_post_recv(&inbox1);
        net.bill_metadata_post_recv(&inbox2);

        let st = net.stats();
        assert_eq!(st.bytes_metadata, 10);
        assert_eq!(st.per_node_metadata[0], 10);
    }

    #[test]
    fn reset_clears_counters_and_inboxes() {
        let topo = Topology::star(2);
        let mut net: Network<Dummy> = Network::from_topology(&topo);
        net.send(0, 1, Dummy { s: 42, m: 9 });
        net.reset();
        let st = net.stats();
        assert_eq!(st.bytes_state, 0);
        assert_eq!(st.bytes_metadata, 0);
        assert_eq!(st.per_node_state[0], 0);
        assert!(net.drain_inbox(1).is_empty());
    }

    #[test]
    #[should_panic(expected = "non-neighbour")]
    fn send_to_non_neighbour_panics() {
        let topo = Topology::tree(3);
        let mut net: Network<Dummy> = Network::from_topology(&topo);
        net.send(1, 2, Dummy { s: 1, m: 1 });
    }
}
