use crate::nodes::core::{
    WavePacket,
    WpBatch
};
use crate::types::core::{
    Time,
};
use crate::concurrency::interaction_store::{
    InteractionStoreSlice,
};
use smallvec::SmallVec;


// NOTE: What you see below is my original hand written code
//
// fn cluster_packets_smart(mut batch_left: WpBatch, mut batch_right: WpBatch) -> (Vec<WavePacket>, Vec<WavePacket>, Vec<(WavePacket, WavePacket)>) {
//     println!("Before clustering:");
//     println!("left:  {:?}", batch_left.batch.iter().map(|wp|wp.time).collect::<Vec<u64>>());
//     println!("right: {:?}", batch_right.batch.iter().map(|wp|wp.time).collect::<Vec<u64>>());
//     let mut right_pivot: usize = 0;
//     let mut right_last_overlap: usize = usize::MAX;
// 
//     let mut left_single: Vec<WavePacket> = Vec::new();
//     let mut right_single: Vec<WavePacket> = Vec::new();
//     let mut pairs: Vec<(WavePacket, WavePacket)> = Vec::new();
// 
//     for left_idx in 0..batch_left.len() {
//         let is_last_left_packet = left_idx == batch_left.len() - 1;
//         let left_packet = &mut batch_left.batch[left_idx];
//         let mut did_overlap = false;
//         for right_idx in right_pivot..batch_right.len() {
//             let right_packet = &mut batch_right.batch[right_idx];
//             if left_packet.overlaps(right_packet) {
//                 // overlap case
//                 pairs.push((left_packet.clone(), right_packet.clone()));
//                 did_overlap = true;
//                 right_last_overlap = right_idx;
//             } else if right_packet.time < left_packet.time {
//                 if right_last_overlap == usize::MAX || right_idx > right_last_overlap {
//                     // everything between last overlap and the left packet should be isolates
//                     // right isolate
//                     right_single.push(right_packet.clone());
//                 }
//                 // tick pivot forward
//                 right_pivot = right_idx + 1;
//             } else if is_last_left_packet {
//                 right_single.push(right_packet.clone());
//             }else {
//                 // right pivot past left packet. Do nothing and early return
//                 break;
//             }
//         }
//         if !did_overlap {
//             // left isolate
//             left_single.push(left_packet.clone());
//         }
//     }
//     return (left_single, right_single, pairs)
// }



// ------------------------------------------------------------------------ //
// DualPortPairingIterator has been implemented by Claude based on my loop  //
// ------------------------------------------------------------------------ //


#[derive(Debug)]
pub enum DualPortPairing {
    LeftIsolate(WavePacket),
    RightIsolate(WavePacket),

    Pair(WavePacket, WavePacket),
}

pub struct DualPortPairingIterator {
    left: Vec<WavePacket>,
    right: Vec<WavePacket>,
    left_idx: usize,
    right_idx: usize,          // running position in the inner scan
    right_pivot: usize,        // where the next left packet restarts scanning
    right_last_overlap: Option<usize>,
    did_overlap: bool,
}

impl DualPortPairingIterator {
    pub fn new(left: WpBatch, right: WpBatch) -> Self {
        Self {
            left: left.batch,
            right: right.batch,
            left_idx: 0,
            right_idx: 0,
            right_pivot: 0,
            right_last_overlap: None,
            did_overlap: false,
        }
    }

    /// Reclaim the buffers for reuse after iteration (optional).
    pub fn into_inner(self) -> (Vec<WavePacket>, Vec<WavePacket>) {
        (self.left, self.right)
    }
}

impl Iterator for DualPortPairingIterator {
    type Item = DualPortPairing;

    fn next(&mut self) -> Option<Self::Item> {
        while self.left_idx < self.left.len() {
            let is_last_left = self.left_idx + 1 == self.left.len();

            while self.right_idx < self.right.len() {
                let r = self.right_idx;
                let lp = &self.left[self.left_idx];
                let rp = &self.right[r];

                if lp.overlaps(rp) {
                    self.did_overlap = true;
                    self.right_last_overlap = Some(r);
                    self.right_idx += 1;
                    return Some(DualPortPairing::Pair(
                        self.left[self.left_idx].clone(),
                        self.right[r].clone(),
                    ));
                } else if rp.time < lp.time {
                    self.right_pivot = r + 1;
                    self.right_idx += 1;
                    if Some(r) > self.right_last_overlap {
                        return Some(DualPortPairing::RightIsolate(self.right[r].clone()));
                    }
                    // already appeared in a pair earlier; keep scanning
                } else if is_last_left {
                    self.right_idx += 1;
                    return Some(DualPortPairing::RightIsolate(self.right[r].clone()));
                } else {
                    // right packet is past this left packet
                    break;
                }
            }

            // finished the inner scan for this left packet
            let li = self.left_idx;
            self.left_idx += 1;
            self.right_idx = self.right_pivot;
            if !std::mem::replace(&mut self.did_overlap, false) {
                return Some(DualPortPairing::LeftIsolate(self.left[li].clone()));
            }
        }
        if self.left.is_empty() && self.right_idx < self.right.len() {
            let r = self.right_idx;
            self.right_idx += 1;
            return Some(DualPortPairing::RightIsolate(self.right[r].clone()));
        }
        None
    }
}

// ------------------------------------------------------------------------ //
// Claude code section end                                                  //
// ------------------------------------------------------------------------ //

// Hand written
pub struct PhotonicCluster {
    pub pairs: SmallVec<[(u8, u8); 1]>,
    pub left_packets: SmallVec<[WavePacket; 1]>,
    pub right_packets: SmallVec<[WavePacket; 1]>,
}

impl PhotonicCluster {
    fn new(wp_left: WavePacket, wp_right: WavePacket) -> Self {
        Self {
            pairs: [(0, 0)].into(),
            left_packets: [wp_left].into(),
            right_packets: [wp_right].into(),
        }
    }
    fn left_indexof(&self, wp: &WavePacket) -> Option<u8> {
        for i in 0..self.left_packets.len() {
            let packet = &self.left_packets[i];
            if packet.snowflake == wp.snowflake {
                return Some(i as u8);
            }
        }
        return None;
    }
    fn right_indexof(&self, wp: &WavePacket) -> Option<u8> {
        for i in 0..self.right_packets.len() {
            let packet = &self.right_packets[i];
            if packet.snowflake == wp.snowflake {
                return Some(i as u8);
            }
        }
        return None;
    }
    // TODO: Investigate correctness
    // I think we might need to buffer some packets to make sure that the arrival is in
    // monotonic ascending order. It is using the latest packet of the bunch, but it's
    // probably wrong, but good enough for coarse resolution time keeping purposes
    pub fn time(&self) -> Time {
        self.left_packets.last().unwrap().leading_edge().max(self.right_packets.last().unwrap().leading_edge())
    }
    pub fn merge_states(&mut self, slice: &mut InteractionStoreSlice) -> u32 {
        let mut state_handles = Vec::new();
        let mut first_handle = 0;
        for wp in self.left_packets.iter_mut().chain(self.right_packets.iter_mut()) {
            let handle = wp.state_handle;
            if state_handles.is_empty() {
                state_handles.push(handle);
                first_handle = handle;
                continue;
            }
            wp.state_handle = first_handle;
            if state_handles.contains(&handle) {
                continue;
            }
            slice.merge_islands(handle, first_handle);
            state_handles.push(handle);
        }
        first_handle
    }
}

pub enum DualPortCluster{
    LeftIsolate (WavePacket),
    RightIsolate (WavePacket),
    Cluster(PhotonicCluster)
}

pub struct DualPortIterator {
    pairwise_iter: DualPortPairingIterator,
    photonic_cluster: Option<PhotonicCluster>,
}

impl DualPortIterator {
    pub fn new(left: WpBatch, right: WpBatch) -> Self {
        Self {
            pairwise_iter: DualPortPairingIterator::new(left, right),
            photonic_cluster: None,
        }
    }
}

impl Iterator for DualPortIterator {
    type Item = DualPortCluster;

    fn next(&mut self) -> Option<Self::Item> {
        for next in &mut self.pairwise_iter {
            match next {
                DualPortPairing::LeftIsolate(wp) => return Some(DualPortCluster::LeftIsolate(wp)),
                DualPortPairing::RightIsolate(wp) => return Some(DualPortCluster::RightIsolate(wp)),
                DualPortPairing::Pair(wp_left, wp_right) => {
                    if let Some(cluster) = &mut self.photonic_cluster {
                        let left_index = cluster.left_indexof(&wp_left);
                        let right_index = cluster.right_indexof(&wp_right);
                        if left_index.is_some() || right_index.is_some() {
                            let left_index = if let Some(idx) = left_index {
                                idx
                            } else {
                                let len = cluster.left_packets.len();
                                assert!(len <= u8::MAX as usize, "cluster exceeds u8 index space");
                                cluster.left_packets.push(wp_left);
                                len as u8
                            };
                            let right_index = if let Some(idx) = right_index {
                                idx
                            } else {
                                let len = cluster.right_packets.len();
                                assert!(len <= u8::MAX as usize, "cluster exceeds u8 index space");
                                cluster.right_packets.push(wp_right);
                                len as u8
                            };
                            cluster.pairs.push((left_index, right_index));
                        } else {
                            let old_cluster = self.photonic_cluster.replace(PhotonicCluster::new(wp_left, wp_right));
                            return Some(DualPortCluster::Cluster(old_cluster.unwrap()));
                        }
                    } else {
                        self.photonic_cluster = Some(PhotonicCluster::new(wp_left, wp_right));
                    }
                },
            }
        }
        self.photonic_cluster.take().map(DualPortCluster::Cluster)
    }
}



// Test cases written by Claude Code
#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::core::{WavePacket, WpBatch};
    use std::collections::HashMap;

    /// Batch with uniform sigma=150 (overlap iff |dt| < 900) and unique snowflakes.
    fn batch(times: &[u64], snowflake_base: u32) -> WpBatch {
        let b: Vec<WavePacket> = times.iter().enumerate().map(|(i, &time)| WavePacket {
            time, time_sigma: 150, wavelength: 1550.0, wavelength_sigma: 2.0,
            state_handle: 0, snowflake: snowflake_base + i as u32,
        }).collect();
        WpBatch { start_time: times.first().copied().unwrap_or(0),
                  end_time: times.last().copied().unwrap_or(0), batch: b }
    }
    const L: u32 = 0;          // left snowflakes: 0..
    const R: u32 = 10_000;     // right snowflakes: 10_000..

    fn collect(l: &[u64], r: &[u64]) -> Vec<DualPortCluster> {
        DualPortIterator::new(batch(l, L), batch(r, R)).collect()
    }

    fn times_of(pkts: &[WavePacket]) -> Vec<u64> { pkts.iter().map(|w| w.time).collect() }

    // ---------- structural invariants, checked on every cluster ----------
    fn check_cluster(pc: &PhotonicCluster) {
        assert!(!pc.pairs.is_empty(), "cluster with no pairs");
        // no duplicate packets within a side
        for pk in [&pc.left_packets, &pc.right_packets] {
            let mut sf: Vec<u32> = pk.iter().map(|w| w.snowflake).collect();
            sf.sort(); sf.dedup();
            assert_eq!(sf.len(), pk.len(), "duplicate packet inside a cluster: {:?}", times_of(pk));
        }
        // every pair index in bounds & genuinely overlapping; every packet used by >= 1 pair
        let (mut lused, mut rused) = (vec![false; pc.left_packets.len()], vec![false; pc.right_packets.len()]);
        for &(a, b) in &pc.pairs {
            let (a, b) = (a as usize, b as usize);
            assert!(a < pc.left_packets.len() && b < pc.right_packets.len(), "pair index out of bounds");
            assert!(pc.left_packets[a].overlaps(&pc.right_packets[b]),
                "pair ({}, {}) does not actually overlap: {} vs {}", a, b,
                pc.left_packets[a].time, pc.right_packets[b].time);
            lused[a] = true; rused[b] = true;
        }
        assert!(lused.iter().all(|&x| x) && rused.iter().all(|&x| x), "packet in cluster but in no pair");
    }

    /// Every input packet must come out exactly once (isolate or cluster member).
    fn check_exactly_once(l: &[u64], r: &[u64], out: &[DualPortCluster]) {
        let mut counts: HashMap<u32, u32> = HashMap::new();
        for c in out {
            match c {
                DualPortCluster::LeftIsolate(w) | DualPortCluster::RightIsolate(w) =>
                    *counts.entry(w.snowflake).or_insert(0) += 1,
                DualPortCluster::Cluster(pc) => {
                    check_cluster(pc);
                    for w in pc.left_packets.iter().chain(pc.right_packets.iter()) {
                        *counts.entry(w.snowflake).or_insert(0) += 1;
                    }
                }
            }
        }
        for id in (0..l.len() as u32).map(|i| L + i).chain((0..r.len() as u32).map(|i| R + i)) {
            assert_eq!(counts.get(&id).copied().unwrap_or(0), 1,
                "packet snowflake={} emitted {} times (want exactly 1)", id, counts.get(&id).copied().unwrap_or(0));
        }
    }

    // ---------- degenerate shapes ----------
    #[test]
    fn both_empty() {
        assert!(collect(&[], &[]).is_empty());
    }

    #[test]
    fn left_empty_yields_all_right_isolates() {
        let out = collect(&[], &[1000, 2000, 3000]);
        check_exactly_once(&[], &[1000, 2000, 3000], &out);
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|c| matches!(c, DualPortCluster::RightIsolate(_))));
    }

    #[test]
    fn right_empty_yields_all_left_isolates() {
        let out = collect(&[1000, 2000, 3000], &[]);
        check_exactly_once(&[1000, 2000, 3000], &[], &out);
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|c| matches!(c, DualPortCluster::LeftIsolate(_))));
    }

    // ---------- single-cluster topologies ----------
    #[test]
    fn single_pair() {
        let out = collect(&[5000], &[5100]);
        check_exactly_once(&[5000], &[5100], &out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            DualPortCluster::Cluster(pc) => {
                assert_eq!(pc.pairs.as_slice(), &[(0, 0)]);
                assert_eq!(times_of(&pc.left_packets), [5000]);
                assert_eq!(times_of(&pc.right_packets), [5100]);
            }
            _ => panic!("expected a cluster"),
        }
    }

    #[test]
    fn fan_out_one_left_two_rights() {
        let out = collect(&[5000], &[4500, 5500]);
        check_exactly_once(&[5000], &[4500, 5500], &out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            DualPortCluster::Cluster(pc) => {
                assert_eq!(times_of(&pc.left_packets), [5000], "left packet must appear once, not per pair");
                assert_eq!(times_of(&pc.right_packets), [4500, 5500]);
                assert_eq!(pc.pairs.as_slice(), &[(0, 0), (0, 1)]);
            }
            _ => panic!("expected a cluster"),
        }
    }

    #[test]
    fn fan_in_two_lefts_one_right() {
        let out = collect(&[4500, 5500], &[5000]);
        check_exactly_once(&[4500, 5500], &[5000], &out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            DualPortCluster::Cluster(pc) => {
                assert_eq!(times_of(&pc.left_packets), [4500, 5500]);
                assert_eq!(times_of(&pc.right_packets), [5000]);
                assert_eq!(pc.pairs.as_slice(), &[(0, 0), (1, 0)]);
            }
            _ => panic!("expected a cluster"),
        }
    }

    #[test]
    fn zigzag_chain() {
        let out = collect(&[4500, 5500, 6500], &[5000, 6000]);
        check_exactly_once(&[4500, 5500, 6500], &[5000, 6000], &out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            DualPortCluster::Cluster(pc) => {
                assert_eq!(times_of(&pc.left_packets), [4500, 5500, 6500]);
                assert_eq!(times_of(&pc.right_packets), [5000, 6000]);
                assert_eq!(pc.pairs.as_slice(), &[(0, 0), (1, 0), (1, 1), (2, 1)]);
            }
            _ => panic!("expected a cluster"),
        }
    }

    #[test]
    fn right_packet_revisited_across_three_lefts() {
        let out = collect(&[4500, 5000, 5500], &[5000]);
        check_exactly_once(&[4500, 5000, 5500], &[5000], &out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            DualPortCluster::Cluster(pc) => {
                assert_eq!(times_of(&pc.right_packets), [5000], "revisited right must be interned once");
                assert_eq!(pc.pairs.as_slice(), &[(0, 0), (1, 0), (2, 0)]);
            }
            _ => panic!("expected a cluster"),
        }
    }

    // ---------- multi-item sequences ----------
    #[test]
    fn trailing_cluster_is_flushed_at_end() {
        // iterator ends while a cluster is still pending -> it must still be yielded
        let out = collect(&[5000], &[5100]);
        assert_eq!(out.len(), 1, "pending cluster dropped at end of iteration");
    }

    #[test]
    fn two_separate_clusters() {
        let out = collect(&[1000, 5000], &[1100, 5100]);
        check_exactly_once(&[1000, 5000], &[1100, 5100], &out);
        assert_eq!(out.len(), 2, "expected two clusters (second one must not be dropped)");
        assert!(out.iter().all(|c| matches!(c, DualPortCluster::Cluster(_))));
    }

    #[test]
    fn original_example_sequence() {
        let l = [1000, 2000, 5000, 9000];
        let r = [5000, 7000, 9000, 10000, 20000];
        let out = collect(&l, &r);
        check_exactly_once(&l, &r, &out);
        // exact emission order (clusters lag their isolate neighbours by design)
        let sig: Vec<String> = out.iter().map(|c| match c {
            DualPortCluster::LeftIsolate(w) => format!("L{}", w.time),
            DualPortCluster::RightIsolate(w) => format!("R{}", w.time),
            DualPortCluster::Cluster(pc) =>
                format!("C{:?}x{:?}", times_of(&pc.left_packets), times_of(&pc.right_packets)),
        }).collect();
        assert_eq!(sig, [
            "L1000", "L2000", "R7000",
            "C[5000]x[5000]",
            "R10000", "R20000",
            "C[9000]x[9000]",
        ]);
    }

    #[test]
    fn right_isolate_before_first_overlap_is_not_dropped() {
        let out = collect(&[5000], &[1000, 5000]);
        check_exactly_once(&[5000], &[1000, 5000], &out);
    }

    // ---------- randomized exactly-once property ----------
    fn lcg(s: &mut u64) -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *s >> 33
    }

    #[test]
    fn property_every_packet_exactly_once() {
        let mut s = 0xC0FFEE_u64;
        for _ in 0..20_000 {
            let nl = (lcg(&mut s) % 12) as usize;
            let nr = (lcg(&mut s) % 12) as usize;
            let spread = 1000 + lcg(&mut s) % 20_000;
            let mut value_gen = |n: usize| -> Vec<u64> {
                let mut v: Vec<u64> = (0..n).map(|_| 1_000_000 + lcg(&mut s) % spread).collect();
                v.sort(); v
            };
            let (l, r) = (value_gen(nl), value_gen(nr));
            let out = collect(&l, &r);
            check_exactly_once(&l, &r, &out);
        }
    }
}
