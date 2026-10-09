//! This module represents the WavePacket entity.
//! WavePackets are passed around in a channel -- a sort of pipe that worker threads use to
//! communicate with each other. This file also contains the associated batch definition.


use crate::types::core::{
    Time,
    BatchConstraint,
};
use rand_distr::StandardNormal;
use std::f64::consts::PI;
use crate::concurrency::snowflake;
use std::sync::mpsc::{Receiver, SyncSender, SendError};
use rand::prelude::*;

#[derive(Clone, Debug)]
pub struct WavePacket {
    pub time: Time,// ps
    pub time_sigma: u32,// ps, single std dev.
    pub wavelength: f32,
    pub wavelength_sigma: f32,
    pub state_handle: u32,

    // snowflake is possibly unnecessary
    pub snowflake: u32,
}

/// speed of light in nm/ps
const C_NM_PER_PS: f64 = 299_792.458;

impl WavePacket {
    pub fn leading_edge(&self) -> u64 {
        // three sigma
        return self.time.saturating_sub(self.time_sigma as u64 * 3);
    }
    pub fn trailing_edge(&self) -> u64 {
        return self.time + self.time_sigma as u64 * 3;
    }
    pub fn overlaps(&self, wp: &WavePacket) -> bool {
        return self.leading_edge() < wp.trailing_edge() && wp.leading_edge() < self.trailing_edge();
    }
    pub fn is_strictly_before(&self, reference_time: u64) -> bool {
        return self.trailing_edge() < reference_time;
    }
    pub fn is_strictly_after(&self, reference_time: u64) -> bool {
        return self.leading_edge() > reference_time;
    }
    /// |<phi_1|phi_2>|^2 for pure, transform-limited Gaussian packets,
    /// identical polarization assumed. Returns a value in [0, 1].
    pub fn indistinguishability(&self, other: &WavePacket) -> f64 {
        let s1 = self.time_sigma as f64;
        let s2 = other.time_sigma as f64;

        // Degenerate (delta-like) packets: identical carriers & times → 1, else 0.
        if s1 == 0.0 || s2 == 0.0 {
            return if self.time == other.time
                && self.wavelength == other.wavelength
                && s1 == s2 { 1.0 } else { 0.0 };
        }

        // Δt in ps — go through i128 first: u64 → f64 loses integer
        // precision above 2^53, and the *difference* is what matters.
        let dt = (self.time as i128 - other.time as i128) as f64;

        // carrier angular frequency detuning, rad/ps
        let w1 = 2.0 * PI * C_NM_PER_PS / self.wavelength as f64;
        let w2 = 2.0 * PI * C_NM_PER_PS / other.wavelength as f64;
        let dw = w1 - w2;

        let ssum = s1 * s1 + s2 * s2;
        let prefactor = 2.0 * s1 * s2 / ssum;
        let exponent = -dt * dt / (2.0 * ssum)
                       - 2.0 * (s1 * s1) * (s2 * s2) * dw * dw / ssum;

        prefactor * exponent.exp()
    }
    pub fn set_snowflake(mut self) -> Self {
        self.snowflake = snowflake::next_u32();
        self
    }
    pub fn sample_time(&self) -> Time {
        let dist: f64 = rand::rng().sample(StandardNormal);
        let offset = (dist * self.time_sigma as f64).round() as i64;

        if offset >= 0 {
            self.time.saturating_add(offset as Time)
        } else {
            self.time.saturating_sub(offset.unsigned_abs())
        }
    }
}



#[derive(Clone)]
pub struct WpBatch {
    pub start_time: Time,
    pub end_time: Time,
    pub batch: Vec<WavePacket>,
}

impl WpBatch {
    pub fn push(&mut self, wp: WavePacket) {
        // wp is assumed to be older or equally as old as the last packet of the batch
        debug_assert!(
        if let Some(last_wp) = self.batch.last() {
            wp.leading_edge() >= last_wp.leading_edge()
        } else {
            wp.leading_edge() >= self.end_time
        }, "Wave packet to be pushed is not older than the last packet or the end time of the batch");
        self.end_time = wp.leading_edge();
        self.batch.push(wp);
    }
    // TODO: this logic breaks down as soon as we introduce ultra-long packets like that of decaying-ion,
    // but it works for now as we are operating in the hundreds-of-picoseconds regime of the PPLN
    // SPDC with 1GHz pump laser.
    // Dumb but safe method would be to loop through every packets to find the last trailing edge,
    // but we could cache the result somewhere like start_time and end_time if we decided to go
    // further with this approach
    pub fn trailing_edge(&self) -> Time {
        if self.batch.is_empty() {
            self.end_time
        } else {
            self.batch.last().unwrap().trailing_edge()
        }
    }
    pub fn get_mut(&mut self, index: usize) -> Option<&mut WavePacket> {
        self.batch.get_mut(index)
    }
    pub fn len(&self) -> usize {
        self.batch.len()
    }
}


#[derive(Clone)]
pub struct TxPort {
    pub time: u64,
    pub tx: SyncSender<WpBatch>,
}

impl TxPort {
    pub fn send_batch(&mut self, batch: WpBatch) -> Result<(), SendError<WpBatch>> {
        self.time = batch.end_time;
        return self.tx.send(batch);
    }
}

pub struct RxPort{
    // period_end and period_start are aligned to wp.leading_edge()
    // which is guaranteed to be monotonic
    pub period_start: u64,
    pub period_end: u64,
    pub rx: Receiver<WpBatch>,
    pub current_period: std::iter::Peekable<std::vec::IntoIter<WavePacket>>,
    // Best effort eagerly advancing clock
    pub current_time: u64,
}



impl RxPort{
    fn recv(&mut self) {
        let batch = self.rx.recv().unwrap();
        self.period_start = batch.start_time;
        self.period_end = batch.end_time;
        self.current_period = batch.batch.into_iter().peekable();
    }
    // this guarantees nothing about the disjointness of trailing_edge().
    // Meaning, there might be an overlapping wave packet with
    // wp_later.leading_edge() < wp_earlier.trailing_edge(),
    // though the leading_edge() should be strictly monotonic
    // This constraint should not affect the calculation,
    // as photons are bosonic, and higher number modes can be
    // treated as an addition of smaller number states in a strict
    // single-spatial mode setting. n-port node will use a scatter
    // matrix to determine the interference between maximal of n
    // wavepackets at a time, with overlapping packets in the same
    // mode treated as additive terms, meaning 2x2 node with
    // photon counts of (2, 3) will be decomposed into 6 independent
    // application of the same scattering operator. This preserves
    // the temporal mode of each wave packets at exit ports,
    // regardless of the port counts.
    pub fn get_batch(&mut self, constraint: BatchConstraint) -> WpBatch {
        let mut batch: Vec<WavePacket> = Vec::new();
        let start_time = self.current_time;
        while batch.len() < constraint.max_size {
            if let Some(wp_ref) = self.current_period.peek() {
                if wp_ref.leading_edge() > constraint.timeout {
                    break;
                }
                let wp = self.current_period.next().unwrap();
                batch.push(wp);
            } else if constraint.timeout < self.period_end {
                // be as lazy as possible in terms of getting the next packet
                break;
            } else {
                self.recv();
            }
        }
        // if the length constraint is satisfied
        if let Some(wp_ref) = self.current_period.peek() {
            self.current_time = wp_ref.leading_edge();
        } else {
            self.current_time = self.period_end;
        }
        let end_time = self.current_time;
        return WpBatch { batch, start_time, end_time };
    }
    // Handles boundary condition for multi-port components
    pub fn get_overlapping_or_before(&mut self, reference_time: u64) -> Option<WavePacket> {
        loop {
            if let Some(wp_ref) = self.current_period.peek() {
                if wp_ref.is_strictly_after(reference_time) {
                    return None;
                } else {
                    let some_wp = self.current_period.next();
                    if let Some(wp_ref) = self.current_period.peek() {
                        self.current_time = wp_ref.leading_edge();
                    } else {
                        self.current_time = self.period_end;
                    }
                    return some_wp;
                }
            } else if reference_time < self.period_end {
                return None;
            } else {
                self.recv();
                if reference_time < self.period_start {
                    return None;
                }
            }
        }
    }
    pub fn is_strictly_after(&mut self, reference_time: u64) -> bool {
        loop {
            if let Some(wp_ref) = self.current_period.peek() {
                return wp_ref.is_strictly_after(reference_time);
            } else if reference_time < self.period_end {
                return true;
            } else {
                self.recv();
                if reference_time < self.period_start {
                    return true;
                }
            }
        }
    }
}
