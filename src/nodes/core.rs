use std::sync::mpsc::{Receiver, SyncSender, SendError};

pub struct WavePacket {
    pub time: u64,// ps
    pub time_spread: u32,// ps, three sigma
    pub wl: f32,
    pub wl_spread: f32,
    pub qs_handle: u32,
    pub snowflake: u32,
}

impl WavePacket {
    pub fn start_time(&self) -> u64 {
        // three sigma
        return self.time - self.time_spread as u64 * 3;
    }
    pub fn end_time(&self) -> u64 {
        return self.time + self.time_spread as u64 * 3;
    }
    pub fn overlaps(&self, wp: &WavePacket) -> bool {
        return self.start_time() < wp.end_time() && wp.start_time() < self.end_time();
    }
    pub fn is_strictly_before(&self, reference_time: u64) -> bool {
        return self.end_time() < reference_time;
    }
    pub fn is_strictly_after(&self, reference_time: u64) -> bool {
        return self.start_time() > reference_time;
    }
}

pub struct WpBatch {
    pub start_time: u64,
    pub end_time: u64,
    pub batch: Vec<WavePacket>,
}


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
    // period_end and period_start are aligned to wp.start_time()
    // which is guaranteed to be monotonic
    pub period_start: u64,
    pub period_end: u64,
    pub rx: Receiver<WpBatch>,
    pub current_period: std::iter::Peekable<std::vec::IntoIter<WavePacket>>,
    // Best effort eagerly advancing clock
    pub current_time: u64,
}


pub struct BatchConstraint {
    pub timeout: u64, // picoseconds
    pub max_size: usize,
}

impl RxPort{
    fn recv(&mut self) {
        let batch = self.rx.recv().unwrap();
        self.period_start = batch.start_time;
        self.period_end = batch.end_time;
        self.current_period = batch.batch.into_iter().peekable();
    }
    // this guarantees nothing about the disjointness of end_time().
    // Meaning, there might be an overlapping wave packet with
    // wp_later.start_time() < wp_earlier.end_time(),
    // though the start_time() should be strictly monotonic
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
    pub fn get_batch(&mut self, constraint: BatchConstraint) -> Vec<WavePacket> {
        let mut batch: Vec<WavePacket> = Vec::new();
        while batch.len() < constraint.max_size {
            if let Some(wp_ref) = self.current_period.peek() {
                if wp_ref.start_time() > constraint.timeout {
                    self.current_time = wp_ref.start_time();
                    return batch;
                }
                let wp = self.current_period.next().unwrap();
                batch.push(wp);
            } else if constraint.timeout < self.period_end {
                // be as lazy as possible in terms of getting the next packet
                self.current_time = self.period_end;
                return batch;
            } else {
                self.recv();
            }
        }
        // if the length constraint is satisfied
        if let Some(wp_ref) = self.current_period.peek() {
            self.current_time = wp_ref.start_time();
        } else {
            self.current_time = self.period_end;
        }
        return batch;
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
                        self.current_time = wp_ref.start_time();
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
}


pub struct WorkerHandle {
    pub ports: Vec<TxPort>,
}
