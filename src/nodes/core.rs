use std::sync::mpsc::{sync_channel, channel, Receiver, SyncSender, Sender, SendError};
use std::cmp::Ordering;
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;
use std::sync::mpsc;
use std::f64::consts::PI;

use crate::concurrency::context::{
    SimulationContext,
    OpStoreHandle,
};

use crate::types::core::{
    PortAddress,
    Time,
    PortId,
    BatchConstraint,
};

#[derive(Clone, Debug)]
pub struct WavePacket {
    pub time: Time,// ps
    pub time_sigma: u32,// ps, three sigma
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
}

#[derive(Clone)]
pub struct WpBatch {
    pub start_time: u64,
    pub end_time: u64,
    pub batch: Vec<WavePacket>,
}

impl WpBatch {
    pub fn push(&mut self, wp: WavePacket) {
        // wp is assumed to be older than the last packet of the batch
        debug_assert!(
        if let Some(last_wp) = self.batch.last() {
            wp.leading_edge() > last_wp.leading_edge()
        } else {
            wp.leading_edge() > self.end_time
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



// Moved from generic_logic.rs


pub struct EntryPortHandle{
    tx: TxPort,
}


pub struct ExitPortHandle<'a, T: NodeHandle> {
    node_handle: &'a T,
    exit_port_id: PortId,
}

impl<'a, T: NodeHandle> ExitPortHandle<'a, T> {
    fn connect(&self, port: EntryPortHandle) {
        self.schedule_connect(port, 0);
    }
    fn schedule_connect(&self, port: EntryPortHandle, time: Time) {
        self.node_handle.get_control_channel().send(NodeControlEvent::Connect{
            exit_port_id: self.exit_port_id,
            tx_port: port.tx,
        }.timed(time)).unwrap();
    }
}

enum NodeControlEvent<CustomControlEvent> {
    Start,
    Stop,
    Connect {
        exit_port_id: PortId,
        tx_port: TxPort,
    },
    Custom(CustomControlEvent),
}

impl <CustomControlEvent> NodeControlEvent<CustomControlEvent> {
    fn timed(self, time: Time) -> TimedControlEvent<CustomControlEvent>{
        TimedControlEvent::<CustomControlEvent> {
            time,
            event: self,
        }
    }
}

pub struct TimedControlEvent<CustomControlEvent> {
    time: Time,
    event: NodeControlEvent<CustomControlEvent>
}

impl <T>PartialEq for TimedControlEvent<T> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl <T>Eq for TimedControlEvent<T> {}

impl <T>Ord for TimedControlEvent<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse the standard comparison to create a min-heap
        other.time.cmp(&self.time) 
    }
}

impl <T>PartialOrd for TimedControlEvent<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// This strcut is world-facing. Even though non-idiomatic in rust, it should use the
// abstract base class trait pattern to warrant cross-node consistency and to reduce
// the amount of boilerplate code. A more idiomatic alternative would be the context
// struct pattern, but it is less viable here for the stated reason.
pub trait NodeHandle: Sized {
    type CustomControlEvent;
    type NodeTemplate;

    // user needs to implement this
    // new should register a new operator store
    fn new(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate, seq: OpStoreHandle, join_handle: std::thread::JoinHandle<()>, ports: Vec<TxPort>, control_channel: Sender<TimedControlEvent<Self::CustomControlEvent>>) -> Self;
    fn get_tx_ports(&self) -> &Vec<TxPort>;
    fn get_control_channel(&self) -> &Sender<TimedControlEvent<Self::CustomControlEvent>>;
    fn join(self);

    // everything else are derived
    fn get_tx_port(&self, id: PortId) -> TxPort {
        return self.get_tx_ports()[id as usize].clone();
    }
    fn entry_port(&self, id: PortId) -> EntryPortHandle {
        EntryPortHandle {
            tx: self.get_tx_ports()[id as usize].clone(),
        }
    }
    fn exit_port(&'_ self, id: PortId) -> ExitPortHandle<'_, Self> {
        ExitPortHandle::<Self>{
            node_handle: self,
            exit_port_id: id,
        }
    }

    fn start(&self) {
        self.schedule_start(0);
    }
    fn stop(&self) {
        self.schedule_stop(0);
    }
    fn send_node_control_event(&self, event: Self::CustomControlEvent) {
        self.schedule_node_control_event(event, 0);
    }

    fn schedule_start(&self, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Start.timed(time)).unwrap();
    }
    fn schedule_stop(&self, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Stop.timed(time)).unwrap();
    }
    fn schedule_node_control_event(&self, event: Self::CustomControlEvent, time: Time) {
        self.get_control_channel().send(NodeControlEvent::Custom(event).timed(time)).unwrap();
    }
}


pub struct RunnerState<T: NodeWorker> {
    pub rx_ports: Vec<RxPort>,
    pub control_rx: Receiver<TimedControlEvent<T::CustomControlEvent>>,
    pub control_event_queue: BinaryHeap<TimedControlEvent<T::CustomControlEvent>>,
    pub time: Time,
}

impl<T: NodeWorker> RunnerState<T> {
    fn ctx<'a>(&'a mut self, global: &'a Arc<SimulationContext>) -> RunnerContext<'a, T> {
        RunnerContext {
            runner: self,
            global,
        }
    }
}

pub struct NodeRunner<T: NodeWorker> {
    state: RunnerState<T>,
    worker: T,
}



pub trait NodeWorker: Send + Sized {
    type CustomControlEvent: Send;
    type NodeTemplate;
    type NodeHandle: NodeHandle<CustomControlEvent = Self::CustomControlEvent, NodeTemplate = Self::NodeTemplate>;

    fn new(template: &Self::NodeTemplate, id: OpStoreHandle) -> Self;
    fn handle_connection(&mut self, ctx: RunnerContext<Self>, exit_port_id: PortId, tx_port: TxPort);
    fn handle_custom_event(&mut self, ctx: RunnerContext<Self>, custom_event: Self::CustomControlEvent);
    fn process_batch(&mut self, ctx: RunnerContext<Self>);
    fn register_operator(ctx: Arc<SimulationContext>, template: &Self::NodeTemplate) -> OpStoreHandle;

}


pub struct RunnerContext<'a, T: NodeWorker>{
    pub runner: &'a mut RunnerState<T>,
    pub global: &'a Arc<SimulationContext>,
}


impl<T: NodeWorker + 'static> NodeRunner<T> {
    fn spawn(ctx: Arc<SimulationContext>, rx_port_count: usize, template: T::NodeTemplate) -> T::NodeHandle {
        let id = T::register_operator(ctx.clone(), &template);
        let (tx_ports, rx_ports): (Vec<_>, Vec<_>) = (0..rx_port_count).map(|_| {
            let (tx_raw, rx_raw) = sync_channel::<WpBatch>(3);
            let tx = TxPort {
                time: 0,
                tx: tx_raw,
            };
            let rx = RxPort {
                period_start: 0,
                period_end: 0,
                rx: rx_raw,
                // set the empty iterator
                current_period: Vec::new().into_iter().peekable(),
                current_time: 0,
            };
            (tx, rx)
        }).unzip();
        let (control_tx, control_rx) = channel::<TimedControlEvent<T::CustomControlEvent>>();

        let mut runner = Self {
            state: RunnerState{
                rx_ports,
                control_rx,
                control_event_queue: BinaryHeap::new(),
                time: 0,
            },
            worker: T::new(&template, id),
        };
        let ctx_cpy = ctx.clone();
        let handle = thread::spawn(move || {
            runner.run(ctx);
        });
        T::NodeHandle::new(ctx_cpy, &template, id, handle, tx_ports, control_tx)
    }
    fn preload_control_events(&mut self){
        loop {
            let evt = self.state.control_rx.recv().unwrap();

            match evt.event {
                NodeControlEvent::Start => {
                    self.state.time = evt.time;
                    break;
                },
                _ => {
                    self.state.control_event_queue.push(evt);
                }
            }
        }
    }
    fn run(&mut self, ctx: Arc<SimulationContext>){
        self.preload_control_events();
        loop {
            while let Ok(evt) = self.state.control_rx.try_recv() {
                self.state.control_event_queue.push(evt);
            }
            while self.state.control_event_queue.peek().is_some_and(|evt|evt.time <= self.state.time) {
                // pop_if is nightly, so we use a less rusty alternative with unwrap()
                let evt = self.state.control_event_queue.pop().unwrap();
                match evt.event {
                    NodeControlEvent::Start => {
                        panic!("double start is currently not supported");
                    },
                    NodeControlEvent::Stop => {
                        // let the thread join
                        return;
                    },
                    NodeControlEvent::Connect{exit_port_id, tx_port} => {
                        self.worker.handle_connection(self.state.ctx(&ctx), exit_port_id, tx_port);
                    },
                    NodeControlEvent::Custom(custom_event) => {
                        self.worker.handle_custom_event(self.state.ctx(&ctx), custom_event);
                    }
                }
            }
            self.worker.process_batch(self.state.ctx(&ctx));
        }

    }
}

